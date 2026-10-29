/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flinkx.api.serializer

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot.OuterSchemaCompatibility
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.setNestedSerializersSnapshots
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer
import org.apache.flink.api.common.typeutils.{
  CompositeTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSchemaCompatibility,
  TypeSerializerSnapshot
}
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.NullFieldException
import org.apache.flinkx.api.evolution.FieldEvolution.{Former, MigratedField}
import org.apache.flinkx.api.evolution.{AdtEvolution, MigrationException}
import org.apache.flinkx.api.serializer.CaseClassSerializer.EmptyByteArray
import org.apache.flinkx.api.serializer.EvolvingSnapshot.{isIncompatible, readEvolution, writeEvolution}
import org.apache.flinkx.api.serializer.ScalaCaseClassSerializerSnapshot.{CurrentVersion, PositionalVersion}
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}
import org.slf4j.{Logger, LoggerFactory}

/** Serializer of a case class. It delegates fields serialization to sub-serializers. Handle nullable case class.
  *
  * A restored serializer reads the former fields in their former order, then migrates them to the current class.
  *
  * @param evolution
  *   The ADT evolution. A current serializer has [[AdtEvolution.NoEvolution]]. A restored serializer has the
  *   [[AdtEvolution]] of the former class name at the former version, which migrates the former data.
  * @param isCaseClassImmutable
  *   Whether all the fields of the case class are `val`s
  * @param fieldNames
  *   Field names of the case class, in declaration order. None for data written before release 2.4.0, whose snapshot
  *   records no field name: its fields are read by position
  * @param paramSerializers
  *   The serializers of the fields, in the same order
  */
@Internal
@SerialVersionUID(7341356073446263475L)
class CaseClassSerializer[T <: Product](
    val evolution: AdtEvolution[T],
    val isCaseClassImmutable: Boolean,
    val fieldNames: Option[Array[String]],
    paramSerializers: Array[TypeSerializer[_]]
) extends TupleSerializerBase[T](evolution.currentClass, paramSerializers)
    with Cloneable
    with ConstructorCompat {

  require(
    fieldNames.forall(_.length == paramSerializers.length), // One field name for each field serializer
    s"${evolution.currentClass} has ${fieldNames.fold(0)(_.length)} field names for ${paramSerializers.length} field" +
      s" serializers"
  )

  @transient private lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  private val nullPadding: Array[Byte] = if (super.getLength > 0) new Array(super.getLength) else EmptyByteArray

  override val isImmutableType: Boolean = isCaseClassImmutable && fieldSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = fieldSerializers.forall(s => s.duplicate().eq(s))

  // In Flink, serializers & serializer snapshotters have strict ser/de requirements.
  // Both need to be capable of creating one another.
  // Anything passed to a serializer therefore needs to be ser/de compatible.
  // The easiest method is to serialize class names during the snapshotting phase.
  // During restoration, those class names are deserialized and instantiated via a class loader.
  // The underlying implementation is major version-specific (Scala 2 vs. Scala 3).
  @transient private lazy val constructor = lookupConstructor(tupleClass)

  /** Whether the former data reads as is, computed once at the first record */
  @transient private lazy val isAsIs = evolution.isAsIs(fieldNames)

  /** The migration plan, computed once at the first record that needs it */
  @transient private lazy val migrationPlan: Array[MigratedField] =
    evolution.plan(fieldNames.getOrElse(Array.empty)) match {
      case Right(migratedFields) => migratedFields
      case Left(failures)        => throw MigrationException(evolution.currentClass, evolution.formerVersion, failures)
    }

  override def duplicate(): CaseClassSerializer[T] = {
    if (isImmutableSerializer) {
      this
    } else {
      clone().asInstanceOf[CaseClassSerializer[T]]
    }
  }

  @throws[CloneNotSupportedException]
  override protected def clone(): Object = {
    val result = super.clone().asInstanceOf[CaseClassSerializer[T]]
    // achieve a deep copy by duplicating the field serializers
    result.fieldSerializers = result.fieldSerializers.map(_.duplicate())
    result
  }

  def createInstance: T =
    try {
      val fields = new Array[AnyRef](arity)
      var i      = 0
      while (i < arity) {
        fields(i) = fieldSerializers(i).createInstance()
        i += 1
      }
      createInstance(fields)
    } catch {
      case t: Throwable =>
        log.warn(s"Failed to create an instance returning null", t)
        null.asInstanceOf[T]
    }

  override def createOrReuseInstance(fields: Array[Object], reuse: T): T =
    createInstance(fields)

  def copy(from: T, reuse: T): T =
    copy(from)

  def copy(from: T): T =
    if (from == null || isImmutableType) {
      from
    } else {
      val fields = new Array[AnyRef](arity)
      var i      = 0
      while (i < arity) {
        fields(i) = fieldSerializers(i).copy(from.productElement(i).asInstanceOf[AnyRef])
        i += 1
      }
      createInstance(fields)
    }

  override val getLength: Int =
    if (super.getLength == VariableLengthDataType) VariableLengthDataType
    else super.getLength + 4 // +4 bytes for the arity field

  def serialize(value: T, target: DataOutputView): Unit = {
    // Write a negative arity to indicate null value
    val sourceArity = if (value == null) NullMarker else arity
    target.writeInt(sourceArity)
    if (value == null) target.write(nullPadding)

    var i = 0
    while (i < sourceArity) {
      val serializer = fieldSerializers(i).asInstanceOf[TypeSerializer[Any]]
      val o          = value.productElement(i)
      try serializer.serialize(o, target)
      catch {
        case e: NullPointerException =>
          throw new NullFieldException(i, e)
      }
      i += 1
    }
  }

  def deserialize(reuse: T, source: DataInputView): T =
    deserialize(source)

  def deserialize(source: DataInputView): T = {
    val sourceArity = source.readInt()
    if (sourceArity < 0) {
      source.skipBytesToRead(nullPadding.length)
      null.asInstanceOf[T]
    } else {
      // Read the former fields in their former order
      val formerValues = new Array[AnyRef](sourceArity)
      var i            = 0
      while (i < sourceArity) {
        formerValues(i) = fieldSerializers(i).deserialize(source)
        i += 1
      }
      if (isAsIs) createInstance(formerValues)
      else evolution.migrate(createInstance(migrateFields(formerValues)))
    }
  }

  private def migrateFields(formerValues: Array[AnyRef]): Array[AnyRef] = {
    val values = new Array[AnyRef](migrationPlan.length)
    var i      = 0
    while (i < values.length) {
      values(i) = migrationPlan(i).migrate(formerValues)
      i += 1
    }
    values
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val sourceArity = source.readInt()
    target.writeInt(sourceArity)
    if (sourceArity < 0) {
      source.skipBytesToRead(nullPadding.length)
      target.skipBytesToWrite(nullPadding.length)
    } else {
      super.copy(source, target)
    }
  }

  override def createInstance(fields: Array[AnyRef]): T = {
    constructor(fields)
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseClassSerializerSnapshot[T](Some(this))

}

object CaseClassSerializer {
  private val EmptyByteArray: Array[Byte] = new Array(0)
}

/** [[TypeSerializerSnapshot]] for [[CaseClassSerializer]]. */
final class ScalaCaseClassSerializerSnapshot[T <: scala.Product](
    serializer: Option[CaseClassSerializer[T]]
) extends CompositeTypeSerializerSnapshot[T, CaseClassSerializer[T]]
    with EvolvingSnapshot[T, ScalaCaseClassSerializerSnapshot[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  private var evolution: AdtEvolution[T]        = _
  private var isCaseClassImmutable: Boolean     = false
  private var fieldNames: Option[Array[String]] = None

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    evolution = s.evolution
    isCaseClassImmutable = s.isCaseClassImmutable
    fieldNames = s.fieldNames
  }

  // Restored from a snapshot written before release 2.4.0, the serializer knows no field name: it writes that format again
  override protected def getCurrentOuterSnapshotVersion: Int =
    if (fieldNames.isDefined) CurrentVersion else PositionalVersion

  override protected def getNestedSerializers(outerSerializer: CaseClassSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): CaseClassSerializer[T] =
    new CaseClassSerializer[T](evolution, isCaseClassImmutable, fieldNames, nestedSerializers)

  override protected def writeOuterSnapshot(out: DataOutputView): Unit = {
    writeEvolution(evolution, out, hasVersion = fieldNames.isDefined)
    out.writeBoolean(isCaseClassImmutable)
    fieldNames.foreach(StringArraySerializer.INSTANCE.serialize(_, out))
  }

  override protected def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    // Before release 2.4.0, the snapshot format version 3 records neither the schema version nor the field names
    evolution = readEvolution(in, cl, hasVersion = readOuterSnapshotVersion > PositionalVersion)
    // If reading a version of 2 or below, don't read the boolean and set isCaseClassImmutable to false
    isCaseClassImmutable = readOuterSnapshotVersion > 2 && in.readBoolean
    fieldNames =
      Option.when(readOuterSnapshotVersion > PositionalVersion)(StringArraySerializer.INSTANCE.deserialize(in))
  }

  override private[serializer] def adtEvolution: Option[AdtEvolution[T]] = Some(evolution)

  override private[serializer] def memberNames: Array[String] = fieldNames.getOrElse(Array.empty)

  override protected def resolveStandardCompatibility(
      former: ScalaCaseClassSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] =
    // No migration is necessary: use the standard resolution of the parent class
    super[CompositeTypeSerializerSnapshot].resolveSchemaCompatibility(former)

  /** `former.evolution` is the ADT evolution of the schema version that the former data was written at. */
  override protected def isEvolutionRequired(former: ScalaCaseClassSerializerSnapshot[T]): Boolean =
    !former.evolution.isAsIs(former.fieldNames)

  /** Check that the declared evolutions describe the full migration from the former schema:
    *   - plans the field evolutions on the former field names, which must give the current field names
    *   - resolves the compatibility of the unchanged, reordered and renamed fields
    *   - does not resolve the compatibility of the added, transformed or deleted fields: their former and current field
    *     serializers do not correspond
    */
  override protected def checkMigration(former: ScalaCaseClassSerializerSnapshot[T]): Option[String] =
    former.evolution.plan(former.memberNames) match {
      case Left(failures)       => Some(failures.mkString("\n"))
      case Right(migrationPlan) =>
        val formerFieldSnapshots  = former.getNestedSerializerSnapshots
        val currentFieldSnapshots = getNestedSerializerSnapshots
        migrationPlan.iterator.zipWithIndex.collectFirst {
          case (Former(formerIndex), currentIndex)
              if isIncompatible(currentFieldSnapshots(currentIndex), formerFieldSnapshots(formerIndex)) =>
            s"Former field '${former.memberNames(formerIndex)}' is incompatible with '${memberNames(currentIndex)}'"
        }
    }

  override protected def resolveOuterSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[T]
  ): CompositeTypeSerializerSnapshot.OuterSchemaCompatibility =
    OuterSchemaCompatibility.COMPATIBLE_AS_IS // resolveSchemaCompatibility already resolves the outer compatibility

}

object ScalaCaseClassSerializerSnapshot {

  // Last version written before release 2.4.0, recording no field name: the fields are read by position
  private val PositionalVersion = 3

  private val CurrentVersion = 4

}
