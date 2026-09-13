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
import org.apache.flinkx.api.evolution.{Evolution, Evolutions}
import org.apache.flinkx.api.serializer.CaseClassSerializer.EmptyByteArray
import org.apache.flinkx.api.serializer.ScalaCaseClassSerializerSnapshot.CurrentVersion
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}
import org.slf4j.{Logger, LoggerFactory}
import java.io.{IOException, ObjectInputStream}

import scala.collection.mutable

/** Serializer for Case Classes. Creation and access is different from our Java Tuples so we have to treat them
  * differently. Copied from Flink 1.14 and merged with ScalaCaseClassSerializer.
  */
@Internal
@SerialVersionUID(7341356073446263475L)
class CaseClassSerializer[T <: Product](
    val evolution: Evolution[T],
    val version: Int,
    val isCaseClassImmutable: Boolean,
    val fieldNames: Array[String],
    paramSerializers: Array[TypeSerializer[_]]
) extends TupleSerializerBase[T](evolution.currentClass, paramSerializers)
    with Cloneable
    with ConstructorCompat {

  require(
    fieldNames.isEmpty || // Keep compatibility with versions < 2.4.0, fields are read by position
      fieldNames.length == paramSerializers.length, // Exactly one field name per field serializer
    s"${evolution.currentClass} has ${fieldNames.length} field names for ${paramSerializers.length} field serializers"
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

  // Cache to check for fast path on first record only
  @transient private lazy val isEvolutionAvoidable = evolution.isAvoidable(fieldNames)

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
      val fieldValues = if (isEvolutionAvoidable || fieldNames.isEmpty) { // Keep compatibility with versions < 2.4.0
        val fields = new Array[AnyRef](sourceArity)
        var i      = 0
        while (i < sourceArity) {
          fields(i) = fieldSerializers(i).deserialize(source)
          i += 1
        }
        fields
      } else {
        val fieldMap = mutable.Map.empty[String, AnyRef]
        var i        = 0
        while (i < fieldNames.length) {
          fieldMap.put(fieldNames(i), fieldSerializers(i).deserialize(source))
          i += 1
        }
        evolution.applyFieldEvolutions(fieldMap)
        evolution.toFieldValues(fieldMap)
      }
      if (evolution.isDeleted) {
        evolution.returnNullOrThrow
      } else {
        evolution.postDeserialize(version, createInstance(fieldValues))
      }
    }
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

  // A TaskManager only Java-deserializes the serializers of the job graph, so each of them registers again the ADT
  // declaration it carries: the derivation ran on the client, and the restore needs the registry
  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    Evolutions.register(evolution)
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
) extends CompositeTypeSerializerSnapshot[T, CaseClassSerializer[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  @transient private lazy val log: Logger = LoggerFactory.getLogger(classOf[ScalaCaseClassSerializerSnapshot[_]])

  private var evolution: Evolution[T]       = _
  private var isCaseClassImmutable: Boolean = false
  // Schema version of the case class this snapshot describes, as declared by @version at write time
  private var caseClassVersion: Int     = 0
  private var fieldNames: Array[String] = Array.empty

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    evolution = s.evolution
    isCaseClassImmutable = s.isCaseClassImmutable
    caseClassVersion = s.version
    fieldNames = s.fieldNames
  }

  override protected def getCurrentOuterSnapshotVersion: Int = CurrentVersion

  override protected def getNestedSerializers(outerSerializer: CaseClassSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): CaseClassSerializer[T] =
    new CaseClassSerializer[T](evolution, caseClassVersion, isCaseClassImmutable, fieldNames, nestedSerializers)

  override protected def writeOuterSnapshot(out: DataOutputView): Unit = {
    out.writeUTF(evolution.className)
    out.writeBoolean(isCaseClassImmutable)
    out.writeInt(caseClassVersion)
    StringArraySerializer.INSTANCE.serialize(fieldNames, out)
  }

  override protected def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    val caseClassName = in.readUTF()
    // If reading a version of 2 or below, don't read the boolean and set isCaseClassImmutable to false
    isCaseClassImmutable = readOuterSnapshotVersion > 2 && in.readBoolean
    caseClassVersion = if (readOuterSnapshotVersion > 3) in.readInt else 0
    evolution = Evolutions.get(caseClassName, caseClassVersion, cl)
    fieldNames = if (readOuterSnapshotVersion > 3) StringArraySerializer.INSTANCE.deserialize(in) else Array.empty
  }

  /** Resolves the schema compatibility including potential evolutions.
    *
    * When evolutions are required, the migration is checked to determine if the schema is COMPATIBLE_AFTER_MIGRATION or
    * INCOMPATIBLE, otherwise delegates to the standard schema compatibility resolution.
    */
  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = oldSerializerSnapshot match {
    case old: ScalaCaseClassSerializerSnapshot[T] if isSameClass(old) && isEvolutionRequired(old) =>
      checkEvolutionMigration(old) match {
        case None =>
          TypeSerializerSchemaCompatibility.compatibleAfterMigration()
        case Some(reason) =>
          log.warn(s"Cannot migrate ${evolution.currentClass} from version ${old.caseClassVersion}: $reason")
          TypeSerializerSchemaCompatibility.incompatible()
      }
    case old: ScalaCaseClassSerializerSnapshot[T] if isSameClass(old) =>
      // No evolution: delegates schema compatibility to standard resolution
      super.resolveSchemaCompatibility(old)
    case _ => TypeSerializerSchemaCompatibility.incompatible()
  }

  /** Whether the former snapshot describes the very same case class.
    *
    * `old.evolution` has been resolved by `readOuterSnapshot`, so a renamed or moved former case class already carries
    * the current one.
    */
  private def isSameClass(old: ScalaCaseClassSerializerSnapshot[T]): Boolean =
    evolution.currentClass == old.evolution.currentClass

  /** Whether reading the former schema described by `old` requires applying the declared evolutions.
    *
    * `old.evolution` holds the evolutions migrating the very version the former data was written at.
    */
  private def isEvolutionRequired(old: ScalaCaseClassSerializerSnapshot[T]): Boolean =
    old.fieldNames.nonEmpty && // Keep compatibility with versions < 2.4.0
      !old.evolution.isAvoidable(old.fieldNames)

  /** Check the declared evolutions entirely describe the migration from the former schema:
    *   - replays evolutions on the former field names to check for exact match with current field names
    *   - resolves the compatibility of unchanged, reordered and renamed field serializers
    *   - doesn't resolve the compatibility of added, transformed or deleted fields as their lineage is broken
    *
    * @return
    *   `None` if the migration is possible, the reason it isn't otherwise
    */
  private def checkEvolutionMigration(old: ScalaCaseClassSerializerSnapshot[T]): Option[String] =
    if (old.caseClassVersion > caseClassVersion) {
      Some(
        s"the former version ${old.caseClassVersion} is more recent than the current version $caseClassVersion." +
          s" Restore from a former savepoint."
      )
    } else {
      old.evolution.dryRun(old.fieldNames) match {
        case Left(failures)      => Some(failures.mkString("\n"))
        case Right(fieldOrigins) =>
          val formerFieldSnapshots  = old.getNestedSerializerSnapshots
          val currentFieldSnapshots = getNestedSerializerSnapshots
          fieldOrigins.indices.iterator
            .flatMap(currentIndex => fieldOrigins(currentIndex).map(currentIndex -> _))
            .collectFirst {
              case (currentIndex, formerIndex)
                  if isFieldIncompatible(formerFieldSnapshots(formerIndex), currentFieldSnapshots(currentIndex)) =>
                s"field '${fieldNames(currentIndex)}' can't be migrated from former field" +
                  s" '${old.fieldNames(formerIndex)}'"
            }
      }
    }

  /** Resolves the compatibility of a former field against its current one, which recursively applies the evolutions
    * declared on the field type.
    */
  private def isFieldIncompatible(
      formerFieldSnapshot: TypeSerializerSnapshot[_],
      currentFieldSnapshot: TypeSerializerSnapshot[_]
  ) = currentFieldSnapshot
    .asInstanceOf[TypeSerializerSnapshot[Any]]
    .resolveSchemaCompatibility(formerFieldSnapshot.asInstanceOf[TypeSerializerSnapshot[Any]])
    .isIncompatible

  override protected def resolveOuterSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[T]
  ): CompositeTypeSerializerSnapshot.OuterSchemaCompatibility =
    OuterSchemaCompatibility.COMPATIBLE_AS_IS // outer compatibility already checked in resolveSchemaCompatibility

}

object ScalaCaseClassSerializerSnapshot {
  private val CurrentVersion = 4
}
