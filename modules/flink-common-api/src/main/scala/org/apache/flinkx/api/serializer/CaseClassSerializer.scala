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
import org.apache.flink.api.common.typeutils.{
  CompositeTypeSerializerSnapshot,
  CompositeTypeSerializerUtil,
  TypeSerializer,
  TypeSerializerSchemaCompatibility,
  TypeSerializerSnapshot
}
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.NullFieldException
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.serializer.CaseClassSerializer.EmptyByteArray
import org.apache.flinkx.api.serializer.ScalaCaseClassSerializerSnapshot.CurrentVersion
import org.apache.flinkx.api.{NullMarker, VariableLengthDataType}
import org.slf4j.{Logger, LoggerFactory}

/** Serializer for Case Classes. Creation and access is different from our Java Tuples so we have to treat them
  * differently. Copied from Flink 1.14 and merged with ScalaCaseClassSerializer.
  */
@Internal
@SerialVersionUID(7341356073446263475L)
class CaseClassSerializer[T <: Product](
    clazz: Class[T],
    scalaFieldSerializers: Array[TypeSerializer[_]],
    val isCaseClassImmutable: Boolean
) extends TupleSerializerBase[T](clazz, scalaFieldSerializers)
    with Cloneable
    with ConstructorCompat {

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
      val fields = new Array[AnyRef](sourceArity)
      var i      = 0
      while (i < sourceArity) {
        fields(i) = fieldSerializers(i).deserialize(source)
        i += 1
      }
      createInstance(fields)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val sourceArity = source.readInt()
    target.writeInt(sourceArity)
    if (sourceArity < 0) {
      source.skipBytesToRead(nullPadding.length)
      target.skipBytesToWrite(nullPadding.length)
    } else {
      // Copy the fields of the source, which can be an older form having less fields, and not all the fields of
      // this serializer, like TupleSerializerBase does
      var i = 0
      while (i < sourceArity) {
        fieldSerializers(i).copy(source, target)
        i += 1
      }
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
    with ConstructorCompat {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  private var serializedClass: Option[Class[T]] = None
  private var isCaseClassImmutable: Boolean     = false

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    serializedClass = Some(s.getTupleClass)
    isCaseClassImmutable = s.isCaseClassImmutable
  }

  override protected def getCurrentOuterSnapshotVersion: Int = CurrentVersion

  override protected def getNestedSerializers(outerSerializer: CaseClassSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): CaseClassSerializer[T] = serializedClass match {
    case Some(clazz) => new CaseClassSerializer[T](clazz, nestedSerializers, isCaseClassImmutable)
    case None        => throw new IllegalStateException("type can not be NULL")
  }

  override protected def writeOuterSnapshot(out: DataOutputView): Unit = serializedClass match {
    case Some(clazz) =>
      out.writeUTF(clazz.getName)
      out.writeBoolean(isCaseClassImmutable)
    case None => throw new IllegalStateException("type can not be NULL")
  }

  override protected def readOuterSnapshot(
      readOuterSnapshotVersion: Int,
      in: DataInputView,
      userCodeClassLoader: ClassLoader
  ): Unit = {
    serializedClass = Some(InstantiationUtil.resolveClassByName(in, userCodeClassLoader))
    // If reading a version of 2 or below, don't read the boolean and set isCaseClassImmutable to false
    isCaseClassImmutable = readOuterSnapshotVersion > 2 && in.readBoolean
  }

  override protected def resolveOuterSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[T]
  ): CompositeTypeSerializerSnapshot.OuterSchemaCompatibility = {
    if (!oldSerializerSnapshot.isInstanceOf[ScalaCaseClassSerializerSnapshot[T]]) {
      return OuterSchemaCompatibility.INCOMPATIBLE
    }
    val caseClassSerializerSnapshot = oldSerializerSnapshot.asInstanceOf[ScalaCaseClassSerializerSnapshot[T]]
    val currentTypeName             = serializedClass.map(_.getName)
    val newTypeName                 = caseClassSerializerSnapshot.serializedClass.map(_.getName)
    if (currentTypeName == newTypeName) {
      OuterSchemaCompatibility.COMPATIBLE_AS_IS
    } else {
      OuterSchemaCompatibility.INCOMPATIBLE
    }
  }

  /** Resolves the schema compatibility, adding the support of fields appended to the case class on top of the standard
    * [[CompositeTypeSerializerSnapshot]] resolution, which considers any change of the nested serializer count, so any
    * change of the field count, as incompatible.
    */
  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = oldSerializerSnapshot match {
    case oldSnapshot: ScalaCaseClassSerializerSnapshot[T]
        if resolveOuterSchemaCompatibility(oldSnapshot) == OuterSchemaCompatibility.COMPATIBLE_AS_IS =>
      val newFieldSnapshots = getNestedSerializerSnapshots
      val oldFieldSnapshots = oldSnapshot.getNestedSerializerSnapshots
      if (newFieldSnapshots.length > oldFieldSnapshots.length) {
        resolveAppendedFieldsSchemaCompatibility(newFieldSnapshots, oldFieldSnapshots)
      } else {
        super.resolveSchemaCompatibility(oldSerializerSnapshot)
      }
    case _ => super.resolveSchemaCompatibility(oldSerializerSnapshot)
  }

  /** Resolves the schema compatibility when the case class has more fields than the one the old snapshot was written
    * with. [[CaseClassSerializer]] writes the field count in the serialized form, so it can read an older form having
    * less fields, as long as the appended fields have a default value to fill the missing ones. Only the compatibility
    * of the common fields is resolved, as Flink requires an identical nested serializer count.
    */
  private def resolveAppendedFieldsSchemaCompatibility(
      newFieldSnapshots: Array[TypeSerializerSnapshot[_]],
      oldFieldSnapshots: Array[TypeSerializerSnapshot[_]]
  ): TypeSerializerSchemaCompatibility[T] = serializedClass match {
    case None        => throw new IllegalStateException("type can not be NULL")
    case Some(clazz) =>
      val fieldIndicesWithDefaultValue = defaultValueIndices(clazz)
      val appendedFieldIndices         = (oldFieldSnapshots.length + 1) to newFieldSnapshots.length
      if (!appendedFieldIndices.forall(fieldIndicesWithDefaultValue.contains)) {
        // Without a default value, an appended field can't be filled when reading an older serialized form
        TypeSerializerSchemaCompatibility.incompatible()
      } else {
        val commonFieldsCompatibility = CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult[T](
          newFieldSnapshots.take(oldFieldSnapshots.length),
          oldFieldSnapshots
        )
        if (commonFieldsCompatibility.isIncompatible) {
          TypeSerializerSchemaCompatibility.incompatible()
        } else if (commonFieldsCompatibility.isCompatibleAfterMigration) {
          TypeSerializerSchemaCompatibility.compatibleAfterMigration()
        } else if (commonFieldsCompatibility.isCompatibleWithReconfiguredSerializer) {
          val appendedFieldSerializers = newFieldSnapshots.drop(oldFieldSnapshots.length).map(_.restoreSerializer())
          TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
            new CaseClassSerializer[T](
              clazz,
              commonFieldsCompatibility.getNestedSerializers ++ appendedFieldSerializers,
              isCaseClassImmutable
            )
          )
        } else {
          // The appended fields are filled with their default value when reading the older serialized form
          TypeSerializerSchemaCompatibility.compatibleAsIs()
        }
      }
  }

}

object ScalaCaseClassSerializerSnapshot {
  private val CurrentVersion = 3
}
