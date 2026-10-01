package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil.setNestedSerializersSnapshots
import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer
import org.apache.flink.api.common.typeutils.{
  CompositeTypeSerializerSnapshot,
  TypeSerializer,
  TypeSerializerSchemaCompatibility,
  TypeSerializerSnapshot
}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{
  DeletedReturnNull,
  DeletedThrowOnInstance,
  Renamed,
  Unchanged
}
import org.apache.flinkx.api.evolution.{Evolution, Evolutions, renamedOrDeletedHint}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}

/** Serializer for Scala 3 enum. Handle nullable value. */
class Scala3EnumSerializer[T <: Product](
    val evolution: Evolution[T],
    val version: Int,
    val enumValueNames: Array[String],
    val enumValueSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  require(
    // The serialized form holds the index of the enum value, so both arrays describe the same values in one order
    enumValueNames.length == enumValueSerializers.length,
    s"${evolution.currentClass} has ${enumValueNames.length} value names for ${enumValueSerializers.length} value serializers"
  )

  override val isImmutableType: Boolean = enumValueSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = enumValueSerializers.forall(s => s.duplicate().eq(s))

  override def copy(from: T): T = {
    if (from == null || isImmutableType) {
      from
    } else {
      val i = enumValueNames.indexOf(from.productPrefix) // productPrefix returns enum value name even for case class
      enumValueSerializers(i).asInstanceOf[TypeSerializer[T]].copy(from)
    }
  }

  override def duplicate(): Scala3EnumSerializer[T] = {
    if (isImmutableSerializer) {
      this
    } else {
      new Scala3EnumSerializer[T](evolution, version, enumValueNames, enumValueSerializers.map(_.duplicate()))
    }
  }

  override def createInstance(): T =
    enumValueSerializers.head.createInstance().asInstanceOf[T]

  override val getLength: Int = {
    val length = enumValueSerializers(0).getLength
    if (enumValueSerializers.forall(_.getLength == length)) {
      length
    } else {
      VariableLengthDataType
    }
  }

  override def serialize(record: T, target: DataOutputView): Unit = {
    if (record == null) {
      target.writeByte(NullMarkerByte)
    } else {
      val enumValueIndex = enumValueNames.indexOf(record.productPrefix) // returns enum value name even for case class
      if (enumValueIndex >= 0) {
        target.writeByte(enumValueIndex)
        enumValueSerializers(enumValueIndex).asInstanceOf[TypeSerializer[T]].serialize(record, target)
      } else {
        throw new IllegalStateException("enum value not found in enum schema")
      }
    }
  }

  override def deserialize(source: DataInputView): T = {
    val index = source.readByte().toInt
    if (index == NullMarkerByte) {
      null.asInstanceOf[T]
    } else {
      // A deleted former value throws or reads as null through the evolution of its own serializer
      val instance = enumValueSerializers(index).asInstanceOf[TypeSerializer[T]].deserialize(source)
      evolution.postEvolve(version, instance)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val index = source.readByte()
    target.writeByte(index)
    if (index != NullMarkerByte) {
      val subtype = enumValueSerializers(index.toInt)
      subtype.asInstanceOf[TypeSerializer[T]].copy(source, target)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] = new Scala3EnumSerializerSnapshot(Some(this))

}

/** Serializer snapshot for Scala 3 enum. */
class Scala3EnumSerializerSnapshot[T <: Product](
    serializer: Option[Scala3EnumSerializer[T]]
) extends CompositeTypeSerializerSnapshot[T, Scala3EnumSerializer[T]]
    with EvolvingSnapshot[T, Scala3EnumSerializerSnapshot[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  private[serializer] var evolution: Evolution[T] = _
  private[serializer] var adtVersion: Int         = 0
  private var enumValueNames: Array[String]       = Array.empty

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    evolution = s.evolution
    adtVersion = s.version
    enumValueNames = s.enumValueNames
  }

  override def getCurrentOuterSnapshotVersion: Int = Scala3EnumSerializerSnapshot.CurrentVersion

  override protected def getNestedSerializers(outerSerializer: Scala3EnumSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.enumValueSerializers

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): Scala3EnumSerializer[T] =
    new Scala3EnumSerializer(evolution, adtVersion, enumValueNames, nestedSerializers)

  override def writeOuterSnapshot(out: DataOutputView): Unit = {
    out.writeUTF(evolution.className)
    out.writeInt(adtVersion)
    StringArraySerializer.INSTANCE.serialize(enumValueNames, out)
  }

  override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    val enumClassName = if (readOuterSnapshotVersion > 1) in.readUTF() else null
    adtVersion = if (readOuterSnapshotVersion > 1) in.readInt() else 0
    evolution = Evolutions.get(enumClassName, adtVersion, cl)
    enumValueNames = StringArraySerializer.INSTANCE.deserialize(in)
  }

  override protected def resolveUnevolvedCompatibility(
      old: Scala3EnumSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] =
    // No evolution: delegates schema compatibility to parent standard resolution
    super[CompositeTypeSerializerSnapshot].resolveSchemaCompatibility(old)

  /** `old.evolution` holds the evolutions migrating the very version the former data was written at. */
  override protected def isEvolutionRequired(old: Scala3EnumSerializerSnapshot[T]): Boolean =
    evolution.currentClass != null && !old.evolution.isAvoidable(old.adtVersion, old.enumValueNames)

  /** Check every former enum value is either declared deleted, or still a value of the current enum, possibly under
    * another name, and that the schema of these surviving values can itself be migrated.
    */
  override protected def checkMigration(old: Scala3EnumSerializerSnapshot[T]): Option[String] = {
    val currentValueSnapshots = getNestedSerializerSnapshots
    val formerValueSnapshots  = old.getNestedSerializerSnapshots

    def checkValue(formerIndex: Int, formerName: String, currentName: String): Option[String] = {
      val currentIndex = enumValueNames.indexOf(currentName)
      if (currentIndex < 0) {
        Some(
          s"former value '$formerName' is no longer a value of ${evolution.currentClass}. " +
            renamedOrDeletedHint(formerName, "renamed")
        )
      } else if (
        EvolvingSnapshot.isIncompatible(currentValueSnapshots(currentIndex), formerValueSnapshots(formerIndex))
      ) {
        Some(s"former value '$formerName' can't be migrated to '$currentName'")
      } else None
    }

    old.enumValueNames.indices.iterator
      .flatMap { i =>
        val formerName = old.enumValueNames(i)
        old.evolution.getEnumValueEvolution(formerName) match {
          // A deleted former value throws or reads as null through the evolution of its own serializer
          case DeletedThrowOnInstance | DeletedReturnNull => None
          case Renamed(currentName)                       => checkValue(i, formerName, currentName)
          case Unchanged                                  => checkValue(i, formerName, formerName)
        }
      }
      .nextOption()
  }

}

object Scala3EnumSerializerSnapshot {
  private val CurrentVersion = 2
}
