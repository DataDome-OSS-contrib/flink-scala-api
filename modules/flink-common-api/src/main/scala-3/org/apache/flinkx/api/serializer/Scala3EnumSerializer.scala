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
import org.apache.flinkx.api.evolution.{AdtEvolution, renamedOrDeletedHint}
import org.apache.flinkx.api.serializer.EvolvingSnapshot.{isIncompatible, readEvolution, writeEvolution}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}

/** Serializer of a Scala 3 enum. It writes the index of the enum value, then the value with the serializer of this enum
  * value. Handle nullable value.
  *
  * A restored serializer reads the value with the serializer of its former enum value. Then the ADT evolution of the
  * enum migrates it.
  *
  * @param evolution
  *   The ADT evolution of the enum. A current serializer has [[AdtEvolution.NoEvolution]]. A restored serializer has
  *   the [[AdtEvolution]] of the former class name at the former version, which migrates the former data. None for a
  *   serializer restored from a snapshot written before release 2.4.0, which records no class name.
  * @param enumValueNames
  *   The names of the enum values, which the snapshot records. Former names for a restored serializer.
  * @param enumValueSerializers
  *   The serializers of the enum values, in the same order: the order of the index.
  */
class Scala3EnumSerializer[T <: Product](
    val evolution: Option[AdtEvolution[T]],
    val enumValueNames: Array[String],
    val enumValueSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  require(
    // The serialized form contains the enum value index: both arrays describe the same values in the same order
    enumValueNames.length == enumValueSerializers.length,
    s"${evolution.fold("The enum")(_.currentClass.toString)} has ${enumValueNames.length} value names for ${enumValueSerializers.length} value serializers"
  )

  // The evolution that migrates each deserialized instance, or null: a current serializer migrates nothing
  private val migration: AdtEvolution[T] = evolution.filterNot(_.isEmpty).orNull

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
      new Scala3EnumSerializer[T](evolution, enumValueNames, enumValueSerializers.map(_.duplicate()))
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
      // The serializer of a deleted former value throws, or gives null
      val instance = enumValueSerializers(index).asInstanceOf[TypeSerializer[T]].deserialize(source)
      if (migration == null) instance else migration.migrate(instance)
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

  import Scala3EnumSerializerSnapshot.{CurrentFormatVersion, PreEvolutionFormatVersion}

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  private var evolution: Option[AdtEvolution[T]] = None
  private var enumValueNames: Array[String]      = Array.empty

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    evolution = s.evolution
    enumValueNames = s.enumValueNames
  }

  // A former serializer of a snapshot written before release 2.4.0 has no class name: it writes this format again
  override def getCurrentOuterSnapshotVersion: Int =
    if (evolution.isDefined) CurrentFormatVersion else PreEvolutionFormatVersion

  override protected def getNestedSerializers(outerSerializer: Scala3EnumSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.enumValueSerializers

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): Scala3EnumSerializer[T] =
    new Scala3EnumSerializer(evolution, enumValueNames, nestedSerializers)

  override def writeOuterSnapshot(out: DataOutputView): Unit = {
    evolution.foreach(writeEvolution(_, out))
    StringArraySerializer.INSTANCE.serialize(enumValueNames, out)
  }

  override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    evolution = Option.when(readOuterSnapshotVersion > PreEvolutionFormatVersion)(readEvolution[T](in, cl))
    enumValueNames = StringArraySerializer.INSTANCE.deserialize(in)
  }

  override private[serializer] def adtEvolution: Option[AdtEvolution[T]] = evolution

  override private[serializer] def memberNames: Array[String] = enumValueNames

  override protected def resolveStandardCompatibility(
      former: Scala3EnumSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] =
    // No migration is necessary: use the standard resolution of the parent class
    super[CompositeTypeSerializerSnapshot].resolveSchemaCompatibility(former)

  /** Check that each former enum value is deleted, or is still a value of the current enum, possibly under another
    * name. The former schema of each remaining value must also be compatible.
    */
  override protected def checkMigration(former: Scala3EnumSerializerSnapshot[T]): Option[String] = {
    val currentValueSnapshots = getNestedSerializerSnapshots
    val formerValueSnapshots  = former.getNestedSerializerSnapshots

    def checkValue(formerIndex: Int, formerName: String, currentName: String): Option[String] = {
      val currentIndex = enumValueNames.indexOf(currentName)
      if (currentIndex < 0) {
        Some(s"Former enum value '$formerName' is not in $adtName. ${renamedOrDeletedHint(formerName)}")
      } else if (isIncompatible(currentValueSnapshots(currentIndex), formerValueSnapshots(formerIndex))) {
        Some(s"Former enum value '$formerName' is incompatible with '$currentName'")
      } else None
    }

    former.enumValueNames.indices.iterator
      .flatMap { i =>
        val formerName = former.enumValueNames(i)
        // A deleted former value has no current name, so it needs no check
        former.evolution
          .fold(Option(formerName))(_.currentEnumValueName(formerName))
          .flatMap(checkValue(i, formerName, _))
      }
      .nextOption()
  }

}

object Scala3EnumSerializerSnapshot {
  // Last snapshot format version before release 2.4.0, it doesn't record the class name of the enum
  private val PreEvolutionFormatVersion = 1

  private val CurrentFormatVersion = 2
}
