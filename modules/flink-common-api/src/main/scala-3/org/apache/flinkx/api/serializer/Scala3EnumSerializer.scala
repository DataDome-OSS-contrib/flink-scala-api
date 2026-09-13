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
import org.apache.flinkx.api.evolution.{Evolution, Evolutions}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}

import java.io.{IOException, ObjectInputStream}
import org.slf4j.{Logger, LoggerFactory}

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
      evolution.postDeserialize.apply(version, instance)
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

  // A TaskManager only Java-deserializes the serializers of the job graph, so each of them registers again the ADT
  // declaration it carries: the derivation ran on the client, and the restore needs the registry
  @throws[IOException]
  @throws[ClassNotFoundException]
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    Evolutions.register(evolution)
  }
  override def snapshotConfiguration(): TypeSerializerSnapshot[T] = new Scala3EnumSerializerSnapshot(Some(this))

}

/** Serializer snapshot for Scala 3 enum. */
class Scala3EnumSerializerSnapshot[T <: Product](
    serializer: Option[Scala3EnumSerializer[T]]
) extends CompositeTypeSerializerSnapshot[T, Scala3EnumSerializer[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  @transient private lazy val log: Logger = LoggerFactory.getLogger(classOf[Scala3EnumSerializerSnapshot[?]])

  private var evolution: Evolution[T] = _
  // Schema version of the enum this snapshot describes, as declared by @version at write time
  private var enumVersion: Int              = 0
  private var enumValueNames: Array[String] = Array.empty

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    evolution = s.evolution
    enumVersion = s.version
    enumValueNames = s.enumValueNames
  }

  override def getCurrentOuterSnapshotVersion: Int = Scala3EnumSerializerSnapshot.CurrentVersion

  override protected def getNestedSerializers(outerSerializer: Scala3EnumSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.enumValueSerializers

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): Scala3EnumSerializer[T] =
    new Scala3EnumSerializer(evolution, enumVersion, enumValueNames, nestedSerializers)

  override def writeOuterSnapshot(out: DataOutputView): Unit = {
    out.writeUTF(evolution.className)
    out.writeInt(enumVersion)
    StringArraySerializer.INSTANCE.serialize(enumValueNames, out)
  }

  override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    val enumClassName = if (readOuterSnapshotVersion > 1) in.readUTF() else null
    enumVersion = if (readOuterSnapshotVersion > 1) in.readInt() else 0
    evolution = Evolutions.get(enumClassName, enumVersion, cl)
    enumValueNames = StringArraySerializer.INSTANCE.deserialize(in)
  }

  /** Resolves the schema compatibility including potential evolutions.
    *
    * When evolutions are required, the migration is checked to determine if the schema is COMPATIBLE_AFTER_MIGRATION or
    * INCOMPATIBLE, otherwise delegates to the standard schema compatibility resolution.
    */
  override def resolveSchemaCompatibility(
      oldSerializerSnapshot: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = oldSerializerSnapshot match {
    case old: Scala3EnumSerializerSnapshot[T] if isSameClass(old) && isEvolutionRequired(old) =>
      checkMigration(old) match {
        case None =>
          TypeSerializerSchemaCompatibility.compatibleAfterMigration()
        case Some(reason) =>
          log.warn(s"Cannot migrate ${evolution.currentClass} from version ${old.enumVersion}: $reason")
          TypeSerializerSchemaCompatibility.incompatible()
      }
    case old: Scala3EnumSerializerSnapshot[T] if isSameClass(old) =>
      // No evolution: delegates schema compatibility to standard resolution
      super.resolveSchemaCompatibility(old)
    case _ => TypeSerializerSchemaCompatibility.incompatible()
  }

  /** Whether the former snapshot describes the very same enum.
    *
    * `old.evolution` has been resolved by `readOuterSnapshot`, so a renamed or moved former enum already carries the
    * current one. A snapshot written before 2.4.0 records no enum name at all, leaving nothing to compare.
    */
  private def isSameClass(old: Scala3EnumSerializerSnapshot[T]): Boolean =
    evolution.currentClass == null || old.evolution.currentClass == null ||
      evolution.currentClass == old.evolution.currentClass

  /** Whether reading the former form described by `old` requires applying the declared evolutions.
    *
    * `old.evolution` holds the evolutions migrating the very version the former data was written at.
    */
  private def isEvolutionRequired(old: Scala3EnumSerializerSnapshot[T]): Boolean =
    evolution.currentClass != null && !old.evolution.isAvoidable(old.enumValueNames)

  /** Check every former enum value is either declared deleted, or still a value of the current enum, possibly under
    * another name, and that the schema of these surviving values can itself be migrated.
    *
    * @return
    *   `None` if the migration is possible, the reason it isn't otherwise
    */
  private def checkMigration(old: Scala3EnumSerializerSnapshot[T]): Option[String] = {
    val currentValueSnapshots = getNestedSerializerSnapshots
    val formerValueSnapshots  = old.getNestedSerializerSnapshots

    def checkValue(formerIndex: Int, formerName: String, currentName: String): Option[String] = {
      val currentIndex = enumValueNames.indexOf(currentName)
      if (currentIndex < 0) {
        Some(
          s"former value '$formerName' is no longer a value of ${evolution.currentClass}." +
            s" Use @renamed(since = <version>,\"$formerName\") to declare it renamed, or" +
            s" @deletedClasses(since = <version>,\"$formerName\") to declare it deleted"
        )
      } else if (isValueIncompatible(currentValueSnapshots(currentIndex), formerValueSnapshots(formerIndex))) {
        Some(s"former value '$formerName' can't be migrated to '$currentName'")
      } else None
    }

    if (old.enumVersion > enumVersion) {
      Some(
        s"the former version ${old.enumVersion} is more recent than the current version $enumVersion. Restore from a" +
          s" former savepoint."
      )
    } else {
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

  /** Resolves the compatibility of a former enum value against its current one, which recursively applies the
    * evolutions declared on that value.
    */
  private def isValueIncompatible(
      currentValue: TypeSerializerSnapshot[_],
      formerValue: TypeSerializerSnapshot[_]
  ): Boolean = currentValue
    .asInstanceOf[TypeSerializerSnapshot[Any]]
    .resolveSchemaCompatibility(formerValue.asInstanceOf[TypeSerializerSnapshot[Any]])
    .isIncompatible

}

object Scala3EnumSerializerSnapshot {
  private val CurrentVersion = 2
}
