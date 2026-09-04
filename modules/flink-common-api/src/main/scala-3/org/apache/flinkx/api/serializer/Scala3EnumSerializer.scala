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
import org.apache.flinkx.api.evolution.Evolutions
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}
import org.slf4j.{Logger, LoggerFactory}

/** Serializer for Scala 3 enum. Handle nullable value. */
class Scala3EnumSerializer[T <: Product](
    val clazz: Class[T],
    val version: Int,
    val enumValueNames: Array[String],
    val enumValueSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  require(
    // The serialized form holds the index of the enum value, so both arrays describe the same values in one order
    enumValueNames.length == enumValueSerializers.length,
    s"$clazz has ${enumValueNames.length} value names for ${enumValueSerializers.length} value serializers"
  )

  override val isImmutableType: Boolean = enumValueSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = enumValueSerializers.forall(s => s.duplicate().eq(s))

  // Cache to lookup Evolution on first record only
  @transient private lazy val evolution = Evolutions.get(clazz)

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
      new Scala3EnumSerializer[T](clazz, version, enumValueNames, enumValueSerializers.map(_.duplicate()))
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
      val fqn      = s"${clazz.getName}#${enumValueNames(index)}"
      val instance = enumValueSerializers(index).asInstanceOf[TypeSerializer[T]].deserialize(source)
      evolution.postDeserialize.apply(version, Evolutions.checkThrowOnInstance(instance, fqn))
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
) extends CompositeTypeSerializerSnapshot[T, Scala3EnumSerializer[T]] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  @transient private lazy val log: Logger = LoggerFactory.getLogger(classOf[Scala3EnumSerializerSnapshot[?]])

  private var clazz: Class[T]               = _
  private var enumVersion: Int              = 0 // version of the enum class (through @version)
  private var enumValueNames: Array[String] = Array.empty

  serializer.foreach { s =>
    // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
    setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
    clazz = s.clazz
    enumVersion = s.version
    enumValueNames = s.enumValueNames
  }

  override def getCurrentOuterSnapshotVersion: Int = Scala3EnumSerializerSnapshot.CurrentVersion

  override protected def getNestedSerializers(outerSerializer: Scala3EnumSerializer[T]): Array[TypeSerializer[_]] =
    outerSerializer.enumValueSerializers

  override protected def createOuterSerializerWithNestedSerializers(
      nestedSerializers: Array[TypeSerializer[_]]
  ): Scala3EnumSerializer[T] =
    new Scala3EnumSerializer(clazz, enumVersion, enumValueNames, nestedSerializers)

  override def writeOuterSnapshot(out: DataOutputView): Unit = {
    out.writeUTF(clazz.getName)
    out.writeInt(enumVersion)
    StringArraySerializer.INSTANCE.serialize(enumValueNames, out)
  }

  override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    clazz = if (readOuterSnapshotVersion > 1) Evolutions.resolveFormerClass(in.readUTF(), cl) else null
    enumVersion = if (readOuterSnapshotVersion > 1) in.readInt() else 0
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
          log.warn(s"Cannot migrate $clazz from version ${old.enumVersion}: $reason")
          TypeSerializerSchemaCompatibility.incompatible()
      }
    case old: Scala3EnumSerializerSnapshot[T] if isSameClass(old) =>
      // No evolution: delegates schema compatibility to standard resolution
      super.resolveSchemaCompatibility(old)
    case _ => TypeSerializerSchemaCompatibility.incompatible()
  }

  /** Whether the former snapshot describes the very same enum.
    *
    * `old.clazz` has been resolved by `readOuterSnapshot`, so a renamed or moved former enum already reads as the
    * current one. A snapshot written before 2.4.0 records no enum name at all, leaving nothing to compare.
    */
  private def isSameClass(old: Scala3EnumSerializerSnapshot[T]): Boolean =
    clazz == null || old.clazz == null || clazz.getName == old.clazz.getName

  /** Whether reading the former form described by `old` requires applying the declared evolutions. */
  private def isEvolutionRequired(old: Scala3EnumSerializerSnapshot[T]): Boolean =
    clazz != null && !Evolutions.get(clazz).isAvoidable(old.enumVersion, old.enumValueNames)

  /** Check every former enum value is either declared deleted, or still a value of the current enum, possibly under
    * another name, and that the schema of these surviving values can itself be migrated.
    *
    * @return
    *   `None` if the migration is possible, the reason it isn't otherwise
    */
  private def checkMigration(old: Scala3EnumSerializerSnapshot[T]): Option[String] = {
    val evolution             = Evolutions.get(clazz)
    val currentValueSnapshots = getNestedSerializerSnapshots
    val formerValueSnapshots  = old.getNestedSerializerSnapshots
    if (old.enumVersion > enumVersion) {
      Some(
        s"the former version ${old.enumVersion} is more recent than the current version $enumVersion. Restore from a" +
          s" former savepoint."
      )
    } else {
      old.enumValueNames.indices.iterator
        // A deleted enum value is registered as `<enum fqn>#<value name>`, as Scala3EnumSerializer.deserialize checks
        .filterNot(i => Evolutions.isDeletedFormerClass(s"${clazz.getName}#${old.enumValueNames(i)}"))
        .flatMap { i =>
          val formerName   = old.enumValueNames(i)
          val currentName  = evolution.resolveFormerEnumValueName(formerName)
          val currentIndex = enumValueNames.indexOf(currentName)
          if (currentIndex < 0) {
            Some(
              s"former value '$formerName' is no longer a value of $clazz. Use @renamed(since = <version>," +
                s"\"$formerName\") to declare it renamed, or @deletedClasses(since = <version>,\"$formerName\") to" +
                s" declare it deleted"
            )
          } else if (isValueIncompatible(currentValueSnapshots(currentIndex), formerValueSnapshots(i))) {
            Some(s"former value '$formerName' can't be migrated to '$currentName'")
          } else None
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
