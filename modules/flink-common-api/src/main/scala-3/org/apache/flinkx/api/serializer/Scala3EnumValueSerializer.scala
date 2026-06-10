package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{
  DeletedReturnNull,
  DeletedThrowOnInstance,
  Renamed,
  Unchanged
}
import org.apache.flinkx.api.evolution.{DeletedInstanceException, Evolution, Evolutions}

/** Serializer for Scala 3 enum value. */
class Scala3EnumValueSerializer[T](
    val evolution: Evolution[T],
    val version: Int,
    val enumValueName: String
) extends ImmutableSerializer[T] {

  // Parameterless enum values are held as static fields of the synthetic companion module class
  @transient private lazy val companionClass: Class[?] =
    Class.forName(evolution.currentClass.getName + "$", false, evolution.currentClass.getClassLoader)

  /** Read the enum value declared under `valueName` from the static fields of the companion module class. */
  private def valueOf(valueName: String): T =
    companionClass.getFields.find(_.getName == valueName).map(_.get(null)).orNull.asInstanceOf[T]

  @transient private lazy val enumValue: T = evolution.getEnumValueEvolution(enumValueName) match {
    case Unchanged              => valueOf(enumValueName)
    case Renamed(currentName)   => valueOf(currentName)
    case DeletedThrowOnInstance => throw DeletedInstanceException(s"${evolution.currentClass.getName}#$enumValueName")
    case DeletedReturnNull      => null.asInstanceOf[T]
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {}
  override def createInstance(): T                                       = enumValue
  override def getLength: Int                                            = 0
  override def serialize(record: T, target: DataOutputView): Unit        = {}
  override def deserialize(source: DataInputView): T                     = enumValue

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new Scala3EnumValueSerializerSnapshot(Some(this))
}

/** Serializer snapshot for Scala 3 enum value. */
class Scala3EnumValueSerializerSnapshot[T](
    serializer: Option[Scala3EnumValueSerializer[T]]
) extends TypeSerializerSnapshot[T] {

  // Empty constructor is required to instantiate this class during deserialization.
  def this() = this(None)

  private var evolution: Evolution[T] = _
  // Schema version of the enum this snapshot describes, as declared by @version at write time
  private var enumVersion: Int      = 0
  private var enumValueName: String = _

  serializer.foreach { s =>
    evolution = s.evolution
    enumVersion = s.version
    enumValueName = s.enumValueName
  }

  override def writeSnapshot(out: DataOutputView): Unit = {
    out.writeInt(enumVersion)
    out.writeUTF(evolution.className)
    out.writeUTF(enumValueName)
  }

  override def readSnapshot(readVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    enumVersion = if (readVersion > 1) in.readInt() else 0
    evolution = Evolutions.get(in.readUTF(), enumVersion, cl)
    enumValueName = in.readUTF()
  }

  override def getCurrentVersion: Int = Scala3EnumValueSerializerSnapshot.CurrentVersion

  override def resolveSchemaCompatibility(
      oldSerializer: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] =
    TypeSerializerSchemaCompatibility.compatibleAsIs()

  override def restoreSerializer(): TypeSerializer[T] =
    new Scala3EnumValueSerializer[T](evolution, enumVersion, enumValueName)

}

object Scala3EnumValueSerializerSnapshot {
  private val CurrentVersion = 2
}
