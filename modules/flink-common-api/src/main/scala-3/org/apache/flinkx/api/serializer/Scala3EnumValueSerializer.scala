package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.AdtEvolution
import org.apache.flinkx.api.serializer.EvolvingSnapshot.{isSameClass, readEvolution, writeEvolution}
import org.apache.flinkx.api.util.ClassUtil

/** Serializer for Scala 3 enum value. */
class Scala3EnumValueSerializer[T](
    val evolution: AdtEvolution[T],
    val enumValueName: String
) extends ImmutableSerializer[T] {

  // The synthetic companion module class holds each enum value without parameters in a static field
  @transient private lazy val companionClass: Class[?] =
    Class.forName(ClassUtil.companionName(evolution.currentClass.getName), false, evolution.currentClass.getClassLoader)

  /** The enum value named `valueName`, read from the static fields of the companion module class. */
  private def valueOf(valueName: String): T =
    companionClass.getFields.find(_.getName == valueName).map(_.get(null)).orNull.asInstanceOf[T]

  @transient private lazy val enumValue: T = evolution.migrateEnumValue(enumValueName)(valueOf)

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

  // The ADT evolution of the enum that declares the value
  private var evolution: AdtEvolution[T] = _
  private var enumValueName: String      = _

  serializer.foreach { s =>
    evolution = s.evolution
    enumValueName = s.enumValueName
  }

  override def writeSnapshot(out: DataOutputView): Unit = {
    writeEvolution(evolution, out)
    out.writeUTF(enumValueName)
  }

  // The snapshot format version 1, before release 2.4.0, records no schema version
  override def readSnapshot(readVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
    evolution = readEvolution(in, cl, hasVersion = readVersion > 1)
    enumValueName = in.readUTF()
  }

  override def getCurrentVersion: Int = Scala3EnumValueSerializerSnapshot.CurrentVersion

  /** Compatible as is when both snapshots describe the same enum value, possibly under a former name. */
  override def resolveSchemaCompatibility(
      former: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = former match {
    case sameKind: Scala3EnumValueSerializerSnapshot[?]
        if isSameClass(evolution, sameKind.evolution) &&
          sameKind.evolution.currentEnumValueName(sameKind.enumValueName).contains(enumValueName) =>
      TypeSerializerSchemaCompatibility.compatibleAsIs()
    case _ =>
      TypeSerializerSchemaCompatibility.incompatible()
  }

  override def restoreSerializer(): TypeSerializer[T] =
    new Scala3EnumValueSerializer[T](evolution, enumValueName)

}

object Scala3EnumValueSerializerSnapshot {
  private val CurrentVersion = 2
}
