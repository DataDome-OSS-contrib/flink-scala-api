package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.{Evolution, Evolutions}
import org.apache.flinkx.api.serializer.ScalaCaseObjectSerializer.ScalaCaseObjectSerializerSnapshot

class ScalaCaseObjectSerializer[T](val evolution: Evolution[T], val version: Int) extends ImmutableSerializer[T] {

  @transient private lazy val caseObject: T = if (evolution.isDeleted) {
    evolution.returnNullOrThrow
  } else {
    evolution.currentClass.getField("MODULE$").get(null).asInstanceOf[T]
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {}
  override def createInstance(): T                                       = caseObject
  override def getLength: Int                                            = 0
  override def serialize(record: T, target: DataOutputView): Unit        = {}
  override def deserialize(source: DataInputView): T                     = caseObject

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseObjectSerializerSnapshot(Some(this))

}

object ScalaCaseObjectSerializer {

  private val CurrentVersion = 2

  class ScalaCaseObjectSerializerSnapshot[T](
      serializer: Option[ScalaCaseObjectSerializer[T]]
  ) extends TypeSerializerSnapshot[T] {

    def this() = this(None)

    private var evolution: Evolution[T] = _
    // Schema version of the ADT this snapshot describes, as declared by @version at write time
    private var caseObjectVersion: Int = 0

    serializer.foreach { s =>
      evolution = s.evolution
      caseObjectVersion = s.version
    }

    override def writeSnapshot(out: DataOutputView): Unit = {
      out.writeInt(caseObjectVersion)
      out.writeUTF(evolution.className)
    }

    override def readSnapshot(readVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
      caseObjectVersion = if (readVersion > 1) in.readInt() else 0
      val caseObjectClassName = in.readUTF()
      evolution = Evolutions.get(caseObjectClassName, caseObjectVersion, cl)
    }

    override def getCurrentVersion: Int = CurrentVersion

    override def resolveSchemaCompatibility(
        oldSerializer: TypeSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def restoreSerializer(): TypeSerializer[T] =
      new ScalaCaseObjectSerializer[T](evolution, caseObjectVersion)

  }
}
