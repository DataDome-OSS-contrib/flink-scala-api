package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.Evolutions
import org.apache.flinkx.api.serializer.ScalaCaseObjectSerializer.ScalaCaseObjectSerializerSnapshot

class ScalaCaseObjectSerializer[T](clazz: Class[T]) extends ImmutableSerializer[T] {

  private lazy val caseObject: T = if (Evolutions.get(clazz).isDeleted) {
    null.asInstanceOf[T]
  } else {
    clazz.getField("MODULE$").get(null).asInstanceOf[T]
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {}
  override def createInstance(): T                                       = caseObject
  override def getLength: Int                                            = 0
  override def serialize(record: T, target: DataOutputView): Unit        = {}
  override def deserialize(source: DataInputView): T                     = caseObject

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new ScalaCaseObjectSerializerSnapshot(clazz)

}

object ScalaCaseObjectSerializer {
  class ScalaCaseObjectSerializerSnapshot[T](var clazz: Class[T]) extends TypeSerializerSnapshot[T] {
    def this() = this(null)

    override def readSnapshot(readVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
      clazz = Evolutions.resolveFormerClass(in.readUTF(), cl)
    }

    override def writeSnapshot(out: DataOutputView): Unit = {
      out.writeUTF(clazz.getName)
    }

    override def getCurrentVersion: Int = 1

    override def resolveSchemaCompatibility(
        oldSerializer: TypeSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] =
      TypeSerializerSchemaCompatibility.compatibleAsIs()

    override def restoreSerializer(): TypeSerializer[T] =
      new ScalaCaseObjectSerializer[T](clazz)

  }
}
