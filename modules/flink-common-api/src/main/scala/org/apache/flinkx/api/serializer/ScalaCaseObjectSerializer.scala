package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.AdtEvolution
import org.apache.flinkx.api.serializer.EvolvingSnapshot.{isSameClass, readEvolution, writeEvolution}
import org.apache.flinkx.api.serializer.ScalaCaseObjectSerializer.ScalaCaseObjectSerializerSnapshot
import org.apache.flinkx.api.util.ClassUtil

/** Serializer of a case object. It writes nothing, and deserialization gives the case object.
  *
  * A restored serializer gives the case object of the current class. For a deleted case object, it gives `null` or
  * throws a [[org.apache.flinkx.api.evolution.DeletedInstanceException]].
  *
  * @param evolution
  *   The ADT evolution. A current serializer has [[AdtEvolution.NoEvolution]]. A restored serializer has the
  *   [[AdtEvolution]] of the former class name at the former version, which migrates the former data.
  */
class ScalaCaseObjectSerializer[T](val evolution: AdtEvolution[T]) extends ImmutableSerializer[T] {

  @transient private lazy val caseObject: T =
    evolution.migrate(ClassUtil.companionInstance[T](evolution.currentClass))

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

    private[serializer] var evolution: AdtEvolution[T] = _

    serializer.foreach(s => evolution = s.evolution)

    override def writeSnapshot(out: DataOutputView): Unit = writeEvolution(evolution, out)

    // The snapshot format version 1, before release 2.4.0, records no schema version
    override def readSnapshot(readVersion: Int, in: DataInputView, cl: ClassLoader): Unit =
      evolution = readEvolution(in, cl, hasVersion = readVersion > 1)

    override def getCurrentVersion: Int = CurrentVersion

    /** Compatible as is when both snapshots describe the same case object, possibly under a former class name. */
    override def resolveSchemaCompatibility(
        former: TypeSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] = former match {
      case sameKind: ScalaCaseObjectSerializerSnapshot[_] if isSameClass(evolution, sameKind.evolution) =>
        TypeSerializerSchemaCompatibility.compatibleAsIs()
      case _ =>
        TypeSerializerSchemaCompatibility.incompatible()
    }

    override def restoreSerializer(): TypeSerializer[T] =
      new ScalaCaseObjectSerializer[T](evolution)

  }
}
