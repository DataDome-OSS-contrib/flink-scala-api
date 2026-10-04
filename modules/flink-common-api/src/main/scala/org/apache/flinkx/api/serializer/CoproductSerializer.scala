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
import org.apache.flinkx.api.evolution.{Evolution, Evolutions, renamedOrDeletedHint}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot
import org.apache.flinkx.api.serializer.EvolvingSnapshot.isIncompatible

class CoproductSerializer[T](
    val evolution: Evolution[T],
    val version: Int,
    val subtypeClasses: Array[Class[_]],
    val subtypeFqns: Array[String],
    val subtypeSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  require(
    // The serialized form holds the index of the subtype, so the three arrays describe the same subtypes in one order
    subtypeClasses.length == subtypeSerializers.length && subtypeFqns.length == subtypeSerializers.length,
    s"${evolution.currentClass} has ${subtypeClasses.length} subtype classes and ${subtypeFqns.length} subtype names for" +
      s" ${subtypeSerializers.length} subtype serializers"
  )

  override val isImmutableType: Boolean = subtypeSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = subtypeSerializers.forall(s => s.duplicate().eq(s))

  override def copy(from: T): T = {
    if (from == null || isImmutableType) {
      from
    } else {
      val i = subtypeClasses.indexWhere(_.isInstance(from))
      subtypeSerializers(i).asInstanceOf[TypeSerializer[T]].copy(from)
    }
  }

  override def duplicate(): CoproductSerializer[T] = {
    if (isImmutableSerializer) {
      this
    } else {
      new CoproductSerializer[T](evolution, version, subtypeClasses, subtypeFqns, subtypeSerializers.map(_.duplicate()))
    }
  }

  override def createInstance(): T =
    // this one may be used for later reuse, but we never reuse coproducts due to their unclear concrete type
    subtypeSerializers.head.createInstance().asInstanceOf[T]

  override val getLength: Int = {
    val length = subtypeSerializers(0).getLength
    if (subtypeSerializers.forall(_.getLength == length)) {
      length
    } else {
      VariableLengthDataType
    }
  }

  override def serialize(record: T, target: DataOutputView): Unit = {
    if (record == null) {
      target.writeByte(NullMarkerByte)
    } else {
      val subtypeIndex = subtypeClasses.indexWhere(_.isInstance(record))
      if (subtypeIndex >= 0) {
        target.writeByte(subtypeIndex)
        subtypeSerializers(subtypeIndex).asInstanceOf[TypeSerializer[T]].serialize(record, target)
      } else {
        throw new IllegalStateException("subtype not found in sealed trait schema")
      }
    }
  }

  override def deserialize(source: DataInputView): T = {
    val index = source.readByte().toInt
    if (index == NullMarkerByte) {
      null.asInstanceOf[T]
    } else {
      // A deleted former subtype throws or reads as null through the evolution of its own serializer
      val instance = subtypeSerializers(index).asInstanceOf[TypeSerializer[T]].deserialize(source)
      evolution.postEvolve(version, instance)
    }
  }

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val index = source.readByte().toInt
    target.writeByte(index)
    if (index != NullMarkerByte) {
      subtypeSerializers(index).asInstanceOf[TypeSerializer[T]].copy(source, target)
    }
  }

  override def snapshotConfiguration(): TypeSerializerSnapshot[T] =
    new CoproductSerializerSnapshot(Some(this))
}

object CoproductSerializer {

  private val CurrentVersion = 4

  class CoproductSerializerSnapshot[T](
      serializer: Option[CoproductSerializer[T]]
  ) extends TypeSerializerSnapshot[T]
      with EvolvingSnapshot[T, CoproductSerializerSnapshot[T]] {

    // Empty constructor is required to instantiate this class during deserialization.
    def this() = this(None)

    private[serializer] var evolution: Evolution[T] = _
    private[serializer] var adtVersion: Int         = 0
    private var subtypeClasses: Array[Class[_]]     = Array.empty
    private var subtypeFqns: Array[String]          = Array.empty

    // An adapter is mandatory to keep the compatibility during the transition to a CompositeTypeSerializerSnapshot
    // because its readSnapshot() method is final
    private val adapter: CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] =
      new CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] {

        serializer.foreach { s =>
          // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
          setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
          evolution = s.evolution
          adtVersion = s.version
          subtypeClasses = s.subtypeClasses
          subtypeFqns = s.subtypeFqns
        }

        override def getCurrentOuterSnapshotVersion: Int = CurrentVersion

        override def getNestedSerializers(outerSerializer: CoproductSerializer[T]): Array[TypeSerializer[_]] =
          outerSerializer.subtypeSerializers

        override def createOuterSerializerWithNestedSerializers(
            nestedSerializers: Array[TypeSerializer[_]]
        ): CoproductSerializer[T] =
          new CoproductSerializer[T](evolution, adtVersion, subtypeClasses, subtypeFqns, nestedSerializers)

        override def writeOuterSnapshot(out: DataOutputView): Unit = {
          out.writeUTF(evolution.className)
          out.writeInt(adtVersion)
          StringArraySerializer.INSTANCE.serialize(subtypeFqns, out)
        }

        override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
          val coproductClassName = if (readOuterSnapshotVersion > 3) in.readUTF() else null
          adtVersion = if (readOuterSnapshotVersion > 3) in.readInt() else 0
          evolution = Evolutions.get(coproductClassName, adtVersion, cl)
          subtypeFqns = StringArraySerializer.INSTANCE.deserialize(in)
          subtypeClasses = subtypeFqns.map(Evolutions.get(_, adtVersion, cl).currentClass)
        }

      }

    override def getCurrentVersion: Int = adapter.getCurrentVersion

    override def writeSnapshot(out: DataOutputView): Unit = adapter.writeSnapshot(out)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit =
      if (readVersion == 2) {
        val len = in.readInt()

        evolution = Evolutions.get(null, 0, userCodeClassLoader)
        adtVersion = 0

        subtypeFqns = (0 until len).map(_ => in.readUTF()).toArray
        subtypeClasses = subtypeFqns.map(Evolutions.get(_, 0, userCodeClassLoader).currentClass)

        val subtypeSerializers = (0 until len)
          .map(_ => TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer())
          .toArray

        setNestedSerializersSnapshots(adapter, subtypeSerializers.map(_.snapshotConfiguration()): _*)
      } else {
        adapter.readSnapshot(readVersion, in, userCodeClassLoader)
      }

    override protected def resolveUnevolvedCompatibility(
        old: CoproductSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] = adapter.resolveSchemaCompatibility(old.adapter)

    /** `old.evolution` holds the evolutions migrating the very version the former data was written at. */
    override protected def isEvolutionRequired(old: CoproductSerializerSnapshot[T]): Boolean =
      evolution.currentClass != null && !old.evolution.isAvoidable(old.adtVersion, old.subtypeFqns)

    /** Check every former subtype is either declared deleted, or still a member of the current sealed trait, possibly
      * under another name, and that the schema of these surviving subtypes can itself be migrated.
      */
    override protected def checkMigration(old: CoproductSerializerSnapshot[T]): Option[String] = {
      val currentSubtypeSnapshots = adapter.getNestedSerializerSnapshots
      val formerSubtypeSnapshots  = old.adapter.getNestedSerializerSnapshots
      old.subtypeFqns.indices.iterator
        .filterNot(i => Evolutions.isDeletedClass(old.subtypeClasses(i)))
        .flatMap { i =>
          val formerFqn    = old.subtypeFqns(i)
          val currentIndex = subtypeClasses.indexOf(old.subtypeClasses(i))
          if (currentIndex < 0) {
            Some(
              s"former subtype '$formerFqn' is no longer a member of ${evolution.currentClass}. " +
                renamedOrDeletedHint(formerFqn, "renamed or moved")
            )
          } else if (isIncompatible(currentSubtypeSnapshots(currentIndex), formerSubtypeSnapshots(i))) {
            Some(s"former subtype '$formerFqn' can't be migrated to ${subtypeClasses(currentIndex)}")
          } else None
        }
        .nextOption()
    }

    override def restoreSerializer(): TypeSerializer[T] = adapter.restoreSerializer()

  }

}
