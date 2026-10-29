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
import org.apache.flinkx.api.evolution.{AdtEvolution, Evolutions, renamedOrDeletedHint}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot
import org.apache.flinkx.api.serializer.EvolvingSnapshot.{isIncompatible, readEvolution, writeEvolution}

/** Serializer of a sealed trait. It writes the index of the subtype, then the instance with the subtype serializer.
  * Handle nullable instance.
  *
  * A restored serializer reads the instance with the serializer of its former subtype. Then the ADT evolution of the
  * sealed trait migrates it.
  *
  * @param evolution
  *   The ADT evolution of the sealed trait. A current serializer has [[AdtEvolution.NoEvolution]]. A restored
  *   serializer has the [[AdtEvolution]] of the former class name at the former version, which migrates the former
  *   data. None for a serializer restored from a snapshot written before release 2.4.0, which records no class name.
  * @param subtypeClasses
  *   The current classes of the subtypes. A restored serializer has the deletion marker for a deleted subtype.
  * @param subtypeNames
  *   The class names of the subtypes, which the snapshot records. Former class names for a restored serializer.
  * @param subtypeSerializers
  *   The serializers of the subtypes. The three arrays have the same order: the order of the subtype index.
  */
class CoproductSerializer[T](
    val evolution: Option[AdtEvolution[T]],
    val subtypeClasses: Array[Class[_]],
    val subtypeNames: Array[String],
    val subtypeSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  require(
    // The serialized form contains the subtype index: the three arrays describe the same subtypes in the same order
    subtypeClasses.length == subtypeSerializers.length && subtypeNames.length == subtypeSerializers.length,
    s"${evolution.fold("The sealed trait")(_.currentClass.toString)} has ${subtypeClasses.length} subtype classes and" +
      s" ${subtypeNames.length} subtype names for ${subtypeSerializers.length} subtype serializers"
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
      new CoproductSerializer[T](evolution, subtypeClasses, subtypeNames, subtypeSerializers.map(_.duplicate()))
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
      val instance = subtypeSerializers(index).asInstanceOf[TypeSerializer[T]].deserialize(source)
      evolution.fold(instance)(_.migrate(instance))
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

  // Last snapshot format version before release 2.4.0, it doesn't record the class name of the sealed trait
  private val PreEvolutionFormatVersion = 3

  private val CurrentFormatVersion = 4

  class CoproductSerializerSnapshot[T](
      serializer: Option[CoproductSerializer[T]]
  ) extends TypeSerializerSnapshot[T]
      with EvolvingSnapshot[T, CoproductSerializerSnapshot[T]] {

    // Empty constructor is required to instantiate this class during deserialization.
    def this() = this(None)

    private var evolution: Option[AdtEvolution[T]] = None
    private var subtypeClasses: Array[Class[_]]    = Array.empty
    private var subtypeNames: Array[String]        = Array.empty

    // An adapter is mandatory to keep the compatibility during the transition to a CompositeTypeSerializerSnapshot
    // because its readSnapshot() method is final
    private val adapter: CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] =
      new CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] {

        serializer.foreach { s =>
          // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
          setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
          evolution = s.evolution
          subtypeClasses = s.subtypeClasses
          subtypeNames = s.subtypeNames
        }

        // A former serializer of a snapshot written before release 2.4.0 has no class name: it writes this format again
        override def getCurrentOuterSnapshotVersion: Int =
          if (evolution.isDefined) CurrentFormatVersion else PreEvolutionFormatVersion

        override def getNestedSerializers(outerSerializer: CoproductSerializer[T]): Array[TypeSerializer[_]] =
          outerSerializer.subtypeSerializers

        override def createOuterSerializerWithNestedSerializers(
            nestedSerializers: Array[TypeSerializer[_]]
        ): CoproductSerializer[T] =
          new CoproductSerializer[T](evolution, subtypeClasses, subtypeNames, nestedSerializers)

        override def writeOuterSnapshot(out: DataOutputView): Unit = {
          evolution.foreach(writeEvolution(_, out))
          StringArraySerializer.INSTANCE.serialize(subtypeNames, out)
        }

        override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
          evolution = Option.when(readOuterSnapshotVersion > PreEvolutionFormatVersion)(readEvolution[T](in, cl))
          subtypeNames = StringArraySerializer.INSTANCE.deserialize(in)
          subtypeClasses = subtypeNames.map(Evolutions.get(_, adtVersion, cl).currentClass)
        }

      }

    override def getCurrentVersion: Int = adapter.getCurrentVersion

    override def writeSnapshot(out: DataOutputView): Unit = adapter.writeSnapshot(out)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit =
      if (readVersion == 2) {
        val len = in.readInt()

        evolution = None // Written before release 2.4.0, without the class name of the sealed trait

        subtypeNames = (0 until len).map(_ => in.readUTF()).toArray
        subtypeClasses = subtypeNames.map(Evolutions.get(_, 0, userCodeClassLoader).currentClass)

        val subtypeSerializers = (0 until len)
          .map(_ => TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer())
          .toArray

        setNestedSerializersSnapshots(adapter, subtypeSerializers.map(_.snapshotConfiguration()): _*)
      } else {
        adapter.readSnapshot(readVersion, in, userCodeClassLoader)
      }

    override private[serializer] def adtEvolution: Option[AdtEvolution[T]] = evolution

    override private[serializer] def memberNames: Array[String] = subtypeNames

    override protected def resolveStandardCompatibility(
        former: CoproductSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] = adapter.resolveSchemaCompatibility(former.adapter)

    /** Check that each former subtype is a deleted class, or is still a subtype of the current sealed trait, possibly
      * under another class name. The former schema of each remaining subtype must also be compatible.
      */
    override protected def checkMigration(former: CoproductSerializerSnapshot[T]): Option[String] = {
      val currentSubtypeSnapshots = adapter.getNestedSerializerSnapshots
      val formerSubtypeSnapshots  = former.adapter.getNestedSerializerSnapshots
      former.subtypeNames.indices.iterator
        .filterNot(i => AdtEvolution.isDeletedClass(former.subtypeClasses(i)))
        .flatMap { i =>
          val formerClassName = former.subtypeNames(i)
          val currentIndex    = subtypeClasses.indexOf(former.subtypeClasses(i))
          if (currentIndex < 0) {
            Some(s"Former subtype '$formerClassName' is not in $adtName. ${renamedOrDeletedHint(formerClassName)}")
          } else if (isIncompatible(currentSubtypeSnapshots(currentIndex), formerSubtypeSnapshots(i))) {
            Some(s"Former subtype '$formerClassName' is incompatible with ${subtypeClasses(currentIndex).getName}")
          } else None
        }
        .nextOption()
    }

    override def restoreSerializer(): TypeSerializer[T] = adapter.restoreSerializer()

  }

}
