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
import org.apache.flinkx.api.evolution.{Evolution, Evolutions}
import org.apache.flinkx.api.{NullMarkerByte, VariableLengthDataType}
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot
import org.slf4j.{Logger, LoggerFactory}

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
      evolution.postDeserialize.apply(version, instance)
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
  ) extends TypeSerializerSnapshot[T] {

    // Empty constructor is required to instantiate this class during deserialization.
    def this() = this(None)

    @transient private lazy val log: Logger = LoggerFactory.getLogger(classOf[CoproductSerializerSnapshot[_]])

    private var evolution: Evolution[T] = _
    // Schema version of the sealed trait this snapshot describes, as declared by @version at write time
    private var coproductVersion: Int           = 0
    private var subtypeClasses: Array[Class[_]] = Array.empty
    private var subtypeFqns: Array[String]      = Array.empty

    // An adapter is mandatory to keep the compatibility during the transition to a CompositeTypeSerializerSnapshot
    // because its readSnapshot() method is final
    private val adapter: CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] =
      new CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] {

        serializer.foreach { s =>
          // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
          setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
          evolution = s.evolution
          coproductVersion = s.version
          subtypeClasses = s.subtypeClasses
          subtypeFqns = s.subtypeFqns
        }

        override def getCurrentOuterSnapshotVersion: Int = CurrentVersion

        override def getNestedSerializers(outerSerializer: CoproductSerializer[T]): Array[TypeSerializer[_]] =
          outerSerializer.subtypeSerializers

        override def createOuterSerializerWithNestedSerializers(
            nestedSerializers: Array[TypeSerializer[_]]
        ): CoproductSerializer[T] =
          new CoproductSerializer[T](evolution, coproductVersion, subtypeClasses, subtypeFqns, nestedSerializers)

        override def writeOuterSnapshot(out: DataOutputView): Unit = {
          out.writeUTF(evolution.className)
          out.writeInt(coproductVersion)
          StringArraySerializer.INSTANCE.serialize(subtypeFqns, out)
        }

        override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
          val coproductClassName = if (readOuterSnapshotVersion > 3) in.readUTF() else null
          coproductVersion = if (readOuterSnapshotVersion > 3) in.readInt() else 0
          evolution = Evolutions.get(coproductClassName, coproductVersion, cl)
          subtypeFqns = StringArraySerializer.INSTANCE.deserialize(in)
          subtypeClasses = subtypeFqns.map(Evolutions.get(_, coproductVersion, cl).currentClass)
        }

      }

    override def getCurrentVersion: Int = adapter.getCurrentVersion

    override def writeSnapshot(out: DataOutputView): Unit = adapter.writeSnapshot(out)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit =
      if (readVersion == 2) {
        val len = in.readInt()

        evolution = Evolutions.get(null, 0, userCodeClassLoader)
        coproductVersion = 0

        subtypeFqns = (0 until len).map(_ => in.readUTF()).toArray
        subtypeClasses = subtypeFqns.map(Evolutions.get(_, 0, userCodeClassLoader).currentClass)

        val subtypeSerializers = (0 until len)
          .map(_ => TypeSerializerSnapshot.readVersionedSnapshot(in, userCodeClassLoader).restoreSerializer())
          .toArray

        setNestedSerializersSnapshots(adapter, subtypeSerializers.map(_.snapshotConfiguration()): _*)
      } else {
        adapter.readSnapshot(readVersion, in, userCodeClassLoader)
      }

    /** Resolves the schema compatibility including potential evolutions.
      *
      * When evolutions are required, the migration is checked to determine if the schema is COMPATIBLE_AFTER_MIGRATION
      * or INCOMPATIBLE, otherwise delegates to the standard schema compatibility resolution.
      */
    override def resolveSchemaCompatibility(
        oldSerializerSnapshot: TypeSerializerSnapshot[T]
    ): TypeSerializerSchemaCompatibility[T] = oldSerializerSnapshot match {
      case old: CoproductSerializerSnapshot[T] if isSameClass(old) && isEvolutionRequired(old) =>
        checkEvolutionMigration(old) match {
          case None =>
            TypeSerializerSchemaCompatibility.compatibleAfterMigration()
          case Some(reason) =>
            log.warn(s"Cannot migrate ${evolution.currentClass} from version ${old.coproductVersion}: $reason")
            TypeSerializerSchemaCompatibility.incompatible()
        }
      case old: CoproductSerializerSnapshot[T] if isSameClass(old) =>
        // No evolution: delegates schema compatibility to standard resolution
        adapter.resolveSchemaCompatibility(old.adapter)
      case _ => TypeSerializerSchemaCompatibility.incompatible()
    }

    /** Whether the former snapshot describes the very same sealed trait.
      *
      * `old.evolution` has been resolved by `readOuterSnapshot`, so a renamed or moved former trait already carries the
      * current one. A snapshot written before 2.4.0 records no trait name at all, leaving nothing to compare.
      */
    private def isSameClass(old: CoproductSerializerSnapshot[T]): Boolean =
      evolution.currentClass == null || old.evolution.currentClass == null ||
        evolution.currentClass == old.evolution.currentClass

    /** Whether reading the former form described by `old` requires applying the declared evolutions.
      *
      * `old.evolution` holds the evolutions migrating the very version the former data was written at.
      */
    private def isEvolutionRequired(old: CoproductSerializerSnapshot[T]): Boolean =
      evolution.currentClass != null && !old.evolution.isAvoidable(old.subtypeFqns)

    /** Check every former subtype is either declared deleted, or still a member of the current sealed trait, possibly
      * under another name, and that the schema of these surviving subtypes can itself be migrated.
      *
      * @return
      *   `None` if the migration is possible, the reason it isn't otherwise
      */
    private def checkEvolutionMigration(old: CoproductSerializerSnapshot[T]): Option[String] = {
      val currentSubtypeSnapshots = adapter.getNestedSerializerSnapshots
      val formerSubtypeSnapshots  = old.adapter.getNestedSerializerSnapshots
      if (old.coproductVersion > coproductVersion) {
        Some(
          s"the former version ${old.coproductVersion} is more recent than the current version $coproductVersion." +
            s" Restore from a former savepoint."
        )
      } else {
        old.subtypeFqns.indices.iterator
          .filterNot(i => Evolutions.isDeletedClass(old.subtypeClasses(i)))
          .flatMap { i =>
            val formerFqn    = old.subtypeFqns(i)
            val currentIndex = subtypeClasses.indexOf(old.subtypeClasses(i))
            if (currentIndex < 0) {
              Some(
                s"former subtype '$formerFqn' is no longer a member of ${evolution.currentClass}." +
                  s" Use @renamed(since = <version>," +
                  s"\"$formerFqn\") to declare it renamed or moved, or @deletedClasses(since = <version>," +
                  s"\"$formerFqn\") to declare it deleted"
              )
            } else if (isSubtypeIncompatible(currentSubtypeSnapshots(currentIndex), formerSubtypeSnapshots(i))) {
              Some(s"former subtype '$formerFqn' can't be migrated to ${subtypeClasses(currentIndex)}")
            } else None
          }
          .nextOption()
      }
    }

    /** Resolves the compatibility of a former subtype against its current one, which recursively applies the evolutions
      * declared on that subtype.
      */
    private def isSubtypeIncompatible(
        currentSubtypeSnapshot: TypeSerializerSnapshot[_],
        formerSubtypeSnapshot: TypeSerializerSnapshot[_]
    ): Boolean = currentSubtypeSnapshot
      .asInstanceOf[TypeSerializerSnapshot[Any]]
      .resolveSchemaCompatibility(formerSubtypeSnapshot.asInstanceOf[TypeSerializerSnapshot[Any]])
      .isIncompatible

    override def restoreSerializer(): TypeSerializer[T] = adapter.restoreSerializer()

  }

}
