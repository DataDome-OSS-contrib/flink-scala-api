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
import org.apache.flinkx.api.serializer.CoproductSerializer.CoproductSerializerSnapshot
import org.slf4j.{Logger, LoggerFactory}

class CoproductSerializer[T](
    val clazz: Class[T],
    val version: Int,
    val subtypeClasses: Array[Class[_]],
    val subtypeFqns: Array[String],
    val subtypeSerializers: Array[TypeSerializer[_]]
) extends MutableSerializer[T] {

  require(
    // The serialized form holds the index of the subtype, so the three arrays describe the same subtypes in one order
    subtypeClasses.length == subtypeSerializers.length && subtypeFqns.length == subtypeSerializers.length,
    s"$clazz has ${subtypeClasses.length} subtype classes and ${subtypeFqns.length} subtype names for" +
      s" ${subtypeSerializers.length} subtype serializers"
  )

  override val isImmutableType: Boolean = subtypeSerializers.forall(_.isImmutableType)
  val isImmutableSerializer: Boolean    = subtypeSerializers.forall(s => s.duplicate().eq(s))

  // Cache to lookup Evolution on first record only
  @transient private lazy val evolution = Evolutions.get(clazz)

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
      new CoproductSerializer[T](clazz, version, subtypeClasses, subtypeFqns, subtypeSerializers.map(_.duplicate()))
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
      evolution.postDeserialize.apply(version, Evolutions.checkThrowOnInstance(instance, subtypeFqns(index)))
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

    private var clazz: Class[T]                 = _
    private var coproductVersion: Int           = 0 // version of the coproduct class (through @version)
    private var subtypeClasses: Array[Class[_]] = Array.empty
    private var subtypeFqns: Array[String]      = Array.empty

    // An adapter is mandatory to keep the compatibility during the transition to a CompositeTypeSerializerSnapshot
    // because its readSnapshot() method is final
    private val adapter: CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] =
      new CompositeTypeSerializerSnapshot[T, CoproductSerializer[T]] {

        serializer.foreach { s =>
          // Scala limitation: can't call parent constructor used for writing the snapshot, reproduce its behavior instead
          setNestedSerializersSnapshots(this, getNestedSerializers(s).map(_.snapshotConfiguration()): _*)
          clazz = s.clazz
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
          new CoproductSerializer[T](clazz, coproductVersion, subtypeClasses, subtypeFqns, nestedSerializers)

        override def writeOuterSnapshot(out: DataOutputView): Unit = {
          out.writeUTF(clazz.getName)
          out.writeInt(coproductVersion)
          StringArraySerializer.INSTANCE.serialize(subtypeFqns, out)
        }

        override def readOuterSnapshot(readOuterSnapshotVersion: Int, in: DataInputView, cl: ClassLoader): Unit = {
          clazz = if (readOuterSnapshotVersion > 3) Evolutions.resolveFormerClass(in.readUTF(), cl) else null
          coproductVersion = if (readOuterSnapshotVersion > 3) in.readInt() else 0
          subtypeFqns = StringArraySerializer.INSTANCE.deserialize(in)
          subtypeClasses = subtypeFqns.map(Evolutions.resolveFormerClass(_, cl))
        }

      }

    override def getCurrentVersion: Int = adapter.getCurrentVersion

    override def writeSnapshot(out: DataOutputView): Unit = adapter.writeSnapshot(out)

    override def readSnapshot(readVersion: Int, in: DataInputView, userCodeClassLoader: ClassLoader): Unit =
      if (readVersion == 2) {
        val len = in.readInt()

        clazz = null
        coproductVersion = 0

        subtypeFqns = (0 until len).map(_ => in.readUTF()).toArray
        subtypeClasses = subtypeFqns.map(Evolutions.resolveFormerClass(_, userCodeClassLoader))

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
            log.warn(s"Cannot migrate $clazz from version ${old.coproductVersion}: $reason")
            TypeSerializerSchemaCompatibility.incompatible()
        }
      case old: CoproductSerializerSnapshot[T] if isSameClass(old) =>
        // No evolution: delegates schema compatibility to standard resolution
        adapter.resolveSchemaCompatibility(old.adapter)
      case _ => TypeSerializerSchemaCompatibility.incompatible()
    }

    /** Whether the former snapshot describes the very same sealed trait.
      *
      * `old.clazz` has been resolved by `readOuterSnapshot`, so a renamed or moved former trait already reads as the
      * current one. A snapshot written before 2.4.0 records no trait name at all, leaving nothing to compare.
      */
    private def isSameClass(old: CoproductSerializerSnapshot[T]): Boolean =
      clazz == null || old.clazz == null || clazz.getName == old.clazz.getName

    /** Whether reading the former form described by `old` requires applying the declared evolutions. */
    private def isEvolutionRequired(old: CoproductSerializerSnapshot[T]): Boolean =
      clazz != null && !Evolutions.get(clazz).isAvoidable(old.coproductVersion, old.subtypeFqns)

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
          .filterNot(i => Evolutions.isDeletedFormerClass(old.subtypeFqns(i)))
          .flatMap { i =>
            val formerFqn    = old.subtypeFqns(i)
            val currentIndex = subtypeClasses.indexOf(old.subtypeClasses(i))
            if (currentIndex < 0) {
              Some(
                s"former subtype '$formerFqn' is no longer a member of $clazz. Use @renamed(since = <version>," +
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
