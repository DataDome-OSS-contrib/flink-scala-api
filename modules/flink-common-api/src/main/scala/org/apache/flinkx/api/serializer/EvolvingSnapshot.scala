package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.{Evolution, Evolutions}
import org.slf4j.{Logger, LoggerFactory}

/** Resolves the schema compatibility of the snapshot of an ADT serializer, applying the declared evolutions.
  *
  * @tparam T
  *   The ADT
  * @tparam S
  *   The concrete snapshot, to compare with a former snapshot of the same kind
  */
private[serializer] trait EvolvingSnapshot[T, S <: EvolvingSnapshot[T, S]] extends TypeSerializerSnapshot[T] {

  @transient private lazy val log: Logger = LoggerFactory.getLogger(getClass)

  /** The evolution of the data this snapshot describes, none for the snapshot of a sealed trait or an enum written
    * before 2.4.0, which records no name.
    */
  private[serializer] def adtEvolution: Option[Evolution[T]]

  /** Names of the members the serialized form refers to, in their order: the fields of a case class, the subtypes of a
    * sealed trait or the values of an enum.
    */
  private[serializer] def memberNames: Array[String]

  /** The current ADT class, for the messages. */
  protected def adtName: String = adtEvolution.fold("the ADT")(_.currentClass.toString)

  /** Schema version of the data this snapshot describes, as declared by `@version` when it was written. */
  private[serializer] def adtVersion: Int = adtEvolution.fold(0)(_.formerVersion)

  /** Whether reading the former data described by `old` requires applying the declared evolutions: an evolution is
    * declared, or a member is reordered, added, renamed or deleted, its members being compared by name.
    */
  protected def isEvolutionRequired(old: S): Boolean =
    adtEvolution.isDefined && old.adtEvolution.exists(!_.isEmpty || !old.memberNames.sameElements(memberNames))

  /** Check the declared evolutions entirely describe the migration from the former schema of `old`.
    *
    * @return
    *   `None` if the migration is possible, the reason it isn't otherwise
    */
  protected def checkMigration(old: S): Option[String]

  /** Resolves the compatibility of `old` when no evolution applies, with the standard resolution. */
  protected def resolveUnevolvedCompatibility(old: S): TypeSerializerSchemaCompatibility[T]

  /** Resolves the compatibility of a former snapshot of the same kind, migrating it when evolutions are required. */
  override def resolveSchemaCompatibility(
      oldSnapshot: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = oldSnapshot match {
    case old if old.getClass eq getClass => resolveSameKindCompatibility(old.asInstanceOf[S])
    case _                               => TypeSerializerSchemaCompatibility.incompatible()
  }

  private def resolveSameKindCompatibility(old: S): TypeSerializerSchemaCompatibility[T] =
    if (isOtherClass(old)) TypeSerializerSchemaCompatibility.incompatible()
    else if (isEvolutionRequired(old)) resolveEvolvingCompatibility(old)
    else resolveUnevolvedCompatibility(old)

  private def resolveEvolvingCompatibility(old: S): TypeSerializerSchemaCompatibility[T] = {
    val reason =
      if (old.adtVersion > adtVersion) {
        Some(
          s"the former version ${old.adtVersion} is more recent than the current version $adtVersion." +
            s" Restore from a former savepoint."
        )
      } else checkMigration(old)
    reason.fold(TypeSerializerSchemaCompatibility.compatibleAfterMigration[T]()) { reason =>
      log.warn(s"Cannot migrate $adtName from version ${old.adtVersion}: $reason")
      TypeSerializerSchemaCompatibility.incompatible()
    }
  }

  /** Whether the former snapshot is known to describe another ADT. */
  private def isOtherClass(old: S): Boolean = (adtEvolution, old.adtEvolution) match {
    case (Some(current), Some(former)) => !EvolvingSnapshot.isSameClass(current, former)
    case _                             => false
  }

}

private[serializer] object EvolvingSnapshot {

  /** Writes the name and the version of the data the given evolution migrates, which [[readEvolution]] reads back. */
  def writeEvolution(evolution: Evolution[_], out: DataOutputView): Unit = {
    out.writeUTF(evolution.formerClassName)
    out.writeInt(evolution.formerVersion)
  }

  /** Reads the name and the version of the data a snapshot describes, and resolves the evolution migrating it.
    *
    * @param hasVersion
    *   `false` for a snapshot written before 2.4.0, which records no version: its data is at version 0
    */
  def readEvolution[T](in: DataInputView, cl: ClassLoader, hasVersion: Boolean = true): Evolution[T] = {
    val className = in.readUTF()
    Evolutions.get(className, if (hasVersion) in.readInt() else 0, cl)
  }

  /** Whether two evolutions migrate their data to the same current class, possibly from different former names. */
  def isSameClass(current: Evolution[_], former: Evolution[_]): Boolean =
    current.currentClass.getName == former.currentClass.getName

  /** Resolves the compatibility of a former member against its current one, which recursively applies the evolutions
    * declared on the member type.
    */
  def isIncompatible(current: TypeSerializerSnapshot[_], former: TypeSerializerSnapshot[_]): Boolean =
    current
      .asInstanceOf[TypeSerializerSnapshot[Any]]
      .resolveSchemaCompatibility(former.asInstanceOf[TypeSerializerSnapshot[Any]])
      .isIncompatible

}
