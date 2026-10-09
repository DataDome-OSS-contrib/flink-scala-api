package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flinkx.api.evolution.Evolution
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

  /** The evolution of the ADT, resolved from the class name the snapshot records, if it records one: a snapshot written
    * before 2.4.0 may not.
    */
  private[serializer] def adtEvolution: Option[Evolution[T]]

  /** The current ADT class, for the messages. */
  protected def adtName: String = adtEvolution.fold("the ADT")(_.currentClass.toString)

  /** Schema version of the ADT this snapshot describes, as declared by `@version` at write time. */
  private[serializer] def adtVersion: Int

  /** Whether reading the former data described by `old` requires applying the declared evolutions. */
  protected def isEvolutionRequired(old: S): Boolean

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
    case (Some(current), Some(former)) => current.currentClass.getName != former.currentClass.getName
    case _                             => false
  }

}

private[serializer] object EvolvingSnapshot {

  /** Resolves the compatibility of a former member against its current one, which recursively applies the evolutions
    * declared on the member type.
    */
  def isIncompatible(current: TypeSerializerSnapshot[_], former: TypeSerializerSnapshot[_]): Boolean =
    current
      .asInstanceOf[TypeSerializerSnapshot[Any]]
      .resolveSchemaCompatibility(former.asInstanceOf[TypeSerializerSnapshot[Any]])
      .isIncompatible

}
