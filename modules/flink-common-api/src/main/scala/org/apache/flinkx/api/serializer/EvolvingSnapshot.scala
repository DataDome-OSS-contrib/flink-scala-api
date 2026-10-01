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

  /** The evolution of the ADT, resolved from the class name the snapshot records. */
  private[serializer] def evolution: Evolution[T]

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
    case old: S if isSameClass(old) && isEvolutionRequired(old) => resolveEvolvingCompatibility(old)
    case old: S if isSameClass(old)                             => resolveUnevolvedCompatibility(old)
    case _                                                      => TypeSerializerSchemaCompatibility.incompatible()
  }

  private def resolveEvolvingCompatibility(old: S): TypeSerializerSchemaCompatibility[T] = {
    val reason =
      if (old.adtVersion > adtVersion) {
        Some(
          s"the former version ${old.adtVersion} is more recent than the current version $adtVersion." +
            s" Restore from a former savepoint."
        )
      } else checkMigration(old)
    reason.fold(TypeSerializerSchemaCompatibility.compatibleAfterMigration[T]()) { reason =>
      log.warn(s"Cannot migrate ${evolution.currentClass} from version ${old.adtVersion}: $reason")
      TypeSerializerSchemaCompatibility.incompatible()
    }
  }

  /** Whether the former snapshot describes the very same ADT.
    *
    * `old.evolution` has been resolved when the snapshot was read, so a renamed or moved former ADT already carries the
    * current one. A snapshot written before 2.4.0 may record no ADT name at all, leaving nothing to compare.
    */
  private def isSameClass(old: S): Boolean =
    evolution.currentClass == null || old.evolution.currentClass == null ||
      evolution.currentClass.getName == old.evolution.currentClass.getName

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
