package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializerSchemaCompatibility, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flinkx.api.evolution.{AdtEvolution, Evolutions}
import org.apache.flinkx.api.serializer.ScalaCaseObjectSerializer.ScalaCaseObjectSerializerSnapshot
import org.slf4j.{Logger, LoggerFactory}

/** Snapshot of an ADT serializer, which resolves the schema compatibility with the declared evolutions.
  *
  * @tparam T
  *   The ADT
  * @tparam S
  *   The concrete snapshot class. It is compatible only with a former snapshot of the same class.
  */
private[serializer] trait EvolvingSnapshot[T, S <: EvolvingSnapshot[T, S]] extends TypeSerializerSnapshot[T] {

  @transient private lazy val log: Logger = LoggerFactory.getLogger(getClass)

  /** The ADT evolution of the data that this snapshot describes. None for the snapshot of a sealed trait or an enum
    * written before release 2.4.0, which records no class name.
    */
  private[serializer] def adtEvolution: Option[AdtEvolution[T]]

  /** Names of the members that the serialized form refers to, in their order: the fields of a case class, the subtypes
    * of a sealed trait or the values of an enum.
    */
  private[serializer] def memberNames: Array[String]

  /** Name of the current ADT class, for the messages. */
  protected def adtName: String = adtEvolution.fold("the ADT")(_.currentClass.toString)

  /** Schema version of the data that this snapshot describes: the `@version` of the ADT when the job wrote it. */
  private[serializer] def adtVersion: Int = adtEvolution.fold(0)(_.formerVersion)

  /** Whether the former data that `former` describes needs a migration. This is the case if an evolution applies to
    * this data, or if a member is reordered, added, renamed or deleted. The members are compared by name.
    */
  protected def isEvolutionRequired(former: S): Boolean =
    adtEvolution.isDefined && former.adtEvolution.exists(!_.isEmpty || !former.memberNames.sameElements(memberNames))

  /** Check that the declared evolutions describe the full migration from the schema of `former`.
    *
    * @return
    *   `None` if the migration is possible, otherwise the reason why it is not possible
    */
  protected def checkMigration(former: S): Option[String]

  /** Resolves the compatibility of `former` with the standard resolution, when no migration is necessary. */
  protected def resolveStandardCompatibility(former: S): TypeSerializerSchemaCompatibility[T]

  /** Resolves the compatibility of a former snapshot of the same class: compatible after migration if the declared
    * evolutions migrate its data.
    */
  override def resolveSchemaCompatibility(
      former: TypeSerializerSnapshot[T]
  ): TypeSerializerSchemaCompatibility[T] = former match {
    case _ if former.getClass eq getClass => resolveSameKindCompatibility(former.asInstanceOf[S])
    case _                                => TypeSerializerSchemaCompatibility.incompatible()
  }

  private def resolveSameKindCompatibility(former: S): TypeSerializerSchemaCompatibility[T] =
    if (isOtherClass(former)) TypeSerializerSchemaCompatibility.incompatible()
    else if (former.adtVersion > adtVersion)
      incompatible(former, s"Rollback to version $adtVersion. Restore a savepoint of version $adtVersion or lower")
    else if (isEvolutionRequired(former)) resolveEvolvingCompatibility(former)
    else resolveStandardCompatibility(former)

  private def resolveEvolvingCompatibility(former: S): TypeSerializerSchemaCompatibility[T] =
    checkMigration(former).fold(TypeSerializerSchemaCompatibility.compatibleAfterMigration[T]())(
      incompatible(former, _)
    )

  private def incompatible(former: S, reason: String): TypeSerializerSchemaCompatibility[T] = {
    log.error(s"Cannot migrate $adtName from version ${former.adtVersion}: $reason")
    TypeSerializerSchemaCompatibility.incompatible()
  }

  /** Whether the former snapshot describes another ADT, which is unknown if a snapshot records no class name. */
  private def isOtherClass(former: S): Boolean = (adtEvolution, former.adtEvolution) match {
    case (Some(currentEvolution), Some(formerEvolution)) =>
      !EvolvingSnapshot.isSameClass(currentEvolution, formerEvolution)
    case _ => false
  }

}

private[serializer] object EvolvingSnapshot {

  /** Writes the class name and the schema version of the data that the given ADT evolution migrates.
    *
    * @param hasVersion
    *   `false` to write the format before release 2.4.0, which records no schema version
    */
  def writeEvolution(evolution: AdtEvolution[_], out: DataOutputView, hasVersion: Boolean = true): Unit = {
    out.writeUTF(evolution.formerClassName)
    if (hasVersion) out.writeInt(evolution.formerVersion)
  }

  /** Reads the class name and the schema version that [[writeEvolution]] wrote, and gets from the registry the ADT
    * evolution that migrates this data.
    *
    * @param hasVersion
    *   `false` for a snapshot written before release 2.4.0. It records no schema version: its data is at version 0.
    */
  def readEvolution[T](in: DataInputView, cl: ClassLoader, hasVersion: Boolean = true): AdtEvolution[T] = {
    val className = in.readUTF()
    Evolutions.get(className, if (hasVersion) in.readInt() else 0, cl)
  }

  /** The current class of the data that the given snapshot describes, if it records its class name. */
  def currentClassOf[T](snapshot: TypeSerializerSnapshot[T]): Option[Class[T]] = snapshot match {
    case evolving: EvolvingSnapshot[T, _]                 => evolving.adtEvolution.map(_.currentClass)
    case caseObject: ScalaCaseObjectSerializerSnapshot[T] => Some(caseObject.evolution.currentClass)
    case _                                                => None
  }

  /** Whether two ADT evolutions migrate their data to the same current class, possibly from two class names. */
  def isSameClass(current: AdtEvolution[_], former: AdtEvolution[_]): Boolean =
    current.currentClass.getName == former.currentClass.getName

  /** Whether the former snapshot of a member is incompatible with its current snapshot. This resolution also applies
    * the evolutions that the type of the member declares.
    */
  def isIncompatible(current: TypeSerializerSnapshot[_], former: TypeSerializerSnapshot[_]): Boolean =
    current
      .asInstanceOf[TypeSerializerSnapshot[Any]]
      .resolveSchemaCompatibility(former.asInstanceOf[TypeSerializerSnapshot[Any]])
      .isIncompatible

}
