package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.AdtEvolution.EnumValueEvolution
import org.apache.flinkx.api.evolution.AdtEvolution.EnumValueEvolution.{Deleted, Renamed}
import org.apache.flinkx.api.evolution.FieldEvolution.MigratedField

/** ADT evolution: all the evolutions that the migration applies to the data written under one former class name, at one
  * former version.
  *
  * Immutable and thread-safe.
  *
  * @param formerClassName
  *   Class name of the former data: a former class name of the ADT, or its current class name
  * @param formerVersion
  *   Schema version of the former data
  * @param currentClass
  *   Current class of the ADT, or the deletion marker for a deleted class
  * @param currentFieldNames
  *   Current field names of a case class, in declaration order. Empty for a sealed trait or an enum: their snapshots
  *   compare their members.
  * @param fieldEvolutions
  *   Field evolutions with a `since` higher than the former version, in the order of application
  * @param enumValueEvolutions
  *   Enum value evolutions that apply to the former data, by former enum value name
  * @param postEvolution
  *   Post-evolution mapper, if the former version is lower than the current version
  * @tparam T
  *   The ADT
  */
@Internal
sealed class AdtEvolution[T] private[evolution] (
    val formerClassName: String,
    val formerVersion: Int,
    val currentClass: Class[T],
    private[evolution] val currentFieldNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val enumValueEvolutions: Map[String, EnumValueEvolution] = Map.empty,
    private val postEvolution: Option[PostEvolutionMapper[T]] = None
) extends Serializable {

  /** Whether no evolution and no post-evolution mapper apply to the former data. */
  def isEmpty: Boolean = fieldEvolutions.isEmpty && enumValueEvolutions.isEmpty && postEvolution.isEmpty

  /** Whether the former data of a case class reads as is, without migration: nothing applies to it and its fields have
    * the current order, or it records no field name and its fields are read by position.
    *
    * @param formerFieldNames
    *   Former field names of the case class, in declaration order. None before release 2.4.0
    */
  def isAsIs(formerFieldNames: Option[Array[String]]): Boolean =
    formerFieldNames.forall(names => isEmpty && names.sameElements(currentFieldNames))

  /** Plan the migration of the former fields of a case class. This method reads no data.
    *
    * @param formerFieldNames
    *   Former field names of the case class, in declaration order
    * @return
    *   The migration plan, in the current declaration order, or why the evolutions cannot migrate the former fields
    */
  def plan(formerFieldNames: Array[String]): Either[Seq[String], Array[MigratedField]] =
    FieldEvolution.plan(fieldEvolutions, formerFieldNames) match {
      case Left(failure) => Left(Seq(failure))
      case Right(fields) =>
        val failures = checkInstantiation(fields.keySet)
        if (failures.nonEmpty) Left(failures) else Right(currentFieldNames.map(fields))
    }

  /** Migrate the `instance` built from the former data with the post-evolution mapper, if one applies. A deleted
    * instance gives `null`, or throw a [[DeletedInstanceException]].
    */
  def migrate(instance: => T): T = if (postEvolution.isEmpty) instance else postEvolution.get(formerVersion, instance)

  /** The current name of the enum value named `formerValueName` in the former data, or none if it is deleted. */
  def currentEnumValueName(formerValueName: String): Option[String] = enumValueEvolutions.get(formerValueName) match {
    case Some(Renamed(_, _, currentName)) => Some(currentName)
    case Some(_: Deleted)                 => None
    case None                             => Some(formerValueName)
  }

  /** Migrate the enum value named `formerValueName` in the former data to its current value, which `valueOf` reads by
    * its current name. A deleted value gives `null`, or throws a [[DeletedInstanceException]].
    */
  def migrateEnumValue(formerValueName: String)(valueOf: String => T): T =
    enumValueEvolutions.get(formerValueName) match {
      case Some(Renamed(_, _, currentName))     => valueOf(currentName)
      case Some(Deleted(_, _, throwOnInstance)) =>
        AdtEvolution.deletedInstance(s"${currentClass.getName}#$formerValueName", throwOnInstance)
      case None => valueOf(formerValueName)
    }

  private def checkInstantiation(fieldNames: collection.Set[String]): Seq[String] =
    currentFieldNames.toSeq.collect {
      case n if !fieldNames.contains(n) =>
        s"Field '$n' is not in the former data. Declare @added(since = <version>) or @renamed on it"
    } ++ fieldNames.toSeq.collect {
      case n if !currentFieldNames.contains(n) =>
        s"Former field '$n' is not in the current schema. Declare @deletedFields(since = <version>, \"$n\") or @renamed"
    }

}

object AdtEvolution {

  private final class DeletedMarker private {}

  /** Deletion marker: the current class of a deleted class name. No instance has this class. */
  private val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Whether the given current class is the deletion marker. */
  private[api] def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Throw a [[DeletedInstanceException]] for the deleted class or enum value `formerName` if `throwOnInstance` is
    * `true`. Else, return `null`.
    */
  private def deletedInstance[T](formerName: String, throwOnInstance: Boolean): T =
    if (throwOnInstance) throw DeletedInstanceException(formerName) else null.asInstanceOf[T]

  /** ADT evolution of a class that `@deletedClasses` declares deleted.
    *
    * @param throwOnInstance
    *   `true` to throw a [[DeletedInstanceException]] for a deleted instance, `false` to give `null`
    */
  private[api] final class DeletedEvolution[T](formerClassName: String, formerVersion: Int, throwOnInstance: Boolean)
      extends AdtEvolution[T](formerClassName, formerVersion, DeletedClass.asInstanceOf[Class[T]]) {

    /** The deletion applies to the former data. */
    override def isEmpty: Boolean = false

    /** The former data never reads as is: [[migrate]] discards it. */
    override def isAsIs(formerFieldNames: Option[Array[String]]): Boolean = false

    /** Migrate a deleted instance to `null`, or throw a [[DeletedInstanceException]]. */
    override def migrate(instance: => T): T = AdtEvolution.deletedInstance(formerClassName, throwOnInstance)

  }

  /** ADT evolution of an ADT that declares no evolution. It only migrates a case class whose fields are reordered. */
  private[api] final class NoEvolution[T](currentClass: Class[T], version: Int, fieldNames: Array[String] = Array.empty)
      extends AdtEvolution(currentClass.getName, version, currentClass, fieldNames) {

    override def isAsIs(formerFieldNames: Option[Array[String]]): Boolean =
      // Only reordered fields migrate: an added or a deleted field keeps the standard resolution
      formerFieldNames.forall(names =>
        names.sameElements(currentFieldNames) || !names.sorted.sameElements(currentFieldNames.sorted)
      )

  }

  /** Evolution of one enum value. An enum value has no ADT evolution of its own: the ADT evolution of its enum holds
    * its evolutions.
    */
  private[evolution] sealed trait EnumValueEvolution {

    /** The first schema version that contains the evolution. */
    def since: Int

    /** Former name of the enum value. */
    def formerName: String

  }

  private[evolution] object EnumValueEvolution {

    /** The enum value `formerName` has the name `currentName` in the current source code. Declared by `@renamed`. */
    final case class Renamed(since: Int, formerName: String, currentName: String) extends EnumValueEvolution

    /** The current source code no longer contains the enum value `formerName`. Declared by `@deletedClasses`.
      *
      * @param throwOnInstance
      *   `true` to throw a [[DeletedInstanceException]] for a deleted instance, `false` to give `null`
      */
    final case class Deleted(since: Int, formerName: String, throwOnInstance: Boolean) extends EnumValueEvolution
  }

}
