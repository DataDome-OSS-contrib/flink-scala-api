package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{Deleted, Renamed}
import org.apache.flinkx.api.evolution.FieldEvolution.EvolvedField

/** Immutable view of evolutions to apply on an ADT during deserialization to migrate its data written at a former
  * version to its current schema.
  *
  * Thread-safe to read concurrently.
  *
  * @param formerClassName
  *   Name the data was written under: a former ADT class name, or the current one
  * @param formerVersion
  *   Schema version the data was written at, which this evolution migrates from
  * @param currentClass
  *   Current ADT class this evolution applies to
  * @param currentFieldNames
  *   Current field names of a case class, in declaration order, none for a sealed trait or an enum whose snapshots
  *   compare their members themselves
  * @param fieldEvolutions
  *   Sorted field-level evolutions migrating the former data
  * @param enumValueEvolutions
  *   Evolutions of the Scala 3 enum values migrating the former data, by former value name
  * @param postEvolution
  *   The mapper applying to the former data, taking as parameters the former version and the current ADT instance
  *   restored from it
  * @tparam T
  *   the type on which the Evolution applies
  */
@Internal
sealed class Evolution[T] private[evolution] (
    val formerClassName: String,
    val formerVersion: Int,
    val currentClass: Class[T],
    private[evolution] val currentFieldNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val enumValueEvolutions: Map[String, EnumValueEvolution] = Map.empty,
    private val postEvolution: Option[PostEvolutionMapper[T]] = None
) extends Serializable {

  /** Whether nothing is declared to migrate the former data: no field, enum value, and no `@postEvolution` mapper. */
  def isEmpty: Boolean = fieldEvolutions.isEmpty && enumValueEvolutions.isEmpty && postEvolution.isEmpty

  /** Whether the former data of a case class reads as is (fast path): nothing migrates it, and the former field names
    * match the current declaration order.
    *
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    */
  def isAvoidable(formerFieldNames: Array[String]): Boolean =
    isEmpty && formerFieldNames.sameElements(currentFieldNames)

  /** Whether the former class is deleted: its data is read as it was written, only to be dropped by [[restore]]. */
  def isDeleted: Boolean = Evolution.isDeletedClass(currentClass)

  /** Restore the current `instance` builds from the former data, mapped by the `@postEvolution` mapper */
  def restore(instance: => T): T = if (postEvolution.isEmpty) instance else postEvolution.get(formerVersion, instance)

  /** The current name of the Scala 3 enum value named `formerValueName` in the former data, none if it is deleted. */
  def currentEnumValueName(formerValueName: String): Option[String] = enumValueEvolutions.get(formerValueName) match {
    case Some(Renamed(currentName)) => Some(currentName)
    case Some(Deleted(_))           => None
    case None                       => Some(formerValueName)
  }

  /** Restore the current Scala 3 enum value of the one named `formerValueName` in the former data, which `valueOf`
    * reads by its current name. A deleted value reads as `null`, or throws a [[DeletedInstanceException]].
    */
  def restoreEnumValue(formerValueName: String)(valueOf: String => T): T =
    enumValueEvolutions.get(formerValueName) match {
      case Some(Renamed(currentName))     => valueOf(currentName)
      case Some(Deleted(throwOnInstance)) =>
        Evolution.deletedInstance(s"${currentClass.getName}#$formerValueName", throwOnInstance)
      case None => valueOf(formerValueName)
    }

  /** Plan how the current fields evolve from the former ones, based on declared field evolutions. Don't read field
    * data.
    *
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    * @return
    *   How current fields evolve (in declaration order), or why the declared evolutions can't migrate the former fields
    */
  def plan(formerFieldNames: Array[String]): Either[Seq[String], Array[EvolvedField]] =
    FieldEvolution.replay(fieldEvolutions, formerFieldNames) match {
      case Left(failure) => Left(Seq(failure))
      case Right(fields) =>
        val failures = checkInstantiation(fields.keySet)
        if (failures.nonEmpty) Left(failures) else Right(currentFieldNames.map(fields))
    }

  private def checkInstantiation(fieldNames: collection.Set[String]): Seq[String] =
    currentFieldNames.toSeq.collect {
      case n if !fieldNames.contains(n) =>
        s"'$n' field missing. Use @added(since=<version>) annotation to indicate it has been added"
    } ++ fieldNames.toSeq.collect {
      case n if !currentFieldNames.contains(n) =>
        s"'$n' former field not used. Use @deletedFields(since=<version>,\"$n\") annotation to indicate its deletion"
    }

}

object Evolution {

  private final class DeletedMarker private {}

  /** Marker class standing for the current class of a former class registered as deleted, which no instance matches. */
  private val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Whether the given current class is the marker of a former class registered as deleted. */
  private[api] def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Throw a [[DeletedInstanceException]] if requesting an instance of this deleted former class registered with
    * `throwOnInstance = true`; return `null` otherwise.
    */
  private def deletedInstance[T](formerName: String, throwOnInstance: Boolean): T =
    if (throwOnInstance) throw DeletedInstanceException(formerName) else null.asInstanceOf[T]

  /** [[Evolution]] of a former class registered as deleted with `@deletedClasses`.
    *
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws, otherwise the instance
    *   is deserialized as `null`
    */
  private[api] final class DeletedEvolution[T](formerClassName: String, formerVersion: Int, throwOnInstance: Boolean)
      extends Evolution[T](formerClassName, formerVersion, DeletedClass.asInstanceOf[Class[T]]) {

    /** A deleted class restore nothing: it reads as `null`, or throws a [[DeletedInstanceException]]. */
    override def restore(instance: => T): T = Evolution.deletedInstance(formerClassName, throwOnInstance)

  }

  /** [[Evolution]] of an ADT declaring none. This evolution is only able to reorder the fields of a case class. */
  private[api] final class NoEvolution[T](currentClass: Class[T], version: Int, fieldNames: Array[String] = Array.empty)
      extends Evolution(currentClass.getName, version, currentClass, fieldNames) {

    override def isAvoidable(formerFieldNames: Array[String]): Boolean =
      // Only a reordering is migrated: unreadable names, an added or a removed field keep the positional resolution
      formerFieldNames.sameElements(currentFieldNames) ||
        !formerFieldNames.sorted.sameElements(currentFieldNames.sorted)

  }

  /** Evolution of a single Scala 3 enum value between the former and the current source code.
    *
    * As an enum value is not a class, it can't hold an [[Evolution]] of its own: these evolutions are held by the
    * [[Evolution]] of the enum declaring the value.
    */
  private[evolution] sealed trait EnumValueEvolution

  private[evolution] object EnumValueEvolution {

    /** The former enum value is now declared under `currentName`. Backs the `@renamed` annotation on an enum value. */
    final case class Renamed(currentName: String) extends EnumValueEvolution

    /** The former enum value is no longer declared. Backs the `@deletedClasses` annotation on an enum.
      *
      * @param throwOnInstance
      *   If `true`, encountering this value during deserialization throws, otherwise it is deserialized as `null`
      */
    final case class Deleted(throwOnInstance: Boolean) extends EnumValueEvolution
  }

}
