package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{Deleted, Renamed}
import org.apache.flinkx.api.evolution.Evolution.{DeletedEvolution, EnumValueEvolution}
import org.apache.flinkx.api.evolution.EvolutionBuilder.Deletion
import org.apache.flinkx.api.util.ClassUtil

import scala.collection.mutable

/** Mutable builder filled from the annotations of an ADT by the code the macros declaring the evolutions generate.
  *
  * Once all evolutions are added, [[build]] produces the immutable [[Evolution]]s for the deserialization phase.
  *
  * Not thread-safe: a builder belongs to a single declaration.
  *
  * @param currentClass
  *   Current ADT class, against which the former class names are resolved
  * @param currentVersion
  *   Current schema version of the ADT
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
  * @tparam T
  *   The type on which the [[Evolution]] applies
  */
@Internal
final class EvolutionBuilder[T](
    val currentClass: Class[T],
    val currentVersion: Int,
    currentMemberNames: Array[String] = Array.empty
) {

  // Former class names, with the version in which they were renamed to the current class
  private val renames = mutable.Map.empty[String, Int]

  // Former class names deleted from the current source code
  private val deletions = mutable.Map.empty[String, Deletion]

  private val fieldEvolutions = mutable.ArrayBuffer.empty[FieldEvolution]

  // Evolutions of the Scala 3 enum values, by former value name
  private val enumValueEvolutions = mutable.Map.empty[String, EnumValueEvolution]

  private var postEvolution: Option[PostEvolutionMapper[T]] = None

  /** The current class was named `formerClassName` (simple, relative or absolute) before version `since`. */
  def renameClass(formerClassName: String, since: Int): Unit =
    renames(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) = since

  /** The subtype or field type `formerClassName` (simple, relative or absolute) is deleted `since` version.
    *
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws, otherwise the instance
    *   is deserialized as `null`
    */
  def deleteClass(formerClassName: String, since: Int, throwOnInstance: Boolean): Unit =
    deletions(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) = Deletion(since, throwOnInstance)

  def addFieldEvolution(evolution: FieldEvolution): Unit = fieldEvolutions += evolution

  /** The Scala 3 enum value `currentName` was named `formerName` before. */
  def renameEnumValue(formerName: String, currentName: String): Unit =
    enumValueEvolutions(formerName) = Renamed(currentName)

  /** The Scala 3 enum value `formerName` is deleted, see [[deleteClass]] for `throwOnInstance`. */
  def deleteEnumValue(formerName: String, throwOnInstance: Boolean): Unit =
    enumValueEvolutions(formerName) = Deleted(throwOnInstance)

  def addPostEvolution(p: postEvolution[T]): Unit = postEvolution = Some(p.mapper)

  /** Build the [[Evolution]]s to register, per class name.
    *
    * A class name is only valid over a window of versions, which bounds the boundaries it gets:
    *   - a former name is valid up to the version before the rename took effect;
    *   - the current name is valid from the version of the last rename (to avoid collision) up to the current one.
    */
  private[evolution] def build(): Map[String, Array[Evolution[_]]] = {
    val sortedFieldEvolutions = fieldEvolutions.sorted.toArray
    val currentNameSince      = renames.values.maxOption.getOrElse(0)
    val windows               = renames.iterator.map { case (className, since) => (className, 0, since - 1) } ++
      Iterator((currentClass.getName, currentNameSince, currentVersion))
    val renamed = windows.map { case (className, firstVersion, lastVersion) =>
      // A field evolution introduced in version N migrates the data written in version N - 1
      val versions = sortedFieldEvolutions.iterator
        .map(_.since - 1)
        .filter(version => version >= firstVersion && version <= lastVersion)
        .toSeq
        .appended(lastVersion) // The name must still resolve for the last version of its window
        .distinct
        .sorted
      className -> versions.map(evolution(className, _, sortedFieldEvolutions)).toArray
    }
    // A class deleted in version N still exists in the data written in version N - 1
    val deleted = deletions.iterator.map { case (className, Deletion(since, throwOnInstance)) =>
      className -> Array[Evolution[T]](new DeletedEvolution[T](className, since - 1, throwOnInstance))
    }
    (renamed ++ deleted).toSeq.groupMapReduce(_._1)(_._2)(_ ++ _).asInstanceOf[Map[String, Array[Evolution[_]]]]
  }

  private def evolution(
      className: String,
      formerVersion: Int,
      sortedFieldEvolutions: Array[FieldEvolution]
  ): Evolution[T] =
    new Evolution[T](
      className = className,
      formerVersion = formerVersion,
      currentVersion = currentVersion,
      currentClass = currentClass,
      currentMemberNames = currentMemberNames.clone(),
      fieldEvolutions = sortedFieldEvolutions.dropWhile(_.since <= formerVersion),
      formerEnumValueToEvolution = enumValueEvolutions.toMap,
      postEvolution = postEvolution
    )

}

object EvolutionBuilder {

  /** Deletion of a former class in version `since`. Backs the `@deleteClass` annotation. */
  private final case class Deletion(since: Int, throwOnInstance: Boolean)

}
