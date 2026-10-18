package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{Deleted, Renamed}
import org.apache.flinkx.api.evolution.Evolution.{DeletedEvolution, EnumValueEvolution}
import org.apache.flinkx.api.evolution.EvolutionBuilder.{Deletion, EnumValueChange}
import org.apache.flinkx.api.evolution.Evolutions.Declaration
import org.apache.flinkx.api.util.ClassUtil

import scala.collection.mutable

/** Mutable builder filled from the annotations of an ADT by the code the macros declaring the evolutions generate.
  *
  * Once all evolutions are added, [[build]] produces the immutable [[Declaration]]s to register for the deserialization
  * phase: the [[Evolution]] of each class name at each version, holding only what migrates the data written at it.
  *
  * Not thread-safe: a builder belongs to a single declaration.
  *
  * @param currentClass
  *   Current ADT class, against which the former class names are resolved
  * @param currentVersion
  *   Current schema version of the ADT
  * @param currentFieldNames
  *   Current field names of a case class, in declaration order, none for a sealed trait or an enum
  * @tparam T
  *   The type on which the [[Evolution]] applies
  */
@Internal
final class EvolutionBuilder[T](
    val currentClass: Class[T],
    val currentVersion: Int,
    currentFieldNames: Array[String] = Array.empty
) {

  // Former class names, with the version in which they were renamed to the current class
  private val renames = mutable.Map.empty[String, Int]

  // Former class names deleted from the current source code, a name taken again later being deleted again
  private val deletions = mutable.ArrayBuffer.empty[Deletion]

  private val fieldEvolutions = mutable.ArrayBuffer.empty[FieldEvolution]

  private val enumValueChanges = mutable.ArrayBuffer.empty[EnumValueChange]

  private var postEvolution: Option[PostEvolutionMapper[T]] = None

  /** The current class was named `formerClassName` (simple, relative or absolute) before version `since`.
    *
    * The former name of an object is the name of its class, ending with `$`: it is added when omitted.
    */
  def renameClass(formerClassName: String, since: Int): Unit = {
    val formerName = ClassUtil.resolveFormerClassName(formerClassName, currentClass)
    renames(if (isObject && !formerName.endsWith("$")) s"$formerName$$" else formerName) = since
  }

  /** The subtype or field type `formerClassName` (simple, relative or absolute) is deleted `since` version.
    *
    * As we can't determine if a deleted class was an object, when the name doesn't end with `$`, registers both names.
    *
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws, otherwise returns `null`
    */
  def deleteClass(formerClassName: String, since: Int, throwOnInstance: Boolean): Unit = {
    val formerName = ClassUtil.resolveFormerClassName(formerClassName, currentClass)
    if (!formerName.endsWith("$")) deletions += Deletion(s"$formerName$$", since, throwOnInstance)
    deletions += Deletion(formerName, since, throwOnInstance)
  }

  // Whether the ADT is a case object, whose former names are names of objects
  private lazy val isObject = ClassUtil.isObjectClass(currentClass)

  def addFieldEvolution(evolution: FieldEvolution): Unit = fieldEvolutions += evolution

  /** The Scala 3 enum value `currentName` was named `formerName` before version `since`. */
  def renameEnumValue(formerName: String, currentName: String, since: Int): Unit =
    enumValueChanges += EnumValueChange(formerName, since, Renamed(currentName))

  /** The Scala 3 enum value `formerName` is deleted `since` version.
    *
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws, otherwise returns `null`
    */
  def deleteEnumValue(formerName: String, since: Int, throwOnInstance: Boolean): Unit =
    enumValueChanges += EnumValueChange(formerName, since, Deleted(throwOnInstance))

  def addPostEvolution(p: postEvolution[T]): Unit = postEvolution = Some(p.mapper)

  /** Build the [[Declaration]]s to register, one per class name the data of the ADT was written under, each holding the
    * evolution of the data written at each version it is declared up to.
    *
    * A class name is declared up to the last version of the data bearing it: the version before the rename or the
    * deletion took effect for a former name, the current version for the current name.
    */
  private[evolution] def build(): Seq[Declaration] = {
    val current = declaration(currentClass.getName, currentVersion)(evolution(currentClass.getName, _))
    // A class renamed in version N bears its former name in the data written up to version N - 1
    val renamed = renames.toSeq.map { case (className, since) =>
      declaration(className, since - 1)(evolution(className, _))
    }
    // A class deleted in version N still exists in the data written up to version N - 1
    val deleted = deletions.toSeq.map { case Deletion(className, since, throwOnInstance) =>
      declaration(className, since - 1)(new DeletedEvolution[T](className, _, throwOnInstance))
    }
    current +: (renamed ++ deleted)
  }

  private def declaration(className: String, lastVersion: Int)(evolutionAt: Int => Evolution[T]): Declaration =
    Declaration(className, (0 to lastVersion).map(evolutionAt), currentClass)

  /** The evolution of the data written under `className` at `formerVersion`, holding the evolutions migrating it. */
  private def evolution(formerClassName: String, formerVersion: Int): Evolution[T] =
    new Evolution[T](
      formerClassName = formerClassName,
      formerVersion = formerVersion,
      currentVersion = currentVersion,
      currentClass = currentClass,
      currentFieldNames = currentFieldNames,
      // An evolution introduced in version N migrates the data written before it
      fieldEvolutions = fieldEvolutions.filter(_.since > formerVersion).sorted.toArray,
      enumValueEvolutions = enumValueChanges
        .filter(_.since > formerVersion)
        .groupBy(_.formerName)
        .map { case (formerName, changes) => formerName -> changes.minBy(_.since).evolution },
      postEvolution = postEvolution.filter(_ => formerVersion < currentVersion)
    )

}

object EvolutionBuilder {

  /** Deletion of the former class `className` in version `since`. Backs the `@deletedClasses` annotation. */
  private final case class Deletion(className: String, since: Int, throwOnInstance: Boolean)

  /** Evolution of the former Scala 3 enum value `formerName` in version `since`. A former name borne again later has
    * several, the first one after the version the data was written at applying to it.
    */
  private final case class EnumValueChange(formerName: String, since: Int, evolution: EnumValueEvolution)

}
