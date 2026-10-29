package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.AdtEvolution.EnumValueEvolution.{Deleted, Renamed}
import org.apache.flinkx.api.evolution.AdtEvolution.{DeletedEvolution, EnumValueEvolution}
import org.apache.flinkx.api.evolution.EvolutionBuilder.Deletion
import org.apache.flinkx.api.evolution.Evolutions.Declaration
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
import org.apache.flinkx.api.util.ClassUtil.{isObjectClass, resolveFormerClassName}

import scala.collection.mutable

/** Builder of the declarations of an ADT. The code that the macros generate fills it from the annotations of the ADT.
  *
  * [[build]] gives the [[Declaration]]s to register: for each class name, the ADT evolution of each version. An ADT
  * evolution holds only the evolutions that apply to the data written at its version.
  *
  * Not thread-safe: one builder builds the declarations of one ADT.
  *
  * @param currentClass
  *   Current class of the ADT, from which the relative class names resolve
  * @param currentVersion
  *   Current schema version of the ADT
  * @param currentFieldNames
  *   Current field names of a case class, in declaration order. Empty for a sealed trait or an enum
  * @param isEnum
  *   Whether the ADT is a Scala 3 enum, whose deleted classes are enum values
  * @tparam A
  *   The ADT
  */
@Internal
final class EvolutionBuilder[A](
    val currentClass: Class[A],
    val currentVersion: Int,
    currentFieldNames: Array[String] = Array.empty,
    isEnum: Boolean = false
) {

  /** Former class names of the current class, with the `since` of their rename */
  private val renames = mutable.Map.empty[String, Int]

  /** Deleted class names, with one deletion for each `since`: a reused name can be deleted again */
  private val deletions = mutable.ArrayBuffer.empty[Deletion]

  private val fieldEvolutions = mutable.ArrayBuffer.empty[FieldEvolution]

  /** A reused enum value name has several evolutions: the first one with a `since` above the former version applies */
  private val enumValueEvolutions = mutable.ArrayBuffer.empty[EnumValueEvolution]

  private var postEvolution: Option[PostEvolutionMapper[A]] = None

  /** Add an evolution annotation of the ADT. */
  def declare(annotation: EvolutionAnnotation): Unit = annotation match {
    case renamed(since, formerName) => renameClass(formerName, since)
    case deleted: deletedFields => deleted.formerNames.foreach(name => fieldEvolutions += Delete(deleted.since, name))
    case deleted: deletedClasses if isEnum => deleted.formerClassNames.foreach(deleteEnumValue(_, deleted))
    case deleted: deletedClasses           => deleted.formerClassNames.foreach(deleteClass(_, deleted))
    case post: postEvolution[_]            => postEvolution = Some(post.mapper.asInstanceOf[PostEvolutionMapper[A]])
    case _                                 => notAllowed(annotation, currentClass.getName)
  }

  /** Add an evolution annotation of the field `name`, at `index` in the constructor of the ADT. */
  def declareField(name: String, index: Int, annotation: EvolutionAnnotation): Unit = annotation match {
    case added(since)               => fieldEvolutions += Add(since, currentClass, name, index)
    case renamed(since, formerName) => fieldEvolutions += Rename(since, formerName, name)
    case transformed(since, mapper) => fieldEvolutions += Transform(since, name, mapper.asInstanceOf[AnyRef => AnyRef])
    case _                          => notAllowed(annotation, s"${currentClass.getName}.$name")
  }

  /** Add an evolution annotation of the enum value `name`. */
  def declareEnumValue(name: String, annotation: EvolutionAnnotation): Unit = annotation match {
    case renamed(since, formerName) => enumValueEvolutions += Renamed(since, formerName, name)
    case _                          => notAllowed(annotation, s"${currentClass.getName}.$name")
  }

  /** Mirrors what the macros already rejected for a misplaced annotation at compile time. */
  private def notAllowed(annotation: EvolutionAnnotation, target: String): Nothing =
    throw new IllegalArgumentException(evolutionNotAllowed(annotation.toString, target))

  /** Rename: the data written at a version lower than `since` has the class name `formerClassName`, relative or
    * absolute. The class name of an object ends with `$`: this method adds it if necessary.
    */
  private def renameClass(formerClassName: String, since: Int): Unit = {
    val formerName = resolveFormerClassName(formerClassName, currentClass)
    renames(if (isObjectClass(currentClass) && !formerName.endsWith("$")) s"$formerName$$" else formerName) = since
  }

  /** Delete: the current source code no longer contains the subtype or field type `formerClassName`, relative or
    * absolute. A deleted class name does not tell if the class was an object: without a final `$`, this method adds
    * both names.
    */
  private def deleteClass(formerClassName: String, deleted: deletedClasses): Unit = {
    val formerName = resolveFormerClassName(formerClassName, currentClass)
    if (!formerName.endsWith("$")) deletions += Deletion(s"$formerName$$", deleted.since, deleted.throwOnInstance)
    deletions += Deletion(formerName, deleted.since, deleted.throwOnInstance)
  }

  /** Delete: the current source code no longer contains the enum value `formerName`. */
  private def deleteEnumValue(formerName: String, deleted: deletedClasses): Unit =
    enumValueEvolutions += Deleted(deleted.since, formerName, deleted.throwOnInstance)

  /** Build the [[Declaration]]s to register: one for each class name of the data of the ADT, with the ADT evolution of
    * each version up to its last version.
    *
    * The last version of the current class name is the current version. The last version of a former class name is the
    * `since` of its rename or deletion, minus 1.
    */
  private[evolution] def build(): Seq[Declaration] = {
    val current = declaration(currentClass.getName, currentVersion)(evolution(currentClass.getName, _))
    // The data written at a version lower than the `since` of a rename has the former class name
    val renamed = renames.toSeq.map { case (className, since) =>
      declaration(className, since - 1)(evolution(className, _))
    }
    // The data written at a version lower than the `since` of a deletion still contains the deleted class
    val deleted = deletions.toSeq.map { case Deletion(className, since, throwOnInstance) =>
      declaration(className, since - 1)(new DeletedEvolution[A](className, _, throwOnInstance))
    }
    current +: (renamed ++ deleted)
  }

  /** The declaration of evolutions for one `className`. */
  private def declaration(className: String, lastVersion: Int)(evolutionAt: Int => AdtEvolution[A]): Declaration =
    Declaration(className, lastVersion, evolutionAt, currentClass)

  /** The ADT evolution of the data written under `formerClassName` at `formerVersion`. */
  private def evolution(formerClassName: String, formerVersion: Int): AdtEvolution[A] =
    new AdtEvolution[A](
      formerClassName = formerClassName,
      formerVersion = formerVersion,
      currentClass = currentClass,
      currentFieldNames = currentFieldNames,
      // An evolution applies only to the data written at a version lower than its `since`
      fieldEvolutions = fieldEvolutions.filter(_.since > formerVersion).sorted.toArray,
      enumValueEvolutions = enumValueEvolutions
        .filter(_.since > formerVersion)
        .groupBy(_.formerName)
        .map { case (formerName, evolutions) => formerName -> evolutions.minBy(_.since) },
      postEvolution = postEvolution.filter(_ => formerVersion < currentVersion)
    )

}

object EvolutionBuilder {

  /** Evolution that deletes the class `className` in version `since`. Declared by `@deletedClasses`. */
  private final case class Deletion(className: String, since: Int, throwOnInstance: Boolean)

}
