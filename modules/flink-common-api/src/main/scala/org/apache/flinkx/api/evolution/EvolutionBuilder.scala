package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{Deleted, Renamed}
import org.apache.flinkx.api.evolution.Evolution.{DeletedEvolution, EnumValueEvolution}
import org.apache.flinkx.api.evolution.EvolutionBuilder.{Deletion, EnumValueChange}
import org.apache.flinkx.api.evolution.Evolutions.Declaration
import org.apache.flinkx.api.evolution.FieldEvolution.{Add, Delete, Rename, Transform}
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
  * @param isEnum
  *   Whether the ADT is a Scala 3 enum, whose deleted classes are values
  * @tparam A
  *   The type on which the [[Evolution]] applies
  */
@Internal
final class EvolutionBuilder[A](
    val currentClass: Class[A],
    val currentVersion: Int,
    currentFieldNames: Array[String] = Array.empty,
    isEnum: Boolean = false
) {

  // Former class names, with the version in which they were renamed to the current class
  private val renames = mutable.Map.empty[String, Int]

  // Former class names deleted from the current source code, a name taken again later being deleted again
  private val deletions = mutable.ArrayBuffer.empty[Deletion]

  private val fieldEvolutions = mutable.ArrayBuffer.empty[FieldEvolution]

  private val enumValueChanges = mutable.ArrayBuffer.empty[EnumValueChange]

  private var postEvolution: Option[PostEvolutionMapper[A]] = None

  /** Declare an evolution annotation of the ADT itself. */
  def declare(annotation: EvolutionAnnotation): Unit = annotation match {
    case renamed(since, formerName) => renameClass(formerName, since)
    case deleted: deletedFields => deleted.formerNames.foreach(name => fieldEvolutions += Delete(deleted.since, name))
    case deleted: deletedClasses if isEnum => deleted.formerClassNames.foreach(deleteEnumValue(_, deleted))
    case deleted: deletedClasses           => deleted.formerClassNames.foreach(deleteClass(_, deleted))
    case post: postEvolution[_]            => postEvolution = Some(post.mapper.asInstanceOf[PostEvolutionMapper[A]])
    case _                                 => notAllowed(annotation, currentClass.getName)
  }

  /** Declare an evolution annotation of the field `name`, at `index` in the constructor of the ADT. */
  def declareField(name: String, index: Int, annotation: EvolutionAnnotation): Unit = annotation match {
    case added(since)               => fieldEvolutions += Add(since, currentClass, name, index)
    case renamed(since, formerName) => fieldEvolutions += Rename(since, formerName, name)
    case transformed(since, mapper) => fieldEvolutions += Transform(since, name, mapper.asInstanceOf[AnyRef => AnyRef])
    case _                          => notAllowed(annotation, s"${currentClass.getName}.$name")
  }

  /** Declare an evolution annotation of the Scala 3 enum value `name`. */
  def declareEnumValue(name: String, annotation: EvolutionAnnotation): Unit = annotation match {
    case renamed(since, formerName) => enumValueChanges += EnumValueChange(formerName, since, Renamed(name))
    case _                          => notAllowed(annotation, s"${currentClass.getName}.$name")
  }

  // The macros reject a misplaced annotation at compile time
  private def notAllowed(annotation: EvolutionAnnotation, target: String): Nothing =
    throw new IllegalArgumentException(evolutionNotAllowed(annotation.toString, target))

  /** The current class was named `formerClassName` (simple, relative or absolute) before version `since`.
    *
    * The former name of an object is the name of its class, ending with `$`: it is added when omitted.
    */
  private def renameClass(formerClassName: String, since: Int): Unit = {
    val formerName = ClassUtil.resolveFormerClassName(formerClassName, currentClass)
    renames(if (isObject && !formerName.endsWith("$")) s"$formerName$$" else formerName) = since
  }

  /** The subtype or field type `formerClassName` (simple, relative or absolute) is deleted by `deleted`.
    *
    * As we can't determine if a deleted class was an object, when the name doesn't end with `$`, registers both names.
    */
  private def deleteClass(formerClassName: String, deleted: deletedClasses): Unit = {
    val formerName = ClassUtil.resolveFormerClassName(formerClassName, currentClass)
    if (!formerName.endsWith("$")) deletions += Deletion(s"$formerName$$", deleted.since, deleted.throwOnInstance)
    deletions += Deletion(formerName, deleted.since, deleted.throwOnInstance)
  }

  // Whether the ADT is a case object, whose former names are names of objects
  private lazy val isObject = ClassUtil.isObjectClass(currentClass)

  /** The Scala 3 enum value `formerName` is deleted by `deleted`. */
  private def deleteEnumValue(formerName: String, deleted: deletedClasses): Unit =
    enumValueChanges += EnumValueChange(formerName, deleted.since, Deleted(deleted.throwOnInstance))

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
      declaration(className, since - 1)(new DeletedEvolution[A](className, _, throwOnInstance))
    }
    current +: (renamed ++ deleted)
  }

  private def declaration(className: String, lastVersion: Int)(evolutionAt: Int => Evolution[A]): Declaration =
    Declaration(className, (0 to lastVersion).map(evolutionAt), currentClass)

  /** The evolution of the data written under `className` at `formerVersion`, holding the evolutions migrating it. */
  private def evolution(formerClassName: String, formerVersion: Int): Evolution[A] =
    new Evolution[A](
      formerClassName = formerClassName,
      formerVersion = formerVersion,
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
