package org.apache.flinkx.api

import org.apache.flink.util.FlinkRuntimeException

import scala.annotation.StaticAnnotation

package object evolution {

  /** Declares the current schema version of an ADT: case class, case object, sealed trait or Scala 3 enum. With the
    * evolution annotations, a job can then restore a savepoint written at a lower version.
    *
    * Two words tell the two sides of a restore apart:
    *   - `former` qualifies an item as the savepoint records it: version, class name, field, subtype or enum value.
    *   - `current` qualifies an item as the source code of the job that restores the savepoint declares it.
    *
    * An ADT without `@version` has the version 0. Thus you can add `@version(1)` to an ADT that has savepoints.
    *
    * Annotation of an ADT, not of a field.
    * @param current
    *   Current schema version, 0 or higher. Increase it at each change of the schema.
    */
  final case class version(current: Int) extends StaticAnnotation

  /** Parent of the evolution annotations, which declare the changes of a schema. `@version` is not one of them. */
  sealed trait EvolutionAnnotation extends StaticAnnotation

  /** Applies a post-evolution mapper to the migrated instance, after the other evolutions. The mapper applies only to
    * data written at a lower version.
    *
    * Use it for a migration that one `@transformed` cannot do: a change of several fields, or the choice of a subtype.
    *
    * Annotation of an ADT.
    * @param mapper
    *   The post-evolution mapper, see [[PostEvolutionMapper.apply]]. It must be visible from the declaration of the
    *   ADT, and serializable.
    */
  final case class postEvolution[A](mapper: PostEvolutionMapper[A]) extends EvolutionAnnotation {
    override def toString: String = s"postEvolution(<mapper>)"
  }

  /** Post-evolution mapper of [[postEvolution]]. */
  @FunctionalInterface
  trait PostEvolutionMapper[A] extends Serializable {

    /** Maps the migrated instance.
      *
      * @param formerVersion
      *   Schema version of the former data, lower than the current version
      * @param instance
      *   Instance that the migration gives. It is `null` for a deleted subtype when `throwOnInstance` is `false`
      * @return
      *   The instance to keep: the given instance or another one
      */
    def apply(formerVersion: Int, instance: A): A
  }

  /** Declares a case class field added in version `since`. The field must have a default value: the migration gives it
    * to the data written at a lower version, and evaluates it again for each instance.
    *
    * If you add a field without this annotation, the compatibility result is incompatible. The log gives the name of
    * the field.
    *
    * Annotation of a case class field.
    * @param since
    *   The first version that contains the field
    */
  final case class added(since: Int) extends EvolutionAnnotation

  /** Declares a rename in version `since`:
    *   - on a case class field or a Scala 3 enum value, its former name;
    *   - on an ADT or a subtype, its former class name. A move to another package or enclosing class is a rename.
    *
    * A former class name is a binary name: a `$` separates a nested class from its enclosing class. A name with a
    * package is absolute. A name that starts with `/` is absolute too: use it for the unnamed package. The library
    * resolves the other names from the enclosing class, or from the package, of the annotated class.
    *
    * Version 0, before the renames:
    * {{{
    * package org.example
    *
    * sealed trait Brood
    *
    * case object Puppy extends Brood
    *
    * object Animal {
    *   object Cat {
    *     case object Kitten extends Brood
    *   }
    * }
    * }}}
    *
    * Version 1, after the renames:
    * {{{
    * package org.example
    *
    * @version(1)
    * @renamed(since = 1, "Brood")
    * sealed trait Animal
    *
    * object Animal extends Evolving[Animal] {
    *   val evolutions = Evolutions[Animal]
    *
    *   @version(1)
    *   @renamed(since = 1, "Cat$Kitten")
    *   case object Cat extends Animal
    *   @version(1)
    *   @renamed(since = 1, "org.example.Puppy")
    *   case object Dog extends Animal
    * }
    * }}}
    *
    * A renamed subtype declares its own `@version`. Its `since` applies to its own versions and to the versions of its
    * sealed trait.
    *
    * A class that reuses a former class name must declare a version equal to or higher than `since`. The submission
    * check verifies it.
    *
    * Annotation of a case class field, an enum value, an ADT or a subtype.
    * @param since
    *   The first version that contains the rename
    * @param formerName
    *   The former name of the field or enum value, or the former class name
    */
  final case class renamed(since: Int, formerName: String) extends EvolutionAnnotation {
    override def toString: String = s"renamed($since,\"$formerName\")"
  }

  /** Declares a case class field whose value, and possibly its type, changes in version `since`.
    *
    * Annotation of a case class field.
    * @param since
    *   The first version that contains the change
    * @param mapper
    *   The field mapper: it converts the former value of the field to its current value. It must be visible from the
    *   declaration of the ADT, and serializable.
    */
  final case class transformed[A, B](since: Int, mapper: A => B) extends EvolutionAnnotation {
    override def toString: String = s"transformed($since,<mapper>)"
  }

  /** Declares case class fields deleted in version `since`. The migration discards their former values.
    *
    * Declare one annotation for each version that deletes fields. If you delete a field without this annotation, the
    * compatibility result is incompatible.
    *
    * Annotation of a case class.
    * @param since
    *   The first version without these fields
    * @param formerNames
    *   Names of the deleted fields in the former schema
    */
  final case class deletedFields(since: Int, formerNames: String*) extends EvolutionAnnotation {
    override def toString: String = s"deletedFields($since,${formerNames.mkString("\"", "\",\"", "\"")})"
  }

  /** Declares classes deleted in version `since`:
    *   - on a sealed trait or a Scala 3 enum, its deleted subtypes or enum values;
    *   - on a case class, the deleted classes that the former snapshots of its deleted fields refer to.
    *
    * An instance of a deleted class in the former data throws a [[DeletedInstanceException]]. With
    * `throwOnInstance = false`, it reads as `null`.
    *
    * Write the former class names as for [[renamed]]. A deleted class no longer tells if it was a case object: a name
    * without the trailing `$` refers to the class and to the case object of this name, a name with it refers to the
    * case object only.
    *
    * Version 0, before the deletions:
    * {{{
    * package org.example
    *
    * sealed trait Animal
    *
    * case object Fish extends Animal
    * case object Ant extends Animal
    *
    * object Animal {
    *   case object Cat extends Animal
    *   case object Dog extends Animal
    *   case object Bird extends Animal
    * }
    * }}}
    *
    * Version 1, after the deletions:
    * {{{
    * package org.example
    *
    * @version(1)
    * @deletedClasses(since = 1, "Fish", "org.example.Ant", "Animal$Bird")
    * sealed trait Animal
    *
    * object Animal extends Evolving[Animal] {
    *   val evolutions = Evolutions[Animal]
    *
    *   case object Cat extends Animal
    *   case object Dog extends Animal
    * }
    * }}}
    *
    * A class that reuses a deleted class name must declare a version equal to or higher than `since`.
    *
    * Annotation of an ADT.
    * @param since
    *   The first version of the deleted classes without them: 1 if they had no `@version`. For an enum value, a version
    *   of its enum
    * @param throwOnInstance
    *   `true` (default) to throw a [[DeletedInstanceException]] on an instance of a deleted class, `false` to read it
    *   as `null`. The migration skips an instance in a deleted field, which never throws
    * @param formerClassNames
    *   The former class names of the deleted classes
    */
  final case class deletedClasses(since: Int, throwOnInstance: Boolean, formerClassNames: String*)
      extends EvolutionAnnotation {
    def this(since: Int, formerClassNames: String*) = this(since, true, formerClassNames: _*)

    override def toString: String = s"deletedClasses($since,${formerClassNames.mkString("\"", "\",\"", "\"")})"
  }

  /** The declared evolutions cannot migrate the data of `currentClass` written at `formerVersion`.
    *
    * @param failures
    *   The reasons: each reason tells which annotation is missing or wrong
    */
  final case class MigrationException(currentClass: Class[_], formerVersion: Int, failures: Seq[String])
      extends FlinkRuntimeException(
        s"Cannot migrate $currentClass from version $formerVersion: ${failures.mkString("\n")}"
      )

  /** No class has the former class name `formerClassName`, and no evolution declares it renamed or deleted at
    * `formerVersion`.
    * @param lastVersion
    *   The last version of the data that the evolutions declare under this class name, if they declare it
    */
  final case class MissingEvolutionException(
      formerClassName: String,
      formerVersion: Int,
      lastVersion: Option[Int],
      cause: Throwable
  ) extends FlinkRuntimeException(
        s"Cannot restore '$formerClassName' written at version $formerVersion: " + lastVersion.fold(
          "no class has this name, and no evolution declares it. Declare @renamed on its current class, or" +
            " @deletedClasses on its ADT"
        )(last =>
          s"the evolutions declare this class name up to version $last. Declare its @renamed or @deletedClasses" +
            s" since a version above $formerVersion"
        ),
        cause
      )

  /** The former class name `formerClassName` refers to two classes in the data of one version. */
  final case class FormerClassConflictException(formerClassName: String, declared: String, conflicting: String)
      extends FlinkRuntimeException(
        s"Former class name '$formerClassName' is $declared, and $conflicting. A class name refers to one class" +
          " in the data of one version. Correct @renamed or @deletedClasses, or increase @version"
      )

  /** The former data contains an instance of the deleted class or enum value `formerName`. */
  final case class DeletedInstanceException(formerName: String)
      extends FlinkRuntimeException(
        s"The former data contains an instance of '$formerName', a deleted class or enum value. Keep this class, or" +
          " declare @deletedClasses(since = <version>, throwOnInstance = false, ...) to read it as null"
      )

  /** Hint for a former subtype or enum value that the current ADT no longer contains. */
  private[api] def renamedOrDeletedHint(formerName: String): String =
    s"Declare @renamed(since = <version>, \"$formerName\") or @deletedClasses(since = <version>, \"$formerName\")"

  /** Message of an evolution annotation at a wrong place, which the macros give at compile time. */
  private[api] def evolutionNotAllowed(annotation: String, target: String): String =
    s"@$annotation annotation is not allowed on $target"

  /** Message of a versioned ADT whose companion does not extend `Evolving`, which the macros give at compile time. */
  private[api] def companionNotEvolving(adt: String): String = {
    val name = adt.split("[.$]").last
    s"$adt declares @version. Its companion must extend Evolving:" +
      s" object $name extends Evolving[$name] { val evolutions = Evolutions[$name] }"
  }

  /** Message of a negative `@version`. */
  private[api] def versionNotAllowed(adt: String, version: Int): String =
    s"The version of $adt must be 0 or higher: @version($version) is not valid"

  /** Message of a version that is not an integer literal, which the macros cannot read. */
  private[api] def notLiteral(annotation: String, parameter: String, target: String): String =
    s"The $parameter of @$annotation on $target must be an integer literal"

  /** Message of an evolution whose `since` is outside the version range of its ADT. */
  private[api] def sinceNotAllowed(adt: String, since: Int, version: Int): String =
    s"An evolution of $adt has since = $since, out of the range 1 to $version. Increase @version or change since"

  /** Message of an `@added` field without a default value. */
  private[api] def addedFieldWithoutDefault(adt: String, field: String): String =
    s"The added field '$field' of $adt must have a default value"

}
