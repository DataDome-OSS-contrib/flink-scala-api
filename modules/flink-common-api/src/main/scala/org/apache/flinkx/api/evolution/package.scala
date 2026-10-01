package org.apache.flinkx.api

import org.apache.flink.util.FlinkRuntimeException

import scala.annotation.StaticAnnotation

package object evolution {

  /** Declares the current schema version of an ADT, opting it in to the annotation-based schema evolution feature
    * allowing to restore former data read from a savepoint to the current source code.
    *
    * This feature commonly employs the following vocabulary to qualify version, class, field, etc.:
    *   - `Former` describes the serialization time when the checkpoint was done.
    *   - `Current` describes the deserialization time with the current source code.
    *
    * Add `@version(1)` on an ADT afterward is valid: it will be restored from a savepoint as if it was from version 0.
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum), never of a field.
    * @param current
    *   Current schema version, increase it every time the ADT changes. A version below 0 is rejected.
    */
  final case class version(current: Int) extends StaticAnnotation

  /** Marker trait for every evolution annotation describing a change, `@version` aside. */
  sealed trait EvolutionAnnotation extends StaticAnnotation

  /** Applies a mapper to the whole ADT instance restored from a former version, once the other evolutions are applied.
    *
    * Useful for cross-field migrations that don't fit a single `@transformed`, or selecting a different sealed trait
    * subtype based on the input.
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    * @param mapper
    *   The mapper to apply. See [[PostEvolutionMapper.apply]] for parameters. Read where the ADT is declared, so it
    *   must be visible from there.
    */
  final case class postEvolution[A](mapper: PostEvolutionMapper[A]) extends EvolutionAnnotation {
    override def toString: String = s"postEvolution(<mapper>)"
  }

  /** Mapper of [[postEvolution]] applied only after evolutions. See [[PostEvolutionMapper.apply]] for parameters. */
  @FunctionalInterface
  trait PostEvolutionMapper[A] extends Serializable {

    /** Maps the ADT instance restored from a former version.
      *
      * @param formerVersion
      *   Version of the ADT the data was written at, below the current one
      * @param instance
      *   Current ADT instance deserialized from the former data, possibly `null` for a deleted sealed trait subtype
      * @return
      *   The instance to restore, possibly another one
      */
    def apply(formerVersion: Int, instance: A): A
  }

  /** Marks a case class field added in a specific version. The annotated field must have a default value.
    *
    * Adding a field in a case class without this annotation makes the schema compatibility resolution report the schema
    * as incompatible, naming the field missing to instantiate the case class.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version in which the field was added.
    */
  final case class added(since: Int) extends EvolutionAnnotation

  /** On case class parameter, marks field renamed from a former name.
    *
    * On ADT or subtype, marks class renamed from a former class name or moved from another location.
    *
    * The former class name must be in binary name format where nested classes are separated by `$` instead of dots. See
    * "Binary names" section in [[java.lang.ClassLoader]] for more details or JLS 13.1 for the complete definition.
    *
    * The former class name can be relative or absolute path.
    *
    * Any path referencing a package (dot-separated in binary name format) is an absolute path. Start with a `/` to
    * force absolute path (should be useful to reference unnamed package only).
    *
    * Other paths are resolved relatively to the parent of the annotated class (i.e. next to the annotated class). They
    * can contain `$` to reference nested classes.
    *
    * ==Examples==
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
    * object Animal {
    *   @renamed(since = 1, "Cat$Kitten")
    *   case object Cat extends Animal
    *   @renamed(since = 1, "org.example.Puppy")
    *   case object Dog extends Animal
    * }
    * }}}
    *
    * Annotation of case class parameter, ADT or sealed trait subtype.
    * @param since
    *   Version in which the rename occurred
    * @param formerName
    *   The former field or former class binary name
    */
  final case class renamed(since: Int, formerName: String) extends EvolutionAnnotation {
    override def toString: String = s"renamed($since,\"$formerName\")"
  }

  /** Marks a case class field whose type has changed: `mapper` function converts from the former to the current type.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version in which the type-change occurred
    * @param mapper
    *   Function converting the former value to the current type. Read where the ADT is declared, so it must be visible
    *   from there.
    */
  final case class transformed[A, B](since: Int, mapper: A => B) extends EvolutionAnnotation {
    override def toString: String = s"transformed($since,<mapper>)"
  }

  /** Marks fields that used to exist on a case class but no longer appear in current schema.
    *
    * Multiple annotations can coexist on the same class to record deletions made in different versions.
    *
    * Removing a field in a case class without this annotation makes the schema compatibility resolution report the
    * schema as incompatible, naming the former field left unused. Annotation of case class.
    * @param since
    *   Version in which the listed fields were deleted.
    * @param formerNames
    *   Names of the deleted fields, as they appeared in the former schema.
    */
  final case class deletedFields(since: Int, formerNames: String*) extends EvolutionAnnotation {
    override def toString: String = s"deletedFields($since,${formerNames.mkString("\"", "\",\"", "\"")})"
  }

  /** Marks deleted classes removed from current schema:
    *   - On a sealed trait or a Scala 3 enum: subtype classes that have been removed. When encountering an instances of
    *     these subtypes, we either throw an exception (default behavior), or we deserialize the instance as `null` (if
    *     `throwOnInstance` is `false`).
    *   - On a case class: classes that were referenced by a now-deleted field (declared via `@deletedFields`).
    *
    * The former class names must be in binary name format where nested classes are separated by `$` instead of dots.
    * See "Binary names" section in [[java.lang.ClassLoader]] for more details or JLS 13.1 for the complete definition.
    *
    * The former class names can be relative or absolute paths.
    *
    * Any path referencing a package (dot-separated in binary name format) is an absolute path. Start with a `/` to
    * force absolute path (should be useful to reference unnamed package only).
    *
    * Other paths are resolved relatively to the parent of the annotated class (i.e. next to the annotated class). They
    * can contain `$` to reference nested classes.
    *
    * ==Examples==
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
    * object Animal {
    *   case object Cat extends Animal
    *   case object Dog extends Animal
    * }
    * }}}
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    *
    * @param since
    *   Version in which the listed classes were deleted (informative only).
    * @param throwOnInstance
    *   When `true` (default), encountering an instance of a deleted subtype during deserialization throws a
    *   [[org.apache.flinkx.api.evolution.DeletedInstanceException]]. `false` to deserialize as `null`
    * @param formerClassNames
    *   The former class binary names of the deleted classes
    */
  final case class deletedClasses(since: Int, throwOnInstance: Boolean, formerClassNames: String*)
      extends EvolutionAnnotation {
    def this(since: Int, formerClassNames: String*) = this(since, true, formerClassNames: _*)

    override def toString: String = s"deletedClasses($since,${formerClassNames.mkString("\"", "\",\"", "\"")})"
  }

  /** Exception indicating version of `currentClass` must be greater or equal to zero. */
  final case class VersionNotAllowedException(currentClass: Class[_], version: Int)
      extends FlinkRuntimeException(s"Current version of $currentClass must be >= 0, got @version($version)")

  /** How to declare a former member, subtype or enum value, that is no longer one of the current ADT.
    *
    * @param renamedAs
    *   What a rename of that member amounts to, e.g. `renamed or moved`
    */
  private[api] def renamedOrDeletedHint(formerName: String, renamedAs: String): String =
    s"Use @renamed(since = <version>,\"$formerName\") to declare it $renamedAs, or" +
      s" @deletedClasses(since = <version>,\"$formerName\") to declare it deleted"

  /** Message of a misplaced evolution annotation, reported by the macros declaring the evolutions. */
  private[api] def evolutionNotAllowed(annotation: String, target: String): String =
    s"@$annotation annotation is not allowed on $target"

  /** Exception indicating `formerField` is not used to instantiate `currentClass`. */
  final case class FieldNotUsedException(currentClass: Class[_], formerField: String)
      extends FlinkRuntimeException(
        s"'$formerField' field not used to instantiate $currentClass. " +
          s"Use @deletedFields(since=<version>,\"$formerField\") annotation to indicate it has been deleted"
      )

  /** Exception indicating `currentField` is missing to instantiate `currentClass`. */
  final case class MissingFieldException(currentClass: Class[_], currentField: String)
      extends FlinkRuntimeException(
        s"'$currentField' field missing to instantiate $currentClass. " +
          s"Use @added(since=<version>) annotation to indicate it has been added"
      )

  /** Exception indicating `field` is not found in `clazz`. */
  final case class FieldNotFoundException(clazz: Class[_], field: String, operation: String, fields: Iterable[String])
      extends FlinkRuntimeException(
        s"Cannot $operation '$field'. Field not found in $clazz. Available fields: ${fields.mkString("[\"", "\",\"", "\"]")}"
      )

  /** Exception indicating `field` already exists in `clazz`. */
  final case class FieldAlreadyExistException(clazz: Class[_], field: String, fields: Iterable[String])
      extends FlinkRuntimeException(
        s"Cannot add '$field'. Field already exists in $clazz. Existing fields: ${fields.mkString("[\"", "\",\"", "\"]")}"
      )

  /** Exception indicating an evolution of `currentClass` declares a `formerVersion` outside its version range. */
  final case class SinceNotAllowedException(currentClass: Class[_], formerVersion: Int, currentVersion: Int)
      extends FlinkRuntimeException(
        s"An evolution of $currentClass is declared since=$formerVersion: it must be between 1 and the current" +
          s" @version($currentVersion). Raise @version or fix the since of the annotation"
      )

  /** Message of a versioned ADT whose companion doesn't extend `Evolving`, reported where its type information is
    * derived.
    */
  private[api] def companionNotEvolving(adt: String): String = {
    val name = adt.split("[.$]").last
    s"$adt declares @version, so its companion must declare its evolutions:" +
      s" object $name extends Evolving[$name] { val evolutions = Evolutions[$name] }"
  }

  /** Message of an `@added` field without default value, reported by the macros declaring the evolutions. */
  private[api] def addedFieldWithoutDefault(adt: String, field: String): String =
    s"'$field' added field in $adt must have a default value"

  /** Exception indicating the evolutions of `fqn` never reached the JVM needing them. */
  final case class EvolutionNotDeclaredException(fqn: String, version: Int, restoring: Boolean = true)
      extends FlinkRuntimeException(
        if (restoring)
          s"Cannot restore '$fqn', written at @version($version): no class of that name exists, and no evolution" +
            s" declares it renamed or deleted. If it was renamed, the companion of the class now bearing it must extend" +
            s" Evolving, in a jar of the job; if it was deleted, the ADT that held it must declare it with @deletedClasses"
        else
          s"Cannot derive the type information of '$fqn': it declares @version($version), but its companion" +
            s" declares no evolution. It must extend Evolving"
      )

  /** Exception indicating `formerFqn` is declared by two different ADTs, so it can't be resolved unambiguously. */
  final case class FormerClassConflictException(formerFqn: String, declared: String, conflicting: String)
      extends FlinkRuntimeException(
        s"Former class '$formerFqn' is already declared as $declared, it can't also be declared as $conflicting." +
          s" Two ADTs can't share the same former class name: fix their @renamed or @deletedClasses annotations"
      )

  /** Exception indicating an instance of deleted `formerFqn` class has been encountered during deserialization. */
  final case class DeletedInstanceException(formerFqn: String)
      extends FlinkRuntimeException(
        s"Encountered an instance of deleted '$formerFqn' class during deserialization. Don't delete a class in usage" +
          s" or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
      )

}
