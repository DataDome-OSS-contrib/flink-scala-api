package org.apache.flinkx.api

import org.apache.flink.util.FlinkRuntimeException

import scala.annotation.StaticAnnotation

package object evolution {

  /** Exception indicating version of `currentClass` must be greater or equal to zero. */
  final case class VersionNotAllowedException(currentClass: Class[_], version: Int)
      extends FlinkRuntimeException(s"Current version of $currentClass must be >= 0, got @version($version)")

  /** Message of a misplaced evolution annotation, reported by the macros declaring the evolutions. */
  private[api] def evolutionNotAllowed(annotation: String, target: String): String =
    s"@$annotation annotation is not allowed on $target"

  /** Exception indicating `evolution` annotation is not allowed on `target`. */
  final case class EvolutionNotAllowedException(evolution: StaticAnnotation, target: String)
      extends FlinkRuntimeException(s"@$evolution annotation is not allowed on $target")

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

  /** Exception indicating added `currentField` in `currentClass` must have a default value. */
  final case class AddedFieldWithoutDefaultException(currentClass: Class[_], currentField: String)
      extends FlinkRuntimeException(s"'$currentField' added field in $currentClass must have a default value")

  /** Message of a versioned ADT whose companion doesn't extend `Evolved`, reported where its type information is
    * derived.
    */
  private[api] def companionNotEvolved(adt: String): String = {
    val name = adt.split("[.$]").last
    s"$adt declares @version, so its companion must declare its evolutions: object $name extends Evolved[$name]"
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
            s" Evolved, in a jar of the job; if it was deleted, the ADT that held it must declare it with @deletedClasses"
        else
          s"Cannot derive the type information of '$fqn': it declares @version($version), but its companion" +
            s" declares no evolution. It must extend Evolved"
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
