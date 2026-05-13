package org.apache.flinkx.api

import org.apache.flink.util.FlinkRuntimeException

import scala.annotation.StaticAnnotation

package object evolution {

  /** Exception indicating version of `currentClass` must be greater or equal to zero. */
  final case class VersionNotAllowedException(currentClass: Class[_], version: Int)
      extends FlinkRuntimeException(s"Current version of $currentClass must be >= 0, got @version($version)")

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

  /** Exception indicating added `currentField` in `currentClass` must have a default value. */
  final case class AddedFieldWithoutDefaultException(currentClass: Class[_], currentField: String)
      extends FlinkRuntimeException(s"'$currentField' added field in $currentClass must have a default value")

  /** Exception indicating an instance of deleted `formerFqn` class has been encountered during deserialization. */
  final case class DeletedInstanceException(formerFqn: String)
      extends FlinkRuntimeException(
        s"Encountered an instance of deleted '$formerFqn' class during deserialization. Don't delete a class in usage" +
          s" or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
      )

}
