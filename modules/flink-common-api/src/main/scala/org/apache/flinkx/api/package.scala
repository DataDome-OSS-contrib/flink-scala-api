package org.apache.flinkx

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer

import scala.annotation.StaticAnnotation

package object api {

  /** Declares the current schema version of an ADT (case class, sealed trait or Scala 3 enum) and opts it in to the
    * annotation-based schema evolution feature allowing to restore former data read from a checkpoint to the current
    * source code.
    *
    * This feature commonly employs the following vocabulary to qualify version, class, field, etc.:
    *   - `Former` describes the serialization time when the checkpoint was done.
    *   - `Current` describes the deserialization time with the current source code.
    *
    * An ADT without this annotation is considered to have version 0 which makes it safe to add `@version(1)` to an
    * existing ADT and restore it from a checkpoint produced by the unversioned code.
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    * @param current
    *   Current schema version, must be >= 0 or [[org.apache.flinkx.api.evolution.VersionNotAllowedException]] is thrown
    */
  final case class version(current: Int) extends StaticAnnotation

  /** Marker trait for every evolution annotation. */
  trait Evolved extends StaticAnnotation

  /** Applies a mapper to the whole current ADT instance after its deserialization. Parameters:
    *   - the former version of the ADT;
    *   - the current ADT instance after its deserialization.
    *   - Return value: a potentially modified ADT instance.
    *
    * Useful for cross-field migrations that don't fit a single `@transformed`, or selecting a different sealed trait
    * subtype based on the input.
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    * @param mapper
    *   A mapper function taking as parameters the former version and the current ADT instance after its deserialization
    */
  final case class postDeserialize[A](mapper: (Int, A) => A) extends Evolved {
    override def toString: String = s"postDeserialize(<mapper>)"
  }

  /** Marks a case class field added in a specific version. The annotated field must have a default value.
    *
    * Adding a field in a case class without this annotation throws a
    * [[org.apache.flinkx.api.evolution.FieldNotUsedException]] during deserialization.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version in which the field was added.
    */
  final case class added(since: Int) extends Evolved

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
  final case class renamed(since: Int, formerName: String) extends Evolved {
    override def toString: String = s"renamed($since,\"$formerName\")"
  }

  /** Marks a case class field whose type has changed: `mapper` function converts from the former to the current type.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version in which the type-change occurred
    * @param mapper
    *   Function converting the former value to the current type
    */
  final case class transformed[A, B](since: Int, mapper: A => B) extends Evolved {
    override def toString: String = s"transformed($since,<mapper>)"
  }

  /** Marks fields that used to exist on a case class but no longer appear in current schema.
    *
    * Multiple annotations can coexist on the same class to record deletions made in different versions.
    *
    * Removing a field in a case class without this annotation throws a
    * [[org.apache.flinkx.api.evolution.MissingFieldException]] during deserialization. Annotation of case class.
    * @param since
    *   Version in which the listed fields were deleted.
    * @param formerNames
    *   Names of the deleted fields, as they appeared in the former schema.
    */
  final case class deletedFields(since: Int, formerNames: String*) extends Evolved {
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
  final case class deletedClasses(since: Int, throwOnInstance: Boolean, formerClassNames: String*) extends Evolved {
    def this(since: Int, formerClassNames: String*) = this(since, true, formerClassNames: _*)

    override def toString: String = s"deletedClasses($since,${formerClassNames.mkString("\"", "\",\"", "\"")})"
  }

  /** Basic type has an arity of 1. See [[BasicTypeInfo#getArity()]] */
  private[api] val BasicTypeArity: Int = 1

  /** Basic type has 1 field. See [[BasicTypeInfo#getTotalFields()]] */
  private[api] val BasicTypeTotalFields: Int = 1

  /** Documentation of [[TypeInformation#getTotalFields()]] states the total number of fields must be at least 1. */
  private[api] val MinimumTotalFields: Int = 1

  /** Documentation of [[TypeSerializer#getLength()]] states data type with variable length must return `-1`. */
  private[api] val VariableLengthDataType: Int = -1

  /** Mark a null value in the stream of serialized data. It is validly used only when these conditions are met:
    *   - Used in both serialize and deserialize methods of the serializer.
    *   - The range of actual data doesn't include [[Int.MinValue]], i.e., the size of a collection can be only >= 0.
    *   - The actual data written in the stream is an Int: the first data to deserialize must be an Int for both null
    *     and non-null cases.
    *
    * If one of these conditions is not met, consider using another marker or wrap your serializer into a
    * [[NullableSerializer]].
    */
  private[api] val NullMarker: Int      = Int.MinValue
  private[api] val NullMarkerByte: Byte = Byte.MinValue

}
