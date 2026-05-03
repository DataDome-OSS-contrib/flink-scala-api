package org.apache.flinkx

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer

import scala.annotation.StaticAnnotation

package object api {

  /** Declares the current schema version of an ADT (case class, sealed trait or Scala 3 enum) and opts it in to the
    * annotation-based schema evolution feature.
    *
    * An ADT without this annotation is considered to have version 0 which makes it safe to add `@version(1)` to an
    * existing ADT and restore it from a checkpoint produced by the unversioned code.
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    * @param current
    *   Current schema version, must be strictly positive
    */
  final case class version(current: Int) extends StaticAnnotation

  /** Marker trait for every evolution annotation. */
  trait Evolved extends StaticAnnotation

  /** Applies a mapper to the whole ADT instance after its deserialization. Parameters:
    *   - the version of the deserialized ADT;
    *   - the ADT instance after its deserialization.
    *   - Return value: a potentially modified ADT instance.
    *
    * Useful for cross-field migrations that don't fit a single `@transformed`, or selecting a different sealed trait
    * subtype based on the input.
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    * @param mapper
    *   A mapper function taking the `data version` and the `ADT instance` after its deserialization as parameters
    */
  final case class postDeserialize[A](mapper: (Int, A) => A) extends Evolved {
    override def toString: String = s"postDeserialize(<mapper>)"
  }

  /** Marks a case class field added in a specific version. The annotated field must have a default value.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version in which the field was added.
    */
  final case class added(since: Int) extends Evolved

  /** On case class parameter, marks field renamed from previous name.
    *
    * On ADT or subtype, marks class renamed from previous name or moved from another location.
    *
    * Annotation of case class parameter, ADT or sealed trait subtype.
    * @param since
    *   Version in which the rename occurred
    * @param from
    *   Previous field or class name. Class names can be:
    *   - A simple name: `"OldName"`
    *   - A relative path: `"Parent.OldName"` or `"api.oldPackage.OldName"` or `"api.lowerClass$OldName"`
    *   - An absolute path: `"org.example.OldName"`
    */
  final case class renamed(since: Int, from: String) extends Evolved {
    override def toString: String = s"renamed($since,\"$from\")"
  }

  /** Marks a case class field whose type has changed: `mapper` function converts from the old type to the current type.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version in which the type-change occurred
    * @param mapper
    *   Function converting the previously serialized value to the current type
    */
  final case class transformed[A, B](since: Int, mapper: A => B) extends Evolved {
    override def toString: String = s"transformed($since,<mapper>)"
  }

  /** Marks fields that used to exist on a case class but no longer appear in current schema.
    *
    * Multiple annotations can coexist on the same class to record deletions made in different versions.
    *
    * Annotation of case class.
    * @param since
    *   Version in which the listed fields were deleted.
    * @param names
    *   Names of the deleted fields, as they appeared in the previous schema.
    */
  final case class deletedFields(since: Int, names: String*) extends Evolved {
    override def toString: String = s"deletedFields($since,${names.mkString("\"", "\",\"", "\"")})"
  }

  /** Marks deleted classes removed from current schema and no longer exist in source code:
    *   - On a sealed trait or a Scala 3 enum: subtype classes that have been removed. Records of these subtypes are
    *     deserialized as `null`.
    *   - On a case class: classes that were referenced by a now-deleted field (declared via `@deletedFields`).
    *
    * Annotation of ADT (case class, sealed trait or Scala 3 enum).
    *
    * @param names
    *   Names of the deleted classes, which can be:
    *   - A simple name: `"OldName"`
    *   - A relative path: `"Parent.OldName"` or `"api.oldPackage.OldName"` or `"api.lowerClass$OldName"`
    *   - An absolute path: `"org.example.OldName"`
    */
  final case class deletedClasses(names: String*) extends Evolved {
    override def toString: String = s"deletedClasses(${names.mkString("\"", "\",\"", "\"")})"
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
