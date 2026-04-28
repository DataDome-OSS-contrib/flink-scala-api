package org.apache.flinkx

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer

import scala.annotation.StaticAnnotation

package object api {

  /** Marks current schema version of an ADT (case class or sealed trait). Enables ADT evolutions while keeping
    * checkpoint state compatibility.
    *
    * An ADT without `@version` annotation is considered to have version 0. You can enable evolution by adding `version`
    * annotation from a checkpoint without.
    *
    * Annotation of ADT (case class or sealed trait).
    * @param current
    *   Current version number
    */
  final case class version(current: Int) extends StaticAnnotation

  /** Trait marker indicating an evolution annotation. */
  trait Evolved extends StaticAnnotation

  /** Applies transformation to whole instance after deserialization.
    *
    * Annotation of ADT (case class or sealed trait).
    * @param mapper
    *   Function transforming deserialized instance
    */
  final case class postDeserialize[A](mapper: A => A) extends Evolved {
    override def toString: String = s"postDeserialize(<mapper>)"
  }

  /** Marks field added in a specific version. Requires default value.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version when addition occurred
    */
  final case class added(since: Int) extends Evolved

  /** On case class parameter, marks field renamed from previous name.
    *
    * On sealed trait subtype, marks subtype renamed from previous name or moved from another location.
    *
    * Annotation of case class parameter or sealed trait subtype.
    * @param since
    *   Version when rename occurred
    * @param from
    *   Previous field name or subtype name, which can be:
    *   - A simple name: `"OldName"`
    *   - A relative path: `"Parent.OldName"` or `"api.oldPackage.OldName"` or `"api.lowerClass$OldName"`
    *   - An absolute path: `"org.example.OldName"`
    */
  final case class renamed(since: Int, from: String) extends Evolved {
    override def toString: String = s"renamed($since,\"$from\")"
  }

  /** Marks field with type transformation. Mapper function converts old type to new type.
    *
    * Annotation of case class parameter.
    * @param since
    *   Version when transformation occurred
    * @param mapper
    *   Function converting old type A to new type B
    */
  final case class transformed[A, B](since: Int, mapper: A => B) extends Evolved {
    override def toString: String = s"transformed($since,<mapper>)"
  }

  /** Marks deleted fields of a case class not present in current schema.
    *
    * Annotation of case class.
    * @param since
    *   Version when deletions occurred
    * @param names
    *   Names of deleted fields
    */
  final case class deletedFields(since: Int, names: String*) extends Evolved {
    override def toString: String = s"deletedFields($since,${names.mkString("\"", "\",\"", "\"")})"
  }

  /** Marks deleted classes removed in current schema and no longer exist in source code:
    *   - On a sealed trait: subtype classes that have been removed
    *   - On a case class: classes that were referenced by a now-deleted field (declared with `@deletedFields`)
    *
    * Annotation of ADT (case class or sealed trait).
    *
    * @param names
    *   Relative or fully qualified names of deleted classes, which can be:
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
