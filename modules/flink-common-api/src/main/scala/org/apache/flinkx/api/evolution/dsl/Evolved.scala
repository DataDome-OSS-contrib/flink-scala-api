package org.apache.flinkx.api.evolution.dsl

import org.apache.flink.annotation.Internal

/** Data describing an ADT's schema evolution intent, produced by the DSL [[Evolution.of]] and consumed by
  * `TypeInformationDerivation` to feed the runtime `EvolutionBuilder` / `Evolution`.
  *
  * The user obtains an `Evolved[T]` by chaining DSL calls (typically as a `given` / `implicit val` next to the ADT
  * definition):
  *
  * {{{
  * given Evolved[Click] = Evolution.of[Click]
  *   .version(3)
  *   .added("fieldInFile")
  *   .version(2)
  *   .renamed(formerName = "identifier", currentName = "id")
  *   .deletedFormerFields("b")
  *   .version(1)
  *   .transformed[Int, String]("fieldNotInFile", _.toString)
  *   .deletedFormerFields("a")
  *   .deletedFormerClasses("ClickEvent")
  *   .postDeserialize(updateClick)
  *   .build
  * }}}
  *
  * The DSL also exposes typed-accessor variants (`.added(_.fieldInFile)`, etc.) via Scala 2 / Scala 3 macros that
  * extract the field name from a `_.field` lambda at compile time. The macro-based variants are equivalent to the
  * string-based ones; they trade discoverability and compile-time field-existence validation for slightly higher
  * complexity.
  *
  * @param currentVersion
  *   Current schema version declared by the first `.version(N)` call
  * @param fieldDeltas
  *   Field-level changes (add / rename / transform / delete) accumulated across versions
  * @param renamedFormerClasses
  *   Former binary names of this ADT itself (the case class, sealed trait, or enum has been renamed/moved)
  * @param deletedFormerClasses
  *   `(formerBinaryName, throwOnInstance)` tuples for deleted subtypes (sealed trait / enum) or field-type classes
  *   (case class)
  * @param renamedFormerSubtypes
  *   `(formerSubtypeBinaryName, currentSubtypeClass)` tuples for renamed sealed trait subtypes
  * @param formerToCurrentEnumValueName
  *   Scala 3 enum case renames keyed by former simple name
  * @param postDeserialize
  *   Optional whole-ADT post-deserialize hook
  * @tparam T
  *   ADT type
  */
@Internal
final case class Evolved[T] private[dsl] (
    currentVersion: Int,
    fieldDeltas: Seq[FieldDelta] = Seq.empty,
    renamedFormerClasses: Seq[String] = Seq.empty,
    deletedFormerClasses: Seq[(String, Boolean)] = Seq.empty,
    renamedFormerSubtypes: Seq[(String, Class[_])] = Seq.empty,
    formerToCurrentEnumValueName: Map[String, String] = Map.empty,
    postDeserialize: Option[(Int, T) => T] = None
)

/** A pending field-level evolution recorded by the DSL. Converted to a runtime
  * `org.apache.flinkx.api.evolution.FieldEvolution` at derivation time, when Magnolia supplies the current class and
  * — for `Add` — the parameter's default value.
  */
@Internal
sealed trait FieldDelta {
  def since: Int
}

object FieldDelta {

  /** Field added in version `since`.
    *
    * `default` is populated by the typed-accessor macro (`.added(_.field)`) — it captures the case class's
    * synthetic default-method at compile time, so the absence of a default value becomes a compile error. For the
    * string-based API (`.added("field")`), `default` is `None` and the merger falls back to Magnolia's
    * `Param.default` at derivation time (failing at runtime if the field has no default).
    */
  @Internal
  final case class Add(fieldName: String, override val since: Int, default: Option[Any] = None) extends FieldDelta

  /** Field renamed from `formerName` to `currentName` in version `since`. */
  @Internal
  final case class Rename(formerName: String, currentName: String, override val since: Int) extends FieldDelta

  /** Field type transformed via `mapper` in version `since`. Only the *last* mapper's output type is checked against
    * the current field type; intermediate stages are not (same trust model as `@transformed`).
    */
  @Internal
  final case class Transform[A, B](fieldName: String, mapper: A => B, override val since: Int) extends FieldDelta

  /** Field deleted in version `since`. */
  @Internal
  final case class Delete(formerName: String, override val since: Int) extends FieldDelta
}
