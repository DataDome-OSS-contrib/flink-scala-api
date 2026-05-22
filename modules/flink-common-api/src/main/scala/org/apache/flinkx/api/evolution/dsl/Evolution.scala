package org.apache.flinkx.api.evolution.dsl

/** Entry point of the typed evolution DSL. Use [[of]] to obtain a builder for a given ADT type.
  *
  * The DSL describes evolutions in *reverse* order — most recent version first. Each `.version(N)` opens a section
  * describing changes that occurred at version `N`. Operations within a section apply only to that version. The first
  * `.version(N)` call declares the current schema version. Subsequent `.version(M)` calls must use strictly decreasing
  * version numbers.
  *
  * The DSL chain has no terminal `.build` call — the final `EvolutionDslAt[T]` is implicitly converted to an
  * [[Evolved]] descriptor by [[EvolutionDslAt.toEvolved]] (Scala 2) or the `given Conversion` in [[EvolutionDslAt]]'s
  * companion (Scala 3). The user assigns it to an `implicit val` / `given` so the derivation can summon it.
  *
  * Example:
  * {{{
  *   case class Click(id: String, fieldInFile: Int = 1, fieldNotInFile: String)
  *   object Click {
  *     // The given is summoned at TypeInformation derivation time.
  *     implicit val evolved: Evolved[Click] = Evolution.of[Click]
  *       .version(3)
  *       .added("fieldInFile")
  *       .version(2)
  *       .renamed(formerName = "identifier", currentName = "id")
  *       .version(1)
  *       .transformed[Int, String]("fieldNotInFile", _.toString)
  *       .deletedFormerFields("a")
  *   }
  * }}}
  */
object Evolution {

  /** Start a new DSL chain for type `T`. */
  def of[T]: EvolutionDsl[T] = new EvolutionDsl[T]()

}

/** Initial DSL stage before the first `.version(N)` call. The only legal next call is `.version(N)`. */
final class EvolutionDsl[T] private[dsl] () {

  /** Declare the current schema version of this ADT and open a section for changes that occurred at version `n`.
    *
    * @param n
    *   Current version, must be `>= 0`. Use `version(0)` for ADTs that haven't evolved yet.
    */
  def version(n: Int): EvolutionDslAt[T] = {
    require(n >= 0, s"Current version must be >= 0, got $n")
    new EvolutionDslAt[T](currentVersion = n, sectionSince = n, evolved = Evolved[T](currentVersion = n))
  }

}

/** DSL stage after at least one `.version(N)` call has been made. Operations apply to the most recent `.version(N)`
  * section.
  *
  * Not thread-safe. Re-using a stage value across branching chains gives undefined results — always use the value
  * returned by the most recent call.
  *
  * @param currentVersion
  *   Current schema version, fixed by the first `.version(...)` call
  * @param sectionSince
  *   The `since` version applied to operations until the next `.version(M)` transition
  * @param evolved
  *   Accumulated evolution data so far
  */
final class EvolutionDslAt[T] private[dsl] (
    private val currentVersion: Int,
    private val sectionSince: Int,
    private[dsl] val evolved: Evolved[T]
) extends EvolutionDslAccessors[T] {

  // -- Version section transition -----------------------------------------------------------------

  /** Close the current `.version(...)` section and open a new section for an older version. The new version must be
    * strictly less than the previous section's version (sections are described from newest to oldest).
    *
    * @param n
    *   Older version number, must be `>= 0` and `< previous section`.
    */
  def version(n: Int): EvolutionDslAt[T] = {
    require(n >= 0, s"Version must be >= 0, got $n")
    require(
      n < sectionSince,
      s"Versions must be described in strictly decreasing order; got .version($n) after .version($sectionSince)"
    )
    new EvolutionDslAt[T](currentVersion, sectionSince = n, evolved)
  }

  // -- Field-level operations (case class) --------------------------------------------------------

  /** Record that `fieldName` was added in this version section. The current field must have a default value, otherwise
    * derivation will throw `AddedFieldWithoutDefaultException`.
    */
  def added(fieldName: String): EvolutionDslAt[T] =
    copyWith(evolved.copy(fieldDeltas = evolved.fieldDeltas :+ FieldDelta.Add(fieldName, sectionSince)))

  /** Record that the field was renamed from `formerName` to `currentName` in this version section. */
  def renamed(formerName: String, currentName: String): EvolutionDslAt[T] =
    copyWith(
      evolved.copy(fieldDeltas =
        evolved.fieldDeltas :+ FieldDelta.Rename(formerName, currentName, sectionSince)
      )
    )

  /** Record that `fieldName`'s value type was transformed in this version section by `mapper`. */
  def transformed[A, B](fieldName: String, mapper: A => B): EvolutionDslAt[T] =
    copyWith(
      evolved.copy(fieldDeltas =
        evolved.fieldDeltas :+ FieldDelta.Transform(fieldName, mapper, sectionSince)
      )
    )

  /** Record that the listed fields were deleted in this version section. */
  def deletedFormerFields(formerNames: String*): EvolutionDslAt[T] =
    copyWith(
      evolved.copy(fieldDeltas =
        evolved.fieldDeltas ++ formerNames.map(FieldDelta.Delete(_, sectionSince))
      )
    )

  // -- Class-level operations (ADT self) ----------------------------------------------------------

  /** Record that this ADT was renamed/moved from `formerClassName` (binary name, see `@renamed` for syntax). The
    * section's version is informational; class-resolution is global. */
  def renamedFromClass(formerClassName: String): EvolutionDslAt[T] =
    copyWith(evolved.copy(renamedFormerClasses = evolved.renamedFormerClasses :+ formerClassName))

  /** Record former subtype or field-type classes that were deleted. Throws on instance by default. */
  def deletedFormerClasses(formerClassNames: String*): EvolutionDslAt[T] =
    deletedFormerClasses(throwOnInstance = true, formerClassNames: _*)

  /** Record former subtype or field-type classes that were deleted, controlling whether instances throw at
    * deserialization time. */
  def deletedFormerClasses(throwOnInstance: Boolean, formerClassNames: String*): EvolutionDslAt[T] =
    copyWith(
      evolved.copy(deletedFormerClasses =
        evolved.deletedFormerClasses ++ formerClassNames.map(_ -> throwOnInstance)
      )
    )

  // -- Sealed trait subtype operations ------------------------------------------------------------

  /** Record that a sealed trait subtype was renamed from `formerName` (binary name). `currentClass` is the runtime
    * `Class[_]` of the current subtype.
    */
  def renamedFormerSubtype(formerName: String, currentClass: Class[_]): EvolutionDslAt[T] =
    copyWith(
      evolved.copy(renamedFormerSubtypes = evolved.renamedFormerSubtypes :+ (formerName -> currentClass))
    )

  // -- Scala 3 enum value operations --------------------------------------------------------------

  /** Record that a Scala 3 enum case was renamed from `formerName` (simple name) to `currentName`. */
  def renamedFormerEnumValue(formerName: String, currentName: String): EvolutionDslAt[T] =
    copyWith(
      evolved.copy(formerToCurrentEnumValueName =
        evolved.formerToCurrentEnumValueName + (formerName -> currentName)
      )
    )

  // -- Whole-ADT post-deserialize hook ------------------------------------------------------------

  /** Attach a post-deserialize hook applied to the whole ADT instance after evolution. At most one is allowed. */
  def postDeserialize(mapper: (Int, T) => T): EvolutionDslAt[T] = {
    require(evolved.postDeserialize.isEmpty, "postDeserialize can be set at most once")
    copyWith(evolved.copy(postDeserialize = Some(mapper)))
  }

  // -- Internal -----------------------------------------------------------------------------------

  private def copyWith(updated: Evolved[T]): EvolutionDslAt[T] =
    new EvolutionDslAt[T](currentVersion, sectionSince, updated)

}

object EvolutionDslAt {

  /** Implicit conversion turning the final DSL chain into the [[Evolved]] descriptor it accumulated. Lets the user
    * write `implicit val ev: Evolved[Click] = Evolution.of[Click].version(2)...` without a terminal `.build`. */
  implicit def evolutionDslAtToEvolved[T](dsl: EvolutionDslAt[T]): Evolved[T] = dsl.evolved

}
