package org.apache.flinkx.api.evolution

/** Where each evolution annotation is allowed. The Scala 2 and Scala 3 macros share these rules. */
private[evolution] object AnnotationRules {

  private val OnVersionedAdt = Set("renamed", "deletedClasses", "postEvolution")
  private val OnField        = Set("added", "renamed", "transformed")

  /** Whether the given annotation is allowed on the ADT. At version 0, an ADT has no lower version to migrate from. */
  def allowedOnAdt(annotation: String, version: Int, isCoproduct: Boolean): Boolean =
    version > 0 && (OnVersionedAdt(annotation) || (annotation == "deletedFields" && !isCoproduct))

  /** Whether the given annotation is allowed on a field of the ADT. */
  def allowedOnField(annotation: String, version: Int): Boolean = version > 0 && OnField(annotation)

  /** Whether the given annotation is allowed on an unversioned subtype. Only an enum value accepts one. */
  def allowedOnUnversionedSubtype(annotation: String, version: Int, isEnum: Boolean): Boolean =
    version > 0 && isEnum && annotation == "renamed"

  /** The highest `since` of the given annotation on an ADT, or none for an annotation without `since`. The `since` of a
    * deleted class counts in the versions of this class, not in the versions of the ADT: it has no upper bound.
    */
  def lastSinceOnAdt(annotation: String, version: Int, isEnum: Boolean): Option[Int] = annotation match {
    case "postEvolution"             => None
    case "deletedClasses" if !isEnum => Some(Int.MaxValue)
    case _                           => Some(version)
  }

  /** Whether the given `since` applies to data: an evolution with a `since` out of this range never applies to it. */
  def isSinceAllowed(since: Int, lastVersion: Int): Boolean = since >= 1 && since <= lastVersion

  /** The name of the target of a rejected annotation: the ADT or one of its fields. */
  def target(adt: String, field: Option[String], version: Int): String =
    adt + field.fold("")(f => s".$f") + (if (version == 0) " with version 0" else "")

}
