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

  /** Whether the given annotation on an ADT has a `since`. */
  def hasSinceOnAdt(annotation: String): Boolean = annotation != "postEvolution"

  /** The highest `since` of the given annotation on an ADT, or none without upper bound. The `since` of a deleted class
    * counts in the versions of this class, not in the versions of the ADT.
    */
  def lastSinceOnAdt(annotation: String, version: Int, isEnum: Boolean): Option[Int] =
    if (annotation == "deletedClasses" && !isEnum) None else Some(version)

  /** Whether the given `since` applies to data: an evolution with a `since` out of this range never applies to it. */
  def isSinceAllowed(since: Int, lastVersion: Option[Int]): Boolean = since >= 1 && lastVersion.forall(since <= _)

  /** The name of the target of a rejected annotation: the ADT or one of its fields. */
  def target(adt: String, field: Option[String], version: Int): String =
    adt + field.fold("")(f => s".$f") + (if (version == 0) " with version 0" else "")

}
