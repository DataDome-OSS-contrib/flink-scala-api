package org.apache.flinkx.api.evolution

/** Where each evolution annotation is allowed, shared by the Scala 2 and Scala 3 macros declaring the evolutions. */
private[evolution] object AnnotationRules {

  private val OnVersionedAdt = Set("renamed", "deletedClasses", "postEvolution")
  private val OnField        = Set("added", "renamed", "transformed")

  /** Whether the given annotation may describe the ADT itself: a version 0 declares nothing to evolve from. */
  def allowedOnAdt(annotation: String, version: Int, isCoproduct: Boolean): Boolean =
    version > 0 && (OnVersionedAdt(annotation) || (annotation == "deletedFields" && !isCoproduct))

  /** Whether the given annotation may describe a field of the ADT. */
  def allowedOnField(annotation: String, version: Int): Boolean = version > 0 && OnField(annotation)

  /** Whether the given annotation may describe an unversioned subtype, which declares nothing of its own. */
  def allowedOnUnversionedSubtype(annotation: String, version: Int, isEnum: Boolean): Boolean =
    version > 0 && isEnum && annotation == "renamed"

  /** How a rejected annotation names its target, the ADT or one of its fields. */
  def target(adt: String, field: Option[String], version: Int): String =
    adt + field.fold("")(f => s".$f") + (if (version == 0) " with version 0" else "")

}
