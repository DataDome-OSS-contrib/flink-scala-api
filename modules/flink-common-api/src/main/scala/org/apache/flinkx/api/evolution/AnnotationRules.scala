package org.apache.flinkx.api.evolution

/** Where each evolution annotation is allowed, shared by the Scala 2 and Scala 3 macros declaring the evolutions. */
private[evolution] object AnnotationRules {

  private val OnVersionedAdt = Set("renamed", "deletedClasses", "postEvolution")
  private val OnField        = Set("added", "renamed", "transformed")

  /** Whether the given annotation may describe the ADT itself.
    *
    * @param hasVersionedAncestor
    *   Whether the ADT is a subtype of a versioned sealed trait, which may rename it at version 0
    */
  def allowedOnAdt(annotation: String, version: Int, isCoproduct: Boolean, hasVersionedAncestor: Boolean): Boolean =
    if (version == 0) annotation == "renamed" && hasVersionedAncestor
    else OnVersionedAdt(annotation) || (annotation == "deletedFields" && !isCoproduct)

  /** Whether the given annotation may describe a field of the ADT. */
  def allowedOnField(annotation: String, version: Int): Boolean = version > 0 && OnField(annotation)

  /** Whether the given annotation may describe an unversioned subtype, which declares nothing of its own. */
  def allowedOnUnversionedSubtype(annotation: String, version: Int, isEnum: Boolean): Boolean =
    version > 0 && isEnum && annotation == "renamed"

  /** How a rejected annotation names its target, the ADT or one of its fields. */
  def target(adt: String, field: Option[String], version: Int): String =
    adt + field.fold("")(f => s".$f") + (if (version == 0) " with version 0" else "")

}
