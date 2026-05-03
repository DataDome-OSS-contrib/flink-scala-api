package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.postDeserialize

import scala.collection.mutable

/** Mutable builder filled during the ADT derivation phase from the annotations on the current source code.
  *
  * Once all registrations are made, [[build]] produces an immutable [[Evolution]] for the deserialization phase.
  *
  * Not thread-safe: a builder belongs to a single derivation pass.
  *
  * @param clazz
  *   ADT class being derived
  * @param fieldNames
  *   Field names currently declared on the case class source, in declaration order; empty for sealed traits
  * @param fieldEvolutions
  *   Field-level evolutions to apply on case class fields; empty for sealed traits
  * @param postDeserialize
 *   A mapper function taking the `data version` and the `ADT instance` after its deserialization as parameters
  * @tparam T
  *   The type on which the [[Evolution]] applies
  */
final class EvolutionBuilder[T](
    val clazz: Class[T],
    val fieldNames: Array[String] = Array.empty,
    val fieldEvolutions: mutable.ArrayBuffer[FieldEvolution] = mutable.ArrayBuffer.empty,
    private var postDeserialize: Option[(Int, T) => T] = None
) {

  def addPostDeserialize(p: postDeserialize[T]): Unit = if (postDeserialize.isEmpty) {
    postDeserialize = Some(p.mapper)
  } else {
    Evolutions.throwEvolutionNotAllowed(p, s"$clazz twice")
  }

  /** Build an immutable [[Evolution]] from accumulated registrations. */
  def build(): Evolution[T] =
    new Evolution[T](
      clazz,
      fieldNames.clone(),
      fieldEvolutions.sortInPlace().toArray,
      postDeserialize.getOrElse((_, i) => i)
    )

}
