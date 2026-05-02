package org.apache.flinkx.api.evolution

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
  *   Mapper to apply on the ADT after deserialization
  * @tparam T
  *   the type on which the [[Evolution]] applies
  */
final class EvolutionBuilder[T](
    val clazz: Class[T],
    val fieldNames: Array[String] = Array.empty,
    val fieldEvolutions: mutable.ArrayBuffer[FieldEvolution] = mutable.ArrayBuffer.empty,
    var postDeserialize: T => T = Evolution.IdentityFunction.asInstanceOf[T => T]
) {

  /** Build an immutable [[Evolution]] from accumulated registrations. */
  def build(): Evolution[T] =
    new Evolution[T](clazz, fieldNames.clone(), fieldEvolutions.sortInPlace().toArray, postDeserialize)

}
