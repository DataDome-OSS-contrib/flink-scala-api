package org.apache.flinkx.api.evolution

import scala.collection.mutable

/** Mutable builder used during the ADT derivation phase to register schema evolutions from current source code.
  *
  * Once all registrations are made, [[build]] produces an immutable [[Evolution]] for the deserialization phase.
  *
  * Not thread-safe: builder intended to be used by a single derivation pass.
  *
  * @param clazz
  *   ADT class being derived
  * @param fieldNames
  *   The array of field names currently declared on the case class source code, in declaration order
  * @param fieldEvolutions
  *   Evolution to apply on case class fields
  * @param postDeserialize
  *   Mapper to apply on the ADT at the end of its deserialization
  * @tparam T
  *   the type on which the [[Evolution]] applies
  */
final case class EvolutionBuilder[T](
    clazz: Class[T],
    fieldNames: Array[String] = Array.empty,
    fieldEvolutions: mutable.ArrayBuffer[FieldEvolution] = mutable.ArrayBuffer.empty,
    var postDeserialize: T => T = Evolution.IdentityFunction.asInstanceOf[T => T]
) {

  /** Build the immutable [[Evolution]] from accumulated registrations. */
  def build(): Evolution[T] =
    new Evolution[T](clazz, fieldNames, fieldEvolutions.sortInPlace().toArray, postDeserialize)

}
