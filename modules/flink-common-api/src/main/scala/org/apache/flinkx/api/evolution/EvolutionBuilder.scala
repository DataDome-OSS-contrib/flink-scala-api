package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.postDeserialize

import scala.collection.mutable

/** Mutable builder filled with annotations on current class during the ADT derivation phase.
  *
  * Once all registrations are made, [[build]] produces an immutable [[Evolution]] for the deserialization phase.
  *
  * Not thread-safe: a builder belongs to a single derivation pass.
  *
  * @param currentClass
  *   Current ADT class being derived
  * @param currentFieldNames
 *   Current case class field names, in declaration order; empty for sealed traits
  * @param fieldEvolutions
  *   Field-level evolutions to apply on case class fields; empty for sealed traits
 * @param formerToCurrentEnumValueName
 *   Mapping from former Scala 3 enum value name to current value name; empty for non-enum ADTs
  * @param postDeserialize
 *   A mapper function taking as parameters the former version and the current ADT instance after its deserialization
  * @tparam T
  *   The type on which the [[Evolution]] applies
  */
final class EvolutionBuilder[T](
    val currentClass: Class[T],
    val currentFieldNames: Array[String] = Array.empty,
    val fieldEvolutions: mutable.ArrayBuffer[FieldEvolution] = mutable.ArrayBuffer.empty,
    val formerToCurrentEnumValueName: mutable.Map[String, String] = mutable.Map.empty,
    private var postDeserialize: Option[(Int, T) => T] = None
) {

  def addPostDeserialize(p: postDeserialize[T]): Unit = if (postDeserialize.isEmpty) {
    postDeserialize = Some(p.mapper)
  } else {
    Evolutions.throwEvolutionNotAllowed(p, s"$currentClass twice")
  }

  /** Build an immutable [[Evolution]] from accumulated registrations. */
  def build(): Evolution[T] =
    new Evolution[T](
      currentClass,
      currentFieldNames.clone(),
      fieldEvolutions.sortInPlace().toArray,
      formerToCurrentEnumValueName.toMap,
      postDeserialize.getOrElse((_, i) => i)
    )

}
