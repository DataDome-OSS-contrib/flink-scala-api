package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
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
  * @param currentVersion
  *   Current schema version of the ADT, the upper bound of the `since` of its evolutions
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
  * @param fieldEvolutions
  *   Field-level evolutions to apply on case class fields; empty for sealed traits
  * @param formerToCurrentEnumValueName
  *   Mapping from former Scala 3 enum value name to current value name; empty for non-enum ADTs
  * @param postDeserialize
  *   A mapper function taking as parameters the former version and the current ADT instance after its deserialization
  * @tparam T
  *   The type on which the [[Evolution]] applies
  */
@Internal
final class EvolutionBuilder[T](
    val currentClass: Class[T],
    val currentVersion: Int,
    val currentMemberNames: Array[String] = Array.empty,
    val fieldEvolutions: mutable.ArrayBuffer[FieldEvolution] = mutable.ArrayBuffer.empty,
    val formerToCurrentEnumValueName: mutable.Map[String, String] = mutable.Map.empty,
    private var postDeserialize: Option[(Int, T) => T] = None
) {

  def addPostDeserialize(p: postDeserialize[T]): Unit = if (postDeserialize.isEmpty) {
    postDeserialize = Some(p.mapper)
  } else {
    throw EvolutionNotAllowedException(p, s"$currentClass twice")
  }

  /** Build an immutable [[Evolution]] from accumulated registrations.
    *
    * @throws SinceNotAllowedException
    *   if an evolution declares a `since` outside the version range of the ADT
    */
  def build(): Evolution[T] = {
    fieldEvolutions
      .find(e => e.since < 1 || e.since > currentVersion)
      // An evolution outside the version range is never applied when it should
      .foreach(e => throw SinceNotAllowedException(currentClass, e.since, currentVersion))
    new Evolution[T](
      currentClass,
      currentMemberNames.clone(),
      fieldEvolutions.sortInPlace().toArray,
      formerToCurrentEnumValueName.toMap,
      postDeserialize.getOrElse((_, i) => i)
    )
  }

}
