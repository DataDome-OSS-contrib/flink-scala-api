package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.Evolution.DeletedClass

import scala.collection.mutable

/** Immutable bundle of evolutions to apply on an ADT during deserialization.
  *
  * Produced by [[EvolutionBuilder.build]] and stored in [[Evolutions]] at derivation time.
  *
  * Thread-safe to read concurrently.
  *
  * @param currentClass
  *   Current ADT class this evolution applies to
  * @param currentFieldNames
  *   Current case class field names, in declaration order
  * @param fieldEvolutions
  *   Sorted field-level evolutions to apply during deserialization
  * @param formerToCurrentEnumValueName
  *   Mapping from former Scala 3 enum value name to current value name
  * @param postDeserialize
  *   A mapper function taking as parameters the former version and the current ADT instance after its deserialization
  * @tparam T
  *   the type on which the Evolution applies
  */
@Internal
sealed class Evolution[T] private[evolution] (
    private val currentClass: Class[T],
    private val currentFieldNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val formerToCurrentEnumValueName: Map[String, String] = Map.empty,
    val postDeserialize: (Int, T) => T = (formerVersion: Int, currentAdtInstance: T) => currentAdtInstance
) {

  /** Whether the evolution can be skipped for data written at `formerVersion`. Returns `true` (fast path) when:
    *   - no field evolution is required from given `formerVersion`, and
    *   - the former field names match the current constructor order.
    *
    * @param formerVersion
    *   Former schema version
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    */
  def isAvoidable(formerVersion: Int, formerFieldNames: Array[String]): Boolean =
    fieldEvolutions.forall(_.since <= formerVersion) && formerFieldNames.sameElements(currentFieldNames)

  /** Apply every field evolution to the given mutable field map starting from the given former version.
    *
    * @param formerVersion
    *   Former schema version
    * @param fieldMap
    *   Mutable field-name to field-value map to evolve, mutated in place
    */
  def applyFieldEvolutions(formerVersion: Int, fieldMap: mutable.Map[String, AnyRef]): Unit = {
    var i = fieldEvolutions.indexWhere(_.since > formerVersion)
    if (i >= 0) {
      while (i < fieldEvolutions.length) {
        fieldEvolutions(i).apply(fieldMap)
        i += 1
      }
    }
  }

  /** `true` if the ADT class was registered as deleted via `@deletedClasses`, `false` otherwise. */
  def isDeleted: Boolean = currentClass == DeletedClass

  /** Resolve a former Scala 3 enum value name to its current value name. */
  def resolveFormerEnumValueName(formerName: String): String =
    formerToCurrentEnumValueName.getOrElse(formerName, formerName)

  /** Convert field map to field-values array using current field-names.
    *
    * @param fieldMap
    *   Field-name to field-value map
    * @return
    *   Array of field-values in declaration order
    * @throws FieldNotUsedException
    *   if the map contains a field currently unknown (forgot `@deletedFields`)
    * @throws MissingFieldException
    *   if the map is missing a current field (forgot `@added`)
    */
  def toFieldValues(fieldMap: mutable.Map[String, AnyRef]): Array[AnyRef] = {
    fieldMap.keys.foreach(n => if (!currentFieldNames.contains(n)) throw FieldNotUsedException(currentClass, n))
    currentFieldNames.map(n => fieldMap.getOrElse(n, throw MissingFieldException(currentClass, n)))
  }

}

object Evolution {

  private[evolution] final class DeletedMarker private {}

  /** Marker class returned by [[Evolutions.resolveFormerClass]] for class names registered as deleted. */
  private[evolution] val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Singleton [[Evolution]] for a class marked as deleted. */
  private[evolution] val DeletedClassEvolution: Evolution[DeletedMarker] = new Evolution(DeletedClass) {
    override def isAvoidable(formerVersion: Int, formerFieldNames: Array[String]): Boolean = true
  }

  /** Singleton no-op [[Evolution]] returned by [[Evolutions.get]] when the queried class has no registration */
  private[evolution] val NoEvolution: Evolution[_] = new Evolution(null) {
    override def isAvoidable(formerVersion: Int, formerFieldNames: Array[String]): Boolean = true
  }

}
