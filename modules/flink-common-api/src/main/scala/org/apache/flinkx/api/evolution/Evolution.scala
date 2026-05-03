package org.apache.flinkx.api.evolution

import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.DeletedClass

import scala.collection.mutable

/** Immutable bundle of evolutions to apply on an ADT during deserialization.
  *
  * Produced by [[EvolutionBuilder.build]] and stored in [[Evolutions]] at derivation time.
  *
  * Thread-safe to read concurrently.
  *
  * @param clazz
  *   ADT class this evolution applies to
  * @param fieldNames
  *   Field names currently declared in the case class source code, in declaration order
  * @param fieldEvolutions
  *   Sorted field-level evolutions to apply during deserialization
  * @param postDeserialize
  *   A mapper function taking the `data version` and the `ADT instance` after its deserialization as parameters
  * @tparam T
  *   the type on which the Evolution applies
  */
sealed class Evolution[T] private[evolution] (
    private val clazz: Class[T],
    private val fieldNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    val postDeserialize: (Int, T) => T = (_: Int, adtInstance: T) => adtInstance
) {

  /** Whether the evolution can be skipped for data written at `dataVersion`. Returns `true` (fast path) when:
    *   - no field evolution is required from given `dataVersion`, and
    *   - the field names of the serialized data match the current constructor order.
    *
    * @param dataVersion
    *   Schema version of the serialized data
    * @param previousFieldNames
    *   Field names of the serialized data, in declaration order
    */
  def isAvoidable(dataVersion: Int, previousFieldNames: Array[String]): Boolean =
    !fieldEvolutions.exists(_.since > dataVersion) && previousFieldNames.sameElements(fieldNames)

  /** Apply every field evolution to the given mutable field map starting from the given version.
    *
    * @param dataVersion
    *   Schema version of the serialized data
    * @param fieldMap
    *   Mutable field-name to field-value map to evolve, mutated in place
    */
  def applyFieldEvolutions(dataVersion: Int, fieldMap: mutable.Map[String, AnyRef]): Unit = {
    var i = fieldEvolutions.indexWhere(_.since > dataVersion)
    if (i >= 0) {
      while (i < fieldEvolutions.length) {
        fieldEvolutions(i).apply(fieldMap)
        i += 1
      }
    }
  }

  /** `true` if the ADT class was registered as deleted via `@deletedClasses`, `false` otherwise. */
  def isDeleted: Boolean = clazz == DeletedClass

  /** Convert field map to field values array using registered field names currently declared in the case class source
    * code.
    *
    * @param fieldMap
    *   Field-name to field-value map
    * @return
    *   Array of field values in declaration order
    * @throws FlinkRuntimeException
    *   if the map contains a field unknown to the current source code (forgot `@deletedFields`) or is missing a field
    *   declared in the current source code (forgot `@added`)
    */
  def toFieldValues(fieldMap: mutable.Map[String, AnyRef]): Array[AnyRef] = {
    fieldMap.keys.foreach(n => if (!fieldNames.contains(n)) throwFieldNotUsed(clazz, n))
    fieldNames.map(n => fieldMap.getOrElse(n, throwMissingField(clazz, n)))
  }

  private def throwFieldNotUsed(clazz: Class[_], field: String): Unit = throw new FlinkRuntimeException(
    s"'$field' field not used to instantiate $clazz. Use @deletedFields(since=<version>,\"$field\") annotation to indicate it has been deleted"
  )

  private def throwMissingField(clazz: Class[_], field: String): AnyRef = throw new FlinkRuntimeException(
    s"'$field' field missing to instantiate $clazz. Use @added(since=<version>) annotation to indicate it has been added"
  )

}

object Evolution {

  private[evolution] final class DeletedMarker {
    throw new FlinkRuntimeException("This class is a replacement of a deleted class and should never be instantiated")
  }

  /** Marker class returned by [[Evolutions.resolveFormerClass]] for class names registered as deleted. */
  private[evolution] val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Singleton [[Evolution]] for a class marked as deleted. */
  private[evolution] val DeletedClassEvolution: Evolution[DeletedMarker] = new Evolution(DeletedClass) {
    override def isAvoidable(dataVersion: Int, previousFieldNames: Array[String]): Boolean = true
  }

  /** Singleton no-op [[Evolution]] returned by [[Evolutions.get]] when the queried class has no registration */
  private[evolution] val NoEvolution: Evolution[_] = new Evolution(null) {
    override def isAvoidable(dataVersion: Int, previousFieldNames: Array[String]): Boolean = true
  }

}
