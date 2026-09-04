package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.evolution.FieldEvolution.FieldIndex

import scala.collection.mutable

/** Immutable bundle of evolutions to apply on an ADT during deserialization.
  *
  * Produced by [[EvolutionBuilder.build]] and stored in [[Evolutions]] at derivation time.
  *
  * Thread-safe to read concurrently.
  *
  * @param currentClass
  *   Current ADT class this evolution applies to
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
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
    private val currentMemberNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val formerToCurrentEnumValueName: Map[String, String] = Map.empty,
    val postDeserialize: (Int, T) => T = (formerVersion: Int, currentAdtInstance: T) => currentAdtInstance
) {

  /** Whether the evolution can be skipped for data written at `formerVersion`. Returns `true` (fast path) when:
    *   - no field evolution is required from given `formerVersion`, and
    *   - the former member names match the current declaration order.
    *
    * @param formerVersion
    *   Former schema version
    * @param formerMemberNames
    *   Former ADT member names, in declaration order
    */
  def isAvoidable(formerVersion: Int, formerMemberNames: Array[String]): Boolean =
    fieldEvolutions.forall(_.since <= formerVersion) && formerMemberNames.sameElements(currentMemberNames)

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
    */
  def toFieldValues(fieldMap: mutable.Map[String, AnyRef]): Array[AnyRef] = currentMemberNames.map(fieldMap)

  /** Check the former schema can be migrated to the current one with the declared evolutions, without reading any data.
    *
    * @param formerVersion
    *   Former schema version
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    * @return
    *   Every failure the deserialization would hit, or the index of every current field, in declaration order
    */
  def dryRun(
      formerVersion: Int,
      formerFieldNames: Array[String]
  ): Either[Seq[FlinkRuntimeException], Array[FieldIndex]] = {
    val fieldIndexes = mutable.Map.from(formerFieldNames.zipWithIndex.map { case (name, i) => name -> Option(i) })
    // The iterator is lazy, so it stops on the first failing evolution
    val failedEvolution = fieldEvolutions.iterator.filter(_.since > formerVersion)
      .flatMap(_.dryRun(fieldIndexes)).nextOption()
    // Check instantiation only when there is no failed evolution
    val failures = failedEvolution.fold(checkInstantiation(fieldIndexes.keySet))(Array(_))
    if (failures.nonEmpty) Left(failures) else Right(currentMemberNames.map(fieldIndexes))
  }

  private def checkInstantiation(fieldNames: collection.Set[String]): Seq[FlinkRuntimeException] =
    currentMemberNames.collect { case n if !fieldNames.contains(n) => MissingFieldException(currentClass, n) } ++
      fieldNames.collect { case n if !currentMemberNames.contains(n) => FieldNotUsedException(currentClass, n) }

}

object Evolution {

  private[evolution] final class DeletedMarker private {}

  /** Marker class returned by [[Evolutions.resolveFormerClass]] for class names registered as deleted. */
  private[evolution] val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Singleton [[Evolution]] for a class marked as deleted. */
  private[evolution] val DeletedClassEvolution: Evolution[DeletedMarker] = new Evolution(DeletedClass) {
    override def isAvoidable(formerVersion: Int, formerMemberNames: Array[String]): Boolean = true
  }

  /** Singleton no-op [[Evolution]] returned by [[Evolutions.get]] when the queried class has no registration */
  private[evolution] val NoEvolution: Evolution[_] = new Evolution(null) {
    override def isAvoidable(formerVersion: Int, formerMemberNames: Array[String]): Boolean = true
  }

}
