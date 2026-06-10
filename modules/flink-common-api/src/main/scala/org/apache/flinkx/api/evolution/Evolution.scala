package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.Unchanged
import org.apache.flinkx.api.evolution.Evolution.{DeletedClass, EnumValueEvolution}
import org.apache.flinkx.api.evolution.FieldEvolution.FieldIndex

import scala.collection.mutable

/** Immutable bundle of evolutions to apply on an ADT during deserialization.
  *
  * Produced by [[EvolutionBuilder.build]] and stored in [[Evolutions]] at derivation time. An [[Evolution]] describes
  * one class name over one version boundary: its `fieldEvolutions` are the ones migrating the data written at
  * `version`, so they are applied without further filtering.
  *
  * Thread-safe to read concurrently.
  *
  * @param className
  *   Name the evolution is registered under: a former ADT class name, or the current one
  * @param version
  *   Former schema version this evolution migrates from, i.e. the version the data was written at
  * @param currentClass
  *   Current ADT class this evolution applies to, or [[Evolution.DeletedClass]] when the ADT was deleted
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
  * @param fieldEvolutions
  *   Sorted field-level evolutions to apply during deserialization
  * @param formerEnumValueToEvolution
  *   Evolutions of the Scala 3 enum values, by former value name
  * @param throwOnInstance
  *   If `true`, encountering an instance of this deleted ADT during deserialization throws
  * @param postDeserialize
  *   A mapper function taking as parameters the former version and the current ADT instance after its deserialization
  * @tparam T
  *   the type on which the Evolution applies
  */
@Internal
sealed class Evolution[T] private[evolution] (
    val className: String,
    val version: Int,
    val currentClass: Class[T],
    private val currentMemberNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val formerEnumValueToEvolution: Map[String, EnumValueEvolution] = Map.empty,
    private val throwOnInstance: Boolean = true,
    val postDeserialize: (Int, T) => T = (formerVersion: Int, currentAdtInstance: T) => currentAdtInstance
) extends Ordered[Evolution[_]]
    with Serializable {

  /** Evolutions of a same class name are sorted by ascending `version`, a deletion coming first among the evolutions
    * declared for the same version.
    *
    * [[Evolutions.get]] returns the first evolution whose version is at least the former version being restored, so
    * this order makes a deleted class win over any other evolution declared for that version.
    */
  override def compare(that: Evolution[_]): Int = {
    val versionComparison = version.compare(that.version)
    if (versionComparison != 0) versionComparison
    else that.isDeleted.compare(isDeleted) // Reversed operands to sort a deletion first
  }

  /** Whether the evolution can be skipped. Returns `true` (fast path) when:
    *   - no field evolution is required from the version this evolution migrates from, and
    *   - the former member names match the current declaration order.
    *
    * @param formerMemberNames
    *   Former ADT member names, in declaration order
    */
  def isAvoidable(formerMemberNames: Array[String]): Boolean =
    isDeleted || (fieldEvolutions.isEmpty && formerMemberNames.sameElements(currentMemberNames))

  /** Apply every field evolution to the given mutable field map.
    *
    * @param fieldMap
    *   Mutable field-name to field-value map to evolve, mutated in place
    */
  def applyFieldEvolutions(fieldMap: mutable.Map[String, AnyRef]): Unit = {
    var i = 0
    while (i < fieldEvolutions.length) {
      fieldEvolutions(i).apply(fieldMap)
      i += 1
    }
  }

  /** `true` if the ADT class was registered as deleted via `@deletedClasses`, `false` otherwise. */
  def isDeleted: Boolean = currentClass == DeletedClass

  /** Throw a [[DeletedInstanceException]] if an instance of this deleted former class was registered with
    * `throwOnInstance = true`; return `null` otherwise.
    */
  def returnNullOrThrow: T = if (throwOnInstance) throw DeletedInstanceException(className) else null.asInstanceOf[T]

  /** Return the evolution declared for the Scala 3 enum value named `formerValueName` when the data was serialized. */
  def getEnumValueEvolution(formerValueName: String): EnumValueEvolution =
    formerEnumValueToEvolution.getOrElse(formerValueName, Unchanged)

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
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    * @return
    *   Every failure the deserialization would hit, or the index of every current field, in declaration order
    */
  def dryRun(formerFieldNames: Array[String]): Either[Seq[FlinkRuntimeException], Array[FieldIndex]] = {
    val fieldIndexes = mutable.Map.from(formerFieldNames.zipWithIndex.map { case (name, i) => name -> Option(i) })
    // The iterator is lazy, so it stops on the first failing evolution
    val failedEvolution = fieldEvolutions.iterator
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

  /** Marker class held by the [[Evolution]] of a class name registered as deleted. */
  private[evolution] val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Singleton no-op [[Evolution]] returned by [[Evolutions.get]] when the queried class has no registration */
  private[evolution] def noEvolution[T](clazz: Class[T], version: Int): Evolution[T] =
    new Evolution(clazz.getName, version, clazz) {
      override def isAvoidable(formerMemberNames: Array[String]): Boolean = true
    }

  /** Singleton no-op [[Evolution]] for a snapshot recording no class name at all, as written before 2.4.0. */
  private[api] val NoEvolution: Evolution[Any] = new Evolution[Any](null, 0, null) {
    override def isAvoidable(formerMemberNames: Array[String]): Boolean = true
  }

  /** Evolution of a single Scala 3 enum value between the former and the current source code.
    *
    * As an enum value is not a class, it can't hold an [[Evolution]] of its own: these evolutions are held by the
    * [[Evolution]] of the enum declaring the value.
    */
  @Internal
  sealed trait EnumValueEvolution

  @Internal
  object EnumValueEvolution {

    /** The former enum value is still declared under the same name. */
    case object Unchanged extends EnumValueEvolution

    /** The former enum value is now declared under `currentName`. Backs the `@renamed` annotation on an enum value. */
    final case class Renamed(currentName: String) extends EnumValueEvolution

    /** The former enum value is no longer declared. Backs the `@deletedClasses(throwOnInstance = true)` annotation. */
    case object DeletedThrowOnInstance extends EnumValueEvolution

    /** The former enum value is no longer declared. Backs the `@deletedClasses(throwOnInstance = false)` annotation. */
    case object DeletedReturnNull extends EnumValueEvolution
  }

}
