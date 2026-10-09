package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.Unchanged
import org.apache.flinkx.api.evolution.FieldEvolution.FieldIndex
import org.apache.flinkx.api.serializer.ConstructorCompat

import scala.collection.mutable

/** Immutable bundle of evolutions to apply on an ADT during deserialization.
  *
  * Produced by [[EvolutionBuilder.build]] and stored in [[Evolutions]] at derivation time. An [[Evolution]] describes
  * one class name over one version boundary: its `fieldEvolutions` are the ones migrating the data written at
  * `formerVersion`, so they are applied without further filtering.
  *
  * Thread-safe to read concurrently.
  *
  * @param className
  *   Name the evolution is registered under: a former ADT class name, or the current one
  * @param formerVersion
  *   Former schema version this evolution migrates from, i.e. the version the data was written at
  * @param currentVersion
  *   Current schema version of the ADT, i.e. the data is migrated to this version
  * @param currentClass
  *   Current ADT class this evolution applies to
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
  * @param fieldEvolutions
  *   Sorted field-level evolutions to apply during deserialization
  * @param formerEnumValueToEvolution
  *   Evolutions of the Scala 3 enum values, by former value name
  * @param postEvolution
  *   A mapper function taking as parameters the former version and the current ADT instance restored from it
  * @tparam T
  *   the type on which the Evolution applies
  */
@Internal
sealed class Evolution[T] private[evolution] (
    val className: String,
    val formerVersion: Int,
    val currentVersion: Int,
    val currentClass: Class[T],
    private[evolution] val currentMemberNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val formerEnumValueToEvolution: Map[String, EnumValueEvolution] = Map.empty,
    private val postEvolution: Option[PostEvolutionMapper[T]] = None
) extends Serializable {

  /** Whether the evolution can be skipped. Returns `true` (fast path) when:
    *   - no field evolution is required from the version this evolution migrates from,
    *   - no `@postEvolution` mapper applies to the former version, and
    *   - the former member names match the current declaration order.
    *
    * @param writtenVersion
    *   Former schema version of the ADT, the one the data was written at
    * @param formerMemberNames
    *   Former ADT member names, in declaration order
    */
  def isAvoidable(writtenVersion: Int, formerMemberNames: Array[String]): Boolean =
    fieldEvolutions.isEmpty && !isPostEvolved(writtenVersion) && formerMemberNames.sameElements(currentMemberNames)

  /** Apply the `@postEvolution` mapper to an instance read from data at given former written version. */
  def postEvolve(writtenVersion: Int, instance: T): T =
    if (isPostEvolved(writtenVersion)) postEvolution.get(writtenVersion, instance) else instance

  private def isPostEvolved(writtenVersion: Int): Boolean = postEvolution.isDefined && writtenVersion < currentVersion

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
      .flatMap(_.dryRun(fieldIndexes))
      .nextOption()
    // Check instantiation only when there is no failed evolution
    val failures = failedEvolution.fold(checkInstantiation(fieldIndexes.keySet))(Seq(_))
    if (failures.nonEmpty) Left(failures) else Right(currentMemberNames.map(fieldIndexes))
  }

  private def checkInstantiation(fieldNames: collection.Set[String]): Seq[FlinkRuntimeException] =
    currentMemberNames.collect { case n if !fieldNames.contains(n) => MissingFieldException(currentClass, n) } ++
      fieldNames.collect { case n if !currentMemberNames.contains(n) => FieldNotUsedException(currentClass, n) }

}

object Evolution {

  private final class DeletedMarker private {}

  /** Marker class standing for the current class of a former class registered as deleted, which no instance matches. */
  private val DeletedClass: Class[DeletedMarker] = classOf[DeletedMarker]

  /** Whether the given current class is the marker of a former class registered as deleted. */
  private[api] def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Throw a [[DeletedInstanceException]] if requesting an instance of this deleted former class registered with
    * `throwOnInstance = true`; return `null` otherwise.
    */
  private[api] def deletedInstance[T](formerName: String, throwOnInstance: Boolean): T =
    if (throwOnInstance) throw DeletedInstanceException(formerName) else null.asInstanceOf[T]

  /** [[Evolution]] of a former class registered as deleted with `@deletedClasses`.
    *
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws, otherwise the instance
    *   is deserialized as `null`
    */
  final class DeletedEvolution[T] private[evolution] (className: String, formerVersion: Int, throwOnInstance: Boolean)
      extends Evolution[T](className, formerVersion, formerVersion + 1, DeletedClass.asInstanceOf[Class[T]]) {

    // The former data is read as it was written, only to be dropped
    override def isAvoidable(writtenVersion: Int, formerMemberNames: Array[String]): Boolean = true

    /** What an instance of this deleted former class reads as: see [[Evolution.deletedInstance]]. */
    def deletedInstance: T = Evolution.deletedInstance(className, throwOnInstance)

  }

  /** [[Evolution]] returned by [[Evolutions.get]] when the queried class has no registration.
    *
    * This evolution is only able to reorder members of its ADT.
    */
  final class NoEvolution[T] private[evolution] (clazz: Class[T], version: Int)
      extends Evolution(clazz.getName, version, version, clazz, ConstructorCompat.lookupFieldNames(clazz)) {
    override def isAvoidable(writtenVersion: Int, formerMemberNames: Array[String]): Boolean =
      // Only a reordering is migrated: sealed traits, enums and unreadable names keep the positional resolution
      formerMemberNames.sameElements(currentMemberNames) ||
        !formerMemberNames.sorted.sameElements(currentMemberNames.sorted)
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

    /** The former enum value is no longer declared. Backs the `@deletedClasses` annotation on an enum.
      *
      * @param throwOnInstance
      *   If `true`, encountering this value during deserialization throws, otherwise it is deserialized as `null`
      */
    final case class Deleted(throwOnInstance: Boolean) extends EnumValueEvolution
  }

}
