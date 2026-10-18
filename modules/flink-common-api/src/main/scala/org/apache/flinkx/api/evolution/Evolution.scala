package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.Unchanged
import org.apache.flinkx.api.evolution.FieldEvolution.{EvolvedField, FieldIndex}

/** Immutable view of evolutions to apply on an ADT during deserialization to migrate its data written at a former
  * version to its current schema.
  *
  * Thread-safe to read concurrently.
  *
  * @param formerClassName
  *   Name the data was written under: a former ADT class name, or the current one
  * @param formerVersion
  *   Schema version the data was written at, which this evolution migrates from
  * @param currentVersion
  *   Current schema version of the ADT, i.e. the data is migrated to this version
  * @param currentClass
  *   Current ADT class this evolution applies to
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
  * @param fieldEvolutions
  *   Sorted field-level evolutions migrating the former data
  * @param enumValueEvolutions
  *   Evolutions of the Scala 3 enum values migrating the former data, by former value name
  * @param postEvolution
  *   The mapper applying to the former data, taking as parameters the former version and the current ADT instance
  *   restored from it
  * @tparam T
  *   the type on which the Evolution applies
  */
@Internal
sealed class Evolution[T] private[evolution] (
    val formerClassName: String,
    val formerVersion: Int,
    val currentVersion: Int,
    val currentClass: Class[T],
    private[evolution] val currentMemberNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val enumValueEvolutions: Map[String, EnumValueEvolution] = Map.empty,
    private val postEvolution: Option[PostEvolutionMapper[T]] = None
) extends Serializable {

  /** Whether the evolution can be skipped. Returns `true` (fast path) when:
    *   - no field evolution migrates the former data,
    *   - no `@postEvolution` mapper applies to it, and
    *   - the former member names match the current declaration order.
    *
    * @param formerMemberNames
    *   Former ADT member names, in declaration order
    */
  def isAvoidable(formerMemberNames: Array[String]): Boolean =
    fieldEvolutions.isEmpty && postEvolution.isEmpty && formerMemberNames.sameElements(currentMemberNames)

  /** Apply the `@postEvolution` mapper to an instance read from the former data. */
  def postEvolve(instance: T): T = if (postEvolution.isEmpty) instance else postEvolution.get(formerVersion, instance)

  /** Return the evolution of the Scala 3 enum value named `formerValueName` in the former data. */
  def getEnumValueEvolution(formerValueName: String): EnumValueEvolution =
    enumValueEvolutions.getOrElse(formerValueName, Unchanged)

  /** Plan how the current fields evolve from the former ones, based on declared field evolutions. Don't read field
    * data.
    *
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    * @return
    *   Failures the deserialization would hit, or how current fields evolve, in declaration order
    */
  def plan(formerFieldNames: Array[String]): Either[Seq[FlinkRuntimeException], Array[EvolvedField]] =
    FieldEvolution.replay(fieldEvolutions, formerFieldNames) match {
      case Left(failure) => Left(Seq(failure))
      case Right(fields) =>
        val failures = checkInstantiation(fields.keySet)
        if (failures.nonEmpty) Left(failures) else Right(currentMemberNames.map(fields))
    }

  /** Check if the former schema can be migrated to the current one with the declared evolutions. Don't read field data.
    *
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    * @return
    *   Failures the deserialization would hit, or the index of current fields, in declaration order
    */
  def dryRun(formerFieldNames: Array[String]): Either[Seq[FlinkRuntimeException], Array[FieldIndex]] =
    plan(formerFieldNames).map(_.map(_.formerIndex))

  private def checkInstantiation(fieldNames: collection.Set[String]): Seq[FlinkRuntimeException] =
    (currentMemberNames.collect { case n if !fieldNames.contains(n) => MissingFieldException(currentClass, n) } ++
      fieldNames.collect { case n if !currentMemberNames.contains(n) => FieldNotUsedException(currentClass, n) }).toSeq

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
  final class DeletedEvolution[T] private[api] (formerClassName: String, formerVersion: Int, throwOnInstance: Boolean)
      extends Evolution[T](formerClassName, formerVersion, formerVersion + 1, DeletedClass.asInstanceOf[Class[T]]) {

    // The former data is read as it was written, only to be dropped
    override def isAvoidable(formerMemberNames: Array[String]): Boolean = true

    /** What an instance of this deleted former class reads as: see [[Evolution.deletedInstance]]. */
    def deletedInstance: T = Evolution.deletedInstance(formerClassName, throwOnInstance)

  }

  /** [[Evolution]] of an ADT declaring none. This evolution is only able to reorder its members. */
  final class NoEvolution[T] private[api] (currentClass: Class[T], currentVersion: Int, memberNames: Array[String])
      extends Evolution(currentClass.getName, currentVersion, currentVersion, currentClass, memberNames) {

    override def isAvoidable(formerMemberNames: Array[String]): Boolean =
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
