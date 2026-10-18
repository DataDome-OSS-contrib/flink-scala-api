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
  * @param currentFieldNames
  *   Current field names of a case class, in declaration order, none for a sealed trait or an enum whose snapshots
  *   compare their members themselves
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
    private[evolution] val currentFieldNames: Array[String] = Array.empty,
    private val fieldEvolutions: Array[FieldEvolution] = Array.empty,
    private val enumValueEvolutions: Map[String, EnumValueEvolution] = Map.empty,
    private val postEvolution: Option[PostEvolutionMapper[T]] = None
) extends Serializable {

  /** Whether nothing is declared to migrate the former data: no field, enum value, and no `@postEvolution` mapper. */
  def isEmpty: Boolean = fieldEvolutions.isEmpty && enumValueEvolutions.isEmpty && postEvolution.isEmpty

  /** Whether the former data of a case class reads as is (fast path): nothing migrates it, and the former field names
    * match the current declaration order.
    *
    * @param formerFieldNames
    *   Former case class field names, in declaration order
    */
  def isAvoidable(formerFieldNames: Array[String]): Boolean =
    isEmpty && formerFieldNames.sameElements(currentFieldNames)

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
        if (failures.nonEmpty) Left(failures) else Right(currentFieldNames.map(fields))
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
    (currentFieldNames.collect { case n if !fieldNames.contains(n) => MissingFieldException(currentClass, n) } ++
      fieldNames.collect { case n if !currentFieldNames.contains(n) => FieldNotUsedException(currentClass, n) }).toSeq

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
    override def isAvoidable(formerFieldNames: Array[String]): Boolean = true

    /** What an instance of this deleted former class reads as: see [[Evolution.deletedInstance]]. */
    def deletedInstance: T = Evolution.deletedInstance(formerClassName, throwOnInstance)

  }

  /** [[Evolution]] of an ADT declaring none. This evolution is only able to reorder the fields of a case class. */
  final class NoEvolution[T] private[api] (
      currentClass: Class[T],
      currentVersion: Int,
      fieldNames: Array[String] = Array.empty
  ) extends Evolution(currentClass.getName, currentVersion, currentVersion, currentClass, fieldNames) {

    override def isAvoidable(formerFieldNames: Array[String]): Boolean =
      // Only a reordering is migrated: unreadable names, an added or a removed field keep the positional resolution
      formerFieldNames.sameElements(currentFieldNames) ||
        !formerFieldNames.sorted.sameElements(currentFieldNames.sorted)

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
