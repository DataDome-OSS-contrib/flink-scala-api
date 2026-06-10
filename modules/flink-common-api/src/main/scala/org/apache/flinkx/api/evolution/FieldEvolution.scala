package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.FieldEvolution.{FieldIndex, Phase}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/** A single field-level evolution step, applied to the in-flight field map during deserialization.
  *
  * Evolutions execute in sorted order by `since` then `phase`. The phases form a canonical pipeline within a version:
  * Delete → Rename → Transform → Add. This ordering enables compositions such as:
  *   - rename a field, then transform its value
  *   - delete a field (its serialized state is dropped), then re-add a field with the same name from a default value
  *
  * @param since
  *   Version in which the evolution was introduced
  * @param phase
  *   Operation phase, defining sort order within a single version
  */
@Internal
abstract class FieldEvolution(val since: Int, val phase: Phase) extends Ordered[FieldEvolution] {

  private[FieldEvolution] lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /** Mutate field-name to field-value map in place to apply this evolution step.
    *
    * Initially, the field map contains only the former fields, including:
    *   - fields that have been deleted since
    *   - values with their former type
    */
  def apply(fieldValues: mutable.Map[String, AnyRef]): Unit

  /** Mutate the field indexes in place the way [[apply]] mutates the field-value map, and return the same exceptions,
    * without reading any data.
    */
  def dryRun(fieldIndexes: mutable.Map[String, FieldIndex]): Option[FlinkRuntimeException]

  override def compare(that: FieldEvolution): Int = {
    val sinceComparison = since.compare(that.since)
    if (sinceComparison != 0) sinceComparison
    else phase.rank.compare(that.phase.rank)
  }

}

object FieldEvolution {

  /** Index of one field, tracked while replaying the evolutions, or `None` when the lineage is broken. */
  type FieldIndex = Option[Int]

  /** Operation phase used to order evolutions within a single version (lower rank applied first). */
  @Internal
  sealed abstract class Phase(val rank: Int)
  object Phase {
    case object Delete    extends Phase(0)
    case object Rename    extends Phase(1)
    case object Transform extends Phase(2)
    case object Add       extends Phase(3)
  }

  /** Remove a field from the field map. Backs the `@deletedFields` annotation.
    *
    * A field already absent is ignored rather than reported: restoring the oldest versions replays every deletion, so a
    * field renamed then deleted is legitimately named by a deletion that no longer finds it.
    */
  @Internal
  final case class Delete(
      override val since: Int,
      currentClass: Class[_],
      formerName: String
  ) extends FieldEvolution(since, Phase.Delete) {

    override def apply(fieldValues: mutable.Map[String, AnyRef]): Unit = {
      fieldValues.remove(formerName)
    }

    override def dryRun(fieldIndexes: mutable.Map[String, FieldIndex]): Option[FlinkRuntimeException] = {
      fieldIndexes.remove(formerName) match {
        case None => logger.warn(s"Ignoring field '$formerName' deletion: not found in $currentClass"); None
        case _    => None
      }
    }

  }

  /** Move the entry under `formerName` to the new key `currentName`. Backs the `@renamed` annotation on a field.
    *
    * @throws FieldNotFoundException
    *   if `formerName` is not present in the field map.
    */
  @Internal
  final case class Rename(
      override val since: Int,
      currentClass: Class[_],
      formerName: String,
      currentName: String
  ) extends FieldEvolution(since, Phase.Rename) {

    override def apply(fieldValues: mutable.Map[String, AnyRef]): Unit = {
      fieldValues.remove(formerName) match {
        case Some(value) => fieldValues.put(currentName, value)
        case _           => throw FieldNotFoundException(currentClass, formerName, "rename", fieldValues.keys)
      }
    }

    override def dryRun(fieldIndexes: mutable.Map[String, FieldIndex]): Option[FlinkRuntimeException] =
      fieldIndexes.remove(formerName) match {
        case Some(origin) => fieldIndexes.put(currentName, origin); None
        case _            => Some(FieldNotFoundException(currentClass, formerName, "rename", fieldIndexes.keys))
      }

  }

  /** Replace the value at `name` with `mapper(value)`. Backs the `@transformed` annotation.
    *
    * @throws FieldNotFoundException
    *   if `name` is not present in the field map.
    */
  @Internal
  final case class Transform[A, B](
      override val since: Int,
      currentClass: Class[_],
      name: String,
      mapper: A => B
  ) extends FieldEvolution(since, Phase.Transform) {

    override def apply(fieldValues: mutable.Map[String, AnyRef]): Unit = {
      fieldValues.get(name) match {
        case Some(value) => fieldValues.update(name, mapper.apply(value.asInstanceOf[A]).asInstanceOf[AnyRef])
        case _           => throw FieldNotFoundException(currentClass, name, "transform", fieldValues.keys)
      }
    }

    override def dryRun(fieldIndexes: mutable.Map[String, FieldIndex]): Option[FlinkRuntimeException] =
      fieldIndexes.get(name) match {
        // The mapper converts the former value to the current type, so their serializers cannot be compared anymore
        case Some(_) => fieldIndexes.update(name, None); None
        case _       => Some(FieldNotFoundException(currentClass, name, "transform", fieldIndexes.keys))
      }

  }

  /** Insert field `name` with the case class default value. Backs the `@added` annotation.
    *
    * @throws AddedFieldWithoutDefaultException
    *   At construction if `default` is empty;
    * @throws FieldAlreadyExistException
    *   Deserialization time if `name` is already present.
    */
  @Internal
  final case class Add[T](
      override val since: Int,
      currentClass: Class[_],
      name: String,
      default: Option[T]
  ) extends FieldEvolution(since, Phase.Add) {

    if (default.isEmpty) throw AddedFieldWithoutDefaultException(currentClass, name)

    override def apply(fieldValues: mutable.Map[String, AnyRef]): Unit = {
      fieldValues.put(name, default.get.asInstanceOf[AnyRef]) match {
        case Some(_) => throw FieldAlreadyExistException(currentClass, name, fieldValues.keys)
        case _       =>
      }
    }

    override def dryRun(fieldIndexes: mutable.Map[String, FieldIndex]): Option[FlinkRuntimeException] =
      fieldIndexes.put(name, None) match {
        case Some(_) => Some(FieldAlreadyExistException(currentClass, name, fieldIndexes.keys))
        case _       => None
      }

  }

}
