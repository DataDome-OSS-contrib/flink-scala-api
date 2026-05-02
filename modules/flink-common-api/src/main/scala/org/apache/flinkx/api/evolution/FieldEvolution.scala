package org.apache.flinkx.api.evolution

import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.FieldEvolution.Phase

import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered

/** A single field-level evolution step, applied to the in-flight field map during deserialization.
  *
  * Evolutions execute in sorted order by `since` then `phase`. The phases form a canonical pipeline within a version:
  * Delete → Rename → Transform → Add. This ordering enables compositions such as:
  *   - rename a field, then transform its value
  *   - delete a field (its serialized state is dropped), then re-add a field with the same name from a default
  *
  * @param since
  *   Version in which the evolution was introduced
  * @param phase
  *   Operation phase, defining sort order within a single version
  */
abstract class FieldEvolution(val since: Int, val phase: Phase) extends Ordered[FieldEvolution] {

  /** Mutate `fields` in place to apply this evolution step. */
  def apply(fields: mutable.Map[String, AnyRef]): Unit

  override def compare(that: FieldEvolution): Int = (since, phase.rank).compare((that.since, that.phase.rank))

}

object FieldEvolution {

  /** Operation phase used to order evolutions within a single version (lower rank applied first). */
  sealed abstract class Phase(val rank: Int)
  object Phase {
    case object Delete    extends Phase(0)
    case object Rename    extends Phase(1)
    case object Transform extends Phase(2)
    case object Add       extends Phase(3)
  }

  /** Remove a field from the field map. Backs the `@deletedFields` annotation.
    *
    * @throws FlinkRuntimeException
    *   if `name` is not present in the field map.
    */
  final case class Delete(
      override val since: Int,
      clazz: Class[_],
      name: String
  ) extends FieldEvolution(since, Phase.Delete) {

    override def apply(fields: mutable.Map[String, AnyRef]): Unit = {
      fields.remove(name) match {
        case None => throwFieldNotFound(clazz, name, "delete", fields.keys)
        case _    =>
      }
    }

  }

  /** Move the entry under `fromName` to the new key `name`. Backs the `@renamed` annotation on a field.
    *
    * @throws FlinkRuntimeException
    *   if `fromName` is not present in the field map.
    */
  final case class Rename(
      override val since: Int,
      clazz: Class[_],
      name: String,
      fromName: String
  ) extends FieldEvolution(since, Phase.Rename) {

    override def apply(fields: mutable.Map[String, AnyRef]): Unit = {
      fields.remove(fromName) match {
        case Some(value) => fields.put(name, value)
        case _           => throwFieldNotFound(clazz, fromName, "rename", fields.keys)
      }
    }

  }

  /** Replace the value at `name` with `mapper(value)`. Backs the `@transformed` annotation.
    *
    * @throws FlinkRuntimeException
    *   if `name` is not present in the field map.
    */
  final case class Transform[A, B](
      override val since: Int,
      clazz: Class[_],
      name: String,
      mapper: A => B
  ) extends FieldEvolution(since, Phase.Transform) {

    override def apply(fields: mutable.Map[String, AnyRef]): Unit = {
      fields.get(name) match {
        case Some(value) => fields.update(name, mapper.apply(value.asInstanceOf[A]).asInstanceOf[AnyRef])
        case _           => throwFieldNotFound(clazz, name, "transform", fields.keys)
      }
    }

  }

  /** Insert field `name` with the case class default value. Backs the `@added` annotation.
    *
    * @throws FlinkRuntimeException
    *   - at construction if `default` is empty;
    *   - at apply time if `name` is already present.
    */
  final case class Add[T](
      override val since: Int,
      clazz: Class[_],
      name: String,
      default: Option[T]
  ) extends FieldEvolution(since, Phase.Add) {

    if (default.isEmpty) throw new FlinkRuntimeException(s"'$name' added field in $clazz must have a default value")

    override def apply(fields: mutable.Map[String, AnyRef]): Unit = {
      fields.put(name, default.get.asInstanceOf[AnyRef]) match {
        case Some(_) => throwFieldAlreadyExist(clazz, name, fields.keys)
        case _       =>
      }
    }

  }

  private def throwFieldNotFound(clazz: Class[_], field: String, operation: String, fields: Iterable[String]): Unit =
    throw new FlinkRuntimeException(
      s"Cannot $operation '$field'. Field not found in $clazz. Available fields: ${fields.mkString("[\"", "\",\"", "\"]")}"
    )

  private def throwFieldAlreadyExist(clazz: Class[_], field: String, fields: Iterable[String]): Unit =
    throw new FlinkRuntimeException(
      s"Cannot add '$field'. Field already exists in $clazz. Existing fields: ${fields.mkString("[\"", "\",\"", "\"]")}"
    )

}
