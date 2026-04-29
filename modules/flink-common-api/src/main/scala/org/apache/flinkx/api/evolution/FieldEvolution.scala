package org.apache.flinkx.api.evolution

import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.FieldEvolution.Phase

import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered

/** Schema evolution operation applied to deserialized case class fields.
  *
  * Evolutions execute in sorted order by `since` then `phase`. The phases form a canonical pipeline within a version:
  * Delete → Rename → Transform → Add. This ordering enables compositions such as:
  *   - a field can be renamed then transformed
  *   - a field can be deleted (its state ignored) then re-added with the same name
  *
  * @param since
  *   Version number when this evolution was introduced
  * @param phase
  *   Operation phase, defining the order within a version
  */
abstract class FieldEvolution(val since: Int, val phase: Phase) extends Ordered[FieldEvolution] {

  def apply(fields: mutable.Map[String, AnyRef]): Unit

  override def compare(that: FieldEvolution): Int = (since, phase.rank).compare((that.since, that.phase.rank))

}

object FieldEvolution {

  /** Phase of an evolution operation. The rank is the canonical sort order applied within a version. */
  sealed abstract class Phase(val rank: Int)
  object Phase {
    case object Delete    extends Phase(0)
    case object Rename    extends Phase(1)
    case object Transform extends Phase(2)
    case object Add       extends Phase(3)
  }

  /** Remove a field from field map. */
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

  /** Rename a field from old name to new name. */
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

  /** Transform a field value using mapper function. */
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

  /** Add a field with default value. */
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
