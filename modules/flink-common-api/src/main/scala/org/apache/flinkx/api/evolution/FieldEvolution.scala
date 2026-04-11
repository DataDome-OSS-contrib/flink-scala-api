package org.apache.flinkx.api.evolution

import org.apache.flink.util.FlinkRuntimeException

import scala.collection.mutable
import scala.math.Ordered.orderingToOrdered

/** Schema evolution operation applied to deserialized case class fields.
  *
  * Evolutions execute in sorted order by `since` then `order`. Operations at the same version are ordered by type:
  * Delete → Rename → Transform → Add.
  *
  * For example:
  *   - a field can be renamed then transformed
  *   - a field can be deleted (its state ignored) then re-added with the same name
  *
  * @param since
  *   Version number when this evolution was introduced
  * @param order
  *   Operation type ordering within a version
  */
abstract class FieldEvolution(val since: Int, val order: Int) extends Ordered[FieldEvolution] {

  def apply(fields: mutable.Map[String, AnyRef]): Unit

  override def compare(that: FieldEvolution): Int = (since, order, ##).compare((that.since, that.order, that.##))

}

object FieldEvolution {

  /** Remove a field from field map. */
  final case class Delete(
      override val since: Int,
      clazz: Class[_],
      name: String
  ) extends FieldEvolution(since, 10) {

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
  ) extends FieldEvolution(since, 20) {

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
  ) extends FieldEvolution(since, 30) {

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
  ) extends FieldEvolution(since, 40) {

    if (default.isEmpty) throw new FlinkRuntimeException(s"'$name' added field in $clazz must have a default value")

    override def apply(fields: mutable.Map[String, AnyRef]): Unit = {
      fields.put(name, default.get.asInstanceOf[AnyRef]) match {
        case Some(_) => throwFieldAlreadyExist(clazz, name, fields.keys)
        case _       =>
      }
    }

  }

  private[evolution] final case class FromFieldEvolution(override val since: Int) extends FieldEvolution(since, 100) {
    override def apply(fields: mutable.Map[String, AnyRef]): Unit = ()
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
