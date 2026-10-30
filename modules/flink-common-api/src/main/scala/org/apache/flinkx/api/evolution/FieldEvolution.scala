package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.FieldEvolution.{Former, MigratedField, Phase}
import org.apache.flinkx.api.util.ClassUtil

import scala.collection.mutable

/** One evolution of a field, declared in version `since`. [[FieldEvolution.plan]] applies it to the former field names.
  *
  * The field evolutions apply in the order of `since`, then of `phase`: Delete, Rename, Transform, Add. For example,
  * this order accepts:
  *   - the rename of a field, then the transform of its value
  *   - the deletion of a field, then the add of a field with the same name and a default value
  *
  * @param since
  *   The first schema version that contains the evolution
  * @param phase
  *   The rank of the evolution among the field evolutions with the same `since`
  */
@Internal
abstract class FieldEvolution(val since: Int, val phase: Phase) extends Ordered[FieldEvolution] with Serializable {

  /** Apply this evolution to the given migrated fields, by field name. At the start of the plan, they are the
    * [[Former]] fields, the deleted fields included.
    *
    * @return
    *   Why the evolution fails, if it fails
    */
  def apply(fields: mutable.Map[String, MigratedField]): Option[String]

  override def compare(that: FieldEvolution): Int = {
    val sinceComparison = since.compare(that.since)
    if (sinceComparison != 0) sinceComparison
    else phase.rank.compare(that.phase.rank)
  }

}

object FieldEvolution {

  /** Plan the migration of the former fields: apply the sorted field evolutions to their names. The plan is valid for
    * all the records.
    *
    * @return
    *   The first failure of an evolution, or the migrated fields that remain, by field name
    */
  private[evolution] def plan(
      fieldEvolutions: Array[FieldEvolution],
      formerFieldNames: Array[String]
  ): Either[String, Map[String, MigratedField]] = {
    val fields = mutable.Map.from(formerFieldNames.zipWithIndex.map { case (name, i) =>
      name -> (Former(i): MigratedField)
    })
    // The lazy iterator stops at the first evolution that fails
    fieldEvolutions.iterator.flatMap(_.apply(fields)).nextOption().toLeft(fields.toMap)
  }

  /** One current field of the migration plan: how the restored serializer gets its value from the former values. */
  @Internal
  sealed trait MigratedField {

    /** Migrate the values of the former fields to the current value of this field. */
    def migrate(formerValues: Array[AnyRef]): AnyRef

  }

  /** A former field, unchanged, reordered or renamed: its current value is its former value. */
  final case class Former(index: Int) extends MigratedField {
    override def migrate(formerValues: Array[AnyRef]): AnyRef = formerValues(index)
  }

  /** A field that a field mapper converts, from the result of the evolutions that apply before it. */
  final case class Transformed(field: MigratedField, mapper: AnyRef => AnyRef) extends MigratedField {
    override def migrate(formerValues: Array[AnyRef]): AnyRef = mapper(field.migrate(formerValues))
  }

  /** An added field: its value is its default value, evaluated again for each record. */
  final case class Added(default: () => AnyRef) extends MigratedField {
    override def migrate(formerValues: Array[AnyRef]): AnyRef = default()
  }

  /** Rank of a field evolution among the field evolutions with the same `since`. A lower rank applies first. */
  @Internal
  sealed abstract class Phase(val rank: Int) extends Serializable
  object Phase {
    case object Delete    extends Phase(0)
    case object Rename    extends Phase(1)
    case object Transform extends Phase(2)
    case object Add       extends Phase(3)
  }

  /** Delete the field `formerName`. Declared by `@deletedFields`.
    *
    * This evolution ignores a field that is not there. The plan of a low version applies all the deletions: a field
    * renamed, then deleted, has a deletion that does not find it.
    */
  private[evolution] final case class Delete(override val since: Int, formerName: String)
      extends FieldEvolution(since, Phase.Delete) {

    override def apply(fields: mutable.Map[String, MigratedField]): Option[String] = {
      fields.remove(formerName)
      None
    }

  }

  /** Rename the field `formerName` to `currentName`. Declared by `@renamed` on a field. */
  private[evolution] final case class Rename(override val since: Int, formerName: String, currentName: String)
      extends FieldEvolution(since, Phase.Rename) {

    override def apply(fields: mutable.Map[String, MigratedField]): Option[String] =
      fields.remove(formerName) match {
        case None                                    => notFound("rename", formerName, fields.keys)
        case Some(_) if fields.contains(currentName) => alreadyThere("rename", currentName, fields.keys)
        case Some(field)                             => fields.put(currentName, field); None
      }

  }

  /** Convert the value of the field `name` with the `mapper`. Declared by `@transformed`. */
  private[evolution] final case class Transform(override val since: Int, name: String, mapper: AnyRef => AnyRef)
      extends FieldEvolution(since, Phase.Transform) {

    override def apply(fields: mutable.Map[String, MigratedField]): Option[String] =
      fields.get(name) match {
        case Some(field) => fields.update(name, Transformed(field, mapper)); None
        case _           => notFound("transform", name, fields.keys)
      }

  }

  /** Add the field `name` with its default value, evaluated for each record. Declared by `@added`.
    *
    * @param fieldIndex
    *   Position of the field in the constructor of `currentClass`, which declares its default value
    */
  private[evolution] final case class Add(
      override val since: Int,
      currentClass: Class[_],
      name: String,
      fieldIndex: Int
  ) extends FieldEvolution(since, Phase.Add) {

    @transient private lazy val defaultValue: () => AnyRef = ClassUtil.defaultFieldValue(currentClass, fieldIndex)

    override def apply(fields: mutable.Map[String, MigratedField]): Option[String] =
      fields.put(name, Added(() => defaultValue())) match {
        case Some(_) => alreadyThere("add", name, fields.keys)
        case _       => None
      }

  }

  /** Why `operation` cannot find the field `name` among the given fields. */
  private def notFound(operation: String, name: String, fields: Iterable[String]): Option[String] =
    Some(s"Cannot $operation field '$name': it is not one of ${fields.mkString("[\"", "\",\"", "\"]")}")

  /** Why `operation` cannot give the field `name`: a field among the given fields already has it. */
  private def alreadyThere(operation: String, name: String, fields: Iterable[String]): Option[String] =
    Some(s"Cannot $operation field '$name': it is already one of ${fields.mkString("[\"", "\",\"", "\"]")}")

}
