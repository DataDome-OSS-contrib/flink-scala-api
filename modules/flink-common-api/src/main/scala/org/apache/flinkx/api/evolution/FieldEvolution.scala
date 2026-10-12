package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.FieldEvolution.{EvolvedField, Former, Phase}
import org.apache.flinkx.api.util.ClassUtil
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/** A single field-level evolution declared between two versions, replayed on the former fields to tell how they evolve.
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
abstract class FieldEvolution(val since: Int, val phase: Phase) extends Ordered[FieldEvolution] with Serializable {

  /** Mutate the given field-name to evolved-field map in place to apply this evolution.
    *
    * Initially, the fields are only the [[Former]] ones, including the fields that have been deleted since.
    *
    * @return
    *   The failure the evolution hits, if any
    */
  def apply(fields: mutable.Map[String, EvolvedField]): Option[FlinkRuntimeException]

  override def compare(that: FieldEvolution): Int = {
    val sinceComparison = since.compare(that.since)
    if (sinceComparison != 0) sinceComparison
    else phase.rank.compare(that.phase.rank)
  }

}

object FieldEvolution {

  /** Replay the given sorted field evolutions on the former field names, once for all the records written with them.
    *
    * @return
    *   The first failure an evolution hits, or how every field left evolves, by field name
    */
  private[evolution] def replay(
      fieldEvolutions: Array[FieldEvolution],
      formerFieldNames: Array[String]
  ): Either[FlinkRuntimeException, Map[String, EvolvedField]] = {
    val fields = mutable.Map.from(formerFieldNames.zipWithIndex.map { case (name, i) =>
      name -> (Former(i): EvolvedField)
    })
    // The iterator is lazy, so it stops on the first failing evolution
    fieldEvolutions.iterator.flatMap(_.apply(fields)).nextOption().toLeft(fields.toMap)
  }

  /** Index of one field, tracked while replaying the evolutions, or `None` when the lineage is broken. */
  type FieldIndex = Option[Int]

  /** A current field evolved from the former fields, applied to every record read. */
  @Internal
  sealed trait EvolvedField {

    /** Evolve the values of the former fields to their current forms. */
    def evolve(formerValues: Array[AnyRef]): AnyRef

    /** Return the index of the former field it is read from, `None` when the lineage is broken. */
    def formerIndex: FieldIndex = None

  }

  /** A former field, unchanged, reordered or renamed. */
  final case class Former(index: Int) extends EvolvedField {
    override def evolve(formerValues: Array[AnyRef]): AnyRef = formerValues(index)
    override def formerIndex: FieldIndex                     = Some(index)
  }

  /** A field converted by a mapper, from however it evolved before. */
  final case class Transformed(field: EvolvedField, mapper: AnyRef => AnyRef) extends EvolvedField {
    override def evolve(formerValues: Array[AnyRef]): AnyRef = mapper(field.evolve(formerValues))
  }

  /** An added field, given its default value evaluated anew for each record. */
  final case class Added(default: () => AnyRef) extends EvolvedField {
    override def evolve(formerValues: Array[AnyRef]): AnyRef = default()
  }

  /** Operation phase used to order evolutions within a single version (lower rank applied first). */
  @Internal
  sealed abstract class Phase(val rank: Int) extends Serializable
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

    @transient private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

    override def apply(fields: mutable.Map[String, EvolvedField]): Option[FlinkRuntimeException] = {
      if (fields.remove(formerName).isEmpty) {
        logger.warn(s"Ignoring field '$formerName' deletion: not found in $currentClass")
      }
      None
    }

  }

  /** Move the entry under `formerName` to the new key `currentName`. Backs the `@renamed` annotation on a field.
    *
    * Fails with [[FieldNotFoundException]] if `formerName` is not present in the field map.
    */
  @Internal
  final case class Rename(
      override val since: Int,
      currentClass: Class[_],
      formerName: String,
      currentName: String
  ) extends FieldEvolution(since, Phase.Rename) {

    override def apply(fields: mutable.Map[String, EvolvedField]): Option[FlinkRuntimeException] =
      fields.remove(formerName) match {
        case Some(field) => fields.put(currentName, field); None
        case _           => Some(FieldNotFoundException(currentClass, formerName, "rename", fields.keys))
      }

  }

  /** Replace the value at `name` with `mapper(value)`. Backs the `@transformed` annotation.
    *
    * Fails with [[FieldNotFoundException]] if `name` is not present in the field map.
    */
  @Internal
  final case class Transform[A, B](
      override val since: Int,
      currentClass: Class[_],
      name: String,
      mapper: A => B
  ) extends FieldEvolution(since, Phase.Transform) {

    override def apply(fields: mutable.Map[String, EvolvedField]): Option[FlinkRuntimeException] =
      fields.get(name) match {
        case Some(field) =>
          fields.update(name, Transformed(field, mapper.asInstanceOf[AnyRef => AnyRef]))
          None
        case _ => Some(FieldNotFoundException(currentClass, name, "transform", fields.keys))
      }

  }

  /** Insert field `name` with the case class default value, evaluated for each record. Backs the `@added` annotation.
    *
    * Fails with [[FieldAlreadyExistException]] if `name` is already present.
    *
    * @param fieldIndex
    *   Position of the field in the constructor
    */
  @Internal
  final case class Add(
      override val since: Int,
      currentClass: Class[_],
      name: String,
      fieldIndex: Int
  ) extends FieldEvolution(since, Phase.Add) {

    @transient private lazy val defaultValue: () => AnyRef = ClassUtil.defaultFieldValue(currentClass, fieldIndex)

    override def apply(fields: mutable.Map[String, EvolvedField]): Option[FlinkRuntimeException] =
      if (fields.contains(name)) {
        Some(FieldAlreadyExistException(currentClass, name, fields.keys))
      } else {
        fields.put(name, Added(() => defaultValue()))
        None
      }

  }

}
