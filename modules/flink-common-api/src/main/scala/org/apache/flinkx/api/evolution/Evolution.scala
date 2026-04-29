package org.apache.flinkx.api.evolution

import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolutions.DeletedClass

import scala.collection.mutable

/** Immutable view of all evolutions to apply on an ADT during deserialization.
  *
  * Built from an [[EvolutionBuilder]] at the end of the registration phase.
  *
  * Thread-safe to read concurrently.
  *
  * @param clazz
  *   ADT class to deserialize and evolve
  * @param fieldNames
  *   The array of field names currently declared on the case class source code, in declaration order
  * @param fieldEvolutions
  *   Field evolutions to apply during deserialization
  * @param postDeserialize
  *   Mapper to apply on the ADT at the end of its deserialization
  * @tparam T
  *   the type on which the Evolution applies
  */
final class Evolution[T] private[evolution] (
    val clazz: Class[T],
    private val fieldNames: Array[String],
    private val fieldEvolutions: Array[FieldEvolution],
    private val postDeserialize: T => T
) {

  /** Is evolution can be skipped? Return `true` if no evolution is required from given version and if fields haven't
    * been reordered, `false` otherwise.
    *
    * @param dataVersion
    *   Schema version of serialized data
    * @param previousFieldNames
    *   Array of field names of serialized data in declaration order
    */
  def isAvoidable(dataVersion: Int, previousFieldNames: Array[String]): Boolean =
    !fieldEvolutions.exists(_.since > dataVersion) && previousFieldNames.sameElements(fieldNames)

  /** Apply evolutions currently declared on the case class source code to field map starting from a specific version.
    *
    * @param dataVersion
    *   Schema version of serialized data
    * @param fieldMap
    *   Mutable field name to field value map to transform
    */
  def applyFieldEvolutions(dataVersion: Int, fieldMap: mutable.Map[String, AnyRef]): Unit = {
    var i = 0
    while (i < fieldEvolutions.length) {
      val fieldEvolution = fieldEvolutions(i)
      if (fieldEvolution.since > dataVersion) fieldEvolution.apply(fieldMap)
      i += 1
    }
  }

  /** Apply `@postDeserialize` mapper function currently declared on the ADT source code to the ADT instance at the end
    * of its deserialization.
    *
    * @param toUpdate
    *   The mapper function is applied on this ADT instance
    */
  def applyPostDeserialize(toUpdate: T): T =
    if ((postDeserialize: AnyRef) eq Evolution.IdentityFunction) toUpdate else postDeserialize.apply(toUpdate)

  /** Return a boolean indicating if the ADT class has been registered as deleted in the current source code.
    *
    * @return
    *   `true` if the ADT class has been registered as deleted, `false` otherwise
    */
  def isDeleted: Boolean = clazz == DeletedClass

  /** Convert field map to field values array using registered field names currently declared in the case class source
    * code.
    *
    * @param fieldMap
    *   Field name to field value map
    * @return
    *   Array of field values in declaration order
    */
  def toFieldValues(fieldMap: mutable.Map[String, AnyRef]): Array[AnyRef] = {
    fieldMap.keys.foreach(n => if (!fieldNames.contains(n)) throwFieldNotUsed(clazz, n))
    fieldNames.map(n => fieldMap.getOrElse(n, throwMissingField(clazz, n)))
  }

  private def throwFieldNotUsed(clazz: Class[_], field: String): Unit = throw new FlinkRuntimeException(
    s"'$field' field not used to instantiate $clazz. Use @deletedFields(since=<version>,\"$field\") annotation to indicate it has been deleted"
  )

  private def throwMissingField(clazz: Class[_], field: String): AnyRef = throw new FlinkRuntimeException(
    s"'$field' field missing to instantiate $clazz. Use @added(since=<version>) annotation to indicate it has been added"
  )

}

object Evolution {
  // Sentinel used to fast-path applyPostDeserialize when no @postDeserialize mapper has been registered
  private[evolution] val IdentityFunction: Any => Any = identity

  /** Default empty [[Evolution]] as fallback for a class with no registered evolutions. */
  private[evolution] def empty[T](clazz: Class[T]): Evolution[T] =
    new Evolution[T](clazz, Array.empty, Array.empty, IdentityFunction.asInstanceOf[T => T])
}
