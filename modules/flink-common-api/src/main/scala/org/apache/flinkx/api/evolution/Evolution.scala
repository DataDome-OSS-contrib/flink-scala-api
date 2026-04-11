package org.apache.flinkx.api.evolution

import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.evolution.FieldEvolution.FromFieldEvolution

import scala.collection.mutable

/** Hold all evolutions to apply on an ADT to deserialize.
  *
  * Data are collected at startup time during the ADT derivation with up-to-date information from current source code
  * and used during deserialization.
  *
  * @param clazz
  *   ADT class to deserialize and evolve
  * @param fieldNames
  *   The array of field names currently declared on the case class source code, in declaration order
  * @param fieldEvolutions
  *   Evolution to apply on case class fields
  * @param postDeserialize
  *   Mapper to apply on the ADT at the end of its deserialization
  * @tparam T
  *   the type on which the Evolution applies
  */
final class Evolution[T](
    val clazz: Class[T],
    private var fieldNames: Array[String] = Array.empty,
    private val fieldEvolutions: mutable.SortedSet[FieldEvolution] = mutable.SortedSet.empty,
    private var postDeserialize: AnyRef => AnyRef = identity[AnyRef]
) {

  /** Register field names currently declared in the case class source code.
    *
    * @param fieldNames
    *   Array of field names in declaration order
    */
  def addFieldNames(fieldNames: Array[String]): Unit =
    this.fieldNames = fieldNames

  /** Register a field evolution currently declared on the case class source code.
    *
    * @param fieldEvolution
    *   Evolution to apply on a case class field
    */
  def addFieldEvolution(fieldEvolution: FieldEvolution): Unit =
    fieldEvolutions += fieldEvolution

  /** Register a @postDeserialize annotation currently declared on the ADT source code.
    *
    * @param postDeserialize
    *   Mapper to apply on the ADT at the end of its deserialization
    */
  def addPostDeserialize[A](postDeserialize: A => A): Unit =
    this.postDeserialize = postDeserialize.asInstanceOf[AnyRef => AnyRef]

  /** Apply evolutions currently declared on the case class source code to field map starting from a specific version.
    *
    * @param since
    *   Schema version of serialized data
    * @param fieldMap
    *   Mutable field name to field value map to transform
    */
  def applyFieldEvolutions(since: Int, fieldMap: mutable.Map[String, AnyRef]): Unit =
    fieldEvolutions
      .rangeFrom(FromFieldEvolution(since))
      .foreach(_.apply(fieldMap))

  /** Apply @postDeserialize mapper function currently declared on the ADT source code to the ADT instance at the end of
    * its deserialization.
    *
    * @param toUpdate
    *   The mapper function is applied on this ADT instance
    */
  def applyPostDeserialize[A](toUpdate: A): A =
    postDeserialize.apply(toUpdate.asInstanceOf[AnyRef]).asInstanceOf[A]

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
    s"'$field' field not used to instantiate $clazz. Use @deletedMembers(since=<version>,Array(\"$field\")) annotation to indicate it has been deleted"
  )

  private def throwMissingField(clazz: Class[_], field: String): AnyRef = throw new FlinkRuntimeException(
    s"'$field' field missing to instantiate $clazz. Use @added(since=<version>) annotation to indicate it has been added"
  )

}

object Evolution {

  private final class DeletedMarker
  private[evolution] val DeletedClass: Class[_] = classOf[DeletedMarker]

}
