package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.Internal
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution
import org.apache.flinkx.api.evolution.EvolutionBuilder.{ClassEvolution, postDeserializeIdentity}
import org.apache.flinkx.api.postDeserialize
import org.apache.flinkx.api.util.ClassUtil

import scala.collection.mutable

/** Mutable builder filled with annotations on current class during the ADT derivation phase.
  *
  * Once all registrations are made, [[build]] produces the immutable [[Evolution]]s for the deserialization phase.
  *
  * Not thread-safe: a builder belongs to a single derivation pass.
  *
  * @param currentClass
  *   Current ADT class being derived
  * @param currentVersion
  *   Current schema version of the ADT, the upper bound of the `since` of its evolutions
  * @param currentMemberNames
  *   Current ADT member names, in declaration order: the field names of a case class, the fully qualified names of the
  *   subtypes of a sealed trait, or the value names of a Scala 3 enum
  * @param formerToCurrentClass
  *   Former ADT class names, with the current class they resolve to and the version of the rename
  * @param fieldEvolutions
  *   Field-level evolutions to apply on case class fields; empty for sealed traits
  * @param formerEnumValues
  *   Evolutions of the Scala 3 enum values, by former value name; empty for non-enum ADTs
  * @param postDeserialize
  *   A mapper function taking as parameters the former version and the current ADT instance after its deserialization
  * @tparam T
  *   The type on which the [[Evolution]] applies
  */
@Internal
final class EvolutionBuilder[T](
    val currentClass: Class[T],
    val currentVersion: Int,
    val currentMemberNames: Array[String] = Array.empty,
    val formerToCurrentClass: mutable.Map[String, ClassEvolution] = mutable.Map.empty,
    val fieldEvolutions: mutable.ArrayBuffer[FieldEvolution] = mutable.ArrayBuffer.empty,
    val formerEnumValues: mutable.Map[String, EnumValueEvolution] = mutable.Map.empty,
    private var postDeserialize: Option[(Int, T) => T] = None
) {

  /** Register the mapping between a former ADT class name and the current ADT class.
    *
    * @param formerClassName
    *   Name of the former ADT class as declared when the data was serialized (simple, relative or absolute)
    * @param currentClass
    *   Current ADT class, used as reference to resolve `formerClassName` to its fully-qualified internal form
    * @param version
    *   Version in which the rename took effect
    */
  def registerFormerClass(formerClassName: String, currentClass: Class[_], version: Int): Unit =
    formerToCurrentClass(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) =
      ClassEvolution(currentClass, version)

  def addPostDeserialize(p: postDeserialize[T]): Unit = if (postDeserialize.isEmpty) {
    postDeserialize = Some(p.mapper)
  } else {
    throw EvolutionNotAllowedException(p, s"$currentClass twice")
  }

  /** Build the [[Evolution]]s to register, per class name.
    *
    * A class name is only valid over a window of versions, which bounds the boundaries it gets:
    *   - a former name is valid up to the version before the rename took effect;
    *   - the current name is valid from the version of the last rename (to avoid collision) up to the current one.
    *
    * @return
    *   the [[Evolution]]s to register, by class name
    * @throws SinceNotAllowedException
    *   if an evolution declares a `since` outside the version range of the ADT
    */
  def build(): Map[String, Array[Evolution[T]]] = {
    fieldEvolutions
      .find(e => e.since < 1 || e.since > currentVersion)
      // An evolution outside the version range is never applied when it should
      .foreach(e => throw SinceNotAllowedException(currentClass, e.since, currentVersion))
    fieldEvolutions.sortInPlace()
    // Computed before adding the current name below, which is only valid from the last rename onwards
    val currentNameSince = formerToCurrentClass.values.map(_.version).maxOption.getOrElse(0)
    formerToCurrentClass.put(currentClass.getName, ClassEvolution(currentClass, currentVersion))
    formerToCurrentClass.iterator.map { case (className, classEvolution) =>
      val isCurrentName = className == currentClass.getName
      val firstVersion  = if (isCurrentName) currentNameSince else 0
      val lastVersion   = if (isCurrentName) currentVersion else classEvolution.version - 1
      // A field evolution introduced in version N migrates the data written in version N - 1
      val versions = fieldEvolutions.iterator
        .map(_.since - 1)
        .filter(version => version >= firstVersion && version <= lastVersion)
        .toSeq
        .appended(lastVersion) // The name must still resolve for the last version of its window
        .distinct
        .sorted
      className -> versions.map(buildEvolution(className, classEvolution, _)).toArray
    }.toMap
  }

  private def buildEvolution(className: String, classEvolution: ClassEvolution, previousVersion: Int): Evolution[T] =
    new Evolution[T](
      className = className,
      version = previousVersion,
      currentClass = classEvolution.clazz.asInstanceOf[Class[T]],
      currentMemberNames = currentMemberNames.clone(),
      fieldEvolutions = fieldEvolutions.dropWhile(_.since <= previousVersion).toArray,
      formerEnumValueToEvolution = formerEnumValues.toMap,
      throwOnInstance = classEvolution.throwOnInstance,
      postDeserialize = postDeserialize.getOrElse(postDeserializeIdentity)
    )

}

object EvolutionBuilder {

  // Serializable, as the Evolution holding it travels with the serializer
  def postDeserializeIdentity[T]: (Int, T) => T = new ((Int, T) => T) with Serializable {
    override def apply(version: Int, instance: T): T = instance
    override def toString(): String                  = "(_, i) => i"
  }

  case class ClassEvolution(clazz: Class[_], version: Int, throwOnInstance: Boolean = false)

}
