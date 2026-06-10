package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.util.ClassUtil
import org.apache.flinkx.api.version

import java.io.IOException
import scala.collection.concurrent

/** Global registry and entry point for the annotation-based schema evolution feature.
  *
  * Schema evolution lets a Flink job restore from a former checkpoint whose ADT (case class, sealed trait or Scala 3
  * enum) schema differs from the one currently declared in source code. Users opt in per ADT by adding the [[version]]
  * annotation and describe each change with [[Evolved]] annotations.
  *
  * This schema evolution feature commonly employs the following vocabulary to qualify version, class, field, etc.:
  *   - `Former` describes the serialization time when the checkpoint was done.
  *   - `Current` describes the deserialization time with the current source code.
  *
  * Lifecycle:
  *   - At derivation time (start-up), [[org.apache.flinkx.api.TypeInformationDerivation]] reads current annotations and
  *     [[register]] an immutable [[Evolution]] for that ADT class.
  *   - At deserialization time, the ADT serializers [[get]] the evolutions and apply them in correct order.
  *
  * Class renames and deletions are resolved through [[EvolutionBuilder.registerFormerClass]] /
  * [[registerDeletedFormerClass]], which translate the fully-qualified class names recorded in the snapshot into the
  * current classes (or a deletion marker).
  *
  * Concurrency: registration runs at startup during derivation (single-threaded per ADT); [[get]] is safe to call
  * concurrently from multiple Flink tasks afterward.
  */
@Internal
object Evolutions {

  private val currentClassToEvolutions: concurrent.Map[String, Array[Evolution[_]]] = concurrent.TrieMap.empty

  /** Build the [[Evolution]]s from the given builder and register them for the deserialization phase. */
  def register[T](builder: EvolutionBuilder[T]): Unit =
    builder.build().foreachEntry { (className, evolutions) =>
      currentClassToEvolutions.updateWith(className) {
        case Some(registered) => Some(declare(className, registered ++ evolutions))
        case _                => Some(declare(className, evolutions.asInstanceOf[Array[Evolution[_]]]))
      }
    }

  /** Sort the evolutions of a class name, and reject a name claimed by two different ADTs.
    *
    * A name legitimately gets several evolutions, one per version boundary, but at a given former version it must
    * resolve to a single current class.
    *
    * @throws FormerClassConflictException
    *   if two evolutions declared for the same version resolve to different current classes
    */
  private def declare(className: String, evolutions: Array[Evolution[_]]): Array[Evolution[_]] = {
    evolutions
      .groupBy(_.version)
      .collectFirst { case (_, sameVersion) if sameVersion.map(_.currentClass).distinct.length > 1 => sameVersion }
      .foreach { conflicting =>
        val classes = conflicting.map(_.currentClass).distinct
        throw FormerClassConflictException(className, describeDeclaration(classes(0)), describeDeclaration(classes(1)))
      }
    evolutions.sorted
  }

  private def describeDeclaration(currentClass: Class[_]): String =
    if (isDeletedClass(currentClass)) "deleted" else s"renamed to $currentClass"

  /** `true` if the given current class is the marker of a former class registered as deleted. */
  def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Return the [[Evolution]] declared for the given former ADT member name at the given former version, if any.
    *
    * Unlike others [[get]], never loads any class, so it also answers for the names that designate no class at all: the
    * `<enum binary name>#<value name>` of a Scala 3 enum value, in particular. Returning `None` means nothing was
    * declared for that name, not that the member is unknown.
    *
    * @param formerName
    *   Former fully qualified class name, or `<enum binary name>#<value name>` for a Scala 3 enum value
    * @param formerVersion
    *   Former schema version of the ADT declaring that member
    */
  def get[T](formerName: String, formerVersion: Int): Option[Evolution[T]] =
    currentClassToEvolutions
      .get(formerName)
      .flatMap(_.find(_.version >= formerVersion))
      .map(_.asInstanceOf[Evolution[T]])

  /** Return the [[Evolution]] associated with the given ADT class, or [[Evolution.noEvolution]] otherwise. */
  def get[T](clazz: Class[T], formerVersion: Int): Evolution[T] =
    get[T](clazz.getName, formerVersion).getOrElse(Evolution.noEvolution(clazz, formerVersion))

  /** Return the [[Evolution]] associated with the given former ADT class name, or [[Evolution.noEvolution]] of the
    * class loaded by name otherwise.
    *
    * @throws IOException
    *   if no evolution is declared for that name, and it can't be loaded either
    */
  def get[T](formerClassName: String, formerVersion: Int, cl: ClassLoader): Evolution[T] =
    if (formerClassName == null) Evolution.NoEvolution.asInstanceOf[Evolution[T]] // Snapshot written before 2.4.0
    else
      get[T](formerClassName, formerVersion).getOrElse {
        val currentClass =
          try Class.forName(formerClassName, false, cl).asInstanceOf[Class[T]]
          catch { // Same behavior as org.apache.flink.util.InstantiationUtil.resolveClassByName
            case e: ClassNotFoundException =>
              throw new IOException(s"Could not find class '$formerClassName' in classpath.", e)
          }
        Evolution.noEvolution(currentClass, formerVersion)
      }

  /** Register that a former ADT subtype or field type has been deleted from the current source code.
    *
    * @param formerClassName
    *   Name of the deleted former subtype or field type (simple, relative or absolute)
    * @param currentClass
    *   Current ADT class, used as reference to resolve `formerClassName` to its fully-qualified internal form
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws via
    *   [[Evolution.returnNullOrThrow]]. If `false`, the instance is deserialized as `null`.
    */
  def registerDeletedFormerClass(
      formerClassName: String,
      currentClass: Class[_],
      formerVersion: Int,
      throwOnInstance: Boolean
  ): Unit = {
    val formerFqn = ClassUtil.resolveFormerClassName(formerClassName, currentClass)
    // A class deleted in version N still exists in the data written in version N - 1
    val deletedEvolution = new Evolution(formerFqn, formerVersion - 1, DeletedClass, throwOnInstance = throwOnInstance)
    currentClassToEvolutions.updateWith(formerFqn) {
      case Some(registered) => Some(declare(formerFqn, registered.appended(deletedEvolution)))
      case _                => Some(Array(deletedEvolution))
    }
  }

  private[api] def findVersionInAnnotations[A](currentClass: Class[_], annotations: Seq[Any]): Int = annotations
    .collectFirst {
      case version(c) if c >= 0 => c
      case version(c)           => throw VersionNotAllowedException(currentClass, c)
    }
    .getOrElse(0)

  @VisibleForTesting
  private[api] def reset(): Unit = {
    org.apache.flinkx.api.auto.cache.clear()
    currentClassToEvolutions.clear()
  }

}
