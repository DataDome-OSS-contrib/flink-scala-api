package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.util.ClassUtil
import org.apache.flinkx.api.version

import java.io.IOException
import scala.annotation.StaticAnnotation
import scala.collection.concurrent

/** Global registry and entry point for the annotation-based schema evolution feature.
  *
  * Schema evolution lets a Flink job restore from a former checkpoint whose ADT (case class, sealed trait or Scala 3
  * enum) schema differs from the one currently declared in source code. Users opt in per ADT by adding [[version]]
  * annotation and describe each change with [[Evolved]] annotations.
  *
  * This schema evolution feature commonly employs the following vocabulary to qualify version, class, field, etc.:
  *   - `Former` describes the serialization time when the checkpoint was done.
  *   - `Current` describes the deserialization time with the current source code.
  *
  * Lifecycle:
  *   - At derivation time, [[org.apache.flinkx.api.TypeInformationDerivation]] reads current annotations and
  *     [[register]] an immutable [[Evolution]] for that ADT class.
  *   - At deserialization time, the ADT serializers [[get]] the evolutions and apply them in correct order.
  *
  * Class renames and deletions are resolved through [[registerFormerClass]] / [[registerDeletedFormerClass]] /
  * [[resolveFormerClass]], which translate the fully-qualified class names recorded in the snapshot into the current
  * classes (or a deletion marker).
  *
  * Concurrency: registration runs at startup during derivation (single-threaded per ADT); [[get]] and
  * [[resolveFormerClass]] are safe to call concurrently from multiple Flink tasks afterward.
  */
object Evolutions {

  private val initEvolutions: Seq[(Class[_], Evolution[_])] = Seq((DeletedClass, Evolution.DeletedClassEvolution))

  private val currentClassToEvolutions: concurrent.Map[Class[_], Evolution[_]] = concurrent.TrieMap(initEvolutions: _*)
  private val formerClassNameToCurrentClass: concurrent.Map[String, Class[_]]  = concurrent.TrieMap.empty
  private val deletedFormerClassThrowOnInstance: concurrent.Map[String, Unit]  = concurrent.TrieMap.empty

  /** Build the [[Evolution]] from the given builder and register it for the deserialization phase. */
  def register[T](builder: EvolutionBuilder[T]): Unit =
    currentClassToEvolutions.put(builder.currentClass, builder.build())

  /** Return the [[Evolution]] associated with the given ADT class, or [[Evolution.NoEvolution]] otherwise. */
  def get[T](clazz: Class[T]): Evolution[T] =
    currentClassToEvolutions.getOrElse(clazz, Evolution.NoEvolution).asInstanceOf[Evolution[T]]

  /** Register the mapping between a former ADT class name and the current ADT class.
    *
    * @param formerClassName
    *   Name of the former ADT class as declared when the data was serialized (simple, relative or absolute)
    * @param currentClass
    *   Current ADT class, used as reference to resolve `formerClassName` to its fully-qualified internal form
    */
  def registerFormerClass(formerClassName: String, currentClass: Class[_]): Unit =
    formerClassNameToCurrentClass(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) = currentClass

  /** Register that a former ADT subtype or field type has been deleted from the current source code.
    *
    * @param formerClassName
    *   Name of the deleted former subtype or field type (simple, relative or absolute)
    * @param currentClass
    *   Current ADT class, used as reference to resolve `formerClassName` to its fully-qualified internal form
    * @param throwOnInstance
    *   If `true`, encountering an instance of this former class during deserialization throws via
    *   [[checkThrowOnInstance]]. If `false`, the instance is deserialized as `null`.
    */
  def registerDeletedFormerClass(formerClassName: String, currentClass: Class[_], throwOnInstance: Boolean): Unit = {
    val formerFqn = ClassUtil.resolveFormerClassName(formerClassName, currentClass)
    formerClassNameToCurrentClass(formerFqn) = DeletedClass
    if (throwOnInstance) deletedFormerClassThrowOnInstance(formerFqn) = ()
  }

  /** Throw a [[FlinkRuntimeException]] if an instance of the deleted former class identified by `formerFqn` was
    * registered with `throwOnInstance = true`; return instance otherwise.
    *
    * @param instance
    *   The ADT instance to check
    * @param formerFqn
    *   Fully qualified former class name
    */
  def checkThrowOnInstance[T](instance: T, formerFqn: String): T =
    if (instance == null && deletedFormerClassThrowOnInstance.contains(formerFqn)) {
      throw new FlinkRuntimeException(
        s"Encountered an instance of deleted class '$formerFqn' during deserialization. Don't delete a class in usage" +
          s" or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
      )
    } else instance

  /** Resolve a fully-qualified former class name read from a checkpoint to the current class:
    *   - Returns the class registered via [[registerFormerClass]] if it was renamed.
    *   - Returns [[Evolution.DeletedClass]] marker if the name was registered via [[registerDeletedFormerClass]].
    *   - Otherwise loads the class by name through the user-code class loader.
    *
    * @param formerFqn
    *   Fully qualified former class name as recorded in the checkpoint
    * @param cl
    *   The user code class loader
    * @throws IOException
    *   if the class is not registered and cannot be loaded
    */
  def resolveFormerClass[T](formerFqn: String, cl: ClassLoader): Class[T] =
    formerClassNameToCurrentClass
      .getOrElse(
        formerFqn,
        try Class.forName(formerFqn, false, cl)
        catch { // Same behavior as org.apache.flink.util.InstantiationUtil.resolveClassByName
          case e: ClassNotFoundException =>
            throw new IOException(s"Could not find class '$formerFqn' in classpath.", e)
        }
      )
      .asInstanceOf[Class[T]]

  private[api] def findVersionInAnnotations[A](currentClass: Class[_], annotations: Seq[Any]): Int = annotations
    .collectFirst {
      case version(c) if c >= 0 => c
      case version(c)           =>
        throw new FlinkRuntimeException(s"Current version of $currentClass must be positive or 0, got @version($c)")
    }
    .getOrElse(0)

  private[api] def throwEvolutionNotAllowed[A](evolution: StaticAnnotation, target: String): A =
    throw new FlinkRuntimeException(s"@$evolution annotation is not allowed on $target")

  @VisibleForTesting
  private[api] def reset(): Unit = {
    currentClassToEvolutions.clear()
    currentClassToEvolutions.addAll(initEvolutions)
    formerClassNameToCurrentClass.clear()
    deletedFormerClassThrowOnInstance.clear()
  }

}
