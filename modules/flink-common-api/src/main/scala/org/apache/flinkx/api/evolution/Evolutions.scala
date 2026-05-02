package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.util.ClassUtil
import org.apache.flinkx.api.version

import java.io.IOException
import scala.annotation.StaticAnnotation
import scala.collection.concurrent
import scala.util.Try

/** Global registry and entry point for the annotation-based schema evolution feature.
  *
  * Schema evolution lets a Flink job restore from a checkpoint whose ADT (case class, sealed trait or Scala 3 enum)
  * schema differs from the one currently declared in source code. Users opt in per ADT by adding [[version @version]]
  * annotation and describe each change with [[Evolved]] annotations.
  *
  * Lifecycle:
  *   - At derivation time, [[org.apache.flinkx.api.TypeInformationDerivation]] reads annotations from the current
  *     source code and [[register]] an immutable [[Evolution]] for that ADT class.
  *   - At deserialization time, the ADT serializers [[get]] the evolutions and apply them in correct order.
  *
  * Class renames and deletions are resolved through [[registerFormerClass]] / [[registerDeletedFormerClass]] /
  * [[resolveFormerClass]], which translate the fully-qualified class names recorded in the snapshot into the classes
  * that exist in the current source code (or a deletion marker).
  *
  * Concurrency: registration runs at startup during derivation (single-threaded per ADT); [[get]] and
  * [[resolveFormerClass]] are safe to call concurrently from multiple Flink tasks afterward. */
object Evolutions {

  private val initEvolutions: Seq[(Class[_], Evolution[_])] = Seq((DeletedClass, Evolution.DeletedClassEvolution))

  // Maps filled at startup time during the ADT derivation with up-to-date information from the source code and used
  // during deserialization
  private val classToEvolutions: concurrent.Map[Class[_], Evolution[_]]       = concurrent.TrieMap(initEvolutions: _*)
  private val formerClassNameToCurrentClass: concurrent.Map[String, Class[_]] = concurrent.TrieMap.empty

  /** Build the [[Evolution]] from the given builder and register it for the deserialization phase. */
  def register[T](builder: EvolutionBuilder[T]): Unit =
    classToEvolutions.put(builder.clazz, builder.build())

  /** Return the [[Evolution]] associated with the given class, or [[Evolution.NoEvolution]] otherwise.
    *
    * @param clazz
    *   ADT class to deserialize and evolve
    */
  def get[T](clazz: Class[T]): Evolution[T] =
    classToEvolutions.getOrElse(clazz, Evolution.NoEvolution).asInstanceOf[Evolution[T]]

  /** Register the mapping between a former ADT class name and the ADT class currently declared on the source code.
    *
    * @param formerClassName
    *   Name of the ADT class as declared when the data was serialized (simple, relative or absolute)
    * @param currentClass
    *   ADT class being derived, used as reference to resolve `formerClassName` to its fully-qualified internal form
    */
  def registerFormerClass(formerClassName: String, currentClass: Class[_]): Unit =
    formerClassNameToCurrentClass(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) = currentClass

  /** Register that a former ADT subtype or field type has been deleted from the current source code.
    *
    * @param formerClassName
    *   Name of the deleted subtype or field type (simple, relative or absolute)
    * @param currentClass
    *   ADT class being derived, used as reference to resolve `formerClassName` to its fully-qualified internal form
    */
  def registerDeletedFormerClass(formerClassName: String, currentClass: Class[_]): Unit =
    formerClassNameToCurrentClass(ClassUtil.resolveFormerClassName(formerClassName, currentClass)) = DeletedClass

  /** Resolve a fully-qualified class name read from a snapshot to the class currently declared in source code:
    *   - Returns the class registered via [[registerFormerClass]] if it was renamed.
    *   - Returns [[Evolution.DeletedClass]] if the name was registered via [[registerDeletedFormerClass]].
    *   - Otherwise loads the class by name through the user-code class loader.
    *
    * @param formerClassName
    *   Fully qualified class name as recorded in the snapshot
    * @param cl
    *   The user code class loader
    * @throws IOException
    *   if the class is not registered and cannot be loaded
    */
  def resolveFormerClass[T](formerClassName: String, cl: ClassLoader): Class[T] =
    formerClassNameToCurrentClass
      .getOrElse(
        formerClassName,
        Try(Class.forName(formerClassName, false, cl)).recover { case e: ClassNotFoundException =>
          throw new IOException(e) // Iso InstantiationUtil.resolveClassByName
        }.get
      )
      .asInstanceOf[Class[T]]

  private[api] def findVersion[A](clazz: Class[_]): PartialFunction[A, Int] = {
    case version(c) if c > 0 => c
    case version(c) => throw new FlinkRuntimeException(s"Current version of $clazz must be positive, got @version($c)")
  }

  private[api] def throwEvolutionNotAllowed[A](evolution: StaticAnnotation, target: String): A =
    throw new FlinkRuntimeException(s"@$evolution annotation is not allowed on $target")

  @VisibleForTesting
  private[api] def reset(): Unit = {
    classToEvolutions.clear()
    classToEvolutions.addAll(initEvolutions)
    formerClassNameToCurrentClass.clear()
  }

}
