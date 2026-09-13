package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flinkx.api.evolution.Evolution.{AdtDeclaration, DeletedClass}
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
  *     [[register]] the immutable [[Evolution]]s of that ADT class.
  *   - On a TaskManager, the derivation never runs: the serializers arrive Java-deserialized from the job graph and
  *     each of them registers again the ADT declaration it carries, before any state is restored.
  *   - At restore time, the ADT serializer snapshots [[get]] the evolution of the former name and version they record,
  *     and at deserialization time the restored serializers apply it.
  *
  * Class renames and deletions are resolved through [[EvolutionBuilder.registerFormerClass]] /
  * [[EvolutionBuilder.registerDeletedFormerClass]], which translate the fully-qualified class names recorded in the
  * snapshot into the current classes (or a deletion marker).
  *
  * Packaging: the declarations are kept per class loader defining the ADTs, so two jobs sharing this library never read
  * each other's. With the default child-first class loading, this library lives in the application jar and a job has a
  * registry entirely of its own; putting it in the `lib` directory of the cluster instead makes the registry itself
  * shared, and its entries then outlive the jobs that filled them, holding their class loaders.
  *
  * Concurrency: registration runs at startup during derivation (single-threaded per ADT) and when the serializers are
  * deserialized; [[get]] is safe to call concurrently from multiple Flink tasks afterward.
  */
@Internal
object Evolutions {

  private type Registry = concurrent.Map[String, Array[Evolution[_]]]

  // The ADTs of two jobs sharing this library are defined by two different class loaders, so their declarations are
  // held apart: a same class name then resolves to the class of the job asking for it, and dies with it
  private val registries: concurrent.Map[ClassLoader, Registry] = concurrent.TrieMap.empty

  /** Build the [[Evolution]]s from the given builder and register them for the deserialization phase. */
  def register[T](builder: EvolutionBuilder[T]): Unit = register(builder.build())

  /** Register again the whole ADT declaration the given [[Evolution]] carries.
    *
    * Called by every ADT serializer when it is Java-deserialized, which is how a TaskManager gets a registry: the
    * derivation runs on the client only, and the job graph carries the serializers, not their registration.
    */
  def register(evolution: Evolution[_]): Unit = register(evolution.declaration)

  private def register(declaration: AdtDeclaration): Unit = if (!declaration.isEmpty) {
    val registry = registries.getOrElseUpdate(definingLoader(declaration.classLoader), concurrent.TrieMap.empty)
    declaration.byClassName.foreachEntry { (className, evolutions) =>
      registry.updateWith(className) {
        case Some(registered) => Some(declare(className, registered ++ evolutions))
        case _                => Some(declare(className, evolutions))
      }
    }
  }

  /** The class loader an ADT is declared by, the one of this library when it is defined by the bootstrap loader. */
  private def definingLoader(classLoader: ClassLoader): ClassLoader =
    if (classLoader == null) getClass.getClassLoader else classLoader

  /** Sort the evolutions of a class name, dropping the declarations already registered, and reject a name claimed by
    * two different ADTs.
    *
    * The same ADT is legitimately registered several times: once per derivation pass, then once per serializer carrying
    * it, so a declaration already known is a no-op. A name also legitimately gets several evolutions, one per version
    * boundary, but at a given former version it must resolve to a single current class.
    *
    * @throws FormerClassConflictException
    *   if two evolutions declared for the same version resolve to different current classes
    */
  private def declare(className: String, evolutions: Array[Evolution[_]]): Array[Evolution[_]] = {
    val declarations = evolutions.distinctBy(evolution => (evolution.version, evolution.currentClass))
    declarations
      .groupBy(_.version)
      .collectFirst { case (_, sameVersion) if sameVersion.length > 1 => sameVersion }
      .foreach { conflicting =>
        val classes = conflicting.map(_.currentClass)
        throw FormerClassConflictException(className, describeDeclaration(classes(0)), describeDeclaration(classes(1)))
      }
    declarations.sorted
  }

  private def describeDeclaration(currentClass: Class[_]): String =
    if (isDeletedClass(currentClass)) "deleted" else s"renamed to $currentClass"

  /** `true` if the given current class is the marker of a former class registered as deleted. */
  def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Return the [[Evolution]] declared for the given former ADT member name at the given former version, if any.
    *
    * Unlike [[get]], never loads any class, so it also answers for the names that designate no class at all: the
    * `<enum binary name>#<value name>` of a Scala 3 enum value, in particular. Returning `None` means nothing was
    * declared for that name, not that the member is unknown.
    *
    * The declarations are looked up in the registry of the given class loader, then in those of its ancestors: an ADT
    * shared by several jobs is declared by the loader defining it, which the loader of each job descends from.
    *
    * @param formerName
    *   Former fully qualified class name, or `<enum binary name>#<value name>` for a Scala 3 enum value
    * @param formerVersion
    *   Former schema version of the ADT declaring that member
    * @param cl
    *   Class loader the former ADT member is looked up for
    */
  private[api] def find[T](formerName: String, formerVersion: Int, cl: ClassLoader): Option[Evolution[T]] =
    LazyList
      .iterate(definingLoader(cl))(_.getParent)
      .takeWhile(_ != null)
      .flatMap(registries.get)
      .flatMap(_.get(formerName))
      .flatMap(_.find(_.version >= formerVersion))
      .headOption
      .map(_.asInstanceOf[Evolution[T]])

  /** Return the [[Evolution]] associated with the given ADT class, or [[Evolution.noEvolution]] otherwise. */
  def get[T](clazz: Class[T], formerVersion: Int): Evolution[T] =
    find[T](clazz.getName, formerVersion, clazz.getClassLoader)
      .getOrElse(Evolution.noEvolution(clazz, formerVersion))

  /** Return the [[Evolution]] associated with the given former ADT class name, or [[Evolution.noEvolution]] of the
    * class loaded by name otherwise.
    *
    * @throws IOException
    *   if no evolution is declared for that name, and it can't be loaded either
    */
  def get[T](formerClassName: String, formerVersion: Int, cl: ClassLoader): Evolution[T] =
    if (formerClassName == null) Evolution.NoEvolution.asInstanceOf[Evolution[T]] // Snapshot written before 2.4.0
    else
      find[T](formerClassName, formerVersion, cl).getOrElse {
        // The data was written by a versioned ADT, so the current source code declares how to migrate it: finding
        // nothing at all under that name means the declaration never reached this JVM, and restoring anyway would
        // read the former form as if it never evolved
        if (formerVersion > 0 && !isDeclared(formerClassName, cl)) {
          throw EvolutionNotDeclaredException(formerClassName, formerVersion)
        }
        val currentClass =
          try Class.forName(formerClassName, false, cl).asInstanceOf[Class[T]]
          catch { // Same behavior as org.apache.flink.util.InstantiationUtil.resolveClassByName
            case e: ClassNotFoundException =>
              throw new IOException(s"Could not find class '$formerClassName' in classpath.", e)
          }
        Evolution.noEvolution(currentClass, formerVersion)
      }

  /** Whether anything at all is declared for the given former ADT member name, at any version.
    *
    * Tells a declaration that never reached this JVM from one that doesn't cover the queried version, which is what a
    * checkpoint written by a more recent source code looks like: that one is reported by the schema compatibility
    * resolution rather than here.
    */
  private def isDeclared(formerName: String, cl: ClassLoader): Boolean =
    LazyList
      .iterate(definingLoader(cl))(_.getParent)
      .takeWhile(_ != null)
      .flatMap(registries.get)
      .exists(_.contains(formerName))

  private[api] def findVersionInAnnotations[A](currentClass: Class[_], annotations: Seq[Any]): Int = annotations
    .collectFirst {
      case version(c) if c >= 0 => c
      case version(c)           => throw VersionNotAllowedException(currentClass, c)
    }
    .getOrElse(0)

  @VisibleForTesting
  private[api] def reset(): Unit = {
    org.apache.flinkx.api.auto.cache.clear()
    registries.clear()
  }

}
