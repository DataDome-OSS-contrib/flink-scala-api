package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.{DeletedEvolution, NoEvolution}

import scala.collection.concurrent
import scala.util.Try

/** The evolutions an ADT declares through its annotations, built at compile time by [[Evolutions.apply]] in the
  * companion of the ADT.
  *
  * Nothing is built before they are needed: the companion may still be initializing, and the mappers the annotations
  * name may live in it.
  *
  * @param currentClass
  *   The ADT class declaring the evolutions
  * @param builders
  *   Builds the evolutions from the annotations of the ADT, and of the case objects among its subtypes, which have no
  *   companion of their own to declare them
  */
@Internal
final class Evolutions[T](val currentClass: Class[T], builders: () => Seq[EvolutionBuilder[_]]) {

  /** The evolutions to register, per class name: the ones of the ADT, and of the case objects among its subtypes. */
  private[evolution] def byClassName: Seq[(String, Array[Evolution[_]])] = builders().flatMap(_.build())

  /** The evolution of `clazz` at its current `version`, the ADT or one of its case objects, none if it declares none.
    */
  private[evolution] def evolution[A](clazz: Class[A], version: Int): Evolution[A] =
    builders()
      .find(_.currentClass == clazz)
      .flatMap(_.build().get(clazz.getName))
      .flatMap(_.find(_.formerVersion >= version))
      .getOrElse(new NoEvolution(clazz, version))
      .asInstanceOf[Evolution[A]]

}

/** Global registry of the annotation-based schema evolution feature, and the macro building the [[Evolutions]] of an
  * ADT (see [[Evolving]]).
  *
  * Schema evolution lets a Flink job restore from a former checkpoint whose ADT (case class, sealed trait or Scala 3
  * enum) schema differs from the one currently declared in source code. Users opt in per ADT by adding the [[version]]
  * annotation and describe each change with [[EvolutionAnnotation]]s.
  *
  * This schema evolution feature commonly employs the following vocabulary to qualify version, class, field, etc.:
  *   - `Former` describes the serialization time when the checkpoint was done.
  *   - `Current` describes the deserialization time with the current source code.
  *
  * Lifecycle:
  *   - The companion of a versioned ADT extends [[Evolving]] and holds its [[Evolutions]], built from its annotations
  *     at compile time. The derivation of the type information reads them from there, see [[Evolvable]].
  *   - The first lookup through a class loader scans the jars it loads from for the companions extending [[Evolving]],
  *     initializes all of them and registers all their declarations: a former name may be declared by any of them, a
  *     renamed or deleted ADT, or even a name another class bears now. A declaration failing fails every lookup alike.
  *   - The Scala execution environment [[check]]s the declarations when it submits the job, so that a job declaring
  *     them wrongly fails to start rather than at its first restore.
  *   - At restore time, the ADT serializer snapshots [[get]] the evolution of the former name and version they record,
  *     and at deserialization time the restored serializers apply it.
  *
  * Class renames and deletions are resolved through [[EvolutionBuilder.renameClass]] /
  * [[EvolutionBuilder.deleteClass]], which translate the fully-qualified class names recorded in the snapshot into the
  * current classes (or a deletion marker).
  *
  * Packaging: the declarations are kept per class loader looking them up, the one of the job, so two jobs sharing this
  * library never read each other's. With the default child-first class loading, this library lives in the application
  * jar and a job has a registry entirely of its own; putting it in the `lib` directory of the cluster instead makes the
  * registry itself shared, and its entries then outlive the jobs that filled them, holding their class loaders.
  *
  * Concurrency: the Flink tasks of a TaskManager restore their state in parallel, so they look up concurrently. The
  * declarations are registered under a lock, a thread arriving meanwhile waiting for them. A companion never looks up
  * while initializing: the derivation it typically runs reads its own evolutions.
  */
@Internal
object Evolutions extends EvolutionsFactory {

  /** The evolutions of an ADT whose companion declares none. */
  val none: Evolutions[_] = new Evolutions(classOf[AnyRef], () => Nil)

  // The ADTs of two jobs sharing this library are loaded by two different class loaders, so their declarations are
  // held apart: a same class name then resolves to the class of the job asking for it, and dies with it
  private val registries: concurrent.Map[ClassLoader, Registry] = concurrent.TrieMap.empty

  /** The declarations of the companions one class loader loads, registered on first use. */
  private final class Registry(classLoader: ClassLoader) {

    // A failure is kept, to be reported again on every lookup
    private lazy val declared: Try[Map[String, Array[Evolution[_]]]] = Try(declareCompanions(classLoader))

    def evolutions: Map[String, Array[Evolution[_]]] = declared.get

  }

  /** The registry of the given class loader, created on first use. */
  private def registryOf(classLoader: ClassLoader): Registry =
    registries.getOrElseUpdate(classLoader, new Registry(classLoader))

  /** Initialize the companions extending [[Evolving]] found in the jars of the given class loader, and register all
    * their declarations.
    *
    * @throws FormerClassConflictException
    *   if two declarations claim the same former name
    */
  private def declareCompanions(cl: ClassLoader): Map[String, Array[Evolution[_]]] =
    CompanionScan
      .companionsOf(cl)
      .flatMap(name => evolutionsOf(initialize(name, cl)))
      .flatMap(_.byClassName)
      .groupMapReduce(_._1)(_._2)(_ ++ _)
      .map { case (className, evolutions) => className -> sortDeclarations(className, evolutions) }

  private def initialize(companion: String, cl: ClassLoader): Class[_] =
    try Class.forName(companion, true, cl)
    // The JVM reports the failure of an initialization once: it is kept to be reported again on every lookup
    catch { case failure: LinkageError => throw new FlinkRuntimeException(s"Cannot initialize $companion", failure) }

  /** The evolutions the given initialized companion declares, none if it is not a top-level object. */
  private def evolutionsOf(companion: Class[_]): Option[Evolutions[_]] =
    Try(companion.getField("MODULE$").get(null)).toOption.collect { case evolving: Evolving[_] => evolving.evolutions }

  /** Sort the evolutions of a class name, dropping the duplicates, and reject a name claimed by two different ADTs.
    * @throws FormerClassConflictException
    *   if two evolutions declared for the same version resolve to different current classes
    */
  private def sortDeclarations(className: String, evolutions: Array[Evolution[_]]): Array[Evolution[_]] = {
    def descr(evolution: Evolution[_]) = evolution match {
      case _: DeletedEvolution[_] => "deleted"
      case _                      => s"renamed to ${evolution.currentClass}"
    }

    val declarations = evolutions.distinctBy(evolution => (evolution.formerVersion, evolution.currentClass))
    declarations
      .groupBy(_.formerVersion)
      .collectFirst { case (_, sameVersion) if sameVersion.length > 1 => sameVersion }
      .foreach { conflict => throw FormerClassConflictException(className, descr(conflict(0)), descr(conflict(1))) }
    // Evolutions.get returns the first evolution whose version is at least the former version being restored
    declarations.sortBy(_.formerVersion)
  }

  /** Return the [[Evolution]] associated with the given former ADT class name, or [[Evolution.NoEvolution]] of the
    * class loaded by name otherwise.
    *
    * The first lookup through the given class loader registers the declarations of all the companions it loads.
    *
    * @throws MissingEvolutionException
    *   if no evolution is declared for that name, and it can't be loaded either
    * @throws Throwable
    *   the failure of a declaration, if registering them failed
    */
  def get[T](formerClassName: String, formerVersion: Int, cl: ClassLoader): Evolution[T] =
    registryOf(cl).evolutions
      .get(formerClassName)
      .flatMap(_.find(_.formerVersion >= formerVersion))
      .getOrElse {
        val currentClass =
          try Class.forName(formerClassName, false, cl)
          catch { case _: ClassNotFoundException => throw MissingEvolutionException(formerClassName, formerVersion) }
        new NoEvolution(currentClass, formerVersion)
      }
      .asInstanceOf[Evolution[T]]

  /** Register the declarations of the companions the job loads, from the thread running its main.
    *
    * @throws Throwable
    *   the failure of a declaration, if registering them failed
    */
  private[api] def check(): Unit = registryOf(Thread.currentThread.getContextClassLoader).evolutions

  /** Return the [[Evolution]] of the given class at the given version, as a snapshot recording them restores it. */
  @VisibleForTesting
  private[api] def get[T](clazz: Class[T], version: Int): Evolution[T] =
    get[T](clazz.getName, version, clazz.getClassLoader)

  /** Every [[Evolution]] registered by the given class loader, by class name. */
  @VisibleForTesting
  private[api] def declaredEvolutions(cl: ClassLoader): Map[String, Array[Evolution[_]]] =
    registries.get(cl).fold(Map.empty[String, Array[Evolution[_]]])(_.evolutions)

  /** Names of every class declared by any class loader, such as the one Flink hands the snapshots. */
  @VisibleForTesting
  private[api] def declaredClassNames: Set[String] = registries.values.flatMap(_.evolutions.keySet).toSet

  /** Empty the registries: the declarations are registered again on the next lookup. */
  @VisibleForTesting
  private[api] def reset(): Unit = registries.clear()

}
