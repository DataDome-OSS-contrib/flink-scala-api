package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.Evolution.{NoEvolution, isDeletedClass}
import org.apache.flinkx.api.evolution.Evolutions.Declaration
import org.apache.flinkx.api.serializer.ConstructorCompat
import org.apache.flinkx.api.util.ClassUtil

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

  /** The class names to register: the ones of the ADT, and of the case objects among its subtypes. */
  private[evolution] def declarations: Seq[Declaration] = builders().flatMap(_.build())

}

/** Global registry of the annotation-based schema evolution feature, and the macro building the [[Evolutions]] of an
  * ADT (see [[Evolving]]).
  *
  * Schema evolution lets a Flink job restore from a former savepoint whose ADT (case class, sealed trait or Scala 3
  * enum) schema differs from the one currently declared in source code. Users opt in per ADT by adding the [[version]]
  * annotation and describe each change with [[EvolutionAnnotation]]s.
  *
  * This schema evolution feature commonly employs the following vocabulary to qualify version, class, field, etc.:
  *   - `Former` describes the serialization time when the savepoint was done.
  *   - `Current` describes the deserialization time with the current source code.
  *
  * Lifecycle:
  *   - The companion of a versioned ADT extends [[Evolving]] and holds its [[Evolutions]], built from its annotations
  *     at compile time. The derivation of the type information only reads the version of the ADT, see [[Evolvable]]: a
  *     derived serializer writes the current schema, which nothing evolves.
  *   - The first lookup through a class loader scans the jars it loads from for the companions extending [[Evolving]],
  *     initializes all of them and registers all their declarations: a former name may be declared by any of them, a
  *     renamed or deleted ADT, or even a name another class bears now. A declaration failing fails every lookup alike.
  *   - The Scala execution environment [[check]]s the declarations when it submits the job, so that a job declaring
  *     them wrongly fails to start rather than at its first restore.
  *   - At restore time, the ADT serializer snapshots [[get]] the evolution of the former name and version they record,
  *     and at deserialization time the restored serializers apply it.
  *
  * A class name is declared up to the last version of the data written under it, see [[EvolutionBuilder.build]]: a
  * renamed or deleted class name resolves to the current class or to a deletion marker, with the evolution computed for
  * the version the data was written at.
  *
  * Packaging: the declarations are kept per class loader looking them up, the one of the job, so two jobs sharing this
  * library never read each other's. With the default child-first class loading, this library lives in the application
  * jar and a job has a registry entirely of its own; putting it in the `lib` directory of the cluster instead makes the
  * registry itself shared, and its entries then outlive the jobs that filled them, holding their class loaders.
  *
  * Concurrency: the Flink tasks of a TaskManager restore their state in parallel, so they look up concurrently. The
  * declarations are registered under a lock, a thread arriving meanwhile waiting for them. A companion never looks up
  * while initializing: the derivation it typically runs doesn't read the evolutions.
  */
@Internal
object Evolutions extends EvolutionsFactory {

  /** All the evolutions declared on an ADT and stored in the registry.
    * @param versions
    *   The evolution of the data written under that name at each version, from 0 up to the last one
    * @param declaredBy
    *   The ADT whose annotations declare that name
    */
  private[evolution] final case class Declaration(
      formerClassName: String,
      versions: IndexedSeq[Evolution[_]],
      declaredBy: Class[_]
  ) {

    /** The last version of the data written under that name. */
    def lastVersion: Int = versions.length - 1

    /** The class that name stands for now, or the marker of a deleted class. */
    def currentClass: Class[_] = versions.head.currentClass

  }

  // The ADTs of two jobs sharing this library are loaded by two different class loaders, so their declarations are
  // held apart: a same class name then resolves to the class of the job asking for it, and dies with it
  private val registries: concurrent.Map[ClassLoader, Registry] = concurrent.TrieMap.empty

  /** The declarations of the companions one class loader loads, registered on first use. */
  private final class Registry(classLoader: ClassLoader) {

    // A failure is kept, to be reported again on every lookup
    private lazy val declared: Try[Map[String, Seq[Declaration]]] = Try(declareCompanions(classLoader))

    def declarations: Map[String, Seq[Declaration]] = declared.get

  }

  /** The registry of the given class loader, created on first use. */
  private def registryOf(classLoader: ClassLoader): Registry =
    registries.getOrElseUpdate(classLoader, new Registry(classLoader))

  /** Initialize the companions extending [[Evolving]] found in the class loader, and register all their declarations.
    *
    * @throws FormerClassConflictException
    *   if a former name stands for two classes in the data of the same version
    */
  private def declareCompanions(cl: ClassLoader): Map[String, Seq[Declaration]] =
    CompanionScan
      .companionsOf(cl)
      .flatMap(name => evolutionsOf(initialize(name, cl)))
      .flatMap(_.declarations)
      .groupBy(_.formerClassName)
      .map { case (className, declarations) => className -> sortDeclarations(className, declarations, cl) }

  private def initialize(companion: String, cl: ClassLoader): Class[_] =
    try Class.forName(companion, true, cl)
    // The JVM reports the failure of an initialization once: it is kept to be reported again on every lookup
    catch { case failure: LinkageError => throw new FlinkRuntimeException(s"Cannot initialize $companion", failure) }

  /** The evolutions the given initialized companion declares, none if it is not a top-level object. */
  private def evolutionsOf(companion: Class[_]): Option[Evolutions[_]] =
    Try(companion.getField("MODULE$").get(null)).toOption.collect { case evolving: Evolving[_] => evolving.evolutions }

  /** Sort the declarations of a class name, dropping the duplicates, and reject a name standing for two classes.
    * @throws FormerClassConflictException
    *   if the name stands for two classes in the data of the same version
    */
  private def sortDeclarations(className: String, declarations: Seq[Declaration], cl: ClassLoader): Seq[Declaration] = {
    // Evolutions.get reads the first declaration whose last version is at least the version the data was written at
    val sorted = declarations
      .distinctBy(declaration => (declaration.lastVersion, declaration.currentClass))
      .sortBy(_.lastVersion)
    checkBearer(className, sorted, cl)
    sorted
      .groupBy(_.lastVersion)
      .collectFirst { case (_, sameVersion) if sameVersion.length > 1 => sameVersion }
      .foreach { decls => throw FormerClassConflictException(className, standsFor(decls(0)), standsFor(decls(1))) }
    sorted
  }

  /** Reject a class bearing the former name of another class without being versioned above it: the data written under
    * that name is told apart by its version only. A class no companion declares counts as unversioned.
    */
  private def checkBearer(className: String, declarations: Seq[Declaration], cl: ClassLoader): Unit = {
    val (current, former) = declarations.partition(_.currentClass.getName == className)
    former.lastOption.foreach { last =>
      val bearer = current.headOption
        .map(declaration => declaration.currentClass -> declaration.lastVersion)
        .orElse(classNamed(className, cl).map(_ -> 0))
      bearer.filter { case (_, version) => version <= last.lastVersion }.foreach { case (clazz, version) =>
        throw FormerClassConflictException(className, standsFor(last), s"used by ${clazz.getName} at version $version")
      }
    }
  }

  /** What the given former name declaration stands for, in the message of a conflict. */
  private def standsFor(declaration: Declaration): String =
    if (isDeletedClass(declaration.currentClass))
      s"deleted by ${declaration.declaredBy.getName} up to version ${declaration.lastVersion}"
    else s"renamed to ${declaration.currentClass.getName} up to version ${declaration.lastVersion}"

  /** The class of the given name whose data may be recorded under it, including case object. */
  private def classNamed(className: String, cl: ClassLoader): Option[Class[_]] =
    (try Some(Class.forName(className, false, cl))
    catch { case _: ClassNotFoundException | _: LinkageError => None })
      .filterNot(clazz => ClassUtil.isObjectClass(clazz) && !classOf[Product].isAssignableFrom(clazz))

  /** Return the [[Evolution]] of the data written under the given class name at the given version, or
    * [[Evolution.NoEvolution]] otherwise.
    *
    * The first lookup through the given class loader registers the declarations of all the companions it loads.
    *
    * @throws MissingEvolutionException
    *   if no evolution is declared for that name, and it can't be loaded either
    * @throws Throwable
    *   the failure of a declaration, if registering them failed
    */
  def get[T](className: String, writtenVersion: Int, cl: ClassLoader): Evolution[T] =
    registryOf(cl).declarations
      .get(className)
      .flatMap(_.find(_.lastVersion >= writtenVersion))
      .map(_.versions(writtenVersion))
      .getOrElse {
        val currentClass =
          try Class.forName(className, false, cl)
          catch { case _: ClassNotFoundException => throw MissingEvolutionException(className, writtenVersion) }
        new NoEvolution(currentClass, writtenVersion, ConstructorCompat.lookupFieldNames(currentClass))
      }
      .asInstanceOf[Evolution[T]]

  /** Register the declarations of the companions the job loads, from the thread running its main.
    *
    * @throws Throwable
    *   the failure of a declaration, if registering them failed
    */
  private[api] def check(): Unit = registryOf(Thread.currentThread.getContextClassLoader).declarations

  /** Return the [[Evolution]] of the given class at the given version, as a snapshot recording them restores it. */
  @VisibleForTesting
  private[api] def get[T](clazz: Class[T], version: Int): Evolution[T] =
    get[T](clazz.getName, version, clazz.getClassLoader)

  /** Every [[Declaration]] registered by the given class loader, by class name. */
  @VisibleForTesting
  private[api] def declaredEvolutions(cl: ClassLoader): Map[String, Seq[Declaration]] =
    registries.get(cl).fold(Map.empty[String, Seq[Declaration]])(_.declarations)

  /** Names of every class declared by any class loader, such as the one Flink hands the snapshots. */
  @VisibleForTesting
  private[api] def declaredClassNames: Set[String] = registries.values.flatMap(_.declarations.keySet).toSet

  /** Empty the registries: the declarations are registered again on the next lookup. */
  @VisibleForTesting
  private[api] def reset(): Unit = registries.clear()

}
