package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.evolution.EvolutionBuilder.AdtDeclaration
import org.apache.flinkx.api.util.ClassUtil

import scala.collection.concurrent
import scala.util.control.NonFatal

/** The evolutions an ADT declares through its annotations, built at compile time by [[Evolutions.apply]] in the
  * companion of the ADT, and applied by the registry once that companion is initialized.
  *
  * Nothing is built before then: the companion may still be initializing, and the mappers the annotations name may live
  * in it.
  *
  * @param currentClass
  *   The ADT class declaring the evolutions
  * @param builders
  *   Builds the evolutions from the annotations of the ADT, and of the case objects among its subtypes, which have no
  *   companion of their own to declare them
  * @param memberClasses
  *   The subtypes of a sealed trait that are classes, whose own companions declare their own evolutions
  */
@Internal
final class Evolutions[T](
    val currentClass: Class[T],
    builders: () => Seq[EvolutionBuilder[_]],
    val memberClasses: () => Seq[Class[_]]
) {
  Evolutions.pending(this)

  private[evolution] def build(): Seq[EvolutionBuilder[_]] = builders()
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
  *     at compile time and handed over here when the companion initializes.
  *   - A lookup that finds nothing initializes the companion of the name it looks for and applies its declaration: the
  *     snapshot of a former ADT pulls the declaration in while the checkpoint is read, before any state is restored,
  *     wherever the state descriptor was built. Nothing else is applied: a declaration handed over is applied when its
  *     own class is looked up, so a faulty one never gets in the way of another.
  *   - A former name that no class bears anymore, a renamed or deleted ADT, is declared by another companion: the jars
  *     of the job are scanned once for the companions extending [[Evolving]], and all of them are initialized.
  *   - At restore time, the ADT serializer snapshots [[get]] the evolution of the former name and version they record,
  *     and at deserialization time the restored serializers apply it.
  *
  * Class renames and deletions are resolved through [[EvolutionBuilder.registerFormerClass]] /
  * [[EvolutionBuilder.registerDeletedFormerClass]], which translate the fully-qualified class names recorded in the
  * snapshot into the current classes (or a deletion marker).
  *
  * Packaging: the declarations are kept per class loader looking them up, the one of the job, so two jobs sharing this
  * library never read each other's. With the default child-first class loading, this library lives in the application
  * jar and a job has a registry entirely of its own; putting it in the `lib` directory of the cluster instead makes the
  * registry itself shared, and its entries then outlive the jobs that filled them, holding their class loaders.
  *
  * Concurrency: the Flink tasks of a TaskManager restore their state in parallel, so they declare and look up
  * concurrently. The declarations are applied under a lock, a thread arriving meanwhile waiting for them rather than
  * taking a declaration for missing, and the companions are initialized outside of it, as their initialization may look
  * up in turn.
  */
@Internal
object Evolutions extends EvolutionsFactory {

  // The ADTs of two jobs sharing this library are loaded by two different class loaders, so their declarations are
  // held apart: a same class name then resolves to the class of the job asking for it, and dies with it
  private val registries: concurrent.Map[ClassLoader, Registry] = concurrent.TrieMap.empty

  // Every declaration a companion handed over, by companion class: a companion initializes once per JVM, so the
  // declarations are kept for good and applied again to a registry emptied by a reset
  private val declarations: concurrent.Map[Class[_], Evolutions[_]] = concurrent.TrieMap.empty

  /** Declarations of the ADTs one class loader loads, and the companions found in the jars it loads from. */
  private final class Registry(val classLoader: ClassLoader) {

    private val evolutions: concurrent.Map[String, Array[Evolution[_]]] = concurrent.TrieMap.empty

    // Applied once each: the identity of a declaration is the one of the companion that built it
    val applied: concurrent.Map[Evolutions[_], Unit] = concurrent.TrieMap.empty

    // A declaration that fails is reported when its own class is looked up, and never blocks another ADT
    val failed: concurrent.Map[String, Throwable] = concurrent.TrieMap.empty

    // The jars are listed once: a companion initializes once, so initializing them all again is cheap
    lazy val companions: Seq[String] = CompanionScan.companionsOf(classLoader)

    def declare(className: String, declared: Array[Evolution[_]]): Unit =
      evolutions.updateWith(className) {
        case Some(registered) => Some(sortDeclarations(className, registered ++ declared))
        case _                => Some(sortDeclarations(className, declared))
      }

    /** The evolution declared for the given former name at the given former version, if any. */
    def find(formerName: String, formerVersion: Int): Option[Evolution[_]] =
      evolutions.get(formerName).flatMap(_.find(_.formerVersion >= formerVersion))

    def declarations: Map[String, Array[Evolution[_]]] = evolutions.toMap

    /** Sort the evolutions of a class name, dropping the declarations already registered, and reject a name claimed by
      * two different ADTs.
      * @throws FormerClassConflictException
      *   if two evolutions declared for the same version resolve to different current classes
      */
    private def sortDeclarations(className: String, evolutions: Array[Evolution[_]]): Array[Evolution[_]] = {
      def descr(currentClass: Class[_]) = if (isDeletedClass(currentClass)) "deleted" else s"renamed to $currentClass"

      val declarations = evolutions.distinctBy(evolution => (evolution.formerVersion, evolution.currentClass))
      declarations
        .groupBy(_.formerVersion)
        .collectFirst { case (_, sameVersion) if sameVersion.length > 1 => sameVersion }
        .foreach { conflicting =>
          val classes = conflicting.map(_.currentClass)
          throw FormerClassConflictException(className, descr(classes(0)), descr(classes(1)))
        }
      // Evolutions.get returns the first evolution whose version is at least the former version being restored
      declarations.sortBy(_.formerVersion)
    }

  }

  /** Hand over the declared evolutions of an ADT, applied by the first lookup of its class. Called when they are built,
    * which the companion of the ADT does when it initializes.
    *
    * Keyed by the companion class, a case object being its own: two jobs sharing this library may define an ADT of the
    * same name each.
    */
  private[evolution] def pending(evolutions: Evolutions[_]): Unit = {
    val adtClass = evolutions.currentClass
    try
      declarations.put(
        Class.forName(ClassUtil.companionName(adtClass.getName), false, adtClass.getClassLoader),
        evolutions
      )
    catch { case _: ClassNotFoundException => } // An ADT without companion object cannot be looked up by name
  }

  private def register(registry: Registry, declaration: AdtDeclaration): Unit =
    declaration.byClassName.foreachEntry(registry.declare)

  /** The registry of the given class loader, created on first use. */
  private def registryOf(classLoader: ClassLoader): Registry =
    registries.getOrElseUpdate(classLoader, new Registry(classLoader))

  /** Initialize the companion of the given ADT class name through the given class loader and apply the declaration it
    * handed over to the registry of that loader, if any, then do the same with the subtypes it declares, whose own
    * companions declare their own evolutions.
    *
    * The registrations run under the lock, so a concurrent lookup waits for them. The initializations run outside of
    * it: a companion may look up while initializing, from another thread holding its class initialization lock.
    */
  private def declare(className: String, cl: ClassLoader): Unit = {
    // A case object is its own companion
    val companion =
      try Some(Class.forName(ClassUtil.companionName(className), false, cl))
      catch { case _: ClassNotFoundException => None }
    // Only a companion extending Evolving is initialized, so that it hands its evolutions over: no other one is touched
    val evolving = companion.filter(classOf[Evolving[_]].isAssignableFrom)
    evolving.foreach(clazz => Class.forName(clazz.getName, true, clazz.getClassLoader))
    evolving.flatMap(declarations.get).foreach { evolutions =>
      applyDeclaration(evolutions, cl).foreach(member => declare(member.getName, cl))
    }
  }

  /** Apply the given declaration once to the registry of the given class loader, and return the subtypes it declares,
    * to declare in turn.
    */
  private def applyDeclaration(evolutions: Evolutions[_], cl: ClassLoader): Seq[Class[_]] = synchronized {
    val registry = registryOf(cl)
    if (registry.applied.putIfAbsent(evolutions, ()).isDefined) Nil
    else
      try {
        evolutions.build().foreach(builder => register(registry, builder.build()))
        evolutions.memberClasses()
      } catch {
        case NonFatal(failure) => registry.failed.put(evolutions.currentClass.getName, failure); Nil
      }
  }

  /** Initialize the companions extending [[Evolving]] found in the jars of the given class loader.
    *
    * This is how a former name that no class bears anymore, renamed or deleted, gets declared: the companion declaring
    * it is the only one that knows, and nothing but the jars lists the companions.
    */
  private def declareScannedCompanions(cl: ClassLoader): Unit =
    registryOf(cl).companions.foreach(declare(_, cl))

  /** `true` if the given current class is the marker of a former class registered as deleted. */
  def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Return the [[Evolution]] declared for the given former ADT class name at the given former version, if any.
    *
    * Unlike [[get]], never loads the class of that name, so it also answers for the names that designate no class
    * anymore. Returning `None` means nothing was declared for that name, not that the class is unknown.
    *
    * A lookup finding nothing initializes the companion of that name, which hands its declaration over, and applies it.
    * The declarations are looked up in the registry of the given class loader only: the companions it initializes are
    * the ones the job sees, so the classes they declare are the ones of the job.
    *
    * @param formerName
    *   Former fully qualified class name
    * @param formerVersion
    *   Former schema version of the ADT
    * @param cl
    *   Class loader the former ADT is looked up for
    * @throws Throwable
    *   the failure of the declaration of that name, if declaring it failed
    */
  def find[T](formerName: String, formerVersion: Int, cl: ClassLoader): Option[Evolution[T]] =
    lookUp[T](formerName, formerVersion, cl).orElse {
      // A class bearing that name is declared by its own companion
      declare(formerName, cl)
      lookUp[T](formerName, formerVersion, cl)
    }

  private def lookUp[T](formerName: String, formerVersion: Int, cl: ClassLoader): Option[Evolution[T]] = {
    val registry = registryOf(cl)
    registry.failed.get(formerName).foreach(failure => throw failure)
    registry.find(formerName, formerVersion).map(_.asInstanceOf[Evolution[T]])
  }

  /** Return the [[Evolution]] the given ADT class declares at the given version, or [[Evolution.noEvolution]] when it
    * declares none.
    *
    * @throws EvolutionNotDeclaredException
    *   if the ADT declares a version but found nothing in the registry
    */
  def get[T](clazz: Class[T], currentVersion: Int): Evolution[T] =
    find[T](clazz.getName, currentVersion, clazz.getClassLoader).getOrElse {
      if (currentVersion > 0) throw EvolutionNotDeclaredException(clazz.getName, currentVersion, restoring = false)
      Evolution.noEvolution(clazz, currentVersion)
    }

  /** Return the [[Evolution]] associated with the given former ADT class name, or [[Evolution.noEvolution]] of the
    * class loaded by name otherwise.
    *
    * A former name that no class bears anymore is declared by another companion, found by scanning the jars.
    *
    * @throws EvolutionNotDeclaredException
    *   if no evolution is declared for that name, and it can't be loaded either
    */
  def get[T](formerClassName: String, formerVersion: Int, cl: ClassLoader): Evolution[T] =
    if (formerClassName == null) Evolution.NoEvolution.asInstanceOf[Evolution[T]] // Snapshot written before 2.4.0
    else
      find[T](formerClassName, formerVersion, cl).getOrElse {
        // A class that loads declares itself, through its own companion: nothing else has to be looked for
        val currentClass =
          try Some(Class.forName(formerClassName, false, cl).asInstanceOf[Class[T]])
          catch { case _: ClassNotFoundException => None }
        currentClass.map(Evolution.noEvolution(_, formerVersion)).getOrElse {
          declareScannedCompanions(cl)
          lookUp[T](formerClassName, formerVersion, cl)
            .getOrElse(throw EvolutionNotDeclaredException(formerClassName, formerVersion))
        }
      }

  /** Current schema version the given annotations declare for the given ADT class, 0 when they declare none.
    *
    * @throws VersionNotAllowedException
    *   if the declared version is negative
    */
  private[api] def findVersion(currentClass: Class[_], annotations: Seq[Any]): Int =
    annotations.collectFirst { case declared: version => declared.current }.fold(0) { declared =>
      if (declared >= 0) declared else throw VersionNotAllowedException(currentClass, declared)
    }

  /** Every [[Evolution]] registered by the given class loader, by class name. */
  @VisibleForTesting
  private[api] def declaredEvolutions(cl: ClassLoader): Map[String, Array[Evolution[_]]] =
    registries.get(cl).fold(Map.empty[String, Array[Evolution[_]]])(_.declarations)

  /** Names of every class declared by any class loader, such as the one Flink hands the snapshots. */
  @VisibleForTesting
  private[api] def declaredClassNames: Set[String] = registries.values.flatMap(_.declarations.keySet).toSet

  /** Empty the registries. The declarations handed over are kept, and applied again when their class is looked up. */
  @VisibleForTesting
  private[api] def reset(): Unit = registries.clear()

}
