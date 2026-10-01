package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.evolution.EvolutionBuilder.AdtDeclaration

import scala.collection.concurrent
import scala.util.control.NonFatal

/** Global registry and entry point for the annotation-based schema evolution feature.
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
  *   - The companion of a versioned ADT extends [[Evolved]], which builds a [[Declaration]] of its annotations at
  *     compile time and hands it over here when the companion initializes.
  *   - A lookup that finds nothing initializes the companion of the name it looks for and applies its declaration: the
  *     snapshot of a former ADT pulls the declaration in while the checkpoint is read, before any state is restored,
  *     wherever the state descriptor was built. Nothing else is applied: a declaration handed over is applied when its
  *     own class is looked up, so a faulty one never gets in the way of another.
  *   - A former name that no class bears anymore, a renamed or deleted ADT, is declared by another companion: the jars
  *     of the job are scanned once for the companions extending [[Evolved]], and all of them are initialized.
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
object Evolutions {

  // The ADTs of two jobs sharing this library are loaded by two different class loaders, so their declarations are
  // held apart: a same class name then resolves to the class of the job asking for it, and dies with it
  private val registries: concurrent.Map[ClassLoader, Registry] = concurrent.TrieMap.empty

  // Every declaration a companion handed over, by companion class: a companion initializes once per JVM, so the
  // declarations are kept for good and applied again to a registry emptied by a reset
  private val declarations: concurrent.Map[Class[_], Declaration[_]] = concurrent.TrieMap.empty

  /** Declarations of the ADTs one class loader loads, and the companions found in the jars it loads from. */
  private final class Registry(val classLoader: ClassLoader) {

    private val evolutions: concurrent.Map[String, Array[Evolution[_]]] = concurrent.TrieMap.empty

    // Applied once each: the identity of a declaration is the one of the companion that built it
    val applied: concurrent.Map[Declaration[_], Unit] = concurrent.TrieMap.empty

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
      declarations.sorted
    }

  }

  /** Hand over the declaration of an ADT, applied by the first lookup of its class. Called by [[Evolved]] when a
    * companion initializes.
    *
    * @param companion
    *   The class of the companion object handing the declaration over
    */
  private[evolution] def pending(declaration: Declaration[_], companion: Class[_]): Unit =
    declarations.put(companion, declaration)

  /** Build the [[Evolution]]s from the given builder and register them for the deserialization phase. */
  private[evolution] def register[T](builder: EvolutionBuilder[T]): Unit =
    register(registryOf(builder.currentClass.getClassLoader), builder.build())

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
    // A case object is its own companion, and an enum value is declared by its enum
    val companion =
      try Some(Class.forName(companionNameOf(className), false, cl))
      catch { case _: ClassNotFoundException => None }
    // Only a companion extending Evolved is initialized, so that it hands its declaration over: no other one is touched
    val evolved = companion.filter(classOf[Evolved[_]].isAssignableFrom)
    evolved.foreach(clazz => Class.forName(clazz.getName, true, clazz.getClassLoader))
    evolved.flatMap(declarations.get).foreach { declaration =>
      apply(declaration, cl).foreach(member => declare(member.getName, cl))
    }
  }

  /** Apply the given declaration once to the registry of the given class loader, and return the subtypes it declares,
    * to declare in turn.
    */
  private def apply(declaration: Declaration[_], cl: ClassLoader): Seq[Class[_]] = synchronized {
    val registry = registryOf(cl)
    if (registry.applied.putIfAbsent(declaration, ()).isDefined) Nil
    else
      try {
        declaration.build().foreach(builder => register(registry, builder.build()))
        declaration.memberClasses()
      } catch {
        case NonFatal(failure) => registry.failed.put(declaration.currentClass.getName, failure); Nil
      }
  }

  /** The companion class of the given ADT class name: a case object is its own, an enum value is declared by its enum.
    */
  private def companionNameOf(className: String): String = {
    val adtName = className.takeWhile(_ != '#')
    if (adtName.endsWith("$")) adtName else s"$adtName$$"
  }

  /** Initialize the companions extending [[Evolved]] found in the jars of the given class loader.
    *
    * This is how a former name that no class bears anymore, renamed or deleted, gets declared: the companion declaring
    * it is the only one that knows, and nothing but the jars lists the companions.
    */
  private def declareScannedCompanions(cl: ClassLoader): Unit =
    registryOf(cl).companions.foreach(declare(_, cl))

  /** `true` if the given current class is the marker of a former class registered as deleted. */
  def isDeletedClass(currentClass: Class[_]): Boolean = currentClass == DeletedClass

  /** Return the [[Evolution]] declared for the given former ADT member name at the given former version, if any.
    *
    * Unlike [[get]], never loads the class of that name, so it also answers for the names that designate no class at
    * all: the `<enum binary name>#<value name>` of a Scala 3 enum value, in particular. Returning `None` means nothing
    * was declared for that name, not that the member is unknown.
    *
    * A lookup finding nothing initializes the companion of that name, which hands its declaration over, and applies it.
    * The declarations are looked up in the registry of the given class loader only: the companions it initializes are
    * the ones the job sees, so the classes they declare are the ones of the job.
    *
    * @param formerName
    *   Former fully qualified class name, or `<enum binary name>#<value name>` for a Scala 3 enum value
    * @param formerVersion
    *   Former schema version of the ADT declaring that member
    * @param cl
    *   Class loader the former ADT member is looked up for
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
  private[api] def reset(): Unit = {
    org.apache.flinkx.api.auto.cache.clear()
    registries.clear()
  }

}
