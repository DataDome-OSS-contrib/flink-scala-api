package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, VisibleForTesting}
import org.apache.flinkx.api.evolution.Evolution.DeletedClass
import org.apache.flinkx.api.evolution.EvolutionBuilder.AdtDeclaration
import org.apache.flinkx.api.version

import java.io.IOException
import java.util.ServiceLoader
import scala.collection.concurrent
import scala.jdk.CollectionConverters._

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
  *   - At build time, the sbt plugin lists every versioned ADT of the module in an [[EvolutionsProvider]], whose
  *     `declare` reads the current annotations and [[register]]s the immutable [[Evolution]]s of that ADT class.
  *   - At restore time, the ADT serializer snapshots [[get]] the evolution of the former name and version they record,
  *     and at deserialization time the restored serializers apply it.
  *   - On a TaskManager, nothing declares anything up front: the first lookup that misses runs the providers of the
  *     jar, which is how the declarations reach a JVM where the derivation never ran.
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
  * Concurrency: the Flink tasks of a TaskManager restore their state in parallel, so they declare and look up
  * concurrently. Every registration is atomic, and the providers of a class loader are read once, the threads asking
  * meanwhile waiting for that reading rather than taking the declaration for missing.
  */
@Internal
object Evolutions {

  // The ADTs of two jobs sharing this library are defined by two different class loaders, so their declarations are
  // held apart: a same class name then resolves to the class of the job asking for it, and dies with it
  private val registries: concurrent.Map[ClassLoader, Registry] = concurrent.TrieMap.empty

  /** Declarations of the ADTs defined by one class loader, filled by [[register]] and by the providers of its jars.
    *
    * Thread-safe: a declaration is read lock-free and registered atomically, and the providers are read once.
    */
  private final class Registry(classLoader: ClassLoader) {

    private val evolutions: concurrent.Map[String, Array[Evolution[_]]] = concurrent.TrieMap.empty

    // Read once, and a thread arriving meanwhile waits for that reading to complete rather than skipping it
    private lazy val providersRead: Unit =
      ServiceLoader.load(classOf[EvolutionsProvider], classLoader).iterator().asScala.foreach(_.declare())

    def readProviders(): Unit = providersRead

    def declare(className: String, declared: Array[Evolution[_]]): Unit =
      evolutions.updateWith(className) {
        case Some(registered) => Some(sortDeclarations(className, registered ++ declared))
        case _                => Some(sortDeclarations(className, declared))
      }

    /** The evolution declared for the given former name at the given former version, if any. */
    def find(formerName: String, formerVersion: Int): Option[Evolution[_]] =
      evolutions.get(formerName).flatMap(_.find(_.version >= formerVersion))

    /** Whether anything at all is declared for the given former name, at any version. */
    def isDeclared(formerName: String): Boolean = evolutions.contains(formerName)

    def declarations: Map[String, Array[Evolution[_]]] = evolutions.toMap

    /** Sort the evolutions of a class name, dropping the declarations already registered, and reject a name claimed by
      * two different ADTs.
      * @throws FormerClassConflictException
      *   if two evolutions declared for the same version resolve to different current classes
      */
    private def sortDeclarations(className: String, evolutions: Array[Evolution[_]]): Array[Evolution[_]] = {
      def descr(currentClass: Class[_]) = if (isDeletedClass(currentClass)) "deleted" else s"renamed to $currentClass"

      val declarations = evolutions.distinctBy(evolution => (evolution.version, evolution.currentClass))
      declarations
        .groupBy(_.version)
        .collectFirst { case (_, sameVersion) if sameVersion.length > 1 => sameVersion }
        .foreach { conflicting =>
          val classes = conflicting.map(_.currentClass)
          throw FormerClassConflictException(className, descr(classes(0)), descr(classes(1)))
        }
      declarations.sorted
    }

  }

  /** Build the [[Evolution]]s from the given builder and register them for the deserialization phase. */
  def register[T](builder: EvolutionBuilder[T]): Unit = register(builder.build())

  private def register(declaration: AdtDeclaration): Unit = {
    val registry = registryOf(declaration.currentClass.getClassLoader)
    declaration.byClassName.foreachEntry(registry.declare)
  }

  /** The registry of the given class loader, created on first use. */
  private def registryOf(classLoader: ClassLoader): Registry =
    registries.getOrElseUpdate(classLoader, new Registry(classLoader))

  /** The class loaders an ADT can be declared by, from the given one up to the bootstrap loader denoted by null. */
  private def loaderChain(classLoader: ClassLoader): LazyList[ClassLoader] =
    LazyList.iterate(classLoader)(_.getParent).takeWhile(_ != null) :+ null

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
    lookUp[T](formerName, formerVersion, cl).orElse {
      // The providers of the jars are what a JVM that never derives an ADT declares from, so a lookup finding nothing
      // reads them before concluding: on a TaskManager that happens while reading a savepoint, before any restore
      registryOf(cl).readProviders()
      lookUp[T](formerName, formerVersion, cl)
    }

  private def lookUp[T](formerName: String, formerVersion: Int, cl: ClassLoader): Option[Evolution[T]] =
    loaderChain(cl)
      .flatMap(registries.get)
      .flatMap(_.find(formerName, formerVersion))
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
        if (formerVersion > 0 && !isDeclared(formerClassName, cl)) {
          // We are looking for a versioned ADT: finding nothing means META-INF/services are missing
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
    loaderChain(cl).flatMap(registries.get).exists(_.isDeclared(formerName))

  /** Current schema version declared by the `version` annotation of the given ADT class, 0 when it declares none.
    *
    * Read reflectively rather than through the derivation: `version` is a Java annotation retained at runtime, which
    * the Scala macros of Magnolia deliberately leave out of the annotations they expose.
    *
    * @throws VersionNotAllowedException
    *   if the declared version is negative
    */
  private[api] def findVersion(currentClass: Class[_]): Int =
    Option(currentClass.getAnnotation(classOf[version])).fold(0) { declared =>
      if (declared.value >= 0) declared.value else throw VersionNotAllowedException(currentClass, declared.value)
    }

  /** Default value of the `index`-th field of the given case class, `None` when it declares none.
    *
    * Read from the companion rather than emitted by the macros declaring the evolutions: a default accessor is
    * synthetic, and splicing a reference to it into generated code defeats the Scala 2 compiler.
    */
  private[api] def defaultFieldValue(currentClass: Class[_], index: Int): Option[Any] =
    try {
      val companion = Class.forName(s"${currentClass.getName}$$", true, currentClass.getClassLoader)
      // Scala 2 names the accessor after `apply`, Scala 3 after the constructor
      val accessorNames = Seq(s"apply$$default$$${index + 1}", s"$$lessinit$$greater$$default$$${index + 1}")
      val accessor = accessorNames.iterator.flatMap(name => companion.getMethods.find(_.getName == name)).nextOption()
      accessor.map(_.invoke(companion.getField("MODULE$").get(null)))
    } catch {
      case _: ClassNotFoundException | _: NoSuchFieldException => None
    }

  /** Check the `version` annotation is not declared on one of the fields of the given ADT class.
    * @throws VersionNotAllowedOnFieldException
    *   if a field declares a version
    */
  private[api] def checkNoVersionOnFields(currentClass: Class[_], fieldNames: Array[String]): Unit =
    currentClass.getDeclaredConstructors
      .find(_.getParameterCount == fieldNames.length)
      .foreach(_.getParameterAnnotations.iterator.zipWithIndex.foreach { case (annotations, i) =>
        if (annotations.exists(_.isInstanceOf[version])) {
          throw VersionNotAllowedOnFieldException(currentClass, fieldNames(i))
        }
      })

  /** Every [[Evolution]] registered by the given class loader, by class name. */
  @VisibleForTesting
  private[api] def declaredEvolutions(cl: ClassLoader): Map[String, Array[Evolution[_]]] =
    registries.get(cl).fold(Map.empty[String, Array[Evolution[_]]])(_.declarations)

  @VisibleForTesting
  private[api] def reset(): Unit = {
    org.apache.flinkx.api.auto.cache.clear()
    registries.clear()
  }

}
