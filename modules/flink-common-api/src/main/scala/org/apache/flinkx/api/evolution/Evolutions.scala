package org.apache.flinkx.api.evolution

import org.apache.flink.annotation.{Internal, PublicEvolving, VisibleForTesting}
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.evolution.AdtEvolution.{DeletedEvolution, NoEvolution}
import org.apache.flinkx.api.evolution.Evolutions.Declaration
import org.apache.flinkx.api.serializer.ConstructorCompat
import org.apache.flinkx.api.util.ClassUtil

import java.util
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** The evolutions that an ADT declares with its annotations. [[Evolutions.apply]] builds them at compile time, in the
  * companion of the ADT.
  *
  * The builders run only at registration. When the companion creates this object, it is not fully initialized, and the
  * mappers of the annotations can be its members.
  *
  * @param builders
  *   The builders of the ADT and of its case objects. A case object has no companion: its sealed trait declares it.
  */
@Internal
final class Evolutions[T](builders: () => Seq[EvolutionBuilder[_]]) {

  /** The declarations to register: the ones of the ADT and of its case objects. */
  private[evolution] def declarations: Seq[Declaration] = builders().flatMap(_.build())

}

/** Global registry of the schema evolution feature, and the macro that builds the [[Evolutions]] of an ADT (see
  * [[Evolving]]).
  *
  * The schema evolution feature restores a savepoint after a change of the schema of an ADT: a case class, a case
  * object, a sealed trait or a Scala 3 enum. An ADT declares [[version]] to use the feature, and an
  * [[EvolutionAnnotation]] for each evolution.
  *
  * The words former and current follow the definitions of [[version]].
  *
  * Lifecycle:
  *   - The evolving companion of a versioned ADT holds its [[Evolutions]], which the macro builds at compile time. The
  *     derivation of the type information reads only the version of the ADT, see [[Evolvable]]: a current serializer
  *     writes the current schema, and needs no evolution.
  *   - The first lookup in a class loader scans its jars for the evolving companions, initializes them and registers
  *     all their declarations. Any companion can declare a former class name, so the registry needs all of them. A
  *     failed registration fails every lookup.
  *   - The submission check ([[checked]]) registers the declarations when the Scala execution environment submits the
  *     job. A wrong declaration then stops the submission, not the first restore.
  *   - During the restore, each ADT serializer snapshot does a lookup ([[get]]) of the former class name and the former
  *     version that it records. The restored serializer then applies this ADT evolution to the former data.
  *
  * A declaration of a class name gives the ADT evolution of each version up to its last version, see
  * [[EvolutionBuilder.build]].
  *
  * Packaging: each class loader has its own registry, so a job never reads the declarations of another job. The
  * evolving ADTs must be in the application jar.
  *
  * Concurrency: the tasks of a TaskManager restore their state in parallel. The registration takes a lock, and the
  * lookups wait for it to complete. A companion does no lookup during its initialization.
  */
@PublicEvolving
object Evolutions extends EvolutionsFactory {

  /** What one ADT declares for one class name.
    * @param lastVersion
    *   The highest schema version of the data written under the class name
    * @param evolutionAt
    *   The ADT evolution of the data written at a version from 0 to `lastVersion`
    * @param declaredBy
    *   The ADT that declares the class name
    */
  private[evolution] final case class Declaration(
      className: String,
      lastVersion: Int,
      evolutionAt: Int => AdtEvolution[_],
      declaredBy: Class[_]
  ) {

    /** The current class that the class name refers to, or the deletion marker. */
    lazy val currentClass: Class[_] = evolutionAt(0).currentClass

    /** For a deleted class name, whether an instance throws or reads as `null`. */
    lazy val throwOnInstance: Option[Boolean] = evolutionAt(0) match {
      case deleted: DeletedEvolution[_] => Some(deleted.throwOnInstance)
      case _                            => None
    }

  }

  /** One registry per class loader: a class name refers to the class of the job that does the lookup */
  // The class loader of the job is a wrapper, dropped when the job stops: the weak key then releases the registry
  private val registries = new util.WeakHashMap[ClassLoader, Registry]()

  /** The declarations of the evolving companions of one class loader, registered at the first lookup. */
  private final class Registry(private var classLoader: ClassLoader) {

    // Every lookup fails with the first failure: only the first initialization of a companion gives the real cause
    // (ExceptionInInitializerError, LinkageError), and the subtasks that restore in parallel do not scan the jars again
    private lazy val declared: Try[Map[String, Seq[Declaration]]] = {
      val registered = Try(registerCompanions(classLoader))
      classLoader = null // The value of a weak key must not keep the key to be released
      registered
    }

    def declarations: Map[String, Seq[Declaration]] = declared.get

  }

  /** The registry of the given class loader, created at the first lookup. */
  private def registryOf(classLoader: ClassLoader): Registry =
    registries.synchronized(registries.computeIfAbsent(classLoader, new Registry(_)))

  /** Initialize the evolving companions of the class loader, and register all their declarations.
    *
    * @throws FormerClassConflictException
    *   if a class name refers to two classes in the data of one version
    */
  private def registerCompanions(cl: ClassLoader): Map[String, Seq[Declaration]] =
    CompanionScan
      .companionsOf(cl)
      .flatMap(name => declarationsOf(ClassUtil.loadClass(name, initialize = true, cl).get))
      .groupBy(_.className)
      .map { case (className, declarations) => className -> sortDeclarations(className, declarations, cl) }

  /** The declarations of the evolutions that the given initialized companion holds, or none if it is not an object.
    *
    * @throws FlinkRuntimeException
    *   if they fail, for example because a mapper of the annotations does not initialize. Its message names the ADT
    */
  private def declarationsOf(companion: Class[_]): Seq[Declaration] =
    try ClassUtil.moduleOf(companion).toSeq.flatMap { case evolving: Evolving[_] => evolving.evolutions.declarations }
    catch {
      case ex @ (NonFatal(_) | _: LinkageError) => // Also a LinkageError: the Try of the registry does not catch it
        throw new FlinkRuntimeException(s"Cannot build the evolutions of ${companion.getName.stripSuffix("$")}", ex)
    }

  /** Sort the declarations of a class name by last version, and keep each declaration once.
    * @throws FormerClassConflictException
    *   if the class name refers to two classes in the data of one version, or is deleted twice with two behaviors
    */
  private def sortDeclarations(className: String, declarations: Seq[Declaration], cl: ClassLoader): Seq[Declaration] = {
    // Evolutions.get uses the first declaration whose last version is equal to or higher than the former version
    val sorted = declarations
      .distinctBy(declaration => (declaration.lastVersion, declaration.currentClass, declaration.throwOnInstance))
      .sortBy(_.lastVersion)
    checkNameReuse(className, sorted, cl)
    sorted
      .groupBy(_.lastVersion)
      .collectFirst { case (_, sameVersion) if sameVersion.length > 1 => sameVersion }
      .foreach { decls => throw FormerClassConflictException(className, refersTo(decls.head), refersTo(decls(1))) }
    sorted
  }

  /** Reject a name reuse when the reusing class has a version equal to or lower than the last version of the other
    * class. Only the version tells their data apart. A class without an evolving companion has version 0.
    */
  private def checkNameReuse(className: String, declarations: Seq[Declaration], cl: ClassLoader): Unit = {
    val (current, former) = declarations.partition(_.currentClass.getName == className)
    former.lastOption.foreach { last =>
      val reusing = current.headOption
        .map(declaration => declaration.currentClass -> declaration.lastVersion)
        .orElse(classNamed(className, cl).map(_ -> 0))
      reusing.filter { case (_, version) => version <= last.lastVersion }.foreach { case (clazz, version) =>
        throw FormerClassConflictException(className, refersTo(last), s"used by ${clazz.getName} at version $version")
      }
    }
  }

  /** What the given declaration of a former class name refers to, for the message of a conflict. */
  private def refersTo(declaration: Declaration): String = {
    val upTo = s"up to version ${declaration.lastVersion}"
    declaration.throwOnInstance match {
      case Some(throws) => s"deleted by ${declaration.declaredBy.getName} with throwOnInstance = $throws $upTo"
      case None         => s"renamed to ${declaration.currentClass.getName} $upTo"
    }
  }

  /** The class with the given name, if a savepoint can contain its data: a class or a case object, no other object and
    * no mirror class of an object.
    */
  private def classNamed(className: String, cl: ClassLoader): Option[Class[_]] =
    ClassUtil
      .loadClass(className, initialize = false, cl)
      .toOption
      .filterNot(clazz =>
        ClassUtil.isMirrorClass(clazz) || ClassUtil.isObjectClass(clazz) && !classOf[Product].isAssignableFrom(clazz)
      )

  /** Lookup: the ADT evolution of the data written under the given former class name, at the given former version. It
    * is [[AdtEvolution.NoEvolution]] if no declaration of this class name includes this version.
    *
    * The first lookup in the given class loader registers the declarations of all its evolving companions.
    *
    * @throws MissingEvolutionException
    *   if no declaration of the class name includes this version, and no class has this name
    * @throws org.apache.flink.util.FlinkRuntimeException
    *   if no declaration of the class name includes this version, and its class does not load
    * @throws java.lang.IllegalStateException
    *   if the companion scan did not find the evolving companion of the class, outside the jars of the job
    * @throws Throwable
    *   the failure of the registration
    */
  def get[T](formerClassName: String, formerVersion: Int, cl: ClassLoader): AdtEvolution[T] = {
    val declarations = registryOf(cl).declarations.getOrElse(formerClassName, Nil)
    declarations
      .find(_.lastVersion >= formerVersion)
      .map(_.evolutionAt(formerVersion))
      .getOrElse {
        val currentClass = ClassUtil.loadClass(formerClassName, initialize = false, cl) match {
          case Success(clazz)                           => clazz
          case Failure(failure: ClassNotFoundException) =>
            throw MissingEvolutionException(
              formerClassName,
              formerVersion,
              declarations.lastOption.map(_.lastVersion),
              failure
            )
          case Failure(failure) => throw failure // The class exists, but does not load: a link error, for example
        }
        // The declarations of a class with an evolving companion are missing only if the scan did not find it
        if (declarations.isEmpty && hasEvolvingCompanion(currentClass, cl))
          throw new IllegalStateException(companionNotScanned(currentClass.getName))
        new NoEvolution(currentClass, formerVersion, ConstructorCompat.lookupFieldNames(currentClass))
      }
      .asInstanceOf[AdtEvolution[T]]
  }

  /** Whether the companion of the given class extends [[Evolving]], without initializing it. */
  private def hasEvolvingCompanion(clazz: Class[_], cl: ClassLoader): Boolean =
    ClassUtil
      .loadClass(ClassUtil.companionName(clazz.getName), initialize = false, cl)
      .toOption
      .exists(classOf[Evolving[_]].isAssignableFrom)

  /** Submission check, then `submit`: register the declarations of the evolving companions of the job, from the thread
    * of its main. A wrong declaration stops the submission.
    *
    * @throws Throwable
    *   the failure of the registration
    */
  def checked[A](submit: => A, cl: ClassLoader = Thread.currentThread.getContextClassLoader): A = {
    registryOf(cl).declarations
    submit
  }

  /** Empty the registries. The next lookup registers the declarations again. */
  @VisibleForTesting
  private[api] def reset(): Unit = registries.synchronized(registries.clear())

}
