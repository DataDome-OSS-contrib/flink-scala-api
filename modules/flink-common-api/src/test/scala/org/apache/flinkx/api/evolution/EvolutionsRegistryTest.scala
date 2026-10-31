package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures.DerivedBeforeDeclared
import org.apache.flinkx.api.evolution.EvolutionMacroTest.Probe
import org.apache.flinkx.api.evolution.EvolutionRenamedTest.{Horse, Pet, Pony}
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.serializer.SnapshotTestUtil.savepointed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{URL, URLClassLoader}
import java.nio.file.Files
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.jar.{Attributes, JarOutputStream, Manifest}
import scala.jdk.CollectionConverters._

/** Test the registry of the evolving companions. The first lookup through a class loader does the companion scan of its
  * jars, so the evolutions reach a JVM that never derived them, for example a TaskManager during a restore.
  */
class EvolutionsRegistryTest extends AnyFlatSpec with Matchers {

  import EvolutionsRegistryTest._

  private val cl = getClass.getClassLoader

  private val FormerProbe = classOf[Probe].getName.replace("Probe", "FormerProbe")

  // No class has the former class name now: only the companion scan of the jars finds the evolving companion
  it should "resolve a former name by scanning the jars for the companions" in {
    Evolutions.get[Probe](FormerProbe, 0, cl).currentClass shouldBe classOf[Probe]
  }

  // The macros accept a companion that extends Evolving through another type: the companion scan must find it too
  it should "resolve the former names declared by companions extending Evolving indirectly" in {
    Evolutions.get[Any](s"${classOf[ThroughTrait].getName}Former", 0, cl).currentClass shouldBe classOf[ThroughTrait]
    Evolutions.get[Any](s"${classOf[ThroughClass].getName}Former", 0, cl).currentClass shouldBe classOf[ThroughClass]
  }

  // After a name reuse, only the companion of the renamed subtype tells which class the former name refers to
  it should "resolve the former name of a subtype now borne by another class" in {
    Evolutions.get[Pet](classOf[Horse].getName, 0, cl).currentClass shouldBe classOf[Pony]
  }

  // After a rollback, the lookup gives the current class: the schema compatibility resolution reports the rollback
  it should "resolve a class name written at a version above its declaration to its current class" in {
    Evolutions.get[Pet](classOf[Pet].getName, 3, cl).currentClass shouldBe classOf[Pet]
  }

  // A lookup must not initialize a companion that is not an evolving companion
  it should "not initialize a companion that doesn't extend Evolving" in {
    Evolutions.get[Plain](classOf[Plain].getName, 0, cl).currentClass shouldBe classOf[Plain]

    plainCompanionInitialized shouldBe false
  }

  // The derivation does not read the evolutions: the registry reads them after the initialization of the companion
  it should "declare the evolutions of a companion deriving its type information before its strict val declaring them" in {
    DerivedBeforeDeclared.typeInfo.getTypeClass shouldBe classOf[DerivedBeforeDeclared]
    val formerName =
      classOf[DerivedBeforeDeclared].getName.replace("DerivedBeforeDeclared", "FormerDerivedBeforeDeclared")
    Evolutions.get(formerName, 0, cl).currentClass shouldBe classOf[DerivedBeforeDeclared]
  }

  // Without a declaration, the restore reads the former data as current data, and no later check finds the error
  it should "fail to restore a former class name that no class has and no companion declares" in {
    val goneName   = "org.example.Gone"
    val serializer = new CaseClassSerializer[Probe](
      evolution = new AdtEvolution[Probe](goneName, 2, classOf[Probe]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("id")),
      scalaFieldSerializers = Array(StringSerializer.INSTANCE)
    )

    val exception = intercept[MissingEvolutionException](savepointed[Probe](serializer))

    exception.getMessage should startWith(
      s"Cannot restore '$goneName' written at version 2: no class has this name, and no evolution declares it."
    )
    exception.getCause shouldBe a[ClassNotFoundException]
  }

  // A job with a wrong declaration must not restore its state: each lookup reports the same failure
  it should "fail every lookup when two ADTs claim the same former name" in {
    ConflictingScan.withFixture("sharedFormerName") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      Set(failure.declared, failure.conflicting) shouldBe Set(
        s"renamed to ${classOf[FirstClaimingFormerName].getName} up to version 0",
        s"renamed to ${classOf[SecondClaimingFormerName].getName} up to version 0"
      )
      intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2)) shouldBe theSameInstanceAs(failure)
    }
  }

  it should "fail every lookup when a former name is declared both renamed and deleted" in {
    ConflictingScan.withFixture("renamedAndDeleted") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      Set(failure.declared, failure.conflicting) shouldBe Set(
        s"renamed to ${classOf[ClaimingByRename].getName} up to version 0",
        s"deleted by ${classOf[ClaimingByDeletion].getName} up to version 0"
      )
    }
  }

  // A mapper initializes during the registration: only its first initialization gives the cause of its failure
  it should "fail every lookup with the first failure of a mapper initialization" in {
    ConflictingScan.withFixture("failingMapper") {
      val first  = intercept[FlinkRuntimeException](evolutionOf(classOf[Probe], 2))
      val second = intercept[FlinkRuntimeException](evolutionOf(classOf[Probe], 2))
      first.getMessage shouldBe s"Cannot build the evolutions of ${classOf[FailingPostEvolution].getName}"
      first.getCause.getCause.getMessage shouldBe "FLINK_SCALA_API_UNSET is not set"
      second shouldBe theSameInstanceAs(first)
    }
  }

  // After a name reuse, only the schema version identifies which class wrote the data
  it should "fail every lookup when a class reuses a former class name without a higher version" in {
    ConflictingScan.withFixture("declaredReuser") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      failure.declared shouldBe s"renamed to ${classOf[GivingFormerName].getName} up to version 2"
      failure.conflicting shouldBe s"used by ${classOf[TakenFormerName].getName} at version 2"
    }
  }

  // A class without an evolving companion writes at version 0, never above the last version of the former name
  it should "count a class that reuses a former class name without an evolving companion as unversioned" in {
    ConflictingScan.withFixture("undeclaredReuser") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      failure.declared shouldBe s"renamed to ${classOf[GivingUndeclaredName].getName} up to version 0"
      failure.conflicting shouldBe s"used by ${classOf[UndeclaredReuser].getName} at version 0"
    }
  }

  // A deleted class name without `$` also refers to the case object of that name
  it should "find a case object that reuses a name deleted without the $ ending its class name" in {
    ConflictingScan.withFixture("caseObjectReuser") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      failure.declared shouldBe s"deleted by ${classOf[DeletingCaseObjectName].getName} up to version 0"
      failure.conflicting shouldBe s"used by ${TakenCaseObject.getClass.getName} at version 0"
    }
  }

  // A top-level case object also has a mirror class under its name without `$`, which holds no data
  it should "resolve a deleted name that a top-level case object reuses at a higher version" in {
    val fishName = TopLevelFish.getClass.getName
    AdtEvolution.isDeletedClass(Evolutions.get[TopLevelReef](fishName, 0, cl).currentClass) shouldBe true
    Evolutions.get[TopLevelReef](fishName, 1, cl).currentClass shouldBe TopLevelFish.getClass
  }

  // The tasks of a TaskManager restore their state in parallel: a lookup during the registration must wait for it
  it should "make concurrent lookups wait for the declarations" in {
    Evolutions.reset()
    val failures         = new ConcurrentLinkedQueue[Throwable]
    val start            = new CountDownLatch(1)
    val lookup: Runnable = () => {
      start.await()
      try Evolutions.get[Probe](FormerProbe, 0, cl)
      catch { case failure: Throwable => failures.add(failure) }
    }
    val lookups = List.fill(8)(new Thread(lookup))

    lookups.foreach(_.start())
    start.countDown()
    lookups.foreach(_.join())

    withClue(s"concurrent lookups failed: ${failures.asScala.map(_.getMessage).toSet}")(
      failures.asScala.toList shouldBe empty
    )
  }

  // Two jobs on a TaskManager can share this library, with one class loader each: each job gets its own declarations
  it should "keep the declarations of two class loaders apart" in {
    val otherJob     = new JobClassLoader(Array(classOf[Probe].getProtectionDomain.getCodeSource.getLocation))
    val otherProbe   = otherJob.loadClass(classOf[Probe].getName)
    val currentProbe = classOf[Probe]
    withClue("the isolated class loader must define a class of its own:")(otherProbe shouldNot be(currentProbe))

    evolutionOf(currentProbe, 2).currentClass shouldBe currentProbe
    evolutionOf(otherProbe, 2).currentClass shouldBe otherProbe
  }

  // Flink gives the snapshots a wrapper that delegates to the class loader of the job, with another parent
  it should "resolve a former name through a wrapper of the class loader defining the ADT" in {
    val jobJar   = classOf[Probe].getProtectionDomain.getCodeSource.getLocation
    val otherJob = new JobClassLoader(Array(jobJar))
    val wrapper  = new SafetyNetLikeClassLoader(otherJob, jobJar)

    val resolved = Evolutions.get[Any](FormerProbe, 0, wrapper).currentClass
    resolved shouldNot be(classOf[Probe])
    resolved.getClassLoader shouldBe otherJob
  }

  // IntelliJ shortens a long class path to a jar whose manifest lists the class path, as the JVM reads it
  it should "resolve a former name declared in a jar that the Class-Path of a manifest lists" in {
    val jobJar     = classOf[Probe].getProtectionDomain.getCodeSource.getLocation
    val pathingJar = Files.createTempFile("pathing", ".jar")
    val manifest   = new Manifest()
    val attributes = manifest.getMainAttributes
    attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    attributes.put(Attributes.Name.CLASS_PATH, jobJar.toString)
    new JarOutputStream(Files.newOutputStream(pathingJar), manifest).close()
    val pathingJob = new URLClassLoader(Array(pathingJar.toUri.toURL), cl)

    Evolutions.get[Probe](FormerProbe, 0, pathingJob).currentClass shouldBe classOf[Probe]
  }

  // Probe has an evolving companion: the scan cannot see it, which must not make its evolutions silently empty
  it should "fail to look up a class whose evolving companion the scan did not find" in {
    val emptyJob = new URLClassLoader(Array(Files.createTempDirectory("job").toUri.toURL), cl)

    val failure = intercept[IllegalStateException](Evolutions.get[Probe](classOf[Probe].getName, 0, emptyJob))
    failure.getMessage should include(classOf[Probe].getName)
  }

  /** Delegates to the class loader of the job, with the application class loader as parent, as Flink does. */
  private class SafetyNetLikeClassLoader(inner: ClassLoader, jobJar: URL)
      extends URLClassLoader(Array(jobJar), getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = inner.loadClass(name)
  }

  /** Loads Probe itself, without delegation, as the class loader of a job does for the classes of the job. */
  private class JobClassLoader(urls: Array[URL]) extends URLClassLoader(urls, getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] =
      // And its enclosing classes: the JVM checks that a nested class and its enclosing class have one class loader
      if (name.startsWith(classOf[EvolutionMacroTest].getName)) Option(findLoadedClass(name)).getOrElse(findClass(name))
      else super.loadClass(name, resolve)
  }

}

object EvolutionsRegistryTest {

  @volatile var plainCompanionInitialized = false

  case class Plain(id: String)
  object Plain {
    plainCompanionInitialized = true
  }

  trait EvolvingTrait[T]          extends Evolving[T]
  abstract class EvolvingClass[T] extends EvolvingTrait[T]

  @version(1)
  @renamed(since = 1, "ThroughTraitFormer")
  case class ThroughTrait(a: String)
  object ThroughTrait extends EvolvingTrait[ThroughTrait] { val evolutions = Evolutions[ThroughTrait] }

  @version(1)
  @renamed(since = 1, "ThroughClassFormer")
  case class ThroughClass(a: String)
  object ThroughClass extends EvolvingClass[ThroughClass] { val evolutions = Evolutions[ThroughClass] }

  // Fixtures in conflict on purpose: the companion scan finds them only when ConflictingScan switches them on

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class FirstClaimingFormerName(a: String)
  object FirstClaimingFormerName extends Evolving[FirstClaimingFormerName] {
    def evolutions: Evolutions[FirstClaimingFormerName] =
      ConflictingScan.evolutionsOf("sharedFormerName", Evolutions[FirstClaimingFormerName])
  }

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class SecondClaimingFormerName(a: String)
  object SecondClaimingFormerName extends Evolving[SecondClaimingFormerName] {
    def evolutions: Evolutions[SecondClaimingFormerName] =
      ConflictingScan.evolutionsOf("sharedFormerName", Evolutions[SecondClaimingFormerName])
  }

  @version(1)
  @renamed(since = 1, "ClaimedByBoth")
  case class ClaimingByRename(a: String)
  object ClaimingByRename extends Evolving[ClaimingByRename] {
    def evolutions: Evolutions[ClaimingByRename] =
      ConflictingScan.evolutionsOf("renamedAndDeleted", Evolutions[ClaimingByRename])
  }

  @version(1)
  @deletedClasses(since = 1, "ClaimedByBoth")
  case class ClaimingByDeletion(a: String)
  object ClaimingByDeletion extends Evolving[ClaimingByDeletion] {
    def evolutions: Evolutions[ClaimingByDeletion] =
      ConflictingScan.evolutionsOf("renamedAndDeleted", Evolutions[ClaimingByDeletion])
  }

  @version(1)
  @postEvolution(FailingMapper)
  case class FailingPostEvolution(a: String)
  object FailingPostEvolution extends Evolving[FailingPostEvolution] {
    def evolutions: Evolutions[FailingPostEvolution] =
      ConflictingScan.evolutionsOf("failingMapper", Evolutions[FailingPostEvolution])
  }

  // Its initialization fails, as a mapper reading a setting that the TaskManager does not have
  object FailingMapper extends PostEvolutionMapper[FailingPostEvolution] {
    private val setting =
      sys.env.getOrElse("FLINK_SCALA_API_UNSET", throw new IllegalStateException("FLINK_SCALA_API_UNSET is not set"))
    override def apply(formerVersion: Int, instance: FailingPostEvolution): FailingPostEvolution =
      instance.copy(a = setting)
  }

  // Reuses the former name of GivingFormerName at version 2, but this name refers to GivingFormerName up to version 2
  @version(2)
  case class TakenFormerName(a: String)
  object TakenFormerName extends Evolving[TakenFormerName] {
    def evolutions: Evolutions[TakenFormerName] =
      ConflictingScan.evolutionsOf("declaredReuser", Evolutions[TakenFormerName])
  }

  @version(3)
  @renamed(since = 3, "TakenFormerName")
  case class GivingFormerName(a: String)
  object GivingFormerName extends Evolving[GivingFormerName] {
    def evolutions: Evolutions[GivingFormerName] =
      ConflictingScan.evolutionsOf("declaredReuser", Evolutions[GivingFormerName])
  }

  // Reuses the former name of GivingUndeclaredName, without an evolving companion to declare its version
  case class UndeclaredReuser(a: String)

  @version(1)
  @renamed(since = 1, "UndeclaredReuser")
  case class GivingUndeclaredName(a: String)
  object GivingUndeclaredName extends Evolving[GivingUndeclaredName] {
    def evolutions: Evolutions[GivingUndeclaredName] =
      ConflictingScan.evolutionsOf("undeclaredReuser", Evolutions[GivingUndeclaredName])
  }

  // Reuses a class name that DeletingCaseObjectName deletes without `$`
  case object TakenCaseObject

  @version(1)
  @deletedClasses(since = 1, "TakenCaseObject")
  case class DeletingCaseObjectName(a: String)
  object DeletingCaseObjectName extends Evolving[DeletingCaseObjectName] {
    def evolutions: Evolutions[DeletingCaseObjectName] =
      ConflictingScan.evolutionsOf("caseObjectReuser", Evolutions[DeletingCaseObjectName])
  }

}

// Top-level, unlike the other fixtures: a top-level object has a mirror class
@version(1)
@deletedClasses(since = 1, "TopLevelFish")
sealed trait TopLevelReef
object TopLevelReef extends Evolving[TopLevelReef] { val evolutions = Evolutions[TopLevelReef] }

// Reuses the name of a deleted case object at a higher version
@version(1)
case object TopLevelFish extends TopLevelReef
