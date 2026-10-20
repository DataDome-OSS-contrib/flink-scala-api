package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures.DerivedBeforeDeclared
import org.apache.flinkx.api.evolution.EvolutionMacroTest.Probe
import org.apache.flinkx.api.evolution.EvolutionRenamedTest.{Horse, Pet, Pony}
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.serializer.SnapshotTestUtil.savepointed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{URL, URLClassLoader}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.jdk.CollectionConverters._

/** The registry of the evolutions the companions extending `Evolving` declare: the first lookup through a class loader
  * scans its jars for them, so they reach a JVM that never derived them, as a TaskManager restoring a savepoint.
  */
class EvolutionsRegistryTest extends AnyFlatSpec with Matchers {

  import EvolutionsRegistryTest._

  private val cl = getClass.getClassLoader

  private val FormerProbe = classOf[Probe].getName.replace("Probe", "FormerProbe")

  // The former name is borne by no class anymore: only the companion of the current class knows it, and only the jars
  // list the companions
  it should "resolve a former name by scanning the jars for the companions" in {
    Evolutions.get[Probe](FormerProbe, 0, cl).currentClass shouldBe classOf[Probe]
  }

  // The macros accept a companion extending Evolving through a type of its own: the scan must find it as well
  it should "resolve the former names declared by companions extending Evolving indirectly" in {
    Evolutions.get[Any](s"${classOf[ThroughTrait].getName}Former", 0, cl).currentClass shouldBe classOf[ThroughTrait]
    Evolutions.get[Any](s"${classOf[ThroughClass].getName}Former", 0, cl).currentClass shouldBe classOf[ThroughClass]
  }

  // The former name of a renamed subtype may be borne by another class now: only the companion of the renamed subtype
  // knows it stands for that subtype in the former data
  it should "resolve the former name of a subtype now borne by another class" in {
    Evolutions.get[Pet](classOf[Horse].getName, 0, cl).currentClass shouldBe classOf[Pony]
  }

  // A savepoint written by a more recent source code, typically after a rollback: the class is there, its declaration
  // just doesn't reach that far, which the schema compatibility resolution reports on its own
  it should "resolve a class name written at a version above its declaration to its current class" in {
    Evolutions.get[Pet](classOf[Pet].getName, 3, cl).currentClass shouldBe classOf[Pet]
  }

  // Looking a class up must not run the initialization of a companion that declares nothing
  it should "not initialize a companion that doesn't extend Evolving" in {
    Evolutions.get[Plain](classOf[Plain].getName, 0, cl).currentClass shouldBe classOf[Plain]

    plainCompanionInitialized shouldBe false
  }

  // The derivation doesn't read the evolutions, which the registry reads once the companion is initialized
  it should "declare the evolutions of a companion deriving its type information before its strict val declaring them" in {
    DerivedBeforeDeclared.typeInfo.getTypeClass shouldBe classOf[DerivedBeforeDeclared]
    val formerName =
      classOf[DerivedBeforeDeclared].getName.replace("DerivedBeforeDeclared", "FormerDerivedBeforeDeclared")
    Evolutions.get(formerName, 0, cl).currentClass shouldBe classOf[DerivedBeforeDeclared]
  }

  // Restoring without the declaration would read the former form as if it never evolved, which no later check can
  // catch once the values are in the state table
  it should "fail to restore a former name no class bears and no companion declares" in {
    val goneName   = "org.example.Gone"
    val serializer = new CaseClassSerializer[Probe](
      evolution = new Evolution[Probe](goneName, 2, classOf[Probe]),
      isCaseClassImmutable = true,
      fieldNames = Array("id"),
      paramSerializers = Array(StringSerializer.INSTANCE)
    )

    val exception = intercept[MissingEvolutionException](savepointed[Probe](serializer))

    exception.getMessage should startWith(
      s"Cannot restore '$goneName' written at @version(2): no class of that name can be loaded, and no evolution" +
        s" declares it renamed or deleted."
    )
    exception.getCause shouldBe a[ClassNotFoundException]
  }

  // A job declaring its evolutions wrongly must not restore any state: every lookup reports the same failure
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

  // The data a class wrote under the former name of another one is told apart by its version only
  it should "fail every lookup when a class bears a former name without being versioned above it" in {
    ConflictingScan.withFixture("declaredBearer") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      failure.declared shouldBe s"renamed to ${classOf[GivingFormerName].getName} up to version 2"
      failure.conflicting shouldBe s"used by ${classOf[TakenFormerName].getName} at version 2"
    }
  }

  // Its data is written at version 0, which the window of the former name always holds
  it should "count a class bearing a former name without declaring its evolutions as unversioned" in {
    ConflictingScan.withFixture("undeclaredBearer") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      failure.declared shouldBe s"renamed to ${classOf[GivingUndeclaredName].getName} up to version 0"
      failure.conflicting shouldBe s"used by ${classOf[UndeclaredBearer].getName} at version 0"
    }
  }

  // A deleted name omitting the $ ending the class name of an object stands for an object of that name as well
  it should "find a case object bearing a name deleted without the $ ending its class name" in {
    ConflictingScan.withFixture("caseObjectBearer") {
      val failure = intercept[FormerClassConflictException](evolutionOf(classOf[Probe], 2))
      failure.declared shouldBe s"deleted by ${classOf[DeletingCaseObjectName].getName} up to version 0"
      failure.conflicting shouldBe s"used by ${TakenCaseObject.getClass.getName} at version 0"
    }
  }

  // The tasks of a TaskManager restore their state in parallel: a lookup landing while the declarations are applied
  // must wait for them, rather than take the declaration for one that never reached this JVM
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

  // Two jobs sharing this library on a TaskManager define their ADTs with two different class loaders: the companions
  // of the two jobs hand over two declarations for one class name, and each job must get its own
  it should "keep the declarations of two class loaders apart" in {
    val otherJob     = new JobClassLoader(Array(classOf[Probe].getProtectionDomain.getCodeSource.getLocation))
    val otherProbe   = otherJob.loadClass(classOf[Probe].getName)
    val currentProbe = classOf[Probe]
    withClue("the isolated class loader must define a class of its own:")(otherProbe shouldNot be(currentProbe))

    evolutionOf(currentProbe, 2).currentClass shouldBe currentProbe
    evolutionOf(otherProbe, 2).currentClass shouldBe otherProbe
  }

  // Flink hands the snapshots a wrapper whose parent is the application class loader, and which delegates to the loader
  // of the job: the ADTs of the job are defined by a loader that is not in the parent chain of the wrapper
  it should "resolve a former name through a wrapper of the class loader defining the ADT" in {
    val jobJar   = classOf[Probe].getProtectionDomain.getCodeSource.getLocation
    val otherJob = new JobClassLoader(Array(jobJar))
    val wrapper  = new SafetyNetLikeClassLoader(otherJob, jobJar)

    val resolved = Evolutions.get[Any](FormerProbe, 0, wrapper).currentClass
    resolved shouldNot be(classOf[Probe])
    resolved.getClassLoader shouldBe otherJob
  }

  /** Delegates to the loader of the job while declaring the application class loader as parent, as Flink does. */
  private class SafetyNetLikeClassLoader(inner: ClassLoader, jobJar: URL)
      extends URLClassLoader(Array(jobJar), getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = inner.loadClass(name)
  }

  /** Loads Probe itself rather than delegating, as the class loader of a job does for its own classes. */
  private class JobClassLoader(urls: Array[URL]) extends URLClassLoader(urls, getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] =
      // The enclosing classes as well: the JVM checks that a nested class and its outer agree on their loader
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

  // The fixtures below conflict on purpose: the scan finds their declarations only while ConflictingScan switches
  // them on

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class FirstClaimingFormerName(a: String)
  object FirstClaimingFormerName extends Evolving[FirstClaimingFormerName] {
    def evolutions: Evolutions[FirstClaimingFormerName] =
      ConflictingScan.evolutionsOf(
        classOf[FirstClaimingFormerName],
        "sharedFormerName",
        Evolutions[FirstClaimingFormerName]
      )
  }

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class SecondClaimingFormerName(a: String)
  object SecondClaimingFormerName extends Evolving[SecondClaimingFormerName] {
    def evolutions: Evolutions[SecondClaimingFormerName] =
      ConflictingScan.evolutionsOf(
        classOf[SecondClaimingFormerName],
        "sharedFormerName",
        Evolutions[SecondClaimingFormerName]
      )
  }

  @version(1)
  @renamed(since = 1, "ClaimedByBoth")
  case class ClaimingByRename(a: String)
  object ClaimingByRename extends Evolving[ClaimingByRename] {
    def evolutions: Evolutions[ClaimingByRename] =
      ConflictingScan.evolutionsOf(classOf[ClaimingByRename], "renamedAndDeleted", Evolutions[ClaimingByRename])
  }

  @version(1)
  @deletedClasses(since = 1, "ClaimedByBoth")
  case class ClaimingByDeletion(a: String)
  object ClaimingByDeletion extends Evolving[ClaimingByDeletion] {
    def evolutions: Evolutions[ClaimingByDeletion] =
      ConflictingScan.evolutionsOf(classOf[ClaimingByDeletion], "renamedAndDeleted", Evolutions[ClaimingByDeletion])
  }

  // Bears the former name of GivingFormerName, versioned at 2 while that name stands for it up to version 2
  @version(2)
  case class TakenFormerName(a: String)
  object TakenFormerName extends Evolving[TakenFormerName] {
    def evolutions: Evolutions[TakenFormerName] =
      ConflictingScan.evolutionsOf(classOf[TakenFormerName], "declaredBearer", Evolutions[TakenFormerName])
  }

  @version(3)
  @renamed(since = 3, "TakenFormerName")
  case class GivingFormerName(a: String)
  object GivingFormerName extends Evolving[GivingFormerName] {
    def evolutions: Evolutions[GivingFormerName] =
      ConflictingScan.evolutionsOf(classOf[GivingFormerName], "declaredBearer", Evolutions[GivingFormerName])
  }

  // Bears the former name of GivingUndeclaredName, with no companion declaring its version
  case class UndeclaredBearer(a: String)

  @version(1)
  @renamed(since = 1, "UndeclaredBearer")
  case class GivingUndeclaredName(a: String)
  object GivingUndeclaredName extends Evolving[GivingUndeclaredName] {
    def evolutions: Evolutions[GivingUndeclaredName] =
      ConflictingScan.evolutionsOf(classOf[GivingUndeclaredName], "undeclaredBearer", Evolutions[GivingUndeclaredName])
  }

  // Bears a name DeletingCaseObjectName declares deleted without the $ ending the class name of an object
  case object TakenCaseObject

  @version(1)
  @deletedClasses(since = 1, "TakenCaseObject")
  case class DeletingCaseObjectName(a: String)
  object DeletingCaseObjectName extends Evolving[DeletingCaseObjectName] {
    def evolutions: Evolutions[DeletingCaseObjectName] =
      ConflictingScan.evolutionsOf(
        classOf[DeletingCaseObjectName],
        "caseObjectBearer",
        Evolutions[DeletingCaseObjectName]
      )
  }

}
