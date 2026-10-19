package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.evolution.EvolutionMacroTest.Probe
import org.apache.flinkx.api.evolution.EvolutionRenamedTest.{Horse, Pet, Pony}
import org.apache.flinkx.api.evolution.EvolvingTest.{Plain, ThroughClass, ThroughTrait, plainCompanionInitialized}
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.jdk.CollectionConverters._

/** The evolutions of the companions extending `Evolving` reach a JVM that never derives them, when a lookup asks. */
class EvolvingTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  private def declaredNames: Set[String] = Evolutions.declaredEvolutions(getClass.getClassLoader).keySet

  private val FormerProbe = classOf[Probe].getName.replace("Probe", "FormerProbe")

  it should "declare an ADT nothing derived, on the first lookup of its name" in {
    val evolution = Evolutions.get(classOf[Probe], 2)

    evolution.currentClass shouldBe classOf[Probe]
    withClue("the declaration must come from the companion, not from a no-op evolution:")(
      declaredNames should contain(FormerProbe)
    )
  }

  // Looking a class up must not run the initialization of a companion that declares nothing
  it should "not initialize a companion that doesn't extend Evolving" in {
    Evolutions.get[Plain](classOf[Plain].getName, 0, getClass.getClassLoader).currentClass shouldBe classOf[Plain]

    plainCompanionInitialized shouldBe false
  }

  // The former name is borne by no class anymore: only the companion of the current class knows it, and only the jars
  // list the companions
  it should "resolve a former name by scanning the jars for the companions" in {
    Evolutions.get[Probe](FormerProbe, 0, getClass.getClassLoader).currentClass shouldBe classOf[Probe]
  }

  // A companion initializes once per JVM: what it declared must survive a registry emptied since
  it should "apply a declaration again once the registry is reset" in {
    Evolutions.get(classOf[Probe], 2)
    Evolutions.reset()
    Evolutions.get(classOf[Probe], 2)

    Evolutions.get[Probe](FormerProbe, 0, getClass.getClassLoader).currentClass shouldBe classOf[Probe]
  }

  // The macros accept a companion extending Evolving through a type of its own: the scan must find it as well
  it should "resolve the former names declared by companions extending Evolving indirectly" in {
    val cl = getClass.getClassLoader
    Evolutions.get[Any](s"${classOf[ThroughTrait].getName}Former", 0, cl).currentClass shouldBe classOf[ThroughTrait]
    Evolutions.get[Any](s"${classOf[ThroughClass].getName}Former", 0, cl).currentClass shouldBe classOf[ThroughClass]
  }

  // Any companion may declare the former name looked up: all of them are declared on the first lookup
  it should "declare every companion of the jars on the first lookup" in {
    Evolutions.get(classOf[Probe], 2)

    declaredNames should contain(classOf[Pet].getName)
  }

  // The former name of a renamed subtype may be borne by another class now: only the companion of the renamed subtype
  // knows it stands for that subtype in the former data
  it should "resolve the former name of a subtype now borne by another class" in {
    Evolutions.get[Pet](classOf[Horse].getName, 0, getClass.getClassLoader).currentClass shouldBe classOf[Pony]
  }

  // A job declaring its evolutions wrongly must not restore any state: every lookup reports the same failure
  it should "fail every lookup with the failure of a declaration" in {
    ConflictingScan.on = true
    try {
      val failure = intercept[FormerClassConflictException](Evolutions.get(classOf[Probe], 2))
      intercept[FormerClassConflictException](Evolutions.get(classOf[Probe], 2)) shouldBe theSameInstanceAs(failure)
    } finally {
      ConflictingScan.on = false
      Evolutions.reset()
    }
  }

  // The data a class wrote under the former name of another one is told apart by its version only
  it should "fail every lookup when a class bears a former name without being versioned above it" in {
    ConflictingScan.bearing = Some("declared")
    try {
      val failure = intercept[FormerClassConflictException](Evolutions.get(classOf[Probe], 2))
      failure.declared shouldBe s"renamed to ${classOf[EvolutionTest.GivingFormerName].getName} up to version 2"
      failure.conflicting shouldBe s"used by ${classOf[EvolutionTest.TakenFormerName].getName} at version 2"
    } finally {
      ConflictingScan.bearing = None
      Evolutions.reset()
    }
  }

  // Its data is written at version 0, which the window of the former name always holds
  it should "count a class bearing a former name without declaring its evolutions as unversioned" in {
    ConflictingScan.bearing = Some("undeclared")
    try {
      val failure = intercept[FormerClassConflictException](Evolutions.get(classOf[Probe], 2))
      failure.declared shouldBe s"renamed to ${classOf[EvolutionTest.GivingUndeclaredName].getName} up to version 0"
      failure.conflicting shouldBe s"used by ${classOf[EvolutionTest.UndeclaredBearer].getName} at version 0"
    } finally {
      ConflictingScan.bearing = None
      Evolutions.reset()
    }
  }

  // A deleted name omitting the $ ending the class name of an object stands for an object of that name as well
  it should "find a case object bearing a name deleted without the $ ending its class name" in {
    ConflictingScan.bearing = Some("caseObject")
    try {
      val failure = intercept[FormerClassConflictException](Evolutions.get(classOf[Probe], 2))
      failure.declared shouldBe s"deleted by ${classOf[EvolutionTest.DeletingCaseObjectName].getName} up to version 0"
      failure.conflicting shouldBe s"used by ${EvolutionTest.TakenCaseObject.getClass.getName} at version 0"
    } finally {
      ConflictingScan.bearing = None
      Evolutions.reset()
    }
  }

  // The tasks of a TaskManager restore their state in parallel: a lookup landing while the declarations are applied
  // must wait for them, rather than take the declaration for one that never reached this JVM
  it should "make concurrent lookups wait for the declarations" in {
    val failures         = new ConcurrentLinkedQueue[Throwable]
    val start            = new CountDownLatch(1)
    val lookup: Runnable = () => {
      start.await()
      try Evolutions.get[Probe](FormerProbe, 0, getClass.getClassLoader)
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
    val out = new DataOutputSerializer(1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration())

    val exception = intercept[MissingEvolutionException] {
      TypeSerializerSnapshot.readVersionedSnapshot(
        new DataInputDeserializer(out.getSharedBuffer),
        getClass.getClassLoader
      )
    }

    exception.getMessage should startWith(
      s"Cannot restore '$goneName' written at @version(2): no class of that name exists, and no evolution declares" +
        s" it renamed or deleted."
    )
  }

}

object EvolvingTest {

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

}
