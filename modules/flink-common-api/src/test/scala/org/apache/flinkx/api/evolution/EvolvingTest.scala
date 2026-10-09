package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.evolution.EvolutionMacroTest.Probe
import org.apache.flinkx.api.evolution.EvolutionTest.{FirstClaimingFormerName, SecondClaimingFormerName}
import org.apache.flinkx.api.evolution.EvolutionRenamedTest.{Pet, Pony}
import org.apache.flinkx.api.evolution.EvolvingTest.{Plain, plainCompanionInitialized}
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

    Evolutions.find[Probe](FormerProbe, 0, getClass.getClassLoader).map(_.currentClass) shouldBe Some(classOf[Probe])
  }

  // A declaration is applied when its own class is looked up, not before: the others stay out of the way
  it should "apply only the declaration of the class looked up" in {
    Evolutions.get(classOf[Probe], 2)

    declaredNames should not contain classOf[Pet].getName
  }

  // A renamed subtype is declared by its own companion, which nothing names but the trait declaring it as a member
  it should "declare the subtypes of a sealed trait along with it" in {
    Evolutions.get(classOf[Pet], 2)

    declaredNames should contain(classOf[Pony].getName.replace("Pony", "Horse"))
  }

  it should "report the failure of a declaration when its class is looked up, and leave the others alone" in {
    Evolutions.get(classOf[FirstClaimingFormerName], 1)
    intercept[FormerClassConflictException](Evolutions.get(classOf[SecondClaimingFormerName], 1))

    Evolutions.get(classOf[Probe], 2).currentClass shouldBe classOf[Probe]
    intercept[FormerClassConflictException](Evolutions.get(classOf[SecondClaimingFormerName], 1))
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
      evolution = new Evolution[Probe](goneName, 2, 2, classOf[Probe]),
      version = 2,
      isCaseClassImmutable = true,
      fieldNames = Array("id"),
      paramSerializers = Array(StringSerializer.INSTANCE)
    )
    val out = new DataOutputSerializer(1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, serializer.snapshotConfiguration())

    val exception = intercept[EvolutionNotDeclaredException] {
      TypeSerializerSnapshot.readVersionedSnapshot(
        new DataInputDeserializer(out.getSharedBuffer),
        getClass.getClassLoader
      )
    }

    exception.getMessage should startWith(
      s"Cannot restore '$goneName', written at @version(2): no class of that name exists, and no evolution declares" +
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

}
