package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.DeclareTest.Probe
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URL
import java.util.Enumeration
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import scala.jdk.CollectionConverters._

/** The declarations of a module reach a JVM that never derives them, through the providers listed in its jars. */
class EvolutionsProviderTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  private def declaredNames: Set[String] = Evolutions.declaredEvolutions(getClass.getClassLoader).keySet

  /** How many version windows each declared name got, which a second provider run would double. */
  private def windowCounts: Map[String, Int] =
    Evolutions.declaredEvolutions(getClass.getClassLoader).view.mapValues(_.length).toMap

  it should "declare an ADT nothing derived, on the first lookup that misses" in {
    val evolution = Evolutions.get(classOf[Probe], Evolutions.findVersion(classOf[Probe]))

    evolution.currentClass shouldBe classOf[Probe]
    withClue("the declaration must come from the provider, not from a no-op evolution:")(
      declaredNames should contain(classOf[Probe].getName.replace("Probe", "FormerProbe"))
    )
  }

  it should "resolve a former name declared by a provider" in {
    val formerName = classOf[Probe].getName.replace("Probe", "FormerProbe")

    Evolutions.find[Probe](formerName, 0, getClass.getClassLoader).map(_.currentClass) shouldBe Some(classOf[Probe])
  }

  it should "run the providers of a class loader only once" in {
    Evolutions.get(classOf[Probe], 2)
    val firstLookup = windowCounts

    Evolutions.get(classOf[Probe], 2)
    val secondLookup = windowCounts

    withClue("a second run would pile up duplicate windows:")(secondLookup shouldBe firstLookup)
  }

  // The tasks of a TaskManager restore their state in parallel: a lookup landing while the providers are being read
  // must wait for them, rather than take the declaration for one that never reached this JVM
  it should "make a lookup concurrent with the reading of the providers wait for it" in {
    val slowLoader       = new SlowServiceClassLoader(getClass.getClassLoader)
    val failures         = new ConcurrentLinkedQueue[Throwable]
    val start            = new CountDownLatch(1)
    val lookup: Runnable = () => {
      start.await()
      try Evolutions.get[Probe](classOf[Probe].getName, 2, slowLoader)
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

  /** Delays the service file listing the providers, to widen the window a concurrent lookup falls into. */
  private class SlowServiceClassLoader(parent: ClassLoader) extends ClassLoader(parent) {
    override def getResources(name: String): Enumeration[URL] = {
      if (name.endsWith(classOf[EvolutionsProvider].getName)) Thread.sleep(300)
      super.getResources(name)
    }
  }

}
