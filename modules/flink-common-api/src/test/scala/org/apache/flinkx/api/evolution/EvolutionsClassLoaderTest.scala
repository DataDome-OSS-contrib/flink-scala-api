package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.EvolutionTest.Dog
import org.apache.flinkx.api.evolution.DeclarationTest.Probe
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.{URL, URLClassLoader}

/** Two jobs sharing this library on a TaskManager define their ADTs with two different class loaders: their
  * declarations must not be taken for a conflict, nor resolve to the class of the other job.
  */
class EvolutionsClassLoaderTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "keep the declarations of two class loaders apart" in {
    val otherJob   = new JobClassLoader(Array(classOf[Dog].getProtectionDomain.getCodeSource.getLocation))
    val otherDog   = otherJob.loadClass(classOf[Dog].getName)
    val currentDog = classOf[Dog]
    withClue("the isolated class loader must define a class of its own:")(otherDog shouldNot be(currentDog))

    Evolutions.register(new EvolutionBuilder(currentDog.asInstanceOf[Class[Any]], 1, Array("name")))
    Evolutions.register(new EvolutionBuilder(otherDog.asInstanceOf[Class[Any]], 1, Array("name")))

    Evolutions.find[Any](currentDog.getName, 0, currentDog.getClassLoader).map(_.currentClass) shouldBe Some(currentDog)
    Evolutions.find[Any](currentDog.getName, 0, otherJob).map(_.currentClass) shouldBe Some(otherDog)
  }

  // The companions of the two jobs hand over two declarations for one class name: each job must get its own
  it should "declare the companions of two class loaders apart" in {
    val otherJob     = new JobClassLoader(Array(classOf[Probe].getProtectionDomain.getCodeSource.getLocation))
    val otherProbe   = otherJob.loadClass(classOf[Probe].getName)
    val currentProbe = classOf[Probe]
    withClue("the isolated class loader must define a class of its own:")(otherProbe shouldNot be(currentProbe))

    Evolutions.get(currentProbe, 2).currentClass shouldBe currentProbe
    Evolutions.get(otherProbe.asInstanceOf[Class[Any]], 2).currentClass shouldBe otherProbe
  }

  // Flink hands the snapshots a wrapper whose parent is the application class loader, and which delegates to the loader
  // of the job: the ADTs of the job are defined by a loader that is not in the parent chain of the wrapper
  it should "resolve a former name through a wrapper of the class loader defining the ADT" in {
    val jobJar     = classOf[Probe].getProtectionDomain.getCodeSource.getLocation
    val otherJob   = new JobClassLoader(Array(jobJar))
    val wrapper    = new SafetyNetLikeClassLoader(otherJob, jobJar)
    val formerName = classOf[Probe].getName.replace("Probe", "FormerProbe")

    val resolved = Evolutions.get[Any](formerName, 0, wrapper).currentClass
    resolved shouldNot be(classOf[Probe])
    resolved.getClassLoader shouldBe otherJob
  }

  /** Delegates to the loader of the job while declaring the application class loader as parent, as Flink does. */
  private class SafetyNetLikeClassLoader(inner: ClassLoader, jobJar: URL)
      extends URLClassLoader(Array(jobJar), getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] = inner.loadClass(name)
  }

  /** Loads the test ADTs itself rather than delegating, as the class loader of a job does for its own classes. */
  private class JobClassLoader(urls: Array[URL]) extends URLClassLoader(urls, getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] =
      // The enclosing classes as well: the JVM checks that a nested class and its outer agree on their loader
      if (name.startsWith("org.apache.flinkx.api.EvolutionTest") || name.startsWith(classOf[DeclarationTest].getName)) {
        Option(findLoadedClass(name)).getOrElse(findClass(name))
      } else super.loadClass(name, resolve)
  }

}
