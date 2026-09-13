package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.EvolutionTest.Dog
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

  /** Loads the test ADTs itself rather than delegating, as the class loader of a job does for its own classes. */
  private class JobClassLoader(urls: Array[URL]) extends URLClassLoader(urls, getClass.getClassLoader) {
    override def loadClass(name: String, resolve: Boolean): Class[_] =
      if (name.startsWith("org.apache.flinkx.api.EvolutionTest$")) {
        Option(findLoadedClass(name)).getOrElse(findClass(name))
      } else super.loadClass(name, resolve)
  }

}
