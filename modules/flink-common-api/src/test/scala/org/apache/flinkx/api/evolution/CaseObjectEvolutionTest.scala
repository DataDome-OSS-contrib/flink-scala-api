package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.{CoproductSerializer, ScalaCaseObjectSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A snapshot records a case object by the name of its class, ending with `$`, which the former name of a renamed case
  * object may omit, and which tells it apart from a case class of the same name.
  */
class CaseObjectEvolutionTest extends AnyFlatSpec with Matchers with TestUtils {

  import CaseObjectEvolutionTest._

  /** The binary name of the class `name` next to Litter. */
  private def named(name: String): String = classOf[Litter].getName.replace("Litter", name)

  /** The name the former case object `name` was recorded under, the name of its class. */
  private def recorded(name: String): String = named(s"$name$$")

  /** The serializer of Litter v0, recording its case objects under their former names. */
  private def formerSerializer: CoproductSerializer[Litter] = {
    def formerCaseObject(name: String, caseObject: Litter): TypeSerializer[_] = {
      val clazz = caseObject.getClass.asInstanceOf[Class[Litter]]
      new ScalaCaseObjectSerializer[Litter](new Evolution(recorded(name), 0, clazz))
    }
    new CoproductSerializer[Litter](
      evolution = Some(new Evolution(classOf[Litter].getName, 0, classOf[Litter])),
      subtypeClasses = Array[Class[_]](Cat.getClass, Dog.getClass),
      subtypeFqns = Array(recorded("Kitten"), recorded("Puppy")),
      subtypeSerializers = Array(formerCaseObject("Kitten", Cat), formerCaseObject("Puppy", Dog))
    )
  }

  it should "restore a case object renamed from a former name without the $ ending its class name" in {
    deserializeFormerForm[Litter](formerSerializer, Cat) shouldBe Cat
  }

  it should "restore a case object renamed from a former name with the $ ending its class name" in {
    deserializeFormerForm[Litter](formerSerializer, Dog) shouldBe Dog
  }

  it should "resolve the schema compatibility of renamed case objects as compatible after migration" in {
    compatibilityWith[Litter](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  // The deleted class no longer tells whether it was a case object: its name stands for both
  it should "resolve a class deleted under a name without the $ ending the class name of a case object" in {
    val cl = getClass.getClassLoader

    Evolutions.get[Litter](recorded("Bird"), 0, cl).isDeleted shouldBe true
    Evolutions.get[Litter](named("Bird"), 0, cl).isDeleted shouldBe true
  }

  // Each deletion keeps its own window and behavior, the case class deleted in version 1 reading as null
  it should "keep each deletion of a name taken again in between" in {
    def restored(name: String, version: Int): Nest =
      Evolutions
        .get[Nest](named(name), version, getClass.getClassLoader)
        .restore(fail("A deleted class builds nothing"))

    restored("Egg", 0) shouldBe null
    intercept[DeletedInstanceException](restored("Egg$", 2))
  }

  it should "not mistake a case class bearing the former name of a renamed case object for it" in {
    Evolutions.get[Any](named("Kitten"), 0, getClass.getClassLoader).currentClass shouldBe classOf[Kitten]
  }

}

object CaseObjectEvolutionTest {

  /* Litter v0
  sealed trait Litter
  case object Kitten extends Litter
  case object Puppy  extends Litter
  case object Bird   extends Litter
   */

  @version(1)
  @deletedClasses(since = 1, throwOnInstance = false, "Bird")
  sealed trait Litter
  object Litter extends Evolving[Litter] { val evolutions = Evolutions[Litter] }

  @version(1)
  @renamed(since = 1, "Kitten")
  case object Cat extends Litter

  @version(1)
  @renamed(since = 1, "Puppy$")
  case object Dog extends Litter

  // Bears the name of the former case object Kitten, which its snapshots tell apart by the $ ending its class name
  case class Kitten(a: String)

  /* Nest v0
  sealed trait Nest
  case class Egg(a: String) extends Nest
  case object Hen           extends Nest
   */

  /* Nest v1 and v2, a case object taking the name of the case class deleted
  @version(2)
  @deletedClasses(since = 1, throwOnInstance = false, "Egg")
  sealed trait Nest
  case object Egg extends Nest
  case object Hen extends Nest
   */

  @version(3)
  @deletedClasses(since = 1, throwOnInstance = false, "Egg")
  @deletedClasses(since = 3, "Egg")
  sealed trait Nest
  object Nest     extends Evolving[Nest] { val evolutions = Evolutions[Nest] }
  case object Hen extends Nest

}
