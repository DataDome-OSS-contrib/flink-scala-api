package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.IntSerializer
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer, ScalaCaseObjectSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A snapshot records a case object with the class name of the object, which ends with `$`. The former name of a
  * renamed case object can omit this `$`. The `$` makes a case object different from a case class with the same name.
  */
class CaseObjectEvolutionTest extends AnyFlatSpec with Matchers with TestUtils {

  import CaseObjectEvolutionTest._

  /** The class name of the class `name` next to Litter. */
  private def named(name: String): String = classOf[Litter].getName.replace("Litter", name)

  /** The former class name of the case object `name`, as its snapshot records it. */
  private def recorded(name: String): String = named(s"$name$$")

  /** The serializer of Litter at version 0. Its snapshot records the case objects with their former class names. */
  private def formerSerializer: CoproductSerializer[Litter] = {
    def formerCaseObject(name: String, caseObject: Litter): TypeSerializer[_] = {
      val clazz = caseObject.getClass.asInstanceOf[Class[Litter]]
      new ScalaCaseObjectSerializer[Litter](new AdtEvolution(recorded(name), 0, clazz))
    }
    new CoproductSerializer[Litter](
      evolution = Some(new AdtEvolution(classOf[Litter].getName, 0, classOf[Litter])),
      subtypeClasses = Array[Class[_]](Cat.getClass, Dog.getClass),
      subtypeNames = Array(recorded("Kitten"), recorded("Puppy")),
      subtypeSerializers = Array(formerCaseObject("Kitten", Cat), formerCaseObject("Puppy", Dog))
    )
  }

  it should "migrate a case object renamed from a former name without the $ ending its class name" in {
    deserializeFormerForm[Litter](formerSerializer, Cat) shouldBe Cat
  }

  it should "migrate a case object renamed from a former name with the $ ending its class name" in {
    deserializeFormerForm[Litter](formerSerializer, Dog) shouldBe Dog
  }

  it should "resolve the schema compatibility of renamed case objects as compatible after migration" in {
    compatibilityWith[Litter](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  // A deleted class name without `$` refers to the case class and to the case object of that name
  it should "resolve a class deleted under a name without the $ ending the class name of a case object" in {
    val cl = getClass.getClassLoader

    AdtEvolution.isDeletedClass(Evolutions.get[Litter](recorded("Bird"), 0, cl).currentClass) shouldBe true
    AdtEvolution.isDeletedClass(Evolutions.get[Litter](named("Bird"), 0, cl).currentClass) shouldBe true
  }

  // Each deletion keeps its versions and its throwOnInstance: the case class deleted in version 1 reads as null
  it should "keep each deletion of a name taken again in between" in {
    def restored(name: String, version: Int): Nest =
      Evolutions
        .get[Nest](named(name), version, getClass.getClassLoader)
        .migrate(fail("A deleted class builds nothing"))

    restored("Egg", 0) shouldBe null
    intercept[DeletedInstanceException](restored("Egg$", 2))
  }

  it should "not mistake a case class that reuses the former name of a renamed case object for it" in {
    Evolutions.get[Any](named("Kitten"), 0, getClass.getClassLoader).currentClass shouldBe classOf[Kitten]
  }

  // A case class and a case object have different serializers: @renamed cannot turn one into the other
  it should "replace a case class by a case object of the same name with a deletion and a post-evolution mapper" in {
    val formerSerializer = new CoproductSerializer[AnyRef](
      evolution = Some(evolutionOf(classOf[Switch]).asInstanceOf[AdtEvolution[AnyRef]]),
      subtypeClasses = Array[Class[_]](classOf[FormerOn], Off.getClass),
      subtypeNames = Array(named("On"), Off.getClass.getName),
      subtypeSerializers = Array[TypeSerializer[_]](
        new CaseClassSerializer[FormerOn](
          new AdtEvolution(named("On"), 0, classOf[FormerOn]),
          isCaseClassImmutable = true,
          fieldNames = Some(Array("level")),
          scalaFieldSerializers = Array(IntSerializer.INSTANCE)
        ),
        new ScalaCaseObjectSerializer[Off.type](evolutionOf(Off.getClass.asInstanceOf[Class[Off.type]]))
      )
    )

    deserializeFormerForm[Switch](formerSerializer, FormerOn(3)) shouldBe On
    deserializeFormerForm[Switch](formerSerializer, Off) shouldBe Off
    compatibilityWith[Switch](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

}

object CaseObjectEvolutionTest {

  /* Litter at version 0
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

  // Reuses the name of the former case object Kitten: the `$` at the end of the object class name makes them different
  case class Kitten(a: String)

  /* Nest at version 0
  sealed trait Nest
  case class Egg(a: String) extends Nest
  case object Hen           extends Nest
   */

  /* Nest at versions 1 and 2: a case object reuses the name of the deleted case class
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

  /* Switch at version 0
  sealed trait Switch
  case class On(level: Int) extends Switch
  case object Off extends Switch
   */

  @version(1)
  @deletedClasses(since = 1, throwOnInstance = false, "On")
  @postEvolution(restoreOn)
  sealed trait Switch
  object Switch extends Evolving[Switch] { val evolutions = Evolutions[Switch] }
  @version(1) // Version 1 at least: the deleted case class On has the version 0
  case object On  extends Switch
  case object Off extends Switch

  def restoreOn(version: Int, switch: Switch): Switch = if (switch == null) On else switch

  // Takes the place of the case class On at version 0
  case class FormerOn(level: Int)

}
