package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.{TestUtils, renamed, version}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionRenamedTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.EvolutionRenamedTest._

  override protected def beforeEach(): Unit = Evolutions.reset()

  /* Test to serialize Pet v0 code into Pet-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Pet-v0", animal)
  }
   */

  it should "deserialize Horse v0 to Pony v2" in {
    val expected: Pet = Pony("Spirit")
    testDeserializeFromFile("Pet-v0", expected)
  }

  /* Test to serialize Pet v2 code into Pet-v2.snapshot file, uncomment to regenerate
  it should "serialize Pet v2" in {
    val pet: Pet = Horse("Spirit")
    serializeToFile("Pet-v2", pet)
  }
   */

  it should "deserialize Horse v2" in {
    val expected: Pet = Horse("Spirit")
    testDeserializeFromFile("Pet-v2", expected)
  }

}

object EvolutionRenamedTest {

  /* Animal-v0
  sealed trait Animal
  case class Horse(name: String) extends Animal
  case class Lion(name: String) extends Animal
   */

  /* Pet-v1
  @version(1)
  @renamed(since = 1, "Animal")
  sealed trait Pet
  @version(1)
  @renamed(since = 1, "Horse")
  case class Pony(
    @renamed(since = 1, "name") nickname: String
  ) extends Animal
  @version(1)
  @renamed(since = 1, "Lion")
  case class Cat(
    @renamed(since = 1, "name") nickname: String
  ) extends Animal
   */

  @version(2)
  @renamed(since = 1, "Animal")
  sealed trait Pet
  @version(1)
  @renamed(since = 1, "Horse")
  case class Pony(
      @renamed(since = 1, "name") nickname: String
  ) extends Pet
  @version(1)
  @renamed(since = 1, "Lion")
  @renamed(since = 2, "Cat")
  case class Horse(
      @renamed(since = 1, "name") nickname: String
  ) extends Pet

}
