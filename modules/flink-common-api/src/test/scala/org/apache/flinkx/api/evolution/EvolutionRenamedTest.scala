package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A sealed trait renamed along with its subtypes and their fields, a subtype taking the former name of another one. */
class EvolutionRenamedTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.evolution.EvolutionRenamedTest._

  /* Test to serialize Animal v0 code into Pet-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Pet-v0", animal)
  }
   */

  it should "deserialize Horse v0 to Pony v2" in {
    testDeserializeFromFile[Pet]("Pet-v0", Pony("Spirit"))
  }

  // Horse was renamed Pony in version 1, then Cat took the name Horse in version 2
  it should "deserialize a subtype bearing the former name of another subtype" in {
    roundtrip[Pet](createSerializer[Pet], Horse("Spirit"))
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
  ) extends Pet
  @version(1)
  @renamed(since = 1, "Lion")
  case class Cat(
    @renamed(since = 1, "name") nickname: String
  ) extends Pet
   */

  @version(2)
  @renamed(since = 1, "Animal")
  sealed trait Pet
  object Pet extends Evolving[Pet] { val evolutions = Evolutions[Pet] }
  @version(1)
  @renamed(since = 1, "Horse")
  case class Pony(
      @renamed(since = 1, "name") nickname: String
  ) extends Pet
  object Pony extends Evolving[Pony] { val evolutions = Evolutions[Pony] }
  @version(1)
  @renamed(since = 1, "Lion")
  @renamed(since = 2, "Cat")
  case class Horse(
      @renamed(since = 1, "name") nickname: String
  ) extends Pet
  object Horse extends Evolving[Horse] { val evolutions = Evolutions[Horse] }

}
