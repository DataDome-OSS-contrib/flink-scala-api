package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, SeqSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Renames of a sealed trait, of its subtypes and of their fields, and the name reuse of a subtype. */
class EvolutionRenamedTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.evolution.EvolutionRenamedTest._

  /* Uncomment this test and Animal at version 0 to write Pet-v0.snapshot again
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Pet-v0", animal)
  }
   */

  it should "deserialize Horse v0 to Pony v2" in {
    testDeserializeFromFile[Pet]("Pet-v0", Pony("Spirit"))
  }

  // Version 1 renames Horse to Pony and Lion to Cat, then version 2 renames Cat to Horse
  it should "deserialize a subtype that reuses the former name of another subtype" in {
    roundtrip[Pet](infoToSer[Pet], Horse("Spirit"))
  }

  it should "deserialize a Seq of a subtype that reuses the former name of another subtype" in {
    roundtrip[Seq[Horse]](infoToSer[Seq[Horse]], Seq(Horse("Spirit")))
  }

  it should "deserialize a Seq of a subtype written under its former name, now reused" in {
    val formerHorse = new CaseClassSerializer[Pony](
      evolution = Evolutions.get[Pony](classOf[Horse].getName, 0, getClass.getClassLoader),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("name")),
      paramSerializers = Array[TypeSerializer[_]](StringSerializer.INSTANCE)
    )
    val formerSerializer = new SeqSerializer[Pony](formerHorse, classOf[Horse].asInstanceOf[Class[Pony]])

    deserializeFormerForm[Seq[Pony]](formerSerializer, Seq(Pony("Spirit"))) shouldBe Seq(Pony("Spirit"))
  }

}

object EvolutionRenamedTest {

  /* Animal at version 0, which wrote Pet-v0.snapshot
  sealed trait Animal
  case class Horse(name: String) extends Animal
  case class Lion(name: String) extends Animal
   */

  /* Pet at version 1
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
  @version(2)
  @renamed(since = 1, "Lion")
  @renamed(since = 2, "Cat")
  case class Horse(
      @renamed(since = 1, "name") nickname: String
  ) extends Pet
  object Horse extends Evolving[Horse] { val evolutions = Evolutions[Horse] }

}
