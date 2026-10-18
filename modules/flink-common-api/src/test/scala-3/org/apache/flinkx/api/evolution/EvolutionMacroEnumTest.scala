package org.apache.flinkx.api.evolution

import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.evolution.Evolution.EnumValueEvolution.{Deleted, Renamed, Unchanged}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks the declaration of a Scala 3 enum, whose value evolutions are declared on its values. */
class EvolutionMacroEnumTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.Scala3EnumTest.FailureCategory
  import EvolutionMacroEnumTest.*

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "declare the enum and the former name it was renamed from" in {
    FailureCategory.evolutions.declarations.map(_.formerClassName).toSet shouldBe Set(
      classOf[FailureCategory].getName,
      classOf[FailureCategory].getName.replace("FailureCategory", "FailureType")
    )
  }

  // A deleted value and a renamed value are both declared by the enum, not by the value
  it should "declare the evolutions of the enum values" in {
    val evolution = Evolutions.get(classOf[FailureCategory], 0)

    evolution.getEnumValueEvolution("MISSING_TYPE") shouldBe Renamed("MISSING")
    evolution.getEnumValueEvolution("OTHER_TYPE") shouldBe Deleted(throwOnInstance = true)
  }

  // The former name of a renamed value is borne again by a value added since
  it should "apply the evolution of an enum value only to the data written before it" in {
    Evolutions.get(classOf[Letter], 0).getEnumValueEvolution("A") shouldBe Renamed("B")
    Evolutions.get(classOf[Letter], 1).getEnumValueEvolution("A") shouldBe Unchanged
  }

  it should "serialize an enum value bearing the former name of a renamed value as itself" in {
    val serializer = createSerializer[Letter]
    val out        = new DataOutputSerializer(64)
    serializer.serialize(Letter.A, out)

    serializer.deserialize(new DataInputDeserializer(out.getSharedBuffer)) shouldBe Letter.A
    roundtrip(serializer, Letter.A) // Read by the serializer restored from its snapshot
  }

  it should "not compile a declaration renaming an enum value since a version above the one of its enum" in {
    assertDoesNotCompile("Evolutions[WrongEnumValueRenamedSinceAboveVersion]")
  }

}

object EvolutionMacroEnumTest {

  /* Letter v0
  enum Letter { case A }
   */

  @version(1)
  enum Letter {
    @renamed(since = 1, "A") case B
    case A
  }
  object Letter extends Evolving[Letter] { val evolutions = Evolutions[Letter] }

  @version(1)
  enum WrongEnumValueRenamedSinceAboveVersion {
    @renamed(since = 2, "A") case B
  }

}
