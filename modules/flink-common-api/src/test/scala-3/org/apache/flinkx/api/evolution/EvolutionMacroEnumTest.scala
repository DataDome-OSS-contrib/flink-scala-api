package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.serializer.{Scala3EnumSerializer, Scala3EnumValueSerializer}
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

    evolution.currentEnumValueName("MISSING_TYPE") shouldBe Some("MISSING")
    evolution.currentEnumValueName("OTHER_TYPE") shouldBe None
    intercept[DeletedInstanceException](evolution.restoreEnumValue("OTHER_TYPE")(_ => fail("A deleted value is read")))
  }

  // The former name of a renamed value is borne again by a value added since
  it should "apply the evolution of an enum value only to the data written before it" in {
    Evolutions.get(classOf[Letter], 0).currentEnumValueName("A") shouldBe Some("B")
    Evolutions.get(classOf[Letter], 1).currentEnumValueName("A") shouldBe Some("A")
  }

  it should "serialize an enum value bearing the former name of a renamed value as itself" in {
    val serializer = createSerializer[Letter]
    val out        = new DataOutputSerializer(64)
    serializer.serialize(Letter.A, out)

    serializer.deserialize(new DataInputDeserializer(out.getSharedBuffer)) shouldBe Letter.A
    roundtrip(serializer, Letter.A) // Read by the serializer restored from its snapshot
  }

  // The value names are the same as before, but they swapped: the evolution of the values is still required
  it should "migrate the values of an enum swapping their names" in {
    given TypeSerializer[Swapped] = createSerializer[Swapped]
    val v0                        = Evolutions.get(classOf[Swapped], 0)
    val formerSerializer          = new Scala3EnumSerializer[Swapped](
      evolution = Some(v0),
      version = 0,
      enumValueNames = Array("A", "B"),
      enumValueSerializers =
        Array[TypeSerializer[?]](new Scala3EnumValueSerializer(v0, 0, "A"), new Scala3EnumValueSerializer(v0, 0, "B"))
    )

    deserializeFormerForm[Swapped](formerSerializer, Swapped.A) shouldBe Swapped.B
    resolveSchemaCompatibilityAfterRestore[Swapped](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
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

  /* Swapped v0
  enum Swapped { case A, B }
   */

  @version(1)
  enum Swapped {
    @renamed(since = 1, "B") case A
    @renamed(since = 1, "A") case B
  }
  object Swapped extends Evolving[Swapped] { val evolutions = Evolutions[Swapped] }

  @version(1)
  enum WrongEnumValueRenamedSinceAboveVersion {
    @renamed(since = 2, "A") case B
  }

}
