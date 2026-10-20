package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.serializer.{Scala3EnumSerializer, Scala3EnumValueSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The ADT evolution of a Scala 3 enum, which holds the evolutions of its enum values. */
class EvolutionMacroEnumTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.Scala3EnumTest.FailureCategory
  import EvolutionMacroEnumTest.*

  it should "declare the enum and the former name it was renamed from" in {
    FailureCategory.evolutions.declarations.map(_.className).toSet shouldBe Set(
      classOf[FailureCategory].getName,
      classOf[FailureCategory].getName.replace("FailureCategory", "FailureType")
    )
  }

  // The ADT evolution of the enum holds the deletion and the rename of its values: a value has no ADT evolution
  it should "declare the evolutions of the enum values" in {
    val evolution = evolutionOf(classOf[FailureCategory])

    evolution.currentEnumValueName("MISSING_TYPE") shouldBe Some("MISSING")
    evolution.currentEnumValueName("OTHER_TYPE") shouldBe None
    intercept[DeletedInstanceException](evolution.migrateEnumValue("OTHER_TYPE")(_ => fail("A deleted value is read")))
  }

  // A value added in version 1 reuses the former name of a renamed value
  it should "apply the evolution of an enum value only to the data written before it" in {
    evolutionOf(classOf[Letter]).currentEnumValueName("A") shouldBe Some("B")
    evolutionOf(classOf[Letter], 1).currentEnumValueName("A") shouldBe Some("A")
  }

  it should "serialize an enum value that reuses the former name of a renamed value as itself" in {
    val serializer = createSerializer[Letter]
    val out        = new DataOutputSerializer(64)
    serializer.serialize(Letter.A, out)

    serializer.deserialize(new DataInputDeserializer(out.getSharedBuffer)) shouldBe Letter.A
    roundtrip(serializer, Letter.A) // Read again by the restored serializer
  }

  // The two values exchange their names: the names stay the same, but the migration is necessary
  it should "migrate the values of an enum swapping their names" in {
    given TypeSerializer[Swapped] = createSerializer[Swapped]
    val v0                        = evolutionOf(classOf[Swapped])
    val formerSerializer          = new Scala3EnumSerializer[Swapped](
      evolution = Some(v0),
      enumValueNames = Array("A", "B"),
      enumValueSerializers =
        Array[TypeSerializer[?]](new Scala3EnumValueSerializer(v0, "A"), new Scala3EnumValueSerializer(v0, "B"))
    )

    deserializeFormerForm[Swapped](formerSerializer, Swapped.A) shouldBe Swapped.B
    compatibilityWith[Swapped](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "not compile a declaration renaming an enum value since a version above the one of its enum" in {
    assertTypeError("Evolutions[WrongEnumValueRenamedSinceAboveVersion]")
  }

}

object EvolutionMacroEnumTest {

  /* Letter at version 0
  enum Letter { case A }
   */

  @version(1)
  enum Letter {
    @renamed(since = 1, "A") case B
    case A
  }
  object Letter extends Evolving[Letter] { val evolutions = Evolutions[Letter] }

  /* Swapped at version 0
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
