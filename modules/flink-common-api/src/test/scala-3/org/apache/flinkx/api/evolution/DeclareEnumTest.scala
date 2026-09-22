package org.apache.flinkx.api.evolution

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Checks `Declare.declare[T]` covers a Scala 3 enum, whose value evolutions are declared on its values. */
class DeclareEnumTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  import org.apache.flinkx.api.Scala3EnumTest.FailureCategory

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "declare the enum and the former name it was renamed from" in {
    Declare.declare[FailureCategory]

    Evolutions.declaredEvolutions(classOf[FailureCategory].getClassLoader).keySet shouldBe Set(
      classOf[FailureCategory].getName,
      classOf[FailureCategory].getName.replace("FailureCategory", "FailureType")
    )
  }

  // A deleted value and a renamed value are both declared by the enum, not by the value
  it should "declare the evolutions of the enum values" in {
    Declare.declare[FailureCategory]
    val evolution = Evolutions.get(classOf[FailureCategory], 1)

    evolution.getEnumValueEvolution("MISSING_TYPE") shouldBe Evolution.EnumValueEvolution.Renamed("MISSING")
    evolution.getEnumValueEvolution("OTHER_TYPE") shouldBe Evolution.EnumValueEvolution.DeletedThrowOnInstance
  }

}
