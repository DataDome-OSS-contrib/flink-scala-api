package org.apache.flinkx.api.evolution.dsl

import org.apache.flinkx.api.evolution.Evolutions
import org.apache.flinkx.api.evolution.dsl.EvolutionDslAccessors._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionDslAccessorsTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  import EvolutionDslAccessorsTest._

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "extract field name from a `_.field` lambda in `.addedField`" in {
    val ev: Evolved[Sample] = Evolution.of[Sample].version(1).addedField(_.addedAtV1)
    ev.fieldDeltas should contain only FieldDelta.Add("addedAtV1", since = 1)
  }

  it should "extract current field name in `.renamedField`" in {
    // Scala 2 macros don't support named arguments; pass formerName positionally.
    val ev: Evolved[Sample] = Evolution.of[Sample].version(1).renamedField(_.name, "oldName")
    ev.fieldDeltas should contain only FieldDelta.Rename("oldName", "name", since = 1)
  }

  it should "extract current field name in `.transformedField`" in {
    val ev: Evolved[Sample] = Evolution.of[Sample].version(1).transformedField[Int, String](_.count, _.toString)
    ev.fieldDeltas should have size 1
    ev.fieldDeltas.head match {
      case t: FieldDelta.Transform[_, _] =>
        t.fieldName shouldBe "count"
        t.since shouldBe 1
      case other => fail(s"expected Transform, got $other")
    }
  }

  it should "reject a non-existent field at compile time" in {
    // Uncommenting the next line should fail compilation:
    //   Evolution.of[Sample].version(1).addedField(_.doesNotExist)
    succeed
  }

}

object EvolutionDslAccessorsTest {
  case class Sample(name: String, count: String, addedAtV1: Int = 0)
}
