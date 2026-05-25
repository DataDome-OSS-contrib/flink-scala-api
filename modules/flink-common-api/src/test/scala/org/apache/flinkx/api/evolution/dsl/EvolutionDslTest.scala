package org.apache.flinkx.api.evolution.dsl

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.Evolutions
import org.apache.flinkx.api.evolution.dsl.EvolutionDslTest._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionDslTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  // -- Core DSL: structural correctness ---------------------------------------

  it should "build an Evolved descriptor from a chain" in {
    val ev: Evolved[Foo] = Evolution
      .of[Foo]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed(formerName = "oldName", currentName = "name")

    ev.currentVersion shouldBe 2
    ev.fieldDeltas should have size 2
  }

  it should "produce field deltas in the order they were declared" in {
    val ev: Evolved[Foo] = Evolution
      .of[Foo]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed("oldName", "name")
      .transformed[Int, String]("count", _.toString)
      .deletedFormerFields("dropped1", "dropped2")

    // Lambdas don't compare by equality across captures; assert structure independently.
    ev.fieldDeltas should have size 5
    ev.fieldDeltas(0) shouldBe FieldDelta.Add("addedAtV2", since = 2)
    ev.fieldDeltas(1) shouldBe FieldDelta.Rename("oldName", "name", since = 1)
    ev.fieldDeltas(2) match {
      case t: FieldDelta.Transform[_, _] =>
        t.fieldName shouldBe "count"
        t.since shouldBe 1
        t.mapper.asInstanceOf[Int => String](42) shouldBe "42"
      case other => fail(s"expected Transform, got $other")
    }
    ev.fieldDeltas(3) shouldBe FieldDelta.Delete("dropped1", since = 1)
    ev.fieldDeltas(4) shouldBe FieldDelta.Delete("dropped2", since = 1)
  }

  it should "reject a non-monotonic version sequence" in {
    val ex = intercept[IllegalArgumentException] {
      Evolution.of[Foo].version(1).version(2)
    }
    ex.getMessage should include("strictly decreasing order")
  }

  it should "reject a negative current version" in {
    val ex = intercept[IllegalArgumentException] {
      Evolution.of[Foo].version(-1)
    }
    ex.getMessage should include(">= 0")
  }

  it should "reject calling postDeserialize twice" in {
    val ex = intercept[IllegalArgumentException] {
      Evolution
        .of[Foo]
        .version(1)
        .postDeserialize((_, f: Foo) => f)
        .postDeserialize((_, f: Foo) => f)
    }
    ex.getMessage should include("at most once")
  }

  // -- Test accessors macros

  it should "extract field name and capture the default value from a `_.field` lambda in `.added`" in {
    val ev: Evolved[Foo] = Evolution.of[Foo].version(2).added(_.addedAtV2)
    // The macro captures `Foo.apply$default$2` (= 0) so the runtime check at derivation time is unnecessary.
    ev.fieldDeltas should contain only FieldDelta.Add("addedAtV2", since = 2, default = Some(0))
  }

  it should "reject added non-existent field at compile time" in {
    "Evolution.of[Foo].version(1).added(_.doesNotExist)" shouldNot compile
  }

  it should "reject `.added(_.field)` at compile time when the field has no default value" in {
    "Evolution.of[Foo].version(2).added(_.name)" shouldNot compile
  }

  it should "extract current field name in `.renamed`" in {
    // Scala 2 macros don't support named arguments; pass formerName positionally.
    val ev: Evolved[Foo] = Evolution.of[Foo].version(2).renamed("oldName", _.name)
    ev.fieldDeltas should contain only FieldDelta.Rename("oldName", "name", since = 2)
  }

  it should "reject renamed non-existent field at compile time" in {
    "Evolution.of[Foo].version(1).renamed(\"oldName\", _.doesNotExist)" shouldNot compile
  }

  it should "extract current field name in `.transformed`" in {
    val ev: Evolved[Foo] = Evolution.of[Foo].version(2).transformed(_.count, (i: Int) => i.toString)
    ev.fieldDeltas should have size 1
    ev.fieldDeltas.head match {
      case t: FieldDelta.Transform[_, _] =>
        t.fieldName shouldBe "count"
        t.since shouldBe 2
      case other => fail(s"expected Transform, got $other")
    }
  }

  it should "reject transformed non-existent field at compile time" in {
    "Evolution.of[Foo].version(1).transformed(_.doesNotExist, (i: Int) => i.toString)" shouldNot compile
  }

  // -- Integration: DSL recognised by TypeInformationDerivation via implicit summoning -----------

  it should "drive the runtime Evolution registry from an implicit Evolved[T]" in {
    // Per-test implicit; would normally live in Bar's companion.
    implicit val barEvolved: Evolved[Bar] = Evolution
      .of[Bar]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed("oldName", "name")

    implicitly[TypeInformation[Bar]] should not be null

    val runtime = Evolutions.get(classOf[Bar])
    runtime should not be null
    runtime.toString should not be empty
  }

  it should "round-trip a DSL-evolved case class with no actual evolution" in {
    implicit val barEvolved: Evolved[Bar] = Evolution.of[Bar].version(2)
    val expected                          = Bar(name = "n", addedAtV2 = 7)
    testTypeInfoAndSerializer[Bar](expected)
  }

  it should "apply the merged FieldEvolutions during deserialization" in {
    // Smallest end-to-end test of the merger producing a working `Evolution`. Constructed by hand to bypass derivation.
    val evolved: Evolved[Bar] = Evolution
      .of[Bar]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed("oldName", "name")

    val builder = new org.apache.flinkx.api.evolution.EvolutionBuilder[Bar](
      classOf[Bar],
      Array("name", "addedAtV2")
    )
    EvolvedMerger.merge[Bar](
      evolved,
      builder,
      classOf[Bar],
      { case "addedAtV2" => Some(42); case _ => None }
    )
    val runtime = builder.build()

    // Simulate v0 data: only "oldName" field present, no "addedAtV2".
    val fieldMap = scala.collection.mutable.Map[String, AnyRef]("oldName" -> "alpha")
    runtime.applyFieldEvolutions(formerVersion = 0, fieldMap)
    val values = runtime.toFieldValues(fieldMap)

    values.toSeq shouldBe Seq("alpha", java.lang.Integer.valueOf(42))
  }

}

object EvolutionDslTest {

  /** Used purely to inspect Evolved descriptor without invoking derivation. */
  case class Foo(name: String, addedAtV2: Int = 0, count: String = "0")

  /** Used to exercise the end-to-end derivation merge path. */
  case class Bar(name: String, addedAtV2: Int = 0)

}
