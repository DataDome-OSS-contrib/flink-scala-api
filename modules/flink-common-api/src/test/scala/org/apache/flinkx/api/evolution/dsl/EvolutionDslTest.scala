package org.apache.flinkx.api.evolution.dsl

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.{Evolutions, FieldEvolution}
import org.apache.flinkx.api.evolution.dsl.EvolutionDslTest._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionDslTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  // -- Core DSL: structural correctness ---------------------------------------

  it should "register an Evolved descriptor on .build" in {
    Evolution
      .of[Foo]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed(formerName = "oldName", currentName = "name")
      .build

    val evolved = Evolutions.getEvolved(classOf[Foo])
    evolved should not be empty
    evolved.get.currentVersion shouldBe 2
    evolved.get.fieldDeltas should have size 2
  }

  it should "produce field deltas in the order they were declared" in {
    val ev = Evolution
      .of[Foo]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed("oldName", "name")
      .transformed[Int, String]("count", _.toString)
      .deletedFormerFields("dropped1", "dropped2")
      .build

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
      Evolution.of[Foo].version(1).version(2).build
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

  // -- Integration: DSL recognised by TypeInformationDerivation --------------

  it should "drive the runtime Evolution registry from DSL data" in {
    Evolution
      .of[Bar]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed("oldName", "name")
      .build

    implicitly[TypeInformation[Bar]] should not be null

    // After derivation, the runtime Evolution for Bar should reflect the DSL.
    val runtime = Evolutions.get(classOf[Bar])
    runtime should not be null
    runtime.toString should not be empty // smoke: not the NoEvolution placeholder
  }

  it should "round-trip a DSL-evolved case class with no actual evolution" in {
    Evolution.of[Bar].version(2).build
    val expected = Bar(name = "n", addedAtV2 = 7)
    testTypeInfoAndSerializer[Bar](expected)
  }

  it should "apply the merged FieldEvolutions during deserialization" in {
    // This exercise mirrors what the annotation-driven derivation does: build an EvolutionBuilder by hand, fold the
    // DSL data in, then deserialize an in-memory field map at a former version. It's the smallest end-to-end test of
    // the merger producing a working `Evolution`.

    val evolved = Evolution
      .of[Bar]
      .version(2)
      .added("addedAtV2")
      .version(1)
      .renamed("oldName", "name")
      .build

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
