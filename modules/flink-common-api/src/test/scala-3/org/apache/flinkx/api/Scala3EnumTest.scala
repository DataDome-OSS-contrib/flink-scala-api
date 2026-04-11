package org.apache.flinkx.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.EvolutionTest.{Action, Web}
import org.apache.flinkx.api.auto.*

class Scala3EnumTest extends AnyFlatSpec with Matchers with TestUtils {

  import Scala3EnumTest.*

  it should "derive type information for a Scala 3 enum" in {
    summon[TypeInformation[Example]] shouldNot be(null)
  }

  it should "derive type information for a Scala 3 enum value" in {
    summon[TypeInformation[Example.Foo]] shouldNot be(null)
  }

  it should "roundtrip a enum" in {
    testTypeInfoAndSerializer(Failure.PARSE_ERROR, false)
  }

  it should "roundtrip an enum with parameter" in {
    testTypeInfoAndSerializer(FailureCategory.PARSING, false)
  }

  it should "roundtrip an enum with a simple case" in {
    testTypeInfoAndSerializer(Example.Bar, false)
  }

  it should "roundtrip an enum with a case with parameters" in {
    testTypeInfoAndSerializer[Example](Example.Foo("a", 2))
  }

  it should "roundtrip an enum value with parameters" in {
    testTypeInfoAndSerializer[Example.Foo](Example.Foo("a", 2))
  }

  it should "roundtrip a case class declaring an enum" in {
    testTypeInfoAndSerializer(FailureEvent("a", FailureCategory.PARSING))
  }

  it should "roundtrip a case class declaring an enum value" in {
    testTypeInfoAndSerializer(FooEvent("a", Example.Foo("a", 2)))
  }

  /*
  it should "serialize FailureType v0" in {
    val failureType: FailureType = FailureType.PARSING_TYPE
    serializeToFile("Failure-Type-v0", failureType)
  }
   */

  it should "deserialize FailureType v0 to FailureCategory v1" in {
    val expected: FailureCategory = FailureCategory.PARSING
    // ClassNotFoundException is thrown because magnolia doesn't get annotations on enum values
    // see https://github.com/softwaremill/magnolia/issues/491
    an[FlinkRuntimeException] shouldBe thrownBy {
      testDeserializeFromFile("Failure-Type-v0", expected)
    }
  }

}

object Scala3EnumTest {

  enum Failure {
    case MISSING_KEY, PARSE_ERROR, UNKNOWN
  }

  /* failure-type-v0
  enum FailureType(a: String) {
    case MISSING_TYPE extends FailureType("a")
    case PARSING_TYPE extends FailureType("b")
    case OTHER_TYPE extends FailureType("c")
  }
   */

  @version(1)
  @renamed(since = 1, "FailureType")
  enum FailureCategory(a: Int) {
    @renamed(since = 1, "MISSING_TYPE")
    case MISSING extends FailureCategory(1)
    @renamed(since = 1, "PARSING_TYPE")
    case PARSING extends FailureCategory(2)
    @renamed(since = 1, "OTHER_TYPE")
    case OTHER extends FailureCategory(3)
  }

  enum Example {
    case Foo(a: String, b: Int)
    case Bar
  }

  case class FailureEvent(step: String, category: FailureCategory)

  case class FooEvent(step: String, foo: Example.Foo)

}
