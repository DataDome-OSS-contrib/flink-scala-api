package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.auto.*
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

  /* Test to serialize FailureType.PARSING_TYPE v0 code into Failure-Type-PARSING_TYPE-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize FailureType.PARSING_TYPE v0" in {
    val failureType: FailureType = FailureType.PARSING_TYPE
    serializeToFile("Failure-Type-PARSING_TYPE-v0", failureType)
  }
   */

  it should "deserialize FailureType.PARSING_TYPE v0 to FailureCategory.PARSING v1" in {
    val expected: FailureCategory = FailureCategory.PARSING
    testDeserializeFromFile("Failure-Type-PARSING_TYPE-v0", expected)
  }

  /* Test to serialize FailureType.OTHER_TYPE v0 code into Failure-Type-OTHER_TYPE-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize FailureType.OTHER_TYPE v0" in {
    val failureType: FailureType = FailureType.OTHER_TYPE
    serializeToFile("Failure-Type-OTHER_TYPE-v0", failureType)
  }
   */

  it should "throw when deserializing deleted FailureType.OTHER_TYPE v0" in {
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile[FailureCategory]("Failure-Type-OTHER_TYPE-v0", null)
    }
    exception.getMessage shouldBe "Encountered an instance of deleted 'org.apache.flinkx.api.Scala3EnumTest$FailureCategory$OTHER_TYPE' class during deserialization. Don't delete a class in usage or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
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
  @deletedClasses(since = 1, throwOnInstance = true, "OTHER_TYPE")
  enum FailureCategory(a: Int) {
    @renamed(since = 1, "MISSING_TYPE")
    case MISSING extends FailureCategory(1)
    @renamed(since = 1, "PARSING_TYPE")
    case PARSING extends FailureCategory(2)
  }

  enum Example {
    case Foo(a: String, b: Int)
    case Bar
  }

  case class FailureEvent(step: String, category: FailureCategory)

  case class FooEvent(step: String, foo: Example.Foo)

}
