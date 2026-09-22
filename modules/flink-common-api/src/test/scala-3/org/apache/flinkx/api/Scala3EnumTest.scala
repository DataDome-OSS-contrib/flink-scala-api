package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.evolution.Declare
import org.apache.flinkx.api.serializer.{Scala3EnumSerializer, Scala3EnumValueSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Scala3EnumTest extends AnyFlatSpec with Matchers with TestUtils {

  import Scala3EnumTest.*

  it should "derive type information for a Scala 3 enum" in {
    Declare.declare[Example]
    summon[TypeInformation[Example]] shouldNot be(null)
  }

  // A value is a member of its enum, not an ADT of its own: what it records has to describe the enum it belongs to
  it should "serialize an enum value with the version of its enum" in {
    Declare.declare[FailureCategory]
    val enumSerializer = createSerializer[FailureCategory].asInstanceOf[Scala3EnumSerializer[FailureCategory & Product]]

    enumSerializer.version shouldBe 1
    forAll(enumSerializer.enumValueSerializers.toSeq) {
      _.asInstanceOf[Scala3EnumValueSerializer[?]].version shouldBe enumSerializer.version
    }
  }

  it should "derive type information for a Scala 3 enum value" in {
    Declare.declare[Example]
    summon[TypeInformation[Example.Foo]] shouldNot be(null)
  }

  it should "roundtrip a enum" in {
    Declare.declare[Failure]
    testTypeInfoAndSerializer(Failure.PARSE_ERROR, false)
  }

  it should "roundtrip an enum with parameter" in {
    Declare.declare[FailureCategory]
    testTypeInfoAndSerializer(FailureCategory.PARSING, false)
  }

  it should "roundtrip an enum with a simple case" in {
    Declare.declare[Example]
    testTypeInfoAndSerializer(Example.Bar, false)
  }

  it should "roundtrip an enum with a case with parameters" in {
    Declare.declare[Example]
    testTypeInfoAndSerializer[Example](Example.Foo("a", 2))
  }

  it should "roundtrip an enum value with parameters" in {
    Declare.declare[Example]
    testTypeInfoAndSerializer[Example.Foo](Example.Foo("a", 2))
  }

  it should "roundtrip a case class declaring an enum" in {
    Declare.declare[FailureEvent]
    Declare.declare[FailureCategory]
    testTypeInfoAndSerializer(FailureEvent("a", FailureCategory.PARSING))
  }

  it should "roundtrip a case class declaring an enum value" in {
    Declare.declare[FooEvent]
    Declare.declare[Example]
    testTypeInfoAndSerializer(FooEvent("a", Example.Foo("a", 2)))
  }

  /* Test to serialize FailureType.PARSING_TYPE v0 code into Failure-Type-PARSING_TYPE-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize FailureType.PARSING_TYPE v0" in {
    val failureType: FailureType = FailureType.PARSING_TYPE
    serializeToFile("Failure-Type-PARSING_TYPE-v0", failureType)
  }
   */

  it should "deserialize FailureType.PARSING_TYPE v0 to FailureCategory.PARSING v1" in {
    Declare.declare[FailureCategory]
    val expected: FailureCategory = FailureCategory.PARSING
    testDeserializeFromFile("Failure-Type-PARSING_TYPE-v0", expected)
  }

  // A state backend gates the restore on the schema compatibility resolution, before deserializing anything: the
  // evolutions are applied by the restored former serializer, so the state has to be migrated with it.
  it should "resolve the schema compatibility of FailureType v0 to FailureCategory v1 as compatible after migration" in {
    Declare.declare[FailureCategory]
    resolveSchemaCompatibilityFromFile[FailureCategory]("Failure-Type-PARSING_TYPE-v0") shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  it should "resolve the schema compatibility of an enum with a value removed without annotation as incompatible" in {
    Declare.declare[ValueRemovedWithoutAnnotation]
    // The current schema drops the last enum value without declaring it with @deletedClasses
    val derived = createSerializer[ValueRemovedWithoutAnnotation]
      .asInstanceOf[Scala3EnumSerializer[ValueRemovedWithoutAnnotation & Product]]
    val formerSerializer = new Scala3EnumSerializer(
      evolution = derived.evolution,
      version = 0,
      enumValueNames = derived.enumValueNames,
      enumValueSerializers = derived.enumValueSerializers
    )
    val currentSerializer = new Scala3EnumSerializer(
      evolution = derived.evolution,
      version = 1,
      enumValueNames = derived.enumValueNames.dropRight(1),
      enumValueSerializers = derived.enumValueSerializers.dropRight(1)
    )

    currentSerializer
      .snapshotConfiguration()
      .resolveSchemaCompatibility(formerSerializer.snapshotConfiguration()) shouldBe Symbol("incompatible")
  }

  // A checkpoint written by a more recent source code, typically after a rollback: the annotations describing the
  // versions in between don't exist here, so nothing can drive the migration.
  it should "resolve the schema compatibility of an enum restored by an outdated source code as incompatible" in {
    Declare.declare[RolledBackEnum]
    val derived          = createSerializer[RolledBackEnum].asInstanceOf[Scala3EnumSerializer[RolledBackEnum & Product]]
    val formerSerializer = new Scala3EnumSerializer(
      evolution = derived.evolution,
      version = derived.version + 1,
      enumValueNames = derived.enumValueNames.dropRight(1),
      enumValueSerializers = derived.enumValueSerializers.dropRight(1)
    )

    resolveSchemaCompatibility(formerSerializer) shouldBe Symbol("incompatible")
  }

  // Two unrelated enums must never be migrated into one another, even with identical value names
  it should "resolve the schema compatibility of an unrelated enum as incompatible" in {
    Declare.declare[SecondUnrelatedEnum]
    Declare.declare[FirstUnrelatedEnum]
    resolveSchemaCompatibilityAfterRestore[SecondUnrelatedEnum](createSerializer[FirstUnrelatedEnum]) shouldBe Symbol(
      "incompatible"
    )
  }

  it should "resolve the schema compatibility of an unevolved enum as compatible as is" in {
    Declare.declare[Failure]
    resolveSchemaCompatibility[Failure](createSerializer[Failure]) shouldBe Symbol("compatibleAsIs")
  }

  /* Test to serialize FailureType.OTHER_TYPE v0 code into Failure-Type-OTHER_TYPE-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize FailureType.OTHER_TYPE v0" in {
    val failureType: FailureType = FailureType.OTHER_TYPE
    serializeToFile("Failure-Type-OTHER_TYPE-v0", failureType)
  }
   */

  it should "throw when deserializing deleted FailureType.OTHER_TYPE v0" in {
    Declare.declare[FailureCategory]
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile[FailureCategory]("Failure-Type-OTHER_TYPE-v0", null)
    }
    exception.getMessage shouldBe "Encountered an instance of deleted 'org.apache.flinkx.api.Scala3EnumTest$FailureCategory#OTHER_TYPE' class during deserialization. Don't delete a class in usage or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
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

  @version(1)
  enum FirstUnrelatedEnum {
    case X, Y
  }

  @version(1)
  enum SecondUnrelatedEnum {
    case X, Y
  }

  @version(2)
  enum RolledBackEnum {
    case First, Second
  }

  @version(1)
  enum ValueRemovedWithoutAnnotation {
    case Remaining, Removed
  }

  enum Example {
    case Foo(a: String, b: Int)
    case Bar
  }

  case class FailureEvent(step: String, category: FailureCategory)

  case class FooEvent(step: String, foo: Example.Foo)

}
