package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.util.FlinkRuntimeException
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.evolution.AdtEvolution.NoEvolution
import org.apache.flinkx.api.evolution.{Evolutions, Evolving, deletedClasses, renamed, version}
import org.apache.flinkx.api.serializer.{Scala3EnumSerializer, Scala3EnumValueSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Scala3EnumTest extends AnyFlatSpec with Matchers with TestUtils {

  import Scala3EnumTest.*

  it should "derive type information for a Scala 3 enum" in {
    summon[TypeInformation[Example]] shouldNot be(null)
  }

  // An enum value is not an ADT: its snapshot records the class name and the schema version of its enum
  it should "serialize an enum value with the version of its enum" in {
    val enumSerializer = infoToSer[FailureCategory].asInstanceOf[Scala3EnumSerializer[FailureCategory & Product]]

    enumSerializer.evolution.map(_.formerVersion) shouldBe Some(1)
    forAll(enumSerializer.enumValueSerializers.toSeq) {
      _.asInstanceOf[Scala3EnumValueSerializer[?]].evolution.formerVersion shouldBe 1
    }
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

  /* Uncomment this test and FailureType at version 0 to write Failure-Type-PARSING_TYPE-v0.snapshot again
  it should "serialize FailureType.PARSING_TYPE v0" in {
    val failureType: FailureType = FailureType.PARSING_TYPE
    serializeToFile("Failure-Type-PARSING_TYPE-v0", failureType)
  }
   */

  it should "deserialize FailureType.PARSING_TYPE v0 to FailureCategory.PARSING v1" in {
    val expected: FailureCategory = FailureCategory.PARSING
    testDeserializeFromFile("Failure-Type-PARSING_TYPE-v0", expected)
  }

  // The restored serializer applies the evolutions, so the state needs a migration before the current serializer
  // can use it
  it should "resolve the schema compatibility of FailureType v0 to FailureCategory v1 as compatible after migration" in {
    compatibilityWithFile[FailureCategory]("Failure-Type-PARSING_TYPE-v0") shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  it should "resolve the schema compatibility of an enum with a value deleted without annotation as incompatible" in {
    // The current schema deletes the last enum value without @deletedClasses
    val derived = infoToSer[ValueRemovedWithoutAnnotation]
      .asInstanceOf[Scala3EnumSerializer[ValueRemovedWithoutAnnotation & Product]]
    val formerSerializer = new Scala3EnumSerializer(
      evolution = derived.evolution,
      enumValueNames = derived.enumValueNames,
      enumValueSerializers = derived.enumValueSerializers
    )
    val currentSerializer = new Scala3EnumSerializer(
      evolution = derived.evolution,
      enumValueNames = derived.enumValueNames.dropRight(1),
      enumValueSerializers = derived.enumValueSerializers.dropRight(1)
    )

    currentSerializer
      .snapshotConfiguration()
      .resolveSchemaCompatibility(formerSerializer.snapshotConfiguration()) shouldBe Symbol("incompatible")
  }

  // After a rollback, the evolutions of the higher versions are missing: the migration is not possible
  it should "resolve the schema compatibility of an enum after a rollback as incompatible" in {
    val derived          = infoToSer[RolledBackEnum].asInstanceOf[Scala3EnumSerializer[RolledBackEnum & Product]]
    val formerSerializer = new Scala3EnumSerializer(
      // Written at a version higher than the current version
      evolution = derived.evolution.map(current => new NoEvolution(current.currentClass, current.formerVersion + 1)),
      enumValueNames = derived.enumValueNames,
      enumValueSerializers = derived.enumValueSerializers
    )

    compatibilityWith[RolledBackEnum](formerSerializer) shouldBe Symbol("incompatible")
  }

  // The migration never converts an enum to an unrelated enum, even if their value names are the same
  it should "resolve the schema compatibility of an unrelated enum as incompatible" in {
    compatibilityWith[SecondUnrelatedEnum](infoToSer[FirstUnrelatedEnum]) shouldBe Symbol(
      "incompatible"
    )
  }

  it should "resolve the schema compatibility of an enum declaring no evolution as compatible as is" in {
    compatibilityWith[Failure](infoToSer[Failure]) shouldBe Symbol("compatibleAsIs")
  }

  // The migration finds the current value of each former value by its name, not by its position
  it should "restore the reordered values of an unversioned enum, a value being added" in {
    given TypeSerializer[Failure]     = infoToSer[Failure]
    val derived                       = summon[TypeSerializer[Failure]].asInstanceOf[Scala3EnumSerializer[Failure]]
    def valueSerializer(name: String) = derived.enumValueSerializers(derived.enumValueNames.indexOf(name))
    // At version 0, Failure had only UNKNOWN and MISSING_KEY, in this order
    val formerSerializer = new Scala3EnumSerializer[Failure](
      evolution = derived.evolution,
      enumValueNames = Array("UNKNOWN", "MISSING_KEY"),
      enumValueSerializers = Array(valueSerializer("UNKNOWN"), valueSerializer("MISSING_KEY"))
    )

    deserializeFormerForm[Failure](formerSerializer, Failure.MISSING_KEY) shouldBe Failure.MISSING_KEY
    compatibilityWith[Failure](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  /* Uncomment this test and FailureType at version 0 to write Failure-Type-OTHER_TYPE-v0.snapshot again
  it should "serialize FailureType.OTHER_TYPE v0" in {
    val failureType: FailureType = FailureType.OTHER_TYPE
    serializeToFile("Failure-Type-OTHER_TYPE-v0", failureType)
  }
   */

  it should "throw when deserializing deleted FailureType.OTHER_TYPE v0" in {
    val exception = intercept[FlinkRuntimeException] {
      testDeserializeFromFile[FailureCategory]("Failure-Type-OTHER_TYPE-v0", null)
    }
    exception.getMessage shouldBe "The former data contains an instance of 'org.apache.flinkx.api.Scala3EnumTest$FailureCategory#OTHER_TYPE', a deleted class or enum value. Keep this class, or declare @deletedClasses(since = <version>, throwOnInstance = false, ...) to read it as null"
  }

}

object Scala3EnumTest {

  enum Failure {
    case MISSING_KEY, PARSE_ERROR, UNKNOWN
  }

  /* FailureType at version 0, which wrote the Failure-Type-*-v0.snapshot files
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
  object FailureCategory extends Evolving[FailureCategory] { val evolutions = Evolutions[FailureCategory] }

  @version(1)
  enum FirstUnrelatedEnum {
    case X, Y
  }
  object FirstUnrelatedEnum extends Evolving[FirstUnrelatedEnum] {
    val evolutions = Evolutions[FirstUnrelatedEnum]
  }

  @version(1)
  enum SecondUnrelatedEnum {
    case X, Y
  }
  object SecondUnrelatedEnum extends Evolving[SecondUnrelatedEnum] {
    val evolutions = Evolutions[SecondUnrelatedEnum]
  }

  @version(2)
  enum RolledBackEnum {
    case First, Second
  }
  object RolledBackEnum extends Evolving[RolledBackEnum] { val evolutions = Evolutions[RolledBackEnum] }

  @version(1)
  enum ValueRemovedWithoutAnnotation {
    case Remaining, Removed
  }
  object ValueRemovedWithoutAnnotation extends Evolving[ValueRemovedWithoutAnnotation] {
    val evolutions = Evolutions[ValueRemovedWithoutAnnotation]
  }

  enum Example {
    case Foo(a: String, b: Int)
    case Bar
  }

  case class FailureEvent(step: String, category: FailureCategory)

  case class FooEvent(step: String, foo: Example.Foo)

}
