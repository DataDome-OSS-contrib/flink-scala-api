package org.apache.flinkx.api.evolution

import EvolutionTest._
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** What the evolutions of a field report when they describe the former schema wrongly. */
class EvolutionFieldErrorTest extends AnyFlatSpec with Matchers with TestUtils {

  // Schema of Click v0, as recorded in its snapshot
  private val ClickV0FieldNames = Array("a", "inFileClicks", "fieldNotInFile", "identifier", "b")

  /** Click v0 restored from its snapshot file, migrating with the evolutions `T` declares from version 0. */
  private def clickV0As[T <: Product](clazz: Class[T]): CaseClassSerializer[T] = {
    val clickV0 = restoreSerializerFromFile[Click]("Click-v0").asInstanceOf[CaseClassSerializer[Click]]
    new CaseClassSerializer[T](
      evolution = Evolutions.get(clazz, 0),
      isCaseClassImmutable = clickV0.isCaseClassImmutable,
      fieldNames = clickV0.fieldNames,
      paramSerializers = clickV0.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )
  }

  /** Reads the record of the Click v0 snapshot file, migrating it with the evolutions `T` declares from version 0. */
  private def readClickV0As[T <: Product](clazz: Class[T]): T = {
    val input = new DataInputViewStreamWrapper(snapshotInput("Click-v0"))
    TypeSerializerSnapshot.readVersionedSnapshot[Click](input, getClass.getClassLoader)
    input.readInt() // Size of the snapshot
    clickV0As(clazz).deserialize(input)
  }

  /** Every failure the plan of the evolutions `clazz` declares from version 0 reports on Click v0. */
  private def planFailures(clazz: Class[_]): Seq[String] =
    Evolutions.get(clazz, 0).plan(ClickV0FieldNames).swap.getOrElse(Seq.empty)

  // Rejected where the declaration is read, at compile time
  it should "not compile a declaration with @added on a case class field without default value" in {
    assertDoesNotCompile(
      "Evolutions[EvolutionErrorFixtures.WrongAddedOnFieldWithoutDefaultValue]"
    )
  }

  it should "throw field already exists when deserializing Click v0 with wrong added field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongAddedField]))
    exception.getMessage shouldBe "Cannot migrate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongAddedField from version 0: Cannot add 'a': field already exists. Existing fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong renamed field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongRenamedField]))
    exception.getMessage shouldBe "Cannot migrate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongRenamedField from version 0: Cannot rename 'wrongFieldName': field not found. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong transformed field" in {
    val exception = intercept[MigrationException](readClickV0As(classOf[WrongTransformedField]))
    exception.getMessage shouldBe "Cannot migrate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongTransformedField from version 0: Cannot transform 'wrongFieldName': field not found. Available fields: [\"a\"]"
  }

  it should "ignore field not found when deserializing Click v0 with wrong deleted field" in {
    readClickV0As(classOf[WrongDeletedField]) shouldBe WrongDeletedField("a")
  }

  // The field names are checked by the plan, when the schema compatibility is resolved, and no longer on every
  // record: these assert the guidance the user gets, which resolveSchemaCompatibility only logs.
  it should "report an unused former field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongFieldNotUsed]) shouldBe Seq(
      "'b' former field not used. Use @deletedFields(since=<version>,\"b\") annotation to indicate its deletion"
    )
  }

  it should "report every offending field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongSeveralFields]) shouldBe Seq(
      "'missing' field missing. Use @added(since=<version>) annotation to indicate it has been added",
      "'identifier' former field not used. Use @deletedFields(since=<version>,\"identifier\") annotation to indicate its deletion",
      "'b' former field not used. Use @deletedFields(since=<version>,\"b\") annotation to indicate its deletion"
    )
  }

  it should "report a missing current field when planning Click v0 evolutions" in {
    planFailures(classOf[WrongMissingField]) shouldBe Seq(
      "'missingField' field missing. Use @added(since=<version>) annotation to indicate it has been added"
    )
  }

  // The declared evolutions are replayed on the former field names when resolving the compatibility, so a missing or
  // wrong annotation refuses the restore up front instead of failing halfway through the migration.
  it should "resolve the schema compatibility of Click v0 with a wrong added field as incompatible" in {
    resolveSchemaCompatibility(clickV0As(classOf[WrongAddedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong renamed field as incompatible" in {
    resolveSchemaCompatibility(clickV0As(classOf[WrongRenamedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong transformed field as incompatible" in {
    resolveSchemaCompatibility(clickV0As(classOf[WrongTransformedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with an extra field as incompatible" in {
    resolveSchemaCompatibility(clickV0As(classOf[WrongFieldNotUsed])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a missing field as incompatible" in {
    resolveSchemaCompatibility(clickV0As(classOf[WrongMissingField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a field type changed without annotation as incompatible" in {
    // The rename requires the evolutions, and 'fieldNotInFile' changed from Int to String without @transformed
    resolveSchemaCompatibility(clickV0As(classOf[WrongUntransformedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of an evolved case class with an incompatible nested field as incompatible" in {
    // The outer case class evolves through a rename, and its nested field type gained a field without @added
    val formerNestedSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = Evolutions.get(classOf[AddedFieldWithoutAnnotation], 0),
      isCaseClassImmutable = true,
      fieldNames = Array("a"),
      paramSerializers = Array(createSerializer[String])
    )
    val formerSerializer = new CaseClassSerializer[OuterEvolvedWithNested](
      evolution = Evolutions.get(classOf[OuterEvolvedWithNested], 0),
      isCaseClassImmutable = true,
      fieldNames = Array("formerNested"),
      paramSerializers = Array(formerNestedSerializer)
    )

    resolveSchemaCompatibility(formerSerializer) shouldBe Symbol("incompatible")
  }

  // A field added without the @added annotation has no declared default value to fill when reading a former form, even
  // when the case class declares one: the evolution has to be described rather than silently guessed.
  it should "resolve the schema compatibility of a case class with a field added without annotation as incompatible" in {
    val formerSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = Evolutions.get(classOf[AddedFieldWithoutAnnotation], 0),
      isCaseClassImmutable = true,
      fieldNames = Array("a"),
      paramSerializers = Array(createSerializer[String])
    )

    resolveSchemaCompatibilityAfterRestore[AddedFieldWithoutAnnotation](formerSerializer) shouldBe Symbol(
      "incompatible"
    )
    // An unversioned ADT declares nothing: its no-op evolution still reports what its plan would hit
    Evolutions.get(classOf[AddedFieldWithoutAnnotation], 0).plan(Array("a")).swap shouldBe Right(
      Seq("'b' field missing. Use @added(since=<version>) annotation to indicate it has been added")
    )
  }

}
