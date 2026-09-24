package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.EvolutionTest._
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.apache.flinkx.api.{
  TestUtils,
  added,
  deletedClasses,
  deletedFields,
  postEvolution,
  renamed,
  transformed,
  version
}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** What the evolutions of a field report when they describe the former schema wrongly. */
class EvolutionFieldErrorTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  // Schema of Click v0, as recorded in its snapshot
  private val ClickV0FieldNames = Array("a", "inFileClicks", "fieldNotInFile", "identifier", "b")
  // Former class name of Click v0, as recorded in its snapshot and declared by @renamed on the fixtures
  private val ClickV0ClassName = "org.apache.flinkx.api.EvolutionTest$Click"

  /** Messages of every failure the dry run of the `formerClassName` evolutions reports from the given former field
    * names.
    */
  private def dryRunFailures(formerClassName: String, formerFieldNames: Array[String]): Seq[String] =
    Evolutions
      .find[Any](formerClassName, 0, getClass.getClassLoader)
      .getOrElse(fail(s"No evolution registered for $formerClassName"))
      .dryRun(formerFieldNames)
      .swap
      .map(_.map(_.getMessage).toSeq)
      .getOrElse(Seq.empty)

  // An evolution outside the version range is never applied when it should, so it is refused when the declaration is
  // applied instead of silently sending an unchanged schema down the migration path.
  it should "throw when a field evolution has a since above the current version" in {
    val exception = intercept[SinceNotAllowedException] {
      Evolutions.get(classOf[WrongSinceAboveVersion], 1)
    }
    exception.getMessage shouldBe "An evolution of class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongSinceAboveVersion is declared since=2: it must be between 1 and the current @version(1). Raise @version or fix the since of the annotation"
  }

  it should "throw when a field evolution has a since below 1" in {
    val exception = intercept[SinceNotAllowedException] {
      Evolutions.get(classOf[WrongSinceBelowOne], 1)
    }
    exception.getMessage shouldBe "An evolution of class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongSinceBelowOne is declared since=0: it must be between 1 and the current @version(1). Raise @version or fix the since of the annotation"
  }

  // Rejected where the declaration is read, at compile time
  it should "not compile a declaration with @added on a case class field without default value" in {
    assertDoesNotCompile(
      "implicitly[org.apache.flinkx.api.evolution.Declaration[EvolutionErrorFixtures.WrongAddedOnFieldWithoutDefaultValue]]"
    )
  }

  it should "throw field not found when deserializing Click v0 with wrong added field" in {
    val expected  = WrongAddedField("123456789")
    val exception = intercept[FieldAlreadyExistException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot add 'a'. Field already exists in class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongAddedField. Existing fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong renamed field" in {
    val expected  = WrongRenamedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot rename 'wrongFieldName'. Field not found in class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongRenamedField. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong transformed field" in {
    val expected  = WrongTransformedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot transform 'wrongFieldName'. Field not found in class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongTransformedField. Available fields: [\"a\"]"
  }

  it should "ignore field not found when deserializing Click v0 with wrong deleted field" in {
    val expected = WrongDeletedField("a")
    testDeserializeFromFile("Click-v0", expected)
  }

  // The field names are checked by the dry run, when the schema compatibility is resolved, and no longer on every
  // record: these assert the guidance the user gets, which resolveSchemaCompatibility only logs.
  it should "report an unused former field when dry running Click v0 evolutions" in {
    implicitly[TypeInformation[WrongFieldNotUsed]]

    dryRunFailures(ClickV0ClassName, ClickV0FieldNames) shouldBe Seq(
      "'b' field not used to instantiate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongFieldNotUsed. Use @deletedFields(since=<version>,\"b\") annotation to indicate it has been deleted"
    )
  }

  it should "report every offending field when dry running Click v0 evolutions" in {
    implicitly[TypeInformation[WrongSeveralFields]]

    dryRunFailures(ClickV0ClassName, ClickV0FieldNames) shouldBe Seq(
      "'missing' field missing to instantiate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongSeveralFields. Use @added(since=<version>) annotation to indicate it has been added",
      "'identifier' field not used to instantiate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongSeveralFields. Use @deletedFields(since=<version>,\"identifier\") annotation to indicate it has been deleted",
      "'b' field not used to instantiate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongSeveralFields. Use @deletedFields(since=<version>,\"b\") annotation to indicate it has been deleted"
    )
  }

  it should "report a missing current field when dry running Click v0 evolutions" in {
    implicitly[TypeInformation[WrongMissingField]]

    dryRunFailures(ClickV0ClassName, ClickV0FieldNames) shouldBe Seq(
      "'missingField' field missing to instantiate class org.apache.flinkx.api.evolution.EvolutionErrorFixtures$WrongMissingField. Use @added(since=<version>) annotation to indicate it has been added"
    )
  }

  // The declared evolutions are replayed on the former field names when resolving the compatibility, so a missing or
  // wrong annotation refuses the restore up front instead of failing halfway through the migration.
  it should "resolve the schema compatibility of Click v0 with a wrong added field as incompatible" in {
    resolveSchemaCompatibilityFromFile[WrongAddedField]("Click-v0") shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong renamed field as incompatible" in {
    resolveSchemaCompatibilityFromFile[WrongRenamedField]("Click-v0") shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong transformed field as incompatible" in {
    resolveSchemaCompatibilityFromFile[WrongTransformedField]("Click-v0") shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with an extra field as incompatible" in {
    resolveSchemaCompatibilityFromFile[WrongFieldNotUsed]("Click-v0") shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a missing field as incompatible" in {
    resolveSchemaCompatibilityFromFile[WrongMissingField]("Click-v0") shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a field type changed without annotation as incompatible" in {
    // The rename requires the evolutions, and 'fieldNotInFile' changed from Int to String without @transformed
    resolveSchemaCompatibilityFromFile[WrongUntransformedField]("Click-v0") shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of an evolved case class with an incompatible nested field as incompatible" in {
    // The outer case class evolves through a rename, and its nested field type gained a field without @added
    val formerNestedSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = Evolutions.get(classOf[AddedFieldWithoutAnnotation], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array("a"),
      paramSerializers = Array(createSerializer[String])
    )
    val formerSerializer = new CaseClassSerializer[OuterEvolvedWithNested](
      evolution = Evolutions.get(classOf[OuterEvolvedWithNested], 0),
      version = 0,
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
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array("a"),
      paramSerializers = Array(createSerializer[String])
    )

    resolveSchemaCompatibilityAfterRestore[AddedFieldWithoutAnnotation](formerSerializer) shouldBe Symbol(
      "incompatible"
    )
    // An unversioned ADT declares nothing: registered by hand to read what its dry run would report
    Evolutions.register(new EvolutionBuilder(classOf[AddedFieldWithoutAnnotation], 0, Array("a", "b")))
    dryRunFailures(classOf[AddedFieldWithoutAnnotation].getName, Array("a")) shouldBe Seq(
      "'b' field missing to instantiate class org.apache.flinkx.api.EvolutionTest$AddedFieldWithoutAnnotation. Use @added(since=<version>) annotation to indicate it has been added"
    )
  }

}
