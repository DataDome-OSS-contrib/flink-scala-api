package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EvolutionTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.EvolutionTest._

  // Schema of Click v0, as recorded in its snapshot, see the commented out fixture below
  private val ClickV0FieldNames = Array("a", "inFileClicks", "fieldNotInFile", "identifier", "b")
  // Former class name of Click v0, as recorded in its snapshot and declared by @renamed on the fixtures below
  private val ClickV0ClassName = classOf[Click].getName

  /** Messages of every failure the dry run of the `formerClassName` evolutions reports from the given former field
    * names.
    *
    * The evolutions are registered under the former class name the snapshot records, which is where the restore looks
    * them up: the current class name only resolves from the version of the rename onwards.
    */
  private def dryRunFailures(formerClassName: String, formerFieldNames: Array[String]): Seq[String] =
    Evolutions
      .find[Any](formerClassName, 0, getClass.getClassLoader)
      .getOrElse(fail(s"No evolution registered for $formerClassName"))
      .dryRun(formerFieldNames)
      .swap
      .map(_.map(_.getMessage))
      .getOrElse(Seq.empty)

  override protected def beforeEach(): Unit = {
    auto.cache.clear()
    Evolutions.reset()
  }

  /* Test to serialize Click v0 code into Click-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Click v0" in {
    val event: Click = Click("a", List(ClickEvent("2021-01-01", 1), ClickEvent("2022-02-02", 2)), 5, "id1", "b")
    serializeToFile("Click-v0", event)
  }
   */

  it should "deserialize Click v0 to Click v3" in {
    val expected: Click = Click("id1", List(ClickAction("1", "2021-01-01"), ClickAction("2", "2022-02-02")), 25, "5")
    testDeserializeFromFile("Click-v0", expected)
  }

  /* Test to serialize Event v0 code into Event-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Event v0" in {
    val event: Event = View(123456789)
    serializeToFile("Event-v0", event)
  }
   */

  it should "deserialize Event v0 to Action v1" in {
    val expected: Action = Web(123456790)
    testDeserializeFromFile("Event-v0", expected)
  }

  // A state backend gates the restore on the schema compatibility resolution, before deserializing anything: the
  // evolutions are applied by the restored former serializer, so the state has to be migrated with it.
  it should "resolve the schema compatibility of Click v0 to Click v3 as compatible after migration" in {
    resolveSchemaCompatibilityFromFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of Event v0 to Action v1 as compatible after migration" in {
    resolveSchemaCompatibilityFromFile[Action]("Event-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of a state of Click v0 as compatible after migration" in {
    // The value serializer of a MapState[String, Click], nested in a serializer resolving its nested serializers
    resolveNestedSchemaCompatibilityFromFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  // A snapshot written before 2.4.0 carries no field name, so the fields are read by position: the evolutions can't
  // apply, and the standard resolution checks that positional layout.
  it should "resolve the schema compatibility of a pre-2.4.0 snapshot of an unchanged case class as compatible as is" in {
    val formerSerializer = new CaseClassSerializer[ClickAction](
      evolution = Evolutions.get(classOf[ClickAction], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array.empty,
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    resolveSchemaCompatibility(formerSerializer) shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of an unevolved case class as compatible as is" in {
    resolveSchemaCompatibility[ClickAction](createSerializer[ClickAction]) shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of an unevolved sealed trait as compatible as is" in {
    resolveSchemaCompatibility[Action](createSerializer[Action]) shouldBe Symbol("compatibleAsIs")
  }

  // Two unrelated ADTs must never be migrated into one another, even when the declared evolutions happen to line their
  // schemas up: the former and the current classes are compared, after resolveFormerClass mapped any rename.
  it should "resolve the schema compatibility of an unrelated case class as incompatible" in {
    val formerSerializer = new CaseClassSerializer[UnrelatedFormerCaseClass](
      evolution = Evolutions.get(classOf[UnrelatedFormerCaseClass], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array("a", "removed"),
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    resolveSchemaCompatibilityAfterRestore[UnrelatedCurrentCaseClass](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of an unrelated sealed trait as incompatible" in {
    val formerSerializer = createSerializer[SharedSubtypeTrait]

    resolveSchemaCompatibilityAfterRestore[OtherSharedSubtypeTrait](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a foreign snapshot as incompatible" in {
    resolveSchemaCompatibility[Action](createSerializer[Click].asInstanceOf[TypeSerializer[Action]]) shouldBe Symbol(
      "incompatible"
    )
  }

  // Error handling
  /* Test to serialize Dog v0 code into Dog-v0.snapshot file, uncomment both test and code to regenerate
  it should "serialize Dog v0" in {
    val dog: Dog = Dog("Beethoven", "St. Bernard")
    serializeToFile("Dog-v0", dog)
  }
   */

  it should "deserialize Dog v0 to Dog v2" in {
    val expected: Dog = Dog("Beethoven")
    testDeserializeFromFile("Dog-v0", expected)
  }

  /* Test to serialize Dog v1 code into Dog-v1.snapshot file, uncomment both test and code to regenerate
  it should "serialize Dog v1" in {
    val dog: Dog = Dog("Beethoven", "St. Bernard")
    serializeToFile("Dog-v1", dog)
  }
   */

  it should "deserialize Dog v1 to Dog v2" in {
    val expected: Dog = Dog("Beethoven")
    testDeserializeFromFile("Dog-v1", expected)
  }

  /* Test to serialize Dog v1 code into Dog-v1.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v0" in {
    val animal: Animal = Horse("Spirit")
    serializeToFile("Animal-v0", animal)
  }
   */

  /* Test to serialize Dog v1 code into Dog-v1.snapshot file, uncomment both test and code to regenerate
  it should "serialize Animal v2" in {
    val animal: Animal = Horse(4)
    serializeToFile("Animal-v2", animal)
  }
   */

  it should "deserialize Animal v0 to null" in {
    val expected: Animal = null
    testDeserializeFromFile("Animal-v0", expected)
  }

  it should "deserialize Animal v2" in {
    val expected: Animal = Horse(4)
    testDeserializeFromFile("Animal-v2", expected)
  }

// Error handling

  it should "throw when @version has a wrong current version" in {
    val exception = intercept[VersionNotAllowedException] {
      implicitly[TypeInformation[WrongCurrentVersion]]
    }
    exception.getMessage shouldBe "Current version of class org.apache.flinkx.api.EvolutionTest$WrongCurrentVersion must be >= 0, got @version(-1)"
  }

  it should "throw when @added is on a case class" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnCaseClass]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnCaseClass"
  }

  it should "throw when @renamed is on a case class with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnCaseClassWithoutVersion with version 0"
  }

//  it should "allow when @renamed is on a sealed trait subtype with version 0" in {
//    implicitly[TypeInformation[CorrectRenamedOnSealedTraitSubtypeWithoutVersion]]
//    implicitly[TypeInformation[CorrectRenamedOnCaseClassWithoutVersion]]
//  }

  it should "throw when @transformed is on a case class" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnCaseClass]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnCaseClass"
  }

  it should "throw when @deletedFields is on a case class with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedFieldsOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedFields(1,\"a\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedFieldsOnCaseClassWithoutVersion with version 0"
  }

  it should "throw when @deletedClasses is on a case class with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnCaseClassWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedClasses(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnCaseClassWithoutVersion with version 0"
  }

  it should "throw when @postDeserialize is on a case class twice" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeTwiceOnCaseClass]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeTwiceOnCaseClass twice"
  }

  it should "throw when @version is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongVersionOnField]]
    }
    exception.getMessage shouldBe "@version(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongVersionOnField.a"
  }

  it should "throw when @added is on a case class field with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongAddedOnFieldWithoutVersion with version 0"
  }

  // An evolution outside the version range is never applied when it should, so it is refused at derivation instead of
  // silently sending an unchanged schema down the migration path.
  it should "throw when a field evolution has a since above the current version" in {
    val exception = intercept[SinceNotAllowedException] {
      implicitly[TypeInformation[WrongSinceAboveVersion]]
    }
    exception.getMessage shouldBe "An evolution of class org.apache.flinkx.api.EvolutionTest$WrongSinceAboveVersion is declared since=2: it must be between 1 and the current @version(1). Raise @version or fix the since of the annotation"
  }

  it should "throw when a field evolution has a since below 1" in {
    val exception = intercept[SinceNotAllowedException] {
      implicitly[TypeInformation[WrongSinceBelowOne]]
    }
    exception.getMessage shouldBe "An evolution of class org.apache.flinkx.api.EvolutionTest$WrongSinceBelowOne is declared since=0: it must be between 1 and the current @version(1). Raise @version or fix the since of the annotation"
  }

  it should "throw when @added is on a case class field without default value" in {
    val exception = intercept[AddedFieldWithoutDefaultException] {
      implicitly[TypeInformation[WrongAddedOnFieldWithoutDefaultValue]]
    }
    exception.getMessage shouldBe "'a' added field in class org.apache.flinkx.api.EvolutionTest$WrongAddedOnFieldWithoutDefaultValue must have a default value"
  }

  it should "throw when @renamed is on a case class field with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"a\") annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnFieldWithoutVersion with version 0"
  }

  it should "throw when @transformed is on a case class field with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnFieldWithoutVersion]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on Param(a) of class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnFieldWithoutVersion with version 0"
  }

  it should "throw when @deletedFields is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedFieldsOnField]]
    }
    exception.getMessage shouldBe "@deletedFields(1,\"a\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedFieldsOnField.a"
  }

  it should "throw when @deletedClasses is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnField]]
    }
    exception.getMessage shouldBe "@deletedClasses(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnField.a"
  }

  it should "throw when @postDeserialize is on a case class field" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeOnField]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeOnField.a"
  }

  it should "throw when @added is on a sealed trait" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnSealedTrait]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongAddedOnSealedTrait"
  }

  it should "throw when @renamed is on a sealed trait with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongRenamedOnSealedTraitWithoutVersion with version 0"
  }

  it should "throw when @transformed is on a sealed trait" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTrait]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSealedTrait"
  }

  it should "throw when @postDeserialize is on a sealed trait twice" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeTwiceOnSealedTrait]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeTwiceOnSealedTrait twice"
  }

  it should "throw when @deletedClasses is on a sealed trait with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnSealedTraitWithoutVersion]]
    }
    exception.getMessage shouldBe "@deletedClasses(1,\"A\") annotation is not allowed on interface org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnSealedTraitWithoutVersion with version 0"
  }

  it should "throw when @added is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnSubtypeWithoutVersion$ with version 0"
  }

  it should "throw when @added is on a sealed trait subtype" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongAddedOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@added(1) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongAddedOnSubtype$"
  }

  it should "throw when @renamed is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongRenamedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@renamed(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongRenamedOnSubtypeWithoutVersion$ with version 0"
  }

  it should "throw when @transformed is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTraitSubtypeWithoutVersion]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSubtypeWithoutVersion$ with version 0"
  }

  it should "throw when @transformed is on a sealed trait subtype" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongTransformedOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@transformed(1,<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongTransformedOnSubtype$"
  }

  it should "throw when @deletedClasses is on a sealed trait subtype" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongDeletedClassesOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@deletedClasses(1,\"A\") annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongDeletedClassesOnSubtype$ with version 0"
  }

  it should "allow when @deletedClasses is on a sealed trait subtype being itself a sealed trait" in {
    implicitly[TypeInformation[CorrectDeletedClassesOnSealedTraitSubtype]]
  }

  it should "throw when @postDeserialize is on a sealed trait subtype with version 0" in {
    val exception = intercept[EvolutionNotAllowedException] {
      implicitly[TypeInformation[WrongPostDeserializeOnSealedTraitSubtype]]
    }
    exception.getMessage shouldBe "@postDeserialize(<mapper>) annotation is not allowed on class org.apache.flinkx.api.EvolutionTest$WrongPostDeserializeOnSubtype$ with version 0"
  }

  it should "allow when @postDeserialize is on a versioned sealed trait subtype" in {
    implicitly[TypeInformation[CorrectPostDeserializeOnSealedTraitSubtype]]
  }

  it should "throw field not found when deserializing Click v0 with wrong added field" in {
    val expected  = WrongAddedField("123456789")
    val exception = intercept[FieldAlreadyExistException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot add 'a'. Field already exists in class org.apache.flinkx.api.EvolutionTest$WrongAddedField. Existing fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong renamed field" in {
    val expected  = WrongRenamedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot rename 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongRenamedField. Available fields: [\"a\"]"
  }

  it should "throw field not found when deserializing Click v0 with wrong transformed field" in {
    val expected  = WrongTransformedField("123456789")
    val exception = intercept[FieldNotFoundException] {
      testDeserializeFromFile("Click-v0", expected)
    }
    exception.getMessage shouldBe "Cannot transform 'wrongFieldName'. Field not found in class org.apache.flinkx.api.EvolutionTest$WrongTransformedField. Available fields: [\"a\"]"
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
      "'b' field not used to instantiate class org.apache.flinkx.api.EvolutionTest$WrongFieldNotUsed. Use @deletedFields(since=<version>,\"b\") annotation to indicate it has been deleted"
    )
  }

  it should "report every offending field when dry running Click v0 evolutions" in {
    implicitly[TypeInformation[WrongSeveralFields]]

    dryRunFailures(ClickV0ClassName, ClickV0FieldNames) shouldBe Seq(
      "'missing' field missing to instantiate class org.apache.flinkx.api.EvolutionTest$WrongSeveralFields. Use @added(since=<version>) annotation to indicate it has been added",
      "'identifier' field not used to instantiate class org.apache.flinkx.api.EvolutionTest$WrongSeveralFields. Use @deletedFields(since=<version>,\"identifier\") annotation to indicate it has been deleted",
      "'b' field not used to instantiate class org.apache.flinkx.api.EvolutionTest$WrongSeveralFields. Use @deletedFields(since=<version>,\"b\") annotation to indicate it has been deleted"
    )
  }

  it should "report a missing current field when dry running Click v0 evolutions" in {
    implicitly[TypeInformation[WrongMissingField]]

    dryRunFailures(ClickV0ClassName, ClickV0FieldNames) shouldBe Seq(
      "'missingField' field missing to instantiate class org.apache.flinkx.api.EvolutionTest$WrongMissingField. Use @added(since=<version>) annotation to indicate it has been added"
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

  // A checkpoint written by a more recent source code, typically after a rollback: the annotations describing the
  // versions in between don't exist here, so nothing can drive the migration.
  it should "resolve the schema compatibility of a sealed trait restored by an outdated source code as incompatible" in {
    val derived          = createSerializer[RolledBackTrait].asInstanceOf[CoproductSerializer[RolledBackTrait]]
    val formerSerializer = new CoproductSerializer[RolledBackTrait](
      evolution = derived.evolution,
      version = derived.version + 1,
      subtypeClasses = derived.subtypeClasses.dropRight(1),
      subtypeFqns = derived.subtypeFqns.dropRight(1),
      subtypeSerializers = derived.subtypeSerializers.dropRight(1)
    )

    resolveSchemaCompatibility(formerSerializer) shouldBe Symbol("incompatible")
  }

  // The former and the current classes are compared after resolveFormerClass mapped the former name to the current
  // class, so a renamed case class must compare equal to itself.
  it should "resolve the schema compatibility of a renamed case class as compatible after migration" in {
    val formerSerializer = new CaseClassSerializer[FormerRenamedCaseClass](
      evolution = Evolutions.get(classOf[FormerRenamedCaseClass], 0),
      version = 0,
      isCaseClassImmutable = true,
      fieldNames = Array("a", "removed"),
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    resolveSchemaCompatibilityAfterRestore[RenamedCaseClass](formerSerializer) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  it should "resolve the schema compatibility of a sealed trait with a subtype removed without annotation as incompatible" in {
    // The current schema drops the last subtype without declaring it with @deletedClasses
    val derived = createSerializer[SubtypeRemovedWithoutAnnotation].asInstanceOf[CoproductSerializer[
      SubtypeRemovedWithoutAnnotation
    ]]
    val formerSerializer = new CoproductSerializer[SubtypeRemovedWithoutAnnotation](
      evolution = derived.evolution,
      version = 0,
      subtypeClasses = derived.subtypeClasses,
      subtypeFqns = derived.subtypeFqns,
      subtypeSerializers = derived.subtypeSerializers
    )
    val currentSerializer = new CoproductSerializer[SubtypeRemovedWithoutAnnotation](
      evolution = derived.evolution,
      version = 1,
      subtypeClasses = derived.subtypeClasses.dropRight(1),
      subtypeFqns = derived.subtypeFqns.dropRight(1),
      subtypeSerializers = derived.subtypeSerializers.dropRight(1)
    )

    currentSerializer
      .snapshotConfiguration()
      .resolveSchemaCompatibility(formerSerializer.snapshotConfiguration()) shouldBe Symbol("incompatible")
  }

  // A serializer restored from a snapshot written before 2.4.0 legitimately holds no field name, as the test above
  // checks, but any other count than one name per field serializer describes no readable form.
  it should "throw when a case class serializer holds a field name count of its own" in {
    val exception = intercept[IllegalArgumentException] {
      new CaseClassSerializer[ClickAction](
        evolution = Evolutions.get(classOf[ClickAction], 1),
        version = 1,
        isCaseClassImmutable = true,
        fieldNames = Array("id"),
        paramSerializers = Array(createSerializer[String], createSerializer[String])
      )
    }
    exception.getMessage should endWith("has 1 field names for 2 field serializers")
  }

  it should "throw when two ADTs declare the same former class name" in {
    implicitly[TypeInformation[FirstClaimingFormerName]]
    val exception = intercept[FormerClassConflictException] {
      implicitly[TypeInformation[SecondClaimingFormerName]]
    }
    exception.getMessage shouldBe "Former class 'org.apache.flinkx.api.EvolutionTest$SharedFormerName' is already declared as renamed to class org.apache.flinkx.api.EvolutionTest$FirstClaimingFormerName, it can't also be declared as renamed to class org.apache.flinkx.api.EvolutionTest$SecondClaimingFormerName. Two ADTs can't share the same former class name: fix their @renamed or @deletedClasses annotations"
  }

  it should "throw when a former class name is declared both renamed and deleted" in {
    implicitly[TypeInformation[ClaimingFormerNameByRename]]
    val exception = intercept[FormerClassConflictException] {
      implicitly[TypeInformation[ClaimingFormerNameByDeletion]]
    }
    exception.getMessage shouldBe "Former class 'org.apache.flinkx.api.EvolutionTest$ClaimedByBoth' is already declared as renamed to class org.apache.flinkx.api.EvolutionTest$ClaimingFormerNameByRename, it can't also be declared as deleted. Two ADTs can't share the same former class name: fix their @renamed or @deletedClasses annotations"
  }

  // The same ADT is derived once per set of member type information, so a given annotation is legitimately read
  // several times and declaring the same resolution again must stay a no-op.
  it should "not throw when the same ADT declares its former class name twice" in {
    implicitly[TypeInformation[FirstClaimingFormerName]]
    auto.cache.clear() // Forces a second derivation of the very same ADT
    implicitly[TypeInformation[FirstClaimingFormerName]] shouldNot be(null)
  }

  it should "throw when deserializing Event v0 with delete subtype instance" in {
    val exception = intercept[DeletedInstanceException] {
      testDeserializeFromFile[WrongDeletedClassesWithSubtypeInstanceThrow]("Event-v0", null)
    }
    exception.getMessage shouldBe "Encountered an instance of deleted 'org.apache.flinkx.api.EvolutionTest$View' class during deserialization. Don't delete a class in usage or use @deletedClasses(since = <version>, throwOnInstance = false, ...) to deserialize it as null instead"
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
    dryRunFailures(classOf[AddedFieldWithoutAnnotation].getName, Array("a")) shouldBe Seq(
      "'b' field missing to instantiate class org.apache.flinkx.api.EvolutionTest$AddedFieldWithoutAnnotation. Use @added(since=<version>) annotation to indicate it has been added"
    )
  }

  it should "return null when deserializing Event v0 with delete subtype instance" in {
    testDeserializeFromFile[WrongDeletedClassesWithSubtypeInstanceToNull]("Event-v0", null)
  }

}

object EvolutionTest {

  // API demonstration

  /* click-v0
  case class Click(
      a: String,
      inFileClicks: List[ClickEvent],
      fieldNotInFile: Int,
      identifier: String,
      b: String
  )

  case class ClickEvent(
      date: String,
      sessionId: Int
  )
   */

  @version(3)
  @deletedFields(since = 1, "a")
  @deletedFields(since = 2, "b")
  @postDeserialize(updateClick)
  case class Click(
      @renamed(since = 2, "identifier") id: String,
      inFileClicks: List[ClickAction],
      @added(since = 3) fieldInFile: Int = 1,
      @transformed(since = 1, mapIntToString) fieldNotInFile: String
  )

  @version(1)
  @renamed(since = 1, "ClickEvent")
  case class ClickAction(
      @renamed(since = 1, "sessionId") @transformed(since = 1, mapIntToString) id: String,
      date: String
  )

  def mapIntToString(a: Int): String                 = a.toString
  def updateClick(version: Int, click: Click): Click =
    click.copy(fieldInFile = click.fieldNotInFile.toInt * 5 + version)

  /* event-v0
  sealed trait Event
  case class View(ts: Long) extends Event
  case class Purchase(price: Double) extends Event
   */

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, "Purchase")
  @postDeserialize(updateAction)
  sealed trait Action

  @version(0)
  case object Login extends Action

  @version(1)
  @renamed(since = 1, "View")
  case class Web(ts: Long) extends Action

  case class Cart(items: Int) extends Action

  def updateAction(version: Int, action: Action): Action = action match {
    case Web(ts) => Web(if (version == 0) ts + 1 else ts)
    case Cart(_) => Login
    case e @ _   => e
  }

  case class AddedFieldWithoutAnnotation(a: String, b: Int = 42)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongUntransformedField(@renamed(since = 1, "a") renamedA: String, fieldNotInFile: String)

  @version(1)
  case class OuterEvolvedWithNested(@renamed(since = 1, "formerNested") nested: AddedFieldWithoutAnnotation)

  case class UnrelatedFormerCaseClass(a: String, removed: String)

  @version(1)
  @deletedFields(since = 1, "removed")
  case class UnrelatedCurrentCaseClass(a: String)

  // Both traits are sealed in this file, so a single case class can be a member of each of them
  @version(1)
  sealed trait SharedSubtypeTrait
  @version(1)
  sealed trait OtherSharedSubtypeTrait
  case class SharedSubtype(a: String) extends SharedSubtypeTrait with OtherSharedSubtypeTrait

  @version(1)
  @renamed(since = 1, "FormerRenamedCaseClass")
  @deletedFields(since = 1, "removed")
  case class RenamedCaseClass(a: String)

  // Still declared to play the former class: the snapshot records its name, which the rename resolves
  case class FormerRenamedCaseClass(a: String, removed: String)

  @version(2)
  sealed trait RolledBackTrait
  case class RolledBackA(a: String) extends RolledBackTrait
  case class RolledBackB(b: Int)    extends RolledBackTrait

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class FirstClaimingFormerName(a: String)

  @version(1)
  @renamed(since = 1, "SharedFormerName")
  case class SecondClaimingFormerName(a: String)

  @version(1)
  @renamed(since = 1, "ClaimedByBoth")
  case class ClaimingFormerNameByRename(a: String)

  @version(1)
  @deletedFields(since = 1, "removed")
  @deletedClasses(since = 1, throwOnInstance = false, "ClaimedByBoth")
  case class ClaimingFormerNameByDeletion(a: String)

  @version(1)
  sealed trait SubtypeRemovedWithoutAnnotation
  case class RemainingSubtype(a: String) extends SubtypeRemovedWithoutAnnotation
  case class RemovedSubtype(b: Int)      extends SubtypeRemovedWithoutAnnotation
  /* Dog-v0
  case class Dog(name: String, kind: String)
   */

  /* Dog-v1
  @version(1)
  case class Dog(
    name: String,
    @renamed(since = 1, "kind") breed: String
  )
   */

  @version(2)
  @deletedFields(since = 1, "kind")
  @deletedFields(since = 2, "breed")
  case class Dog(name: String)

  /* Animal-v0
  sealed trait Animal
  case class Horse(name: String) extends Animal
  case class Lion(name: String) extends Animal
   */

  /* Animal-v1
  @version(1)
  @deletedClasses(since = 1, "Horse")
  sealed trait Animal
  case class Lion(name: String) extends Animal
   */

  @version(2)
  @deletedClasses(since = 1, throwOnInstance = false, "Horse")
  sealed trait Animal
  @version(2)
  case class Horse(legs: Int)   extends Animal
  case class Lion(name: String) extends Animal

  // Error handling

  @version(-1)
  case class WrongCurrentVersion()

  @version(1)
  @added(since = 1)
  case class WrongAddedOnCaseClass()

  @renamed(since = 1, "A")
  case class WrongRenamedOnCaseClassWithoutVersion()

  @version(1)
  sealed trait CorrectRenamedOnSealedTraitSubtypeWithoutVersion
  @renamed(since = 1, "A")
  case class CorrectRenamedOnCaseClassWithoutVersion() extends CorrectRenamedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  @transformed(since = 1, identity[Int])
  case class WrongTransformedOnCaseClass()

  @deletedFields(1, "a")
  case class WrongDeletedFieldsOnCaseClassWithoutVersion()

  @deletedClasses(since = 1, "A")
  case class WrongDeletedClassesOnCaseClassWithoutVersion()

  @version(1)
  @postDeserialize(updateClick)
  @postDeserialize(updateAction)
  case class WrongPostDeserializeTwiceOnCaseClass()

  @version(1)
  case class WrongVersionOnField(@version(1) a: String)

  case class WrongAddedOnFieldWithoutVersion(@added(1) a: String)

  @version(1)
  case class WrongAddedOnFieldWithoutDefaultValue(@added(1) a: String)

  @version(1)
  case class WrongSinceAboveVersion(@added(since = 2) a: String = "")

  @version(1)
  @deletedFields(since = 0, "removed")
  case class WrongSinceBelowOne(a: String)

  case class WrongRenamedOnFieldWithoutVersion(@renamed(1, "a") a: String)

  case class WrongTransformedOnFieldWithoutVersion(@transformed(1, identity[String]) a: String)

  @version(1)
  case class WrongDeletedFieldsOnField(@deletedFields(1, "a") a: String)

  @version(1)
  case class WrongDeletedClassesOnField(@deletedClasses(since = 1, "A") a: String)

  @version(1)
  case class WrongPostDeserializeOnField(@postDeserialize(updateClick) a: String)

  @version(1)
  @added(since = 1)
  sealed trait WrongAddedOnSealedTrait
  case object Subtype1 extends WrongAddedOnSealedTrait

  @renamed(since = 1, "A")
  sealed trait WrongRenamedOnSealedTraitWithoutVersion
  case object Subtype2 extends WrongRenamedOnSealedTraitWithoutVersion

  @version(1)
  @transformed(since = 1, identity[Int])
  sealed trait WrongTransformedOnSealedTrait
  case object Subtype3 extends WrongTransformedOnSealedTrait

  @version(1)
  @postDeserialize(updateClick)
  @postDeserialize(updateAction)
  sealed trait WrongPostDeserializeTwiceOnSealedTrait
  case object Subtype4 extends WrongPostDeserializeTwiceOnSealedTrait

  @deletedClasses(since = 1, "A")
  sealed trait WrongDeletedClassesOnSealedTraitWithoutVersion
  case object Subtype5 extends WrongDeletedClassesOnSealedTraitWithoutVersion

  @version(1)
  sealed trait WrongAddedOnSealedTraitSubtypeWithoutVersion
  @added(since = 1)
  case object WrongAddedOnSubtypeWithoutVersion extends WrongAddedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongAddedOnSealedTraitSubtype
  @version(1)
  @added(since = 1)
  case object WrongAddedOnSubtype extends WrongAddedOnSealedTraitSubtype

  sealed trait WrongRenamedOnSealedTraitSubtypeWithoutVersion
  @renamed(since = 1, "A")
  case object WrongRenamedOnSubtypeWithoutVersion extends WrongRenamedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongTransformedOnSealedTraitSubtypeWithoutVersion
  @transformed(since = 1, identity[Int])
  case object WrongTransformedOnSubtypeWithoutVersion extends WrongTransformedOnSealedTraitSubtypeWithoutVersion

  @version(1)
  sealed trait WrongTransformedOnSealedTraitSubtype
  @version(1)
  @transformed(since = 1, identity[Int])
  case object WrongTransformedOnSubtype extends WrongTransformedOnSealedTraitSubtype

  @version(1)
  sealed trait WrongDeletedClassesOnSealedTraitSubtype
  @deletedClasses(since = 1, "A")
  case object WrongDeletedClassesOnSubtype extends WrongDeletedClassesOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectDeletedClassesOnSealedTraitSubtype
  @version(1)
  @deletedClasses(since = 1, "A")
  sealed trait CorrectDeletedClassesOnSubtype extends CorrectDeletedClassesOnSealedTraitSubtype
  case object CorrectDeletedClassesCaseObject extends CorrectDeletedClassesOnSubtype

  @version(1)
  sealed trait WrongPostDeserializeOnSealedTraitSubtype
  @postDeserialize(updateAction)
  case object WrongPostDeserializeOnSubtype extends WrongPostDeserializeOnSealedTraitSubtype

  @version(1)
  sealed trait CorrectPostDeserializeOnSealedTraitSubtype
  @version(1)
  @postDeserialize(updateAction)
  case class CorrectPostDeserializeOnSubtype() extends CorrectPostDeserializeOnSealedTraitSubtype

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongAddedField(@added(since = 2) a: String = "")

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongRenamedField(@renamed(since = 2, "wrongFieldName") a: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongTransformedField(@transformed(since = 2, identity[String]) wrongFieldName: String)

  @version(2)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedFields(since = 2, "wrongFieldName")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongDeletedField(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongSeveralFields(a: String, missing: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongFieldNotUsed(a: String)

  @version(1)
  @renamed(since = 1, "Click")
  @deletedFields(since = 1, "inFileClicks", "fieldNotInFile", "identifier", "b")
  @deletedClasses(since = 1, throwOnInstance = false, "ClickEvent")
  case class WrongMissingField(a: String, missingField: String)

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, throwOnInstance = true, "View")
  sealed trait WrongDeletedClassesWithSubtypeInstanceThrow

  @version(1)
  @renamed(since = 1, "Purchase")
  case object WrongDeletedClassesWithSubtypeInstanceThrowSubtype extends WrongDeletedClassesWithSubtypeInstanceThrow

  @version(1)
  @renamed(since = 1, "Event")
  @deletedClasses(since = 1, throwOnInstance = false, "View")
  sealed trait WrongDeletedClassesWithSubtypeInstanceToNull

  @version(1)
  @renamed(since = 1, "Purchase")
  case object WrongDeletedClassesWithSubtypeInstanceToNullSubtype extends WrongDeletedClassesWithSubtypeInstanceToNull

}
