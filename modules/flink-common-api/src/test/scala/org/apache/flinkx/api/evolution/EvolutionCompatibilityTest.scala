package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.Evolution.NoEvolution
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.evolution.EvolutionTest._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A state backend gates the restore of a savepoint on the schema compatibility resolution, before deserializing
  * anything: the declared evolutions must entirely describe the migration from the former schema, applied by the
  * restored former serializer, or the restore is refused up front instead of failing halfway through the migration.
  */
class EvolutionCompatibilityTest extends AnyFlatSpec with Matchers with TestUtils {

  /** Click v0 restored from its snapshot file, migrating with the evolutions `T` declares from version 0. */
  private def clickV0As[T <: Product](clazz: Class[T]): CaseClassSerializer[T] = {
    val clickV0 = restoreSerializerFromFile[Click]("Click-v0").asInstanceOf[CaseClassSerializer[Click]]
    new CaseClassSerializer[T](
      evolution = evolutionOf(clazz),
      isCaseClassImmutable = clickV0.isCaseClassImmutable,
      fieldNames = clickV0.fieldNames,
      paramSerializers = clickV0.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )
  }

  it should "resolve the schema compatibility of Click v0 to Click v3 as compatible after migration" in {
    compatibilityWithFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of Event v0 to Action v1 as compatible after migration" in {
    compatibilityWithFile[Action]("Event-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of a state of Click v0 as compatible after migration" in {
    // The value serializer of a MapState[String, Click], nested in a serializer resolving its nested serializers
    nestedCompatibilityWithFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  // The renamed case class is compared with itself once its former name resolves to its current class
  it should "resolve the schema compatibility of a renamed case class as compatible after migration" in {
    // No class bears the former name anymore: the snapshot records it, as the former code wrote it
    val formerName       = classOf[RenamedCaseClass].getName.replace("RenamedCaseClass", "FormerRenamedCaseClass")
    val formerSerializer = new CaseClassSerializer[RenamedCaseClass](
      evolution = new Evolution(formerName, 0, classOf[RenamedCaseClass]),
      isCaseClassImmutable = true,
      fieldNames = Array("a", "removed"),
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    compatibilityWith[RenamedCaseClass](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of a versioned case class written at its current version as is" in {
    compatibilityWith[ClickAction](createSerializer[ClickAction]) shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of a versioned sealed trait written at its current version as is" in {
    compatibilityWith[Action](createSerializer[Action]) shouldBe Symbol("compatibleAsIs")
  }

  // A snapshot written before 2.4.0 carries no field name, so the fields are read by position: the evolutions can't
  // apply, and the standard resolution checks that positional layout
  it should "resolve the schema compatibility of a case class snapshot recording no field name as is" in {
    val formerSerializer = new CaseClassSerializer[ClickAction](
      evolution = evolutionOf(classOf[ClickAction]),
      isCaseClassImmutable = true,
      fieldNames = Array.empty,
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    compatibilityWith[ClickAction](formerSerializer) shouldBe Symbol("compatibleAsIs")
  }

  // Two unrelated ADTs must never be migrated into one another, even when the declared evolutions happen to line their
  // schemas up: the former and the current classes are compared, once the former name resolves to its current class
  it should "resolve the schema compatibility of an unrelated case class as incompatible" in {
    val formerSerializer = new CaseClassSerializer[UnrelatedFormerCaseClass](
      evolution = evolutionOf(classOf[UnrelatedFormerCaseClass]),
      isCaseClassImmutable = true,
      fieldNames = Array("a", "removed"),
      paramSerializers = Array(createSerializer[String], createSerializer[String])
    )

    compatibilityWith[UnrelatedCurrentCaseClass](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of an unrelated sealed trait as incompatible" in {
    compatibilityWith[OtherSharedSubtypeTrait](createSerializer[SharedSubtypeTrait]) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a foreign snapshot as incompatible" in {
    compatibilityWith[Action](createSerializer[Click]) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong added field as incompatible" in {
    compatibilityWith[WrongAddedField](clickV0As(classOf[WrongAddedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong renamed field as incompatible" in {
    compatibilityWith[WrongRenamedField](clickV0As(classOf[WrongRenamedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a wrong transformed field as incompatible" in {
    compatibilityWith[WrongTransformedField](clickV0As(classOf[WrongTransformedField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with an extra field as incompatible" in {
    compatibilityWith[WrongFieldNotUsed](clickV0As(classOf[WrongFieldNotUsed])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a missing field as incompatible" in {
    compatibilityWith[WrongMissingField](clickV0As(classOf[WrongMissingField])) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of Click v0 with a field type changed without annotation as incompatible" in {
    // The rename requires the evolutions, and 'fieldNotInFile' changed from Int to String without @transformed
    compatibilityWith[WrongUntransformedField](clickV0As(classOf[WrongUntransformedField])) shouldBe Symbol(
      "incompatible"
    )
  }

  it should "resolve the schema compatibility of an evolved case class with an incompatible nested field as incompatible" in {
    // The outer case class evolves through a rename, and its nested field type gained a field without @added
    val formerNestedSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = evolutionOf(classOf[AddedFieldWithoutAnnotation]),
      isCaseClassImmutable = true,
      fieldNames = Array("a"),
      paramSerializers = Array(createSerializer[String])
    )
    val formerSerializer = new CaseClassSerializer[OuterEvolvedWithNested](
      evolution = evolutionOf(classOf[OuterEvolvedWithNested]),
      isCaseClassImmutable = true,
      fieldNames = Array("formerNested"),
      paramSerializers = Array(formerNestedSerializer)
    )

    compatibilityWith[OuterEvolvedWithNested](formerSerializer) shouldBe Symbol("incompatible")
  }

  // A field added without the @added annotation has no declared default value to fill when reading a former form, even
  // when the case class declares one: the evolution has to be described rather than silently guessed
  it should "resolve the schema compatibility of a case class with a field added without annotation as incompatible" in {
    val formerSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = evolutionOf(classOf[AddedFieldWithoutAnnotation]),
      isCaseClassImmutable = true,
      fieldNames = Array("a"),
      paramSerializers = Array(createSerializer[String])
    )

    compatibilityWith[AddedFieldWithoutAnnotation](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a sealed trait with a subtype removed without annotation as incompatible" in {
    // The current schema drops the last subtype without declaring it with @deletedClasses
    val derived = createSerializer[SubtypeRemovedWithoutAnnotation]
      .asInstanceOf[CoproductSerializer[SubtypeRemovedWithoutAnnotation]]
    val currentSerializer = new CoproductSerializer[SubtypeRemovedWithoutAnnotation](
      evolution = derived.evolution,
      subtypeClasses = derived.subtypeClasses.dropRight(1),
      subtypeFqns = derived.subtypeFqns.dropRight(1),
      subtypeSerializers = derived.subtypeSerializers.dropRight(1)
    )

    compatibilityWith[SubtypeRemovedWithoutAnnotation](derived)(currentSerializer) shouldBe Symbol("incompatible")
  }

  // A savepoint written by a more recent source code, typically after a rollback: the annotations describing the
  // versions in between don't exist here, so nothing can drive the migration
  it should "resolve the schema compatibility of a sealed trait restored by an outdated source code as incompatible" in {
    val derived          = createSerializer[RolledBackTrait].asInstanceOf[CoproductSerializer[RolledBackTrait]]
    val formerSerializer = new CoproductSerializer[RolledBackTrait](
      // Written at the next version, which this source code doesn't know
      evolution = derived.evolution.map(current => new NoEvolution(current.currentClass, current.formerVersion + 1)),
      subtypeClasses = derived.subtypeClasses.dropRight(1),
      subtypeFqns = derived.subtypeFqns.dropRight(1),
      subtypeSerializers = derived.subtypeSerializers.dropRight(1)
    )

    compatibilityWith[RolledBackTrait](formerSerializer) shouldBe Symbol("incompatible")
  }

}
