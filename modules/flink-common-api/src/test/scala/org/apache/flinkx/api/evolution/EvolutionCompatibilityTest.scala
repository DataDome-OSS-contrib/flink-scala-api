package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.AdtEvolution.NoEvolution
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.evolution.EvolutionTest._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The schema compatibility resolution of the ADT serializers, which Flink does before it reads the state. The declared
  * evolutions must describe all the migration from the former schema, else the restore stops before the migration.
  */
class EvolutionCompatibilityTest extends AnyFlatSpec with Matchers with TestUtils {

  /** The restored serializer of Click-v0.snapshot, with the ADT evolution of `T` for the data written at version 0. */
  private def clickV0As[T <: Product](clazz: Class[T]): CaseClassSerializer[T] = {
    val clickV0 = restoreSerializerFromFile[Click]("Click-v0").asInstanceOf[CaseClassSerializer[Click]]
    new CaseClassSerializer[T](
      evolution = evolutionOf(clazz),
      isCaseClassImmutable = clickV0.isCaseClassImmutable,
      fieldNames = clickV0.fieldNames,
      scalaFieldSerializers = clickV0.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )
  }

  it should "resolve the schema compatibility of Click v0 to Click v3 as compatible after migration" in {
    compatibilityWithFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of Event v0 to Action v1 as compatible after migration" in {
    compatibilityWithFile[Action]("Event-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of a state of Click v0 as compatible after migration" in {
    // The value serializer of a MapState[String, Click], nested in a Flink composite serializer
    nestedCompatibilityWithFile[Click]("Click-v0") shouldBe Symbol("compatibleAfterMigration")
  }

  // The lookup gives the current class of the former class name, so the case class is compared with itself
  it should "resolve the schema compatibility of a renamed case class as compatible after migration" in {
    // No class has the former class name now: only the former snapshot records it
    val formerName       = classOf[RenamedCaseClass].getName.replace("RenamedCaseClass", "FormerRenamedCaseClass")
    val formerSerializer = new CaseClassSerializer[RenamedCaseClass](
      evolution = new AdtEvolution(formerName, 0, classOf[RenamedCaseClass]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("a", "deleted")),
      scalaFieldSerializers = Array(infoToSer[String], infoToSer[String])
    )

    compatibilityWith[RenamedCaseClass](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  it should "resolve the schema compatibility of a versioned case class written at its current version as is" in {
    compatibilityWith[ClickAction](infoToSer[ClickAction]) shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of a versioned sealed trait written at its current version as is" in {
    compatibilityWith[Action](infoToSer[Action]) shouldBe Symbol("compatibleAsIs")
  }

  // A snapshot written before release 2.4.0 has no field names: the standard resolution compares the fields by position
  it should "resolve the schema compatibility of a case class snapshot recording no field name as is" in {
    val formerSerializer = new CaseClassSerializer[ClickAction](
      evolution = evolutionOf(classOf[ClickAction]),
      isCaseClassImmutable = true,
      fieldNames = None,
      scalaFieldSerializers = Array(infoToSer[String], infoToSer[String])
    )

    compatibilityWith[ClickAction](formerSerializer) shouldBe Symbol("compatibleAsIs")
  }

  // The migration never converts an ADT to an unrelated ADT, even if the evolutions make their schemas identical
  it should "resolve the schema compatibility of an unrelated case class as incompatible" in {
    val formerSerializer = new CaseClassSerializer[UnrelatedFormerCaseClass](
      evolution = evolutionOf(classOf[UnrelatedFormerCaseClass]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("a", "deleted")),
      scalaFieldSerializers = Array(infoToSer[String], infoToSer[String])
    )

    compatibilityWith[UnrelatedCurrentCaseClass](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of an unrelated sealed trait as incompatible" in {
    compatibilityWith[OtherSharedSubtypeTrait](infoToSer[SharedSubtypeTrait]) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a foreign snapshot as incompatible" in {
    compatibilityWith[Action](infoToSer[Click]) shouldBe Symbol("incompatible")
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
    // The rename makes the migration necessary, and 'fieldNotInFile' changes from Int to String without @transformed
    compatibilityWith[WrongUntransformedField](clickV0As(classOf[WrongUntransformedField])) shouldBe Symbol(
      "incompatible"
    )
  }

  it should "resolve the schema compatibility of an evolved case class with an incompatible nested field as incompatible" in {
    // The outer case class has a rename, and the type of its nested field has one more field without @added
    val formerNestedSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = evolutionOf(classOf[AddedFieldWithoutAnnotation]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("a")),
      scalaFieldSerializers = Array(infoToSer[String])
    )
    val formerSerializer = new CaseClassSerializer[OuterEvolvedWithNested](
      evolution = evolutionOf(classOf[OuterEvolvedWithNested]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("formerNested")),
      scalaFieldSerializers = Array(formerNestedSerializer)
    )

    compatibilityWith[OuterEvolvedWithNested](formerSerializer) shouldBe Symbol("incompatible")
  }

  // Without @added, the migration does not use the default value of the case class: the evolution must be declared
  it should "resolve the schema compatibility of a case class with a field added without annotation as incompatible" in {
    val formerSerializer = new CaseClassSerializer[AddedFieldWithoutAnnotation](
      evolution = evolutionOf(classOf[AddedFieldWithoutAnnotation]),
      isCaseClassImmutable = true,
      fieldNames = Some(Array("a")),
      scalaFieldSerializers = Array(infoToSer[String])
    )

    compatibilityWith[AddedFieldWithoutAnnotation](formerSerializer) shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a sealed trait with a subtype deleted without annotation as incompatible" in {
    // The current schema deletes the last subtype without @deletedClasses
    val derived = infoToSer[SubtypeRemovedWithoutAnnotation]
      .asInstanceOf[CoproductSerializer[SubtypeRemovedWithoutAnnotation]]
    val currentSerializer = new CoproductSerializer[SubtypeRemovedWithoutAnnotation](
      evolution = derived.evolution,
      subtypeClasses = derived.subtypeClasses.dropRight(1),
      subtypeNames = derived.subtypeNames.dropRight(1),
      subtypeSerializers = derived.subtypeSerializers.dropRight(1)
    )

    compatibilityWith[SubtypeRemovedWithoutAnnotation](derived)(currentSerializer) shouldBe Symbol("incompatible")
  }

  // After a rollback, the evolutions of the higher versions are missing: the migration is not possible
  it should "resolve the schema compatibility of a sealed trait after a rollback as incompatible" in {
    val derived          = infoToSer[RolledBackTrait].asInstanceOf[CoproductSerializer[RolledBackTrait]]
    val formerSerializer = new CoproductSerializer[RolledBackTrait](
      // Written at a version higher than the current version
      evolution = derived.evolution.map(current => new NoEvolution(current.currentClass, current.formerVersion + 1)),
      subtypeClasses = derived.subtypeClasses,
      subtypeNames = derived.subtypeNames,
      subtypeSerializers = derived.subtypeSerializers
    )

    compatibilityWith[RolledBackTrait](formerSerializer) shouldBe Symbol("incompatible")
  }

  // Click v4 declares an evolution that Click v3 does not know, without a change of its fields
  it should "resolve the schema compatibility of a case class after a rollback as incompatible" in {
    val derived          = infoToSer[Click].asInstanceOf[CaseClassSerializer[Click]]
    val formerSerializer = new CaseClassSerializer[Click](
      evolution = evolutionOf(classOf[Click], 4),
      isCaseClassImmutable = derived.isCaseClassImmutable,
      fieldNames = derived.fieldNames,
      scalaFieldSerializers = derived.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )

    compatibilityWith[Click](formerSerializer) shouldBe Symbol("incompatible")
  }

}
