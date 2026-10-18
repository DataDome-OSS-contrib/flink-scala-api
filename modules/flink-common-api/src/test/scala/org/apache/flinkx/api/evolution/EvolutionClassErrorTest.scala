package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeinfo.TypeInformation
import EvolutionTest._
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** What the evolutions of an ADT report when they describe the former schema wrongly. */
class EvolutionClassErrorTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "allow when @deletedClasses is on a sealed trait subtype being itself a sealed trait" in {
    implicitly[TypeInformation[CorrectDeletedClassesOnSealedTraitSubtype]]
  }

  it should "allow when @postEvolution is on a versioned sealed trait subtype" in {
    implicitly[TypeInformation[CorrectPostEvolutionOnSealedTraitSubtype]]
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

  // The former and the current classes are compared once the former name resolves to the current class, so a renamed
  // case class must compare equal to itself.
  it should "resolve the schema compatibility of a renamed case class as compatible after migration" in {
    // No class bears the former name anymore: the snapshot records it, as the former code wrote it
    val formerName       = classOf[RenamedCaseClass].getName.replace("RenamedCaseClass", "FormerRenamedCaseClass")
    val formerSerializer = new CaseClassSerializer[RenamedCaseClass](
      evolution = new Evolution(formerName, 0, classOf[RenamedCaseClass]),
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

  // The same ADT is derived once per set of member type information, so a given annotation is legitimately read
  // several times and declaring the same resolution again must stay a no-op.
  it should "not throw when the same ADT declares its former class name twice" in {
    implicitly[TypeInformation[FirstClaimingFormerName]]
    org.apache.flinkx.api.auto.cache.clear() // Forces a second derivation of the very same ADT
    implicitly[TypeInformation[FirstClaimingFormerName]] shouldNot be(null)
  }

  // A versioned ADT whose companion declares nothing would restore without any of its rules: the derivation rejects it
  it should "not compile the derivation of a versioned ADT whose companion doesn't extend Evolving" in {
    assertDoesNotCompile("implicitly[TypeInformation[NeverDeclared]]")
  }

  // The derivation doesn't read the evolutions, which the restore reads once the companion is initialized
  it should "derive the type information in the companion before the strict val declaring its evolutions" in {
    DerivedBeforeDeclared.typeInfo.getTypeClass shouldBe classOf[DerivedBeforeDeclared]
    val formerName =
      classOf[DerivedBeforeDeclared].getName.replace("DerivedBeforeDeclared", "FormerDerivedBeforeDeclared")
    Evolutions.get(formerName, 0, getClass.getClassLoader).currentClass shouldBe classOf[DerivedBeforeDeclared]
  }

  it should "not compile a declaration renaming an ADT since a version above its current one" in {
    assertDoesNotCompile("Evolutions[WrongRenamedSinceAboveVersion]")
  }

  it should "not compile a declaration deleting classes since a version below 1" in {
    assertDoesNotCompile("Evolutions[WrongDeletedClassesSinceBelowOne]")
  }

  // Its own snapshot always records version 0, so its former name could never be told apart by the version
  it should "not compile a declaration renaming an unversioned subtype" in {
    assertDoesNotCompile("Evolutions[RenamingUnversionedSubtype]")
  }

  // A subtype may be renamed in the versions of its sealed trait, as Horse is, but not above them
  it should "not compile a declaration renaming a subtype since a version above the ones of its sealed traits" in {
    assertDoesNotCompile("Evolutions[WrongRenamedSubtypeSinceAboveVersion]")
  }

}
