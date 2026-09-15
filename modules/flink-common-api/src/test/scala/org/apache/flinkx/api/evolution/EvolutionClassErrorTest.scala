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
  postDeserialize,
  renamed,
  transformed,
  version
}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** What the evolutions of an ADT report when they describe the former schema wrongly. */
class EvolutionClassErrorTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "allow when @deletedClasses is on a sealed trait subtype being itself a sealed trait" in {
    Declare.declare[CorrectDeletedClassesOnSealedTraitSubtype]
    Declare.declare[CorrectDeletedClassesOnSubtype] // A versioned subtype declares its own evolutions
    implicitly[TypeInformation[CorrectDeletedClassesOnSealedTraitSubtype]]
  }

  it should "allow when @postDeserialize is on a versioned sealed trait subtype" in {
    Declare.declare[CorrectPostDeserializeOnSealedTraitSubtype]
    Declare.declare[CorrectPostDeserializeOnSubtype]
    implicitly[TypeInformation[CorrectPostDeserializeOnSealedTraitSubtype]]
  }

  // A checkpoint written by a more recent source code, typically after a rollback: the annotations describing the
  // versions in between don't exist here, so nothing can drive the migration.
  it should "resolve the schema compatibility of a sealed trait restored by an outdated source code as incompatible" in {
    Declare.declare[RolledBackTrait]
    Declare.declare[RolledBackA]
    Declare.declare[RolledBackB]
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
    // The former class is still declared to play its part in the snapshot, but its name belongs to the renamed one
    Declare.declare[RenamedCaseClass]
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
    Declare.declare[SubtypeRemovedWithoutAnnotation]
    Declare.declare[RemainingSubtype]
    Declare.declare[RemovedSubtype]
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
    Declare.declare[FirstClaimingFormerName]
    implicitly[TypeInformation[FirstClaimingFormerName]]
    org.apache.flinkx.api.auto.cache.clear() // Forces a second derivation of the very same ADT
    implicitly[TypeInformation[FirstClaimingFormerName]] shouldNot be(null)
  }

  // A provider absent, mislisted or outdated would otherwise build a serializer without any of the declared rules
  it should "throw when deriving a versioned ADT no provider declares" in {
    val exception = intercept[EvolutionNotDeclaredException](implicitly[TypeInformation[NeverDeclared]])

    exception.getMessage should startWith(
      s"Cannot derive the type information of '${classOf[NeverDeclared].getName}': it declares @version(1), but no" +
        s" evolution is declared for that class here."
    )
  }

}
