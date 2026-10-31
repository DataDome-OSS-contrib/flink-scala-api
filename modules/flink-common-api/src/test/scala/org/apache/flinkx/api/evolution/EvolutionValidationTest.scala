package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pony
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The compile-time checks of the evolution annotations: `Evolutions[T]` and the derivation of the type information
  * reject a misplaced or wrong annotation.
  */
class EvolutionValidationTest extends AnyFlatSpec with Matchers {

  // An ADT at version 0 has no lower version to migrate from
  it should "reject an evolution annotation on an ADT without version" in {
    assertTypeError("Evolutions[WrongRenamedOnCaseClassWithoutVersion]")
    assertTypeError("Evolutions[WrongDeletedFieldsOnCaseClassWithoutVersion]")
    assertTypeError("Evolutions[WrongDeletedClassesOnCaseClassWithoutVersion]")
    assertTypeError("Evolutions[WrongRenamedOnSealedTraitWithoutVersion]")
    assertTypeError("Evolutions[WrongDeletedClassesOnSealedTraitWithoutVersion]")
  }

  it should "reject a field annotation on an ADT" in {
    assertTypeError("Evolutions[WrongAddedOnCaseClass]")
    assertTypeError("Evolutions[WrongTransformedOnCaseClass]")
    assertTypeError("Evolutions[WrongAddedOnSealedTrait]")
    assertTypeError("Evolutions[WrongTransformedOnSealedTrait]")
  }

  it should "reject a field annotation on a field of an ADT without version" in {
    assertTypeError("Evolutions[WrongAddedOnFieldWithoutVersion]")
    assertTypeError("Evolutions[WrongRenamedOnFieldWithoutVersion]")
    assertTypeError("Evolutions[WrongTransformedOnFieldWithoutVersion]")
  }

  it should "reject an ADT annotation on a field" in {
    assertTypeError("Evolutions[WrongDeletedFieldsOnField]")
    assertTypeError("Evolutions[WrongDeletedClassesOnField]")
    assertTypeError("Evolutions[WrongPostEvolutionOnField]")
  }

  it should "reject a version on a field" in {
    assertTypeError("Evolutions[WrongVersionOnField]")
  }

  it should "reject two postEvolution on the same ADT" in {
    assertTypeError("Evolutions[WrongPostEvolutionTwiceOnCaseClass]")
    assertTypeError("Evolutions[WrongPostEvolutionTwiceOnSealedTrait]")
  }

  // The snapshot of an unversioned subtype records version 0, so the version cannot identify its former name.
  // A subtype declares its evolutions in its own companion.
  it should "reject an evolution annotation on an unversioned subtype" in {
    assertTypeError("Evolutions[RenamingUnversionedSubtype]")
    assertTypeError("Evolutions[WrongRenamedOnSealedTraitSubtypeWithoutVersion]")
    assertTypeError("Evolutions[WrongAddedOnSealedTraitSubtypeWithoutVersion]")
    assertTypeError("Evolutions[WrongTransformedOnSealedTraitSubtypeWithoutVersion]")
    assertTypeError("Evolutions[WrongDeletedClassesOnSealedTraitSubtype]")
    assertTypeError("Evolutions[WrongPostEvolutionOnSealedTraitSubtype]")
  }

  // The sealed trait declares its case objects, and checks their annotations as for an ADT
  it should "reject a field annotation on a versioned case object" in {
    assertTypeError("Evolutions[WrongAddedOnSealedTraitSubtype]")
    assertTypeError("Evolutions[WrongTransformedOnSealedTraitSubtype]")
  }

  it should "reject a negative version" in {
    assertTypeError("Evolutions[WrongCurrentVersion]")
  }

  // The macros read the version and the since at compile time: only a literal has a value there
  it should "reject a version or a since that is not an integer literal" in {
    assertTypeError("Evolutions[WrongVersionNotLiteral]")
    assertTypeError("Evolutions[WrongSinceNotLiteral]")
  }

  // The migration never applies an evolution outside the version range: a subtype counts in its own versions
  it should "reject an evolution whose since is outside the version range" in {
    assertTypeError("Evolutions[WrongSinceAboveVersion]")
    assertTypeError("Evolutions[WrongSinceBelowOne]")
    assertTypeError("Evolutions[WrongRenamedSinceAboveVersion]")
    assertTypeError("Evolutions[WrongDeletedClassesSinceBelowOne]")
    assertTypeError("Evolutions[WrongRenamedSubtypeSinceAboveVersion]")
  }

  it should "reject an added field without default value" in {
    assertTypeError("Evolutions[WrongAddedOnFieldWithoutDefaultValue]")
  }

  it should "accept the evolutions of well declared ADTs" in {
    assertCompiles("Evolutions[Pony]")
    assertCompiles("Evolutions[CorrectDeletedClassesOnSealedTraitSubtype]")
    assertCompiles("Evolutions[CorrectDeletedClassesOnSubtype]")
    assertCompiles("Evolutions[CorrectPostEvolutionOnSubtype]")
  }

  // Without an evolving companion, a versioned ADT restores without its evolutions: the derivation rejects it
  it should "reject the derivation of a versioned ADT whose companion doesn't extend Evolving" in {
    assertTypeError("{ import org.apache.flinkx.api.auto._; deriveTypeInformation[NeverDeclared] }")
  }

  // Without an evolving companion, only the derivation can reject the annotations of an ADT without version
  it should "reject the derivation of an ADT without version that declares an evolution" in {
    assertTypeError(
      "{ import org.apache.flinkx.api.auto._; deriveTypeInformation[WrongRenamedOnCaseClassWithoutVersion] }"
    )
    assertTypeError("{ import org.apache.flinkx.api.auto._; deriveTypeInformation[WrongAddedOnFieldWithoutVersion] }")
  }

  it should "reject the derivation of an ADT with a version on a field" in {
    assertTypeError("{ import org.apache.flinkx.api.auto._; deriveTypeInformation[WrongVersionOnField] }")
  }

  it should "accept the derivation of versioned ADTs whose companion extends Evolving" in {
    assertCompiles("{ import org.apache.flinkx.api.auto._; deriveTypeInformation[Pony] }")
    assertCompiles(
      "{ import org.apache.flinkx.api.auto._; deriveTypeInformation[CorrectDeletedClassesOnSealedTraitSubtype] }"
    )
    assertCompiles(
      "{ import org.apache.flinkx.api.auto._; deriveTypeInformation[CorrectPostEvolutionOnSealedTraitSubtype] }"
    )
  }

  it should "build the message of a misplaced annotation" in {
    evolutionNotAllowed("added", "class Foo with version 0") shouldBe
      "@added annotation is not allowed on class Foo with version 0"
  }

  it should "build the message of a companion declaring nothing" in {
    companionNotEvolving("com.example.Outer$Order") shouldBe
      "com.example.Outer$Order declares @version. Its companion must extend Evolving:" +
      " object Order extends Evolving[Order] { val evolutions = Evolutions[Order] }"
  }

}
