package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.evolution.EvolutionErrorFixtures._
import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pony
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A misplaced or wrongly written evolution annotation declares nothing: it is rejected where it is read, at compile
  * time, by `Evolutions[T]` in the companion of the ADT, or by the derivation of its type information.
  */
class EvolutionValidationTest extends AnyFlatSpec with Matchers {

  // An ADT at version 0 has no former version to evolve from
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

  // Its own snapshot always records version 0, so a former name could never be told apart by the version, and the
  // evolutions of a subtype are declared by its own companion
  it should "reject an evolution annotation on an unversioned subtype" in {
    assertTypeError("Evolutions[RenamingUnversionedSubtype]")
    assertTypeError("Evolutions[WrongRenamedOnSealedTraitSubtypeWithoutVersion]")
    assertTypeError("Evolutions[WrongAddedOnSealedTraitSubtypeWithoutVersion]")
    assertTypeError("Evolutions[WrongTransformedOnSealedTraitSubtypeWithoutVersion]")
    assertTypeError("Evolutions[WrongDeletedClassesOnSealedTraitSubtype]")
    assertTypeError("Evolutions[WrongPostEvolutionOnSealedTraitSubtype]")
  }

  // A case object is declared by the sealed trait it belongs to, which checks its annotations as an ADT of its own
  it should "reject a field annotation on a versioned case object" in {
    assertTypeError("Evolutions[WrongAddedOnSealedTraitSubtype]")
    assertTypeError("Evolutions[WrongTransformedOnSealedTraitSubtype]")
  }

  it should "reject a negative version" in {
    assertTypeError("Evolutions[WrongCurrentVersion]")
  }

  // The version and the since are read at compile time, where only a literal has a value
  it should "reject a version or a since that is not an integer literal" in {
    assertTypeError("Evolutions[WrongVersionNotLiteral]")
    assertTypeError("Evolutions[WrongSinceNotLiteral]")
  }

  // An evolution outside the version range is never applied when it should. A subtype may be renamed in the versions
  // of its sealed traits, as Horse is, but not above them
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

  // Wherever the type information is summoned, whatever the shape of the state descriptor asking for it: a versioned ADT
  // whose companion declares nothing would restore without any of its rules
  it should "reject the derivation of a versioned ADT whose companion doesn't extend Evolving" in {
    assertTypeError("{ import org.apache.flinkx.api.auto._; deriveTypeInformation[NeverDeclared] }")
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
      "com.example.Outer$Order declares @version, so its companion must declare its evolutions:" +
      " object Order extends Evolving[Order] { val evolutions = Evolutions[Order] }"
  }

}
