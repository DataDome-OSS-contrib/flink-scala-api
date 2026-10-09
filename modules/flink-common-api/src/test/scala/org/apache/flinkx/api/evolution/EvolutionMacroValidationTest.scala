package org.apache.flinkx.api.evolution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A misplaced evolution annotation declares nothing: it is rejected where it is read, at compile time. */
class EvolutionMacroValidationTest extends AnyFlatSpec with Matchers {

  it should "reject an evolution annotation on an ADT without version" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongRenamedOnCaseClassWithoutVersion]")
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongDeletedFieldsOnCaseClassWithoutVersion]")
  }

  it should "reject a field annotation on an ADT" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongAddedOnCaseClass]")
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongTransformedOnCaseClass]")
  }

  it should "reject a version on a field" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongVersionOnField]")
  }

  it should "reject an ADT annotation on a field" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongDeletedFieldsOnField]")
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongPostEvolutionOnField]")
  }

  it should "reject a field annotation without version" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongAddedOnFieldWithoutVersion]")
  }

  it should "reject a negative version" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongCurrentVersion]")
  }

  // The version and the since are read at compile time, where only a literal has a value
  it should "reject a version or a since that is not an integer literal" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongVersionNotLiteral]")
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongSinceNotLiteral]")
  }

  // An evolution outside the version range is never applied when it should
  it should "reject a field evolution whose since is outside the version range" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongSinceAboveVersion]")
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongSinceBelowOne]")
  }

  it should "reject an added field without default value" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongAddedOnFieldWithoutDefaultValue]")
  }

  it should "reject two postEvolution on the same ADT" in {
    assertDoesNotCompile("Evolutions[EvolutionErrorFixtures.WrongPostEvolutionTwiceOnCaseClass]")
  }

  // The mappers of an ADT must be visible where its declaration is read, so the declaration of Probe, whose mappers are
  // hidden in its companion, is read from that companion only
  it should "accept the evolutions of a well declared ADT" in {
    assertCompiles("Evolutions[EvolutionRenamedTest.Pony]")
  }

  // Wherever the type information is summoned, whatever the shape of the state descriptor asking for it
  it should "reject the derivation of a versioned ADT whose companion doesn't extend Evolving" in {
    assertDoesNotCompile(
      "{ import org.apache.flinkx.api.auto._; deriveTypeInformation[EvolutionErrorFixtures.NeverDeclared] }"
    )
  }

  it should "reject the derivation of an ADT with a version on a field" in {
    assertDoesNotCompile(
      "{ import org.apache.flinkx.api.auto._; deriveTypeInformation[EvolutionErrorFixtures.WrongVersionOnField] }"
    )
  }

  it should "accept the derivation of a versioned ADT whose companion extends Evolving" in {
    assertCompiles("{ import org.apache.flinkx.api.auto._; deriveTypeInformation[EvolutionRenamedTest.Pony] }")
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
