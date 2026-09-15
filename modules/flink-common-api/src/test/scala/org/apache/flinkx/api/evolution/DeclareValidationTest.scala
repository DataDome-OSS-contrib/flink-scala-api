package org.apache.flinkx.api.evolution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A misplaced evolution annotation declares nothing: it is rejected where it is read, at compile time. */
class DeclareValidationTest extends AnyFlatSpec with Matchers {

  it should "reject an evolution annotation on an ADT without version" in {
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongRenamedOnCaseClassWithoutVersion]"
    )
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongDeletedFieldsOnCaseClassWithoutVersion]"
    )
  }

  it should "reject a field annotation on an ADT" in {
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongAddedOnCaseClass]"
    )
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongTransformedOnCaseClass]"
    )
  }

  it should "reject a version on a field" in {
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.evolution.EvolutionErrorFixtures.WrongVersionOnField]"
    )
  }

  it should "reject an ADT annotation on a field" in {
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongDeletedFieldsOnField]"
    )
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongPostDeserializeOnField]"
    )
  }

  it should "reject a field annotation without version" in {
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongAddedOnFieldWithoutVersion]"
    )
  }

  it should "reject two postDeserialize on the same ADT" in {
    assertDoesNotCompile(
      "org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.EvolutionTest.WrongPostDeserializeTwiceOnCaseClass]"
    )
  }

  it should "accept the evolutions of a well declared ADT" in {
    assertCompiles("org.apache.flinkx.api.evolution.Declare.declare[org.apache.flinkx.api.evolution.DeclareTest.Probe]")
  }

  it should "build the message of a misplaced annotation" in {
    evolutionNotAllowed("added", "class Foo with version 0") shouldBe
      "@added annotation is not allowed on class Foo with version 0"
  }

}
