package org.example

import org.apache.flinkx.api.evolution.{Declare, EvolutionsCheck}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The evolution API as a user sees it: every generated call must reach what the library exposes, from outside it. */
class UserApiTest extends AnyFlatSpec with Matchers {

  it should "declare an ADT carrying every evolution annotation" in {
    Declare.declare[UserFixtures.Order]
  }

  it should "declare a whole package" in {
    Declare.declarePackage("org.example")
  }

  it should "check a package" in {
    Declare.declarePackage("org.example")

    EvolutionsCheck.undeclaredIn(new java.io.File(sys.props("flinkx.test.sources")), "org.example") shouldBe empty
  }

}
