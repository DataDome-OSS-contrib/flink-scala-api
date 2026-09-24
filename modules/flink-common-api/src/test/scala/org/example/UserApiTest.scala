package org.example

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.Evolutions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The evolution API as a user sees it: every generated call must reach what the library exposes, from outside it. */
class UserApiTest extends AnyFlatSpec with Matchers {

  it should "derive an ADT carrying every evolution annotation, declared by its companion" in {
    implicitly[TypeInformation[UserFixtures.Order]] shouldNot be(null)

    val formerName = classOf[UserFixtures.Order].getName.replace("Order", "FormerOrder")
    Evolutions.find[UserFixtures.Order](formerName, 0, getClass.getClassLoader).map(_.currentClass) shouldBe
      Some(classOf[UserFixtures.Order])
  }

}
