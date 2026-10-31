package org.example

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.Evolutions
import org.example.UserFixtures.Invoice
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The evolution API from a package of the user: the code that the macros generate must compile outside the library. */
class UserApiTest extends AnyFlatSpec with Matchers {

  it should "derive an ADT carrying every evolution annotation, declared by its companion" in {
    implicitly[TypeInformation[UserFixtures.Order]] shouldNot be(null)

    val formerName = classOf[UserFixtures.Order].getName.replace("Order", "FormerOrder")
    Evolutions.get[UserFixtures.Order](formerName, 0, getClass.getClassLoader).currentClass shouldBe
      classOf[UserFixtures.Order]
  }

  it should "apply the mappers of an ADT, private to its package" in {
    val evolution = Evolutions.get[Invoice](classOf[Invoice].getName, 0, getClass.getClassLoader)

    evolution.plan(Array("amount")).map(_.map(_.migrate(Array[AnyRef](Int.box(42)))).toSeq) shouldBe
      Right(Seq("42 cents"))
    evolution.migrate(Invoice("42 cents")) shouldBe Invoice("42 cents since v0")
  }

}
