package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.util.ClassUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A case object outside a sealed trait declares its own evolutions: it is its own evolving companion. In Scala 2.13,
  * this case object makes the compiler fail with a cyclic reference, so this test is Scala 3 only.
  */
class SelfEvolvingCaseObjectTest extends AnyFlatSpec with Matchers {

  import SelfEvolvingCaseObjectTest.*

  it should "resolve the former name of a case object that is its own evolving companion" in {
    val formerName = ClassUtil.resolveFormerClassName("FormerSelfEvolving$", SelfEvolving.getClass)

    Evolutions.get[Any](formerName, 0, getClass.getClassLoader).currentClass shouldBe SelfEvolving.getClass
  }

}

object SelfEvolvingCaseObjectTest {

  @version(1)
  @renamed(since = 1, "FormerSelfEvolving")
  case object SelfEvolving extends Evolving[SelfEvolving.type] { val evolutions = Evolutions[SelfEvolving.type] }

}
