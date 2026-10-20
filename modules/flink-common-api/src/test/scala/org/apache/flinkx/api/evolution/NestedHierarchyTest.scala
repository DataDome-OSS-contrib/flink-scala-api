package org.apache.flinkx.api.evolution

import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.util.ClassUtil
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A sealed trait with sealed subtypes. An unchanged hierarchy restores as is, and the top sealed trait declares the
  * case objects below an intermediate sealed trait.
  */
class NestedHierarchyTest extends AnyFlatSpec with Matchers with TestUtils {

  import org.apache.flinkx.api.evolution.NestedHierarchyTest._

  it should "resolve an unchanged nested hierarchy as compatible as is" in {
    compatibilityWith[Top](createSerializer[Top]) shouldBe Symbol("compatibleAsIs")
  }

  // The derivation flattens the intermediate sealed traits, so the top sealed trait declares their case objects
  it should "resolve the former name of a case object below an intermediate sealed trait" in {
    val formerName = ClassUtil.resolveFormerClassName("FormerNested$", Nested.getClass) // As its snapshot records it

    Evolutions.get[Top](formerName, 0, getClass.getClassLoader).currentClass shouldBe Nested.getClass
  }

}

object NestedHierarchyTest {

  @version(1)
  sealed trait Top
  object Top                 extends Evolving[Top] { val evolutions = Evolutions[Top] }
  sealed trait Middle        extends Top
  case class Leaf(a: String) extends Middle
  @version(1)
  @renamed(since = 1, "FormerNested")
  case object Nested        extends Middle
  case class Direct(b: Int) extends Top
}
