package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.{TestUtils, version}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A sealed trait whose subtypes are themselves sealed: the declaration must describe the same members as the
  * derivation, or an unchanged schema is reported as needing a migration.
  */
class NestedHierarchyTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.NestedHierarchyTest._

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "describe the same members as the derivation" in {
    Declare.declare[Top]

    val serializer = createSerializer[Top]
    val subtypes   = serializer.asInstanceOf[org.apache.flinkx.api.serializer.CoproductSerializer[Top]].subtypeFqns

    withClue(s"the declared members must match the derived subtypes ${subtypes.mkString(", ")}:")(
      Evolutions.get(classOf[Top], 1).isAvoidable(subtypes) shouldBe true
    )
  }

  it should "resolve an unchanged nested hierarchy as compatible as is" in {
    Declare.declare[Top]
    implicit val serializer: org.apache.flink.api.common.typeutils.TypeSerializer[Top] = createSerializer[Top]

    resolveSchemaCompatibility(serializer) shouldBe Symbol("compatibleAsIs")
  }

}

object NestedHierarchyTest {

  @version(1)
  sealed trait Top
  sealed trait Middle        extends Top
  case class Leaf(a: String) extends Middle
  case class Direct(b: Int)  extends Top
}
