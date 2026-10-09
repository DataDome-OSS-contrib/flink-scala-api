package org.apache.flinkx.api.util

import org.apache.flink.util.FlinkRuntimeException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClassUtilTest extends AnyFlatSpec with Matchers {

  import org.apache.flinkx.api.util.ClassUtilTest._

  "isCaseClassImmutable" should "return true when all the fields are val" in {
    val anImmutable = classOf[Immutable]
    ClassUtil.isCaseClassImmutable(anImmutable, Array("a", "b")) shouldBe true
  }

  it should "return false when one of the fields is a var" in {
    val aMutable = classOf[Mutable]
    ClassUtil.isCaseClassImmutable(aMutable, Array("a", "b")) shouldBe false
  }

  it should "return true when all the fields are private val" in {
    val aPrivateImmutable = classOf[PrivateImmutable]
    ClassUtil.isCaseClassImmutable(aPrivateImmutable, Array("a", "b")) shouldBe true
  }

  it should "return false when one of the fields is a private var" in {
    val aPrivateMutable = classOf[PrivateMutable]
    ClassUtil.isCaseClassImmutable(aPrivateMutable, Array("a", "b")) shouldBe false
  }

  it should "return true when the field is a disrupted private val" in {
    val aDisruptedPrivateImmutable = classOf[DisruptedPrivateImmutable]
    ClassUtil.isCaseClassImmutable(aDisruptedPrivateImmutable, Array("a", "b")) shouldBe true
  }

  it should "return false when the field is a disrupted private var" in {
    val aDisruptedPrivateMutable = classOf[DisruptedPrivateMutable]
    ClassUtil.isCaseClassImmutable(aDisruptedPrivateMutable, Array("a", "b")) shouldBe false
  }

  it should "return true when the fields are in parent classes" in {
    val anExtendingImmutable = classOf[ExtendingImmutable]
    ClassUtil.isCaseClassImmutable(anExtendingImmutable, Array("a", "b", "c")) shouldBe true
  }

  it should "return false when one of the fields which is not in parent classes is a var" in {
    val anExtendingMutable = classOf[ExtendingMutable]
    ClassUtil.isCaseClassImmutable(anExtendingMutable, Array("a", "b", "c")) shouldBe false
  }

  it should "return true when the field doesn't exist" in {
    val anImmutable = classOf[Immutable]
    ClassUtil.isCaseClassImmutable(anImmutable, Array("wrongField")) shouldBe true
  }

  "resolveFormerClassName" should "reject empty string input" in {
    val exception = intercept[FlinkRuntimeException] {
      ClassUtil.resolveFormerClassName("", classOf[Simple])
    }
    exception.getMessage should include("Former binary name is mandatory")
  }

  it should "resolve a renaming relative to a top level class" in {
    val formerClassName = ClassUtil.resolveFormerClassName("ClassTest", classOf[ClassUtilTest])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassTest"
  }

  it should "resolve a renaming relative to a nested class" in {
    val formerClassName = ClassUtil.resolveFormerClassName("View", classOf[Simple])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$View"
  }

  it should "resolve a renaming relative to a nested object" in {
    val formerClassName = ClassUtil.resolveFormerClassName("InnerObject", classOf[Nested.NestedObject.type])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$InnerObject"
  }

  it should "resolve a move relative to parent" in {
    val formerClassName = ClassUtil.resolveFormerClassName("Inner$InnerObject", classOf[Nested.NestedObject.type])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$Inner$InnerObject"
  }

  it should "resolve a move relative to the top level class" in {
    val formerClassName = ClassUtil.resolveFormerClassName("Nested$InnerObject", classOf[Simple])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$InnerObject"
  }

  it should "resolve a move relative to the current package" in {
    val formerClassName = ClassUtil.resolveFormerClassName("Nested$InnerObject", classOf[ClassUtilTest])
    formerClassName shouldBe "org.apache.flinkx.api.util.Nested$InnerObject"
  }

  it should "resolve an absolute move" in {
    val formerClassName = ClassUtil.resolveFormerClassName("org.flinkx.ClassUtilTest$InnerObject", classOf[Simple])
    formerClassName shouldBe "org.flinkx.ClassUtilTest$InnerObject"
  }

  it should "resolve an absolute move from a uppercase package" in {
    val formerClassName = ClassUtil.resolveFormerClassName("COM.flinkx.ClassUtilTest$Simple", classOf[Simple])
    formerClassName shouldBe "COM.flinkx.ClassUtilTest$Simple"
  }

  it should "be force to resolve as absolute a path starting with '/'" in {
    val formerClassName = ClassUtil.resolveFormerClassName("/ClassInUnnamedPackage", classOf[nested.NestedObject.type])
    formerClassName shouldBe "ClassInUnnamedPackage"
  }

  it should "resolve as relative a renaming containing dollar sign" in {
    val formerClassName = ClassUtil.resolveFormerClassName("Ev$ntInner", classOf[Nested.NestedObject.type])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$Ev$ntInner"
  }

  it should "resolve a renaming relative to current class containing dollar sign" in {
    val formerClassName = ClassUtil.resolveFormerClassName("ObjectInner", classOf[Nested.Ne$tedObject.type])
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$ObjectInner"
  }

}

object ClassUtilTest {

  case class Immutable(a: String, b: String)

  case class Mutable(a: String, var b: String)

  case class PrivateImmutable(private val a: String, private val b: String)

  case class PrivateMutable(private val a: String, private var b: String)

  object DisruptiveObject {
    def apply(value: Int): DisruptedPrivateImmutable = DisruptedPrivateImmutable(String.valueOf(value))

    def apply(value: Long): DisruptedPrivateMutable = DisruptedPrivateMutable(String.valueOf(value))
  }

  case class DisruptedPrivateImmutable(private val a: String)

  case class DisruptedPrivateMutable(private var a: String)

  abstract class AbstractClass(val a: String) // var variant is not possible: "Mutable variable cannot be overridden"

  class IntermediateClass(override val a: String, val b: String) extends AbstractClass(a)

  case class ExtendingImmutable(override val a: String, override val b: String, c: String)
      extends IntermediateClass(a, b)

  case class ExtendingMutable(override val a: String, override val b: String, var c: String)
      extends IntermediateClass(a, b)

  case class Simple()

  object Nested {
    case object NestedObject
    case object Ne$tedObject
  }

}

object nested {
  case object NestedObject
}
