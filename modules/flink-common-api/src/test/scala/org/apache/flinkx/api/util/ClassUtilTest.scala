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
      ClassUtil.resolveFormerClassName("", Simple().getClass)
    }
    exception.getMessage should include("Class name '' in from parameter is malformed.")
  }

  it should "reject leading dot in input" in {
    val exception = intercept[FlinkRuntimeException] {
      ClassUtil.resolveFormerClassName(".Object", Simple().getClass)
    }
    exception.getMessage should include("Class name '.Object' in from parameter is malformed.")
  }

  it should "reject trailing dot in input" in {
    val exception = intercept[FlinkRuntimeException] {
      ClassUtil.resolveFormerClassName("Object.", Simple().getClass)
    }
    exception.getMessage should include("Class name 'Object.' in from parameter is malformed.")
  }

  it should "reject multiple consecutive dots in input" in {
    val exception = intercept[FlinkRuntimeException] {
      ClassUtil.resolveFormerClassName("Object..Inner", Simple().getClass)
    }
    exception.getMessage should include("Class name 'Object..Inner' in from parameter is malformed.")
  }

  it should "resolve a simple renaming" in {
    val formerClassName = ClassUtil.resolveFormerClassName("View", Simple().getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$View"
  }

  it should "resolve a simple renaming at root level" in {
    val formerClassName = ClassUtil.resolveFormerClassName("ClassTest", ClassUtilTest.getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassTest"
  }

  it should "resolve a simple renaming in nested object" in {
    val formerClassName = ClassUtil.resolveFormerClassName("InnerObject", Nested.NestedObject.getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$InnerObject"
  }

  it should "resolve a move relative to parent" in {
    val formerClassName = ClassUtil.resolveFormerClassName("Nested.InnerObject", Nested.NestedObject.getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$InnerObject"
  }

  it should "resolve a move relative to the root parent" in {
    val formerClassName = ClassUtil.resolveFormerClassName("ClassUtilTest.Nested.InnerObject", Simple().getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$InnerObject"
  }

  it should "resolve a move relative to a common package" in {
    val formerClassName = ClassUtil.resolveFormerClassName("api.async.Nested.InnerObject", Simple().getClass)
    formerClassName shouldBe "org.apache.flinkx.api.async.Nested$InnerObject"
  }

  it should "resolve a move with an absolute package" in {
    val formerClassName = ClassUtil.resolveFormerClassName("org.flinkx.ClassUtilTest.InnerObject", Simple().getClass)
    formerClassName shouldBe "org.flinkx.ClassUtilTest$InnerObject"
  }

  it should "resolve a move to a completely different absolute package" in {
    val formerClassName = ClassUtil.resolveFormerClassName("com.flinkx.ClassUtilTest.Simple", Simple().getClass)
    formerClassName shouldBe "com.flinkx.ClassUtilTest$Simple"
  }

  it should "resolve multiple consecutive uppercase components" in {
    val formerClassName = ClassUtil.resolveFormerClassName("UI.DOM.Element", Simple().getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$UI$DOM$Element"
  }

  it should "resolve input containing dollar sign" in {
    val formerClassName = ClassUtil.resolveFormerClassName("Ev$ntInner", Nested.NestedObject.getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$Ev$ntInner"
  }

  it should "resolve with current class containing dollar sign" in {
    val formerClassName = ClassUtil.resolveFormerClassName("ObjectInner", Nested.Ne$tedObject.getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$Nested$ObjectInner"
  }

  it should "resolve single character class names" in {
    val formerClassName = ClassUtil.resolveFormerClassName("A.B.C", Simple().getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.ClassUtilTest$A$B$C"
  }

  it should "resolve class name without titlecase" in {
    val formerClassName = ClassUtil.resolveFormerClassName("InnerObject", nested.NestedObject.getClass)
    formerClassName shouldBe "org.apache.flinkx.api.util.nested$InnerObject"
  }

  it should "resolve class name without titlecase in the 'from' thanks to dollar sign" in {
    val from            = "util.inner$InnerObject" // cannot detect "inner" is a lowercase class name: $ has to be used
    val formerClassName = ClassUtil.resolveFormerClassName(from, nested.NestedObject.getClass)
    formerClassName shouldBe s"org.apache.flinkx.api.$from"
  }

}

object ClassUtilTest {

  case class Immutable(a: String, b: String)
  case class Mutable(a: String, var b: String)
  case class PrivateImmutable(private val a: String, private val b: String)
  case class PrivateMutable(private val a: String, private var b: String)
  object DisruptiveObject {
    def apply(value: Int): DisruptedPrivateImmutable = DisruptedPrivateImmutable(String.valueOf(value))
    def apply(value: Long): DisruptedPrivateMutable  = DisruptedPrivateMutable(String.valueOf(value))
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
    case class HowScalaResolveReference(
        absolute: org.apache.flinkx.api.OutputTag.type,   // absolute reference
        ne$tedObject: Ne$tedObject.type,                  // relative to current object
        immutable: Immutable,                             // relative to parent object <- impossible to reproduce
        mutable: ClassUtilTest.Mutable,                   // relative to package
        nestedCaseClass: nested.HowScalaResolveReference, // relative to package
        nestedObject: nested.NestedObject.type            // relative to package
    )
  }

}

object nested {
  case object NestedObject
  case class HowScalaResolveReference(
      absolute: org.apache.flinkx.api.util.ClassUtilTest.AbstractClass, // absolute reference
      immutable: ClassUtilTest.Immutable,                               // relative to package
      nestedCaseClass: ClassUtilTest.Nested.HowScalaResolveReference,   // relative to package
      nestedObject: NestedObject.type                                   // relative to current object
  )
}
