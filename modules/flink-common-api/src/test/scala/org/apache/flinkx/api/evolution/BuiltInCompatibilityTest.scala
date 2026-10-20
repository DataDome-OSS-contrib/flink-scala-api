package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.common.typeutils.base.{IntSerializer, StringSerializer}
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.BuiltInCompatibilityTest._
import org.apache.flinkx.api.serializer.CoproductSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The schema changes that need no evolution annotation. */
class BuiltInCompatibilityTest extends AnyFlatSpec with Matchers with TestUtils {

  it should "restore the reordered fields of an unversioned case class" in {
    val (snapshot, restored) =
      deserializeFormerFields(
        classOf[Pair],
        0,
        ("last", StringSerializer.INSTANCE, "Doe"),
        ("first", StringSerializer.INSTANCE, "John")
      )

    restored shouldBe Pair("John", "Doe")
    createSerializer[Pair].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  it should "restore the reordered fields of different types of an unversioned case class" in {
    val (snapshot, restored) =
      deserializeFormerFields(
        classOf[Counter],
        0,
        ("count", IntSerializer.INSTANCE, 42),
        ("id", StringSerializer.INSTANCE, "a")
      )

    restored shouldBe Counter("a", 42)
    createSerializer[Counter].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAfterMigration"
    )
  }

  it should "restore an unchanged unversioned case class as is" in {
    val (snapshot, restored) =
      deserializeFormerFields(
        classOf[Pair],
        0,
        ("first", StringSerializer.INSTANCE, "John"),
        ("last", StringSerializer.INSTANCE, "Doe")
      )

    restored shouldBe Pair("John", "Doe")
    createSerializer[Pair].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAsIs"
    )
  }

  // The migration finds the current subtype of each former subtype by its name, not by its position
  it should "restore the reordered subtypes of an unversioned sealed trait, a subtype being added" in {
    implicit val current: TypeSerializer[Shape] = createSerializer[Shape]
    // At version 0, Shape had only Square and Circle, in this order
    val formerSerializer = new CoproductSerializer[Shape](
      evolution = Some(evolutionOf(classOf[Shape])),
      subtypeClasses = Array(classOf[Square], classOf[Circle]),
      subtypeNames = Array(classOf[Square].getName, classOf[Circle].getName),
      subtypeSerializers = Array(createSerializer[Square], createSerializer[Circle])
    )

    deserializeFormerForm[Shape](formerSerializer, Circle(1)) shouldBe Circle(1)
    compatibilityWith[Shape](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

}

object BuiltInCompatibilityTest {

  case class Pair(first: String, last: String)

  case class Counter(id: String, count: Int)

  sealed trait Shape
  case class Circle(radius: Int) extends Shape
  case class Square(side: Int)   extends Shape
  case object Dot                extends Shape

}
