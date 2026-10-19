package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.base.{IntSerializer, StringSerializer}
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.BuiltInCompatibilityTest._
import org.apache.flinkx.api.serializer.{CaseClassSerializer, CoproductSerializer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The changes an ADT declaring no evolution survives without any annotation. */
class BuiltInCompatibilityTest extends AnyFlatSpec with Matchers with TestUtils {

  it should "restore the reordered fields of an unversioned case class" in {
    val (snapshot, restored) =
      restoreFormerOrder(
        classOf[Pair],
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
      restoreFormerOrder(
        classOf[Counter],
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
      restoreFormerOrder(
        classOf[Pair],
        ("first", StringSerializer.INSTANCE, "John"),
        ("last", StringSerializer.INSTANCE, "Doe")
      )

    restored shouldBe Pair("John", "Doe")
    createSerializer[Pair].snapshotConfiguration().resolveSchemaCompatibility(snapshot) shouldBe Symbol(
      "compatibleAsIs"
    )
  }

  // The former subtypes are matched to the current ones by name, whatever their order
  it should "restore the reordered subtypes of an unversioned sealed trait, a subtype being added" in {
    implicit val current: TypeSerializer[Shape] = createSerializer[Shape]
    // Shape v0 only had Square and Circle, in this order
    val formerSerializer = new CoproductSerializer[Shape](
      evolution = Some(Evolutions.get(classOf[Shape], 0)),
      subtypeClasses = Array(classOf[Square], classOf[Circle]),
      subtypeFqns = Array(classOf[Square].getName, classOf[Circle].getName),
      subtypeSerializers = Array(createSerializer[Square], createSerializer[Circle])
    )

    deserializeFormerForm[Shape](formerSerializer, Circle(1)) shouldBe Circle(1)
    resolveSchemaCompatibilityAfterRestore[Shape](formerSerializer) shouldBe Symbol("compatibleAfterMigration")
  }

  /** Restores the snapshot of `clazz` written when its fields were declared in the given former order, as Flink
    * restores a savepoint, and reads back the given former field values with the restored serializer.
    */
  private def restoreFormerOrder[T <: Product](
      clazz: Class[T],
      formerFields: (String, TypeSerializer[_], Any)*
  ): (TypeSerializerSnapshot[T], T) = {
    val formerSerializer = new CaseClassSerializer[T](
      evolution = Evolutions.get(clazz, 0),
      isCaseClassImmutable = true,
      fieldNames = formerFields.map(_._1).toArray,
      paramSerializers = formerFields.map(_._2).toArray
    )
    val out = new DataOutputSerializer(256)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, formerSerializer.snapshotConfiguration())
    // The former form: its arity, then its fields in their former order
    out.writeInt(formerFields.length)
    formerFields.foreach { case (_, serializer, value) =>
      serializer.asInstanceOf[TypeSerializer[Any]].serialize(value, out)
    }

    val in       = new DataInputDeserializer(out.getCopyOfBuffer)
    val snapshot = TypeSerializerSnapshot.readVersionedSnapshot[T](in, clazz.getClassLoader)
    (snapshot, snapshot.restoreSerializer().deserialize(in))
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
