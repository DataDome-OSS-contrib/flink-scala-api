package org.apache.flinkx.api

import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot
import org.apache.flink.api.common.typeutils.base.MapSerializer
import org.apache.flink.core.memory._
import org.apache.flinkx.api.SchemaEvolutionTest.{Basket, Click, ClickEvent, Event, Order}
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
import java.nio.file.{Files, Path}
import java.io.FileInputStream

class SchemaEvolutionTest extends AnyFlatSpec with Matchers {
  private implicit val newClickTypeInfo: TypeInformation[Click] = deriveTypeInformation[Click]
  private implicit val eventTypeInfo: TypeInformation[Event]    = deriveTypeInformation[Event]
  private val clicks                                            =
    List(ClickEvent("a", "2021-01-01"), ClickEvent("b", "2021-01-01"), ClickEvent("c", "2021-01-01"))

  def createSerializer[T: TypeInformation] =
    implicitly[TypeInformation[T]].createSerializer(new SerializerConfigImpl())

  /** Serializer before schema change: without serializers for the "new" default fields. */
  private def oldClickSerializer: CaseClassSerializer[Click] = new CaseClassSerializer[Click](
    clazz = classOf[Click],
    scalaFieldSerializers = Array(stringSerializer, createSerializer[List[ClickEvent]]),
    isCaseClassImmutable = true
  )

  it should "serialize click with old serializer and deserialize it with new serializer" in {
    val newClickSerializer = createSerializer[Click] // Serializer derived from the "new" case class
    val expected           = Click(null, clicks)

    // serialize "old" Click with old serializer
    val out = new DataOutputSerializer(1024 * 1024)
    oldClickSerializer.serialize(expected, out)

    // deserialize old Click with new serializer
    val in     = new DataInputDeserializer(out.getSharedBuffer)
    val result = newClickSerializer.deserialize(in)
    result shouldBe expected

    // serialize modified Click with new serializer
    val modifiedExpected = expected.copy(fieldInFile = "modified1", fieldNotInFile = "modified2")
    newClickSerializer.serialize(modifiedExpected, out)

    // deserialize modified Click with new serializer
    val modifiedResult = newClickSerializer.deserialize(in)
    modifiedResult shouldBe modifiedExpected
  }

  def generateBlobForEvent() = {
    val buffer          = new ByteArrayOutputStream()
    val eventSerializer = createSerializer[Event]
    eventSerializer.serialize(Click("p1", clicks), new DataOutputViewStreamWrapper(buffer))
    val path = Path.of("target/test/resources/click.dat")
    Files.createDirectories(path.getParent())
    Files.write(path, buffer.toByteArray)
  }

  it should "decode click when we added view" in {
    generateBlobForEvent()
    val buffer = new FileInputStream("target/test/resources/click.dat")
    val click  = createSerializer[Event].deserialize(new DataInputViewStreamWrapper(buffer))
    click shouldBe Click("p1", clicks)
  }

  it should "resolve the schema compatibility of a case class with added default fields as compatible as is" in {
    val compatibility = createSerializer[Click]
      .snapshotConfiguration()
      .resolveSchemaCompatibility(oldClickSerializer.snapshotConfiguration())

    compatibility shouldBe Symbol("compatibleAsIs")
  }

  it should "resolve the schema compatibility of a state of case classes with added default fields as compatible as is" in {
    // The value serializer of a MapState[String, Click], nested in a serializer checking its nested serializer count
    val oldMapSerializer = new MapSerializer[String, Click](stringSerializer, oldClickSerializer)
    val newMapSerializer = new MapSerializer[String, Click](stringSerializer, createSerializer[Click])

    val compatibility = newMapSerializer
      .snapshotConfiguration()
      .resolveSchemaCompatibility(oldMapSerializer.snapshotConfiguration())

    compatibility shouldBe Symbol("compatibleAsIs")
  }

  it should "read a click of a restored snapshot having less fields" in {
    val expected = Click("p1", clicks)
    // Write the old snapshot and the old data, as Flink does in a savepoint
    val out = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, oldClickSerializer.snapshotConfiguration())
    oldClickSerializer.serialize(expected, out)

    // Restore the old snapshot, as Flink does when restoring a savepoint
    val in               = new DataInputDeserializer(out.getSharedBuffer)
    val restoredSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[Click](in, getClass.getClassLoader)

    val newClickSerializer = createSerializer[Click]
    newClickSerializer.snapshotConfiguration().resolveSchemaCompatibility(restoredSnapshot) shouldBe Symbol(
      "compatibleAsIs"
    )
    // The new serializer reads the old data, filling the added fields with their default value
    newClickSerializer.deserialize(in) shouldBe expected
  }

  it should "copy clicks of an older serialized form having less fields" in {
    val expected = Click("p1", clicks)
    val out      = new DataOutputSerializer(1024 * 1024)
    oldClickSerializer.serialize(expected, out)
    oldClickSerializer.serialize(expected, out)

    // Copy the old forms with the new serializer, as Flink does when it doesn't need to deserialize the records
    val newClickSerializer = createSerializer[Click]
    val in      = new DataInputDeserializer(out.getCopyOfBuffer) // Bounded to detect a copy of too many fields
    val copyOut = new DataOutputSerializer(1024 * 1024)
    newClickSerializer.copy(in, copyOut)
    newClickSerializer.copy(in, copyOut)

    val copyIn = new DataInputDeserializer(copyOut.getCopyOfBuffer)
    newClickSerializer.deserialize(copyIn) shouldBe expected
    newClickSerializer.deserialize(copyIn) shouldBe expected
  }

  it should "resolve the schema compatibility of a case class with an added field without default value as incompatible" in {
    val oldOrderSerializer = new CaseClassSerializer[Order](
      clazz = classOf[Order],
      scalaFieldSerializers = Array(stringSerializer),
      isCaseClassImmutable = true
    )

    val compatibility = createSerializer[Order]
      .snapshotConfiguration()
      .resolveSchemaCompatibility(oldOrderSerializer.snapshotConfiguration())

    compatibility shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a case class with an added field after a default one as incompatible" in {
    val oldBasketSerializer = new CaseClassSerializer[Basket](
      clazz = classOf[Basket],
      scalaFieldSerializers = Array(stringSerializer),
      isCaseClassImmutable = true
    )

    // The default value of the first field can't fill the added field, which has no default value
    val compatibility = createSerializer[Basket]
      .snapshotConfiguration()
      .resolveSchemaCompatibility(oldBasketSerializer.snapshotConfiguration())

    compatibility shouldBe Symbol("incompatible")
  }

  it should "resolve the schema compatibility of a case class with a removed field as incompatible" in {
    val oldClickSerializerWithMoreFields = new CaseClassSerializer[Click](
      clazz = classOf[Click],
      scalaFieldSerializers =
        Array(stringSerializer, createSerializer[List[ClickEvent]], stringSerializer, stringSerializer, longSerializer),
      isCaseClassImmutable = true
    )

    val compatibility = createSerializer[Click]
      .snapshotConfiguration()
      .resolveSchemaCompatibility(oldClickSerializerWithMoreFields.snapshotConfiguration())

    compatibility shouldBe Symbol("incompatible")
  }

}

object SchemaEvolutionTest {
  sealed trait Event
  case class Click(
      id: String,
      inFileClicks: List[ClickEvent],
      fieldInFile: String = "test1",
      fieldNotInFile: String = "test2"
  ) extends Event
  case class Purchase(price: Double) extends Event
  case class View(ts: Long)          extends Event
  case class ClickEvent(sessionId: String, date: String)
  // The added quantity field has no default value
  case class Order(id: String, quantity: Int)
  // The added quantity field has no default value, unlike the first one
  case class Basket(id: String = "unknown", quantity: Int)
}
