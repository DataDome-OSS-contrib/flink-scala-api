package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.StringSerializer
import org.apache.flink.api.java.typeutils.runtime.RowSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.CaseClassSerializerTest.{Immutable, Mutable, OuterImmutable, OuterMutable}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CaseClassSerializerTest extends AnyFlatSpec with Matchers {

  "isImmutableType" should "be true when parameters are immutable" in {
    val serializer = new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(StringSerializer.INSTANCE))
    serializer.isImmutableType should be(true)
  }

  it should "be false when one parameter is mutable" in {
    val serializer = new CaseClassSerializer[Mutable](classOf[Mutable], false, 0, Array.empty, Array(StringSerializer.INSTANCE))
    serializer.isImmutableType should be(false)
  }

  it should "be false when the content of one parameter is mutable" in {
    val mutableSerializer = new CaseClassSerializer[Mutable](classOf[Mutable], false, 0, Array.empty, Array(StringSerializer.INSTANCE))
    val serializer = new CaseClassSerializer[OuterImmutable](classOf[OuterImmutable], true, 0, Array.empty, Array(mutableSerializer))
    serializer.isImmutableType should be(false)
  }

  "isImmutableSerializer" should "be true when sub-serializers are immutable" in {
    val serializer = new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(StringSerializer.INSTANCE))
    serializer.isImmutableSerializer should be(true)
  }

  it should "be false when one sub-serializer is mutable" in {
    val serializer = new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(new RowSerializer(Array.empty)))
    serializer.isImmutableSerializer should be(false)
  }

  "copy" should "return the same case class when immutable" in {
    val immutableSerializer =
      new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(StringSerializer.INSTANCE))
    val expectedData = Immutable("a")

    val resultData = immutableSerializer.copy(expectedData)

    resultData should be theSameInstanceAs expectedData
  }

  it should "return null when the given case class is null" in {
    val immutableSerializer =
      new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(StringSerializer.INSTANCE))

    val resultData = immutableSerializer.copy(null)

    resultData shouldEqual null
  }

  it should "copy the case class when mutable" in {
    val mutableSerializer = new CaseClassSerializer[Mutable](classOf[Mutable], false, 0, Array.empty, Array(StringSerializer.INSTANCE))
    val expectedData      = Mutable("a")

    val resultData = mutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData shouldEqual expectedData
  }

  it should "copy the case class and its content when the content of one parameter is mutable" in {
    val mutableSerializer = new CaseClassSerializer[Mutable](classOf[Mutable], false, 0, Array.empty, Array(StringSerializer.INSTANCE))
    val outerImmutableSerializer =
      new CaseClassSerializer[OuterImmutable](classOf[OuterImmutable], true, 0, Array.empty, Array(mutableSerializer))
    val expectedData = OuterImmutable(Mutable("a"))

    val resultData = outerImmutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData.a shouldNot be theSameInstanceAs expectedData.a
    resultData shouldEqual expectedData
  }

  it should "copy the case class when mutable but not its immutable content" in {
    val immutableSerializer =
      new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(StringSerializer.INSTANCE))
    val outerMutableSerializer =
      new CaseClassSerializer[OuterMutable](classOf[OuterMutable], false, 0, Array.empty, Array(immutableSerializer))
    val expectedData = OuterMutable(Immutable("a"))

    val resultData = outerMutableSerializer.copy(expectedData)

    resultData shouldNot be theSameInstanceAs expectedData
    resultData.a should be theSameInstanceAs expectedData.a
    resultData shouldEqual expectedData
  }

  it should "copy the serialized stream" in {
    val serializer      = new CaseClassSerializer[Immutable](classOf[Immutable], true, 0, Array.empty, Array(StringSerializer.INSTANCE))
    val outerSerializer = new CaseClassSerializer[OuterMutable](classOf[OuterMutable], true, 0, Array.empty, Array(serializer))
    val expectedData    = OuterMutable(Immutable("a"))

    val output = new DataOutputSerializer(1024)
    outerSerializer.serialize(expectedData, output)
    val input     = new DataInputDeserializer(output.getSharedBuffer)
    val newOutput = new DataOutputSerializer(1024)
    outerSerializer.copy(input, newOutput)
    val newInput   = new DataInputDeserializer(newOutput.getSharedBuffer)
    val resultData = outerSerializer.deserialize(newInput)

    resultData shouldEqual expectedData
  }

  "duplicate" should "return itself when the serializer is immutable" in {
    val serializer = new CaseClassSerializer[Mutable](classOf[Mutable], false, 0, Array.empty, Array(StringSerializer.INSTANCE))
    serializer.duplicate() should be theSameInstanceAs serializer
  }

  it should "return a new instance of itself when the serializer is mutable" in {
    val serializer = new CaseClassSerializer[Mutable](classOf[Mutable], false, 0, Array.empty, Array(new RowSerializer(Array.empty)))
    val duplicatedSerializer = serializer.duplicate()
    duplicatedSerializer shouldNot be theSameInstanceAs serializer
    duplicatedSerializer should be(serializer)
  }

}

object CaseClassSerializerTest {

  case class Immutable(a: String) {
    var b: String = "" // This field is not taken into account because it's not serialized.
  }
  case class Mutable(var a: String) {
    val b: String = ""
  }

  case class OuterImmutable(a: Mutable)

  case class OuterMutable(var a: Immutable)

}
