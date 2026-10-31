package org.apache.flinkx.api

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory._
import org.apache.flink.util.ChildFirstClassLoader
import org.apache.flinkx.api.SerializerSnapshotTest._
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.{Evolutions, Evolving, added, version}
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URLClassLoader
import java.nio.file.{Files, Paths}
import java.util.UUID
import java.io.FileInputStream

class SerializerSnapshotTest extends AnyFlatSpec with Matchers with TestUtils {

  it should "roundtrip product serializer snapshot" in {
    val ser = infoToSer[SimpleClass1]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip coproduct serializer snapshot" in {
    val ser = infoToSer[OuterTrait]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip coproduct serializer snapshot with singletons" in {
    val ser = infoToSer[ADT2]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip serializer snapshot with list of primitives" in {
    val ser = infoToSer[List[Double]]
    assertRoundtripSerializer(ser)
  }

  it should "roundtrip serializer snapshot with set as array of primitives" in {
    val ser = infoToSer[Set[Double]]
    assertRoundtripSerializer(ser)
  }

  it should "do array ser snapshot" in {
    val set = infoToSer[SimpleClassArray]
    assertRoundtripSerializer(set)
  }

  it should "do map ser snapshot" in {
    assertRoundtripSerializer(infoToSer[SimpleClassMap1])
    assertRoundtripSerializer(infoToSer[SimpleClassMap2])
  }

  it should "do list ser snapshot" in {
    assertRoundtripSerializer(infoToSer[SimpleClassList])
  }

  it should "do map ser snapshot adt " in {
    implicit val ti: TypeInformation[OuterTrait] = deriveTypeInformation[OuterTrait]
    assertRoundtripSerializer(infoToSer[TraitMap])
  }

  it should "be compatible after snapshot deserialization in different classloader" in {
    val ser = infoToSer[SimpleClass1]
    val cl  = newClassLoader(classOf[SimpleClass1])
    try {
      val restored      = roundtripSerializer(ser, cl)
      val compatibility = ser.snapshotConfiguration().resolveSchemaCompatibility(restored.snapshotConfiguration())
      compatibility shouldBe Symbol("compatibleAsIs")
    } finally {
      cl.close()
    }
  }

  def serializeWithOldSerializer() = {
    val uuid                 = UUID.fromString("4daf2791-abbe-420f-9594-f57ded1fee8c")
    val expectedData         = OuterClass(Map(uuid -> List(SimpleClass2("a", 1))))
    val outerClassSerializer = implicitly[TypeSerializer[OuterClass]]
    val oldSnapshot          = outerClassSerializer.snapshotConfiguration()

    // Serialize the old snapshot
    val out = new DataOutputSerializer(1483)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, oldSnapshot) // Flink always calls this

    // Serialize the data
    outerClassSerializer.serialize(expectedData, out)
    val path = Paths.get("target/test/resources/old-serializer-snapshot.dat")
    Files.createDirectories(path.getParent())
    Files.write(path, out.getSharedBuffer)
  }

  it should "deserialize old serialization with the new serializer snapshot" in {
    serializeWithOldSerializer()
    val uuid         = UUID.fromString("4daf2791-abbe-420f-9594-f57ded1fee8c")
    val expectedData = OuterClass(Map(uuid -> List(SimpleClass2("a", 1))))

    // Deserialize the old serialization
    val buffer               = new FileInputStream("target/test/resources/old-serializer-snapshot.dat").readAllBytes()
    val in                   = new DataInputDeserializer(buffer)
    val deserializedSnapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[OuterClass](in, getClass.getClassLoader) // Flink always calls this
    val deserializedSerializer = deserializedSnapshot.restoreSerializer()

    // Deserialize the data
    val deserializedData = deserializedSerializer.deserialize(in)
    deserializedData should be(expectedData)
  }

  it should "serialize and deserialize with the new snapshot" in {
    val uuid                 = UUID.randomUUID()
    val expectedData         = OuterClass(Map(uuid -> List(SimpleClass2("a", 1))))
    val outerClassSerializer = implicitly[TypeSerializer[OuterClass]]
    val oldSnapshot          = outerClassSerializer.snapshotConfiguration()

    // Serialize the new snapshot
    val oldOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(oldOutput, oldSnapshot) // Flink always calls this

    // Serialize the data
    outerClassSerializer.serialize(expectedData, oldOutput)

    // Deserialize the new snapshot
    val oldInput                = new DataInputDeserializer(oldOutput.getSharedBuffer)
    val deserializedOldSnapshot = TypeSerializerSnapshot
      .readVersionedSnapshot[OuterClass](oldInput, getClass.getClassLoader) // Flink always calls this
    val deserializedSerializer = deserializedOldSnapshot.restoreSerializer()

    // Deserialize the data
    val deserializedData = deserializedSerializer.deserialize(oldInput)
    deserializedData should be(expectedData)
  }

  it should "serialize and deserialize case class with null and default" in {
    val expectedData = WithDefault(null)
    // Serializer before schema change: without serializers for the second "new" default fields
    val oldSerializer = new CaseClassSerializer[WithDefault](
      evolution = evolutionOf(classOf[WithDefault]),
      isCaseClassImmutable = false,
      fieldNames = Some(Array("sc1")),
      scalaFieldSerializers = Array(implicitly[TypeSerializer[SimpleClass1]])
    )
    deserializeFormerForm[WithDefault](oldSerializer, expectedData) shouldBe expectedData

    // The job restarts with the current source code, which serializes the added field
    expectedData.sc2 = SimpleClass2("c", 3)
    roundtrip(implicitly[TypeSerializer[WithDefault]], expectedData)
  }

  def roundtripSerializer[T](ser: TypeSerializer[T], cl: ClassLoader = getClass.getClassLoader): TypeSerializer[T] = {
    val snap   = ser.snapshotConfiguration()
    val buffer = new ByteArrayOutputStream()
    val output = new DataOutputViewStreamWrapper(buffer)
    TypeSerializerSnapshot.writeVersionedSnapshot(output, snap)
    output.close()
    val input     = new DataInputViewStreamWrapper(new ByteArrayInputStream(buffer.toByteArray))
    val deserSnap = TypeSerializerSnapshot.readVersionedSnapshot[T](input, cl)
    deserSnap.restoreSerializer()
  }

  def assertRoundtripSerializer[T](ser: TypeSerializer[T]): Assertion = {
    val restored = roundtripSerializer(ser)
    ser shouldBe restored
  }

  def newClassLoader(cls: Class[_]): URLClassLoader = {
    val urls = Array(cls.getProtectionDomain.getCodeSource.getLocation)
    new ChildFirstClassLoader(urls, cls.getClassLoader, Array(), _ => {})
  }

}

object SerializerSnapshotTest {
  sealed trait OuterTrait
  case class SimpleClass1(a: String, b: Int)  extends OuterTrait
  case class SimpleClass2(a: String, b: Long) extends OuterTrait

  case class SimpleClassArray(a: Array[SimpleClass1])
  case class SimpleClassMap1(a: Map[String, SimpleClass1])
  case class SimpleClassMap2(a: Map[SimpleClass1, String])
  case class SimpleClassList(a: List[SimpleClass1])

  case class TraitMap(a: Map[OuterTrait, String])

  sealed trait ADT2
  case object Foo2 extends ADT2
  case object Bar2 extends ADT2

  case class OuterClass(map: Map[UUID, List[OuterTrait]])

  @version(1)
  case class WithDefault(
      var sc1: SimpleClass1 = SimpleClass1("a", 1),
      @added(since = 1) var sc2: SimpleClass2 = SimpleClass2("b", 2)
  )
  object WithDefault extends Evolving[WithDefault] { val evolutions = Evolutions[WithDefault] }

}
