package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.base.array.StringArraySerializer
import org.apache.flink.api.common.typeutils.base.{IntSerializer, StringSerializer}
import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.apache.flinkx.api.serializer.ScalaCaseClassSerializerSnapshotTest._
import org.apache.flinkx.api.serializer.SnapshotTestUtil.savepointed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScalaCaseClassSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  "CaseClassSerializer.snapshotConfiguration" should "produce a ScalaCaseClassSerializerSnapshot" in {
    serializerOf(stringSer).snapshotConfiguration() should be(a[ScalaCaseClassSerializerSnapshot[_]])
  }

  "resolveSchemaCompatibility" should "be compatibleAsIs when the schema is unchanged" in {
    val registered = serializerOf(stringSer).snapshotConfiguration()
    val restored   = savepointed[Evolved](serializerOf(stringSer))

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("compatibleAsIs")
  }

  it should "be incompatible when a field is appended to the end without @added" in {
    val registered = serializerOf(stringSer, intSer).snapshotConfiguration() // new code: 2 fields
    val restored   = savepointed[Evolved](serializerOf(stringSer))           // savepoint: 1 field

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when a field is removed" in {
    val registered = serializerOf(stringSer).snapshotConfiguration()       // new code: 1 field
    val restored   = savepointed[Evolved](serializerOf(stringSer, intSer)) // savepoint: 2 fields

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when a shared leading field changes type" in {
    val registered = serializerOf(intSer).snapshotConfiguration()
    val restored   = savepointed[Evolved](serializerOf(stringSer))

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  it should "be incompatible when the class name differs" in {
    val registered = serializerOf(stringSer).snapshotConfiguration()
    // A snapshot of a different class is erased to the same raw type at runtime, the way Flink sees it.
    val restored = savepointed[Other](
      new CaseClassSerializer[Other](evolutionOf(classOf[Other]), false, Array("a"), Array(stringSer))
    )
      .asInstanceOf[TypeSerializerSnapshot[Evolved]]

    registered.resolveSchemaCompatibility(restored) shouldBe Symbol("incompatible")
  }

  // Guards that we still emit the exact byte layout CompositeTypeSerializerSnapshot used to write, so savepoints taken
  // before this snapshot stopped extending it keep restoring unchanged.
  "the snapshot wire format" should "start with the composite magic number, outer version and class name" in {
    val out = new DataOutputSerializer(1024)
    serializerOf(stringSer).snapshotConfiguration().writeSnapshot(out)

    val in = new DataInputDeserializer(out.getSharedBuffer)
    in.readInt() should be(911108)                   // CompositeTypeSerializerSnapshot.MAGIC_NUMBER
    in.readInt() should be(4)                        // outer-snapshot version
    in.readUTF() should be(classOf[Evolved].getName) // outer snapshot: class name
    in.readInt() should be(0)                        // schema version, since outer-snapshot version 4
    in.readBoolean() should be(false)                // immutability, since outer-snapshot version 3
    StringArraySerializer.INSTANCE.deserialize(in) should be(Array("a")) // field names, since version 4
  }

  // Savepoints of 2.3.x and before record neither the schema version nor the field names: the fields are read by position
  "a snapshot written before 2.4.0" should "restore a serializer reading the fields by position" in {
    val out = new DataOutputSerializer(1024)
    out.write(writtenBefore240(serializerOf(stringSer, intSer)))
    serializerOf(stringSer, intSer).serialize(Evolved("x", 1), out)

    val in       = new DataInputDeserializer(out.getCopyOfBuffer)
    val restored = TypeSerializerSnapshot.readVersionedSnapshot[Evolved](in, getClass.getClassLoader)

    serializerOf(stringSer, intSer).snapshotConfiguration().resolveSchemaCompatibility(restored) shouldBe Symbol(
      "compatibleAsIs"
    )
    val restoredSerializer = restored.restoreSerializer().asInstanceOf[CaseClassSerializer[Evolved]]
    restoredSerializer.fieldNames shouldBe empty
    restoredSerializer.deserialize(in) shouldBe Evolved("x", 1)
  }

  it should "round-trip through writeSnapshot/readSnapshot" in {
    val original = serializerOf(stringSer, intSer).snapshotConfiguration()

    val out = new DataOutputSerializer(1024)
    original.writeSnapshot(out)

    val restored = new ScalaCaseClassSerializerSnapshot[Evolved]()
    restored.readSnapshot(
      original.getCurrentVersion,
      new DataInputDeserializer(out.getSharedBuffer),
      getClass.getClassLoader
    )

    restored.restoreSerializer().asInstanceOf[CaseClassSerializer[Evolved]].getArity should be(2)
    original.resolveSchemaCompatibility(restored) shouldBe Symbol("compatibleAsIs")
  }

}

object ScalaCaseClassSerializerSnapshotTest {

  /** The versioned snapshot of the given serializer, as 2.3.x wrote it: the outer snapshot version 3 records the class
    * name and the immutability only.
    */
  def writtenBefore240(serializer: CaseClassSerializer[_]): Array[Byte] = {
    val current  = new DataOutputSerializer(1024)
    val snapshot = serializer.snapshotConfiguration()
    snapshot.writeSnapshot(current)
    val in          = new DataInputDeserializer(current.getCopyOfBuffer)
    val magicNumber = in.readInt()
    in.readInt() // Outer snapshot version
    val className = in.readUTF()
    in.readInt() // Schema version
    val isCaseClassImmutable = in.readBoolean()
    StringArraySerializer.INSTANCE.deserialize(in) // Field names
    val nestedSnapshots = new Array[Byte](in.available())
    in.readFully(nestedSnapshots)

    val out = new DataOutputSerializer(1024)
    out.writeUTF(snapshot.getClass.getName)
    out.writeInt(snapshot.getCurrentVersion)
    out.writeInt(magicNumber)
    out.writeInt(3)
    out.writeUTF(className)
    out.writeBoolean(isCaseClassImmutable)
    out.write(nestedSnapshots)
    out.getCopyOfBuffer
  }

  case class Evolved(a: String, b: Int = 0)
  case class Other(a: String)

  val stringSer: TypeSerializer[_] = StringSerializer.INSTANCE
  val intSer: TypeSerializer[_]    = IntSerializer.INSTANCE

  def serializerOf(fieldSerializers: TypeSerializer[_]*): CaseClassSerializer[Evolved] =
    new CaseClassSerializer[Evolved](
      evolutionOf(classOf[Evolved]),
      false,
      Array("a", "b").take(fieldSerializers.length),
      fieldSerializers.toArray
    )

}
