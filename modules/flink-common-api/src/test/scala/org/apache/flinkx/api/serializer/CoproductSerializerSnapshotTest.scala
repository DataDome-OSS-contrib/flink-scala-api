package org.apache.flinkx.api.serializer

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flinkx.api.serializer.CoproductSerializerSnapshotTest.{ADT, Bar, Foo}
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.TestUtils.evolutionOf
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoproductSerializerSnapshotTest extends AnyFlatSpec with Matchers {

  it should "serialize v3 then deserialize v3" in {
    // Create SerializerSnapshot
    val subtypeClasses: Array[Class[?]]              = Array(classOf[Foo], classOf[Bar])
    val subtypeSerializers: Array[TypeSerializer[?]] = Array(
      implicitly[TypeSerializer[Foo]],
      implicitly[TypeSerializer[Bar]]
    )
    val serializerSnapshot: CoproductSerializer.CoproductSerializerSnapshot[ADT] =
      new CoproductSerializer.CoproductSerializerSnapshot(
        Some(
          new CoproductSerializer[ADT](
            Some(evolutionOf(classOf[ADT])),
            subtypeClasses,
            subtypeClasses.map(_.getName),
            subtypeSerializers
          )
        )
      )

    val expectedSerializer = serializerSnapshot.restoreSerializer()

    // Serialize SerializerSnapshot
    val snapshotOutput = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(snapshotOutput, serializerSnapshot)
    val snapshotInput = new DataInputDeserializer(snapshotOutput.getSharedBuffer)

    // Deserialize SerializerSnapshot
    val deserializedSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[ADT](snapshotInput, getClass.getClassLoader)

    // Check the compatibility of the current snapshot with the restored snapshot
    val compatibility = serializerSnapshot.resolveSchemaCompatibility(deserializedSnapshot)
    compatibility.isIncompatible shouldBe false

    val deserializedSerializer = deserializedSnapshot.restoreSerializer()
    deserializedSerializer shouldNot be theSameInstanceAs expectedSerializer
    deserializedSerializer should be(expectedSerializer)
  }

  it should "serialize v2 then deserialize v3" in {
    val subtypeClasses: Array[Class[?]]              = Array(classOf[Foo], classOf[Bar])
    val subtypeSerializers: Array[TypeSerializer[?]] = Array(
      implicitly[TypeSerializer[Foo]],
      implicitly[TypeSerializer[Bar]]
    )

    // Write a snapshot of format version 2, as CoproductSerializerSnapshot#writeSnapshot did before
    val out = new DataOutputSerializer(1024 * 1024)
    out.writeInt(subtypeClasses.length)
    subtypeClasses.foreach(c => out.writeUTF(c.getName))
    subtypeSerializers.foreach(s => TypeSerializerSnapshot.writeVersionedSnapshot(out, s.snapshotConfiguration()))

    // Restore the snapshot of format version 2 as Flink does: no-arg constructor, then readSnapshot with its version
    val oldSnapshot = new CoproductSerializer.CoproductSerializerSnapshot[ADT]()
    oldSnapshot.readSnapshot(2, new DataInputDeserializer(out.getSharedBuffer), getClass.getClassLoader)

    // Current snapshot for the same schema
    val newSnapshot = new CoproductSerializer.CoproductSerializerSnapshot[ADT](
      Some(
        new CoproductSerializer[ADT](
          Some(evolutionOf(classOf[ADT])),
          subtypeClasses,
          subtypeClasses.map(_.getName),
          subtypeSerializers
        )
      )
    )

    val compatibility = newSnapshot.resolveSchemaCompatibility(oldSnapshot)
    compatibility.isIncompatible shouldBe false
  }

  // A state that the job does not access before the next checkpoint keeps its restored serializer: this serializer has
  // no sealed trait name, so its snapshot uses a format without this name
  it should "snapshot again a serializer restored from a snapshot of format version 2" in {
    val subtypeClasses: Array[Class[?]]              = Array(classOf[Foo], classOf[Bar])
    val subtypeSerializers: Array[TypeSerializer[?]] = Array(
      implicitly[TypeSerializer[Foo]],
      implicitly[TypeSerializer[Bar]]
    )
    val out = new DataOutputSerializer(1024 * 1024)
    out.writeInt(subtypeClasses.length)
    subtypeClasses.foreach(c => out.writeUTF(c.getName))
    subtypeSerializers.foreach(s => TypeSerializerSnapshot.writeVersionedSnapshot(out, s.snapshotConfiguration()))
    val oldSnapshot = new CoproductSerializer.CoproductSerializerSnapshot[ADT]()
    oldSnapshot.readSnapshot(2, new DataInputDeserializer(out.getSharedBuffer), getClass.getClassLoader)
    val restoredSerializer = oldSnapshot.restoreSerializer()

    val rewritten = new DataOutputSerializer(1024 * 1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(rewritten, restoredSerializer.snapshotConfiguration())
    val reread = TypeSerializerSnapshot.readVersionedSnapshot[ADT](
      new DataInputDeserializer(rewritten.getSharedBuffer),
      getClass.getClassLoader
    )

    reread.restoreSerializer() should be(restoredSerializer)
    val currentSnapshot = new CoproductSerializer.CoproductSerializerSnapshot[ADT](
      Some(
        new CoproductSerializer[ADT](
          Some(evolutionOf(classOf[ADT])),
          subtypeClasses,
          subtypeClasses.map(_.getName),
          subtypeSerializers
        )
      )
    )
    currentSnapshot.resolveSchemaCompatibility(reread).isIncompatible shouldBe false
  }

}

object CoproductSerializerSnapshotTest {
  sealed trait ADT
  case class Foo(a: String) extends ADT
  case class Bar(b: Int)    extends ADT
}
