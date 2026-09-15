package org.apache.flinkx.api

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.{DataInputDeserializer, DataInputViewStreamWrapper, DataOutputSerializer}
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.Scala3EnumTest.FailureCategory
import org.apache.flinkx.api.serializer.Scala3EnumSerializer
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.evolution.{Declare, EvolutionNotDeclaredException, Evolutions}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream

/** Reproduces what a TaskManager does with an enum serializer: it is Java-deserialized from the job graph into a JVM
  * where the derivation never ran, so the [[Evolutions]] registry only holds what the providers of the jar declare.
  */
class Scala3EnumOnTaskManagerTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "restore a renamed enum value on a TaskManager" in {
    Declare.declare[FailureCategory]
    val jobGraphBytes = InstantiationUtil.serializeObject(createSerializer[FailureCategory])
    Evolutions.reset()               // Fresh TaskManager JVM: the derivation never ran here
    Declare.declare[FailureCategory] // What the provider listed in the jar does, on the first lookup that misses
    InstantiationUtil.deserializeObject[TypeSerializer[FailureCategory]](jobGraphBytes, getClass.getClassLoader)

    val input = new DataInputViewStreamWrapper(new FileInputStream(snapshotPath("Failure-Type-PARSING_TYPE-v0")))
    val restoredSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[FailureCategory](input, getClass.getClassLoader)
    input.readInt() // Snapshot size
    restoredSnapshot.restoreSerializer().deserialize(input) shouldBe FailureCategory.PARSING
  }

  // The value snapshot records the version of its enum, so a missing declaration is caught there too
  it should "fail to restore an enum value when no declaration reached the TaskManager" in {
    val enumSerializer = createSerializer[FailureCategory].asInstanceOf[Scala3EnumSerializer[FailureCategory & Product]]
    val valueSnapshot  = enumSerializer.enumValueSerializers.head.snapshotConfiguration()
    val out            = new DataOutputSerializer(1024)
    TypeSerializerSnapshot.writeVersionedSnapshot(out, valueSnapshot)
    Evolutions.reset() // Fresh TaskManager JVM: the derivation never ran here

    val exception = intercept[EvolutionNotDeclaredException] {
      TypeSerializerSnapshot.readVersionedSnapshot(
        new DataInputDeserializer(out.getSharedBuffer),
        getClass.getClassLoader
      )
    }

    exception.getMessage should startWith(
      s"Cannot restore '${classOf[FailureCategory].getName}': the checkpoint was written at @version(1),"
    )
  }

}
