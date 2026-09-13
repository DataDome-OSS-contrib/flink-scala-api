package org.apache.flinkx.api

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.Scala3EnumTest.FailureCategory
import org.apache.flinkx.api.auto.*
import org.apache.flinkx.api.evolution.Evolutions
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream

/** Reproduces what a TaskManager does with an enum serializer: it is Java-deserialized from the job graph into a JVM
  * where the derivation never ran, so the [[Evolutions]] registry is only what the serializers themselves carry.
  */
class Scala3EnumOnTaskManagerTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = Evolutions.reset()

  it should "restore a renamed enum value on a TaskManager" in {
    val jobGraphBytes = InstantiationUtil.serializeObject(createSerializer[FailureCategory])
    Evolutions.reset() // Fresh TaskManager JVM: the derivation never ran here
    InstantiationUtil.deserializeObject[TypeSerializer[FailureCategory]](jobGraphBytes, getClass.getClassLoader)

    val input = new DataInputViewStreamWrapper(new FileInputStream(snapshotPath("Failure-Type-PARSING_TYPE-v0")))
    val restoredSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[FailureCategory](input, getClass.getClassLoader)
    input.readInt() // Snapshot size
    restoredSnapshot.restoreSerializer().deserialize(input) shouldBe FailureCategory.PARSING
  }

}
