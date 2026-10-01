package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.TestUtils
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.EvolutionTest.Animal
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream

/** Reproduces what a TaskManager does with a serializer: it is Java-deserialized from the job graph into a JVM where
  * the derivation never ran, so the [[Evolutions]] registry only holds what the companions declare when asked.
  */
class EvolutionOnTaskManagerTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.evolution.EvolutionRenamedTest.Pet

  override protected def beforeEach(): Unit = Evolutions.reset()

  /** Derive the serializer as the client does, ship it through Java serialization as the job graph does, then hand it
    * back in a JVM whose registry has been emptied, as a fresh TaskManager would.
    */
  private def shipToTaskManager[T](serializer: => TypeSerializer[T]): TypeSerializer[T] = {
    val jobGraphBytes = InstantiationUtil.serializeObject(serializer)
    Evolutions.reset() // Fresh TaskManager JVM: the derivation never ran here
    InstantiationUtil.deserializeObject[TypeSerializer[T]](jobGraphBytes, getClass.getClassLoader)
  }

  /** Restore the former serializer from the checkpoint and read the former data with it, as a TaskManager does. */
  private def restoreFromFile[T](fileName: String): T = {
    val input            = new DataInputViewStreamWrapper(new FileInputStream(snapshotPath(fileName)))
    val restoredSnapshot = TypeSerializerSnapshot.readVersionedSnapshot[T](input, getClass.getClassLoader)
    input.readInt() // Snapshot size
    restoredSnapshot.restoreSerializer().deserialize(input)
  }

  // The former names of the trait, of its subtype and of its field are only known from the annotations of the current
  // source code: the trait is borne by no class anymore, so only the scan of the jars finds the companion declaring it
  it should "restore a renamed sealed trait on a TaskManager" in {
    shipToTaskManager(createSerializer[Pet])

    restoreFromFile[Pet]("Pet-v0") shouldBe EvolutionRenamedTest.Pony("Spirit")
  }

  // A checkpoint written by a more recent source code, typically after a rollback: the class is there, its declaration
  // just doesn't reach that far, which the schema compatibility resolution reports on its own
  it should "restore a checkpoint more recent than the declaration" in {
    Evolutions.get[Pet](classOf[Pet].getName, 3, getClass.getClassLoader).currentClass shouldBe classOf[Pet]
  }

  // The deleted subtype is declared by the trait, whose companion the snapshot of the trait initializes by name
  it should "restore a deleted subtype on a TaskManager" in {
    shipToTaskManager(createSerializer[Animal])

    restoreFromFile[Animal]("Animal-v0") shouldBe null
  }

}
