package org.apache.flinkx.api

import org.apache.flink.api.common.typeutils.{TypeSerializer, TypeSerializerSnapshot}
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.util.InstantiationUtil
import org.apache.flinkx.api.auto._
import org.apache.flinkx.api.evolution.{EvolutionBuilder, EvolutionNotDeclaredException, Evolutions}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.FileInputStream

/** Reproduces what a TaskManager does with a serializer: it is Java-deserialized from the job graph into a JVM where
  * the derivation never ran, so the [[Evolutions]] registry only holds what the serializers themselves carry.
  */
class EvolutionOnTaskManagerTest extends AnyFlatSpec with Matchers with TestUtils with BeforeAndAfterEach {

  import org.apache.flinkx.api.EvolutionTest._
  import org.apache.flinkx.api.evolution.EvolutionRenamedTest._

  override protected def beforeEach(): Unit = Evolutions.reset()

  /** Derive the serializer as the client does, ship it through Java serialization as the job graph does, then hand it
    * back in a JVM whose registry has been emptied, as a fresh TaskManager would.
    */
  private def shipToTaskManager[T](serializer: TypeSerializer[T]): TypeSerializer[T] = {
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
  // source code, which is read on the client: the registry has to be reinstated here to resolve them
  it should "restore a renamed sealed trait on a TaskManager" in {
    shipToTaskManager(createSerializer[Pet])

    restoreFromFile[Pet]("Pet-v0") shouldBe Pony("Spirit")
  }

  // Restoring without the declaration would read the former form as if it never evolved, which no later check can
  // catch once the values are in the state table
  it should "fail to restore a versioned checkpoint when no declaration reached the TaskManager" in {
    val exception = intercept[EvolutionNotDeclaredException](restoreFromFile[Pet]("Pet-v2"))

    exception.getMessage should startWith(
      s"Cannot restore '${classOf[Pet].getName}': the checkpoint was written at @version(2), but no evolution is" +
        s" declared for that class here."
    )
  }

  // A checkpoint written by a more recent source code, typically after a rollback: the declaration did reach the
  // TaskManager, it just doesn't reach that far, which the schema compatibility resolution reports on its own
  it should "restore a checkpoint more recent than the declaration" in {
    Evolutions.register(new EvolutionBuilder(classOf[Pet], 1))

    Evolutions.get[Pet](classOf[Pet].getName, 3, getClass.getClassLoader).currentClass shouldBe classOf[Pet]
  }

  it should "restore a deleted subtype on a TaskManager" in {
    shipToTaskManager(createSerializer[Animal])

    restoreFromFile[Animal]("Animal-v0") shouldBe null
  }

}
