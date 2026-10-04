package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.{
  CheckpointingOptions,
  Configuration,
  ExternalizedCheckpointRetention,
  StateRecoveryOptions
}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flinkx.api.evolution.EvolutionRestoreFixtures._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.serializers._
import org.apache.flinkx.api.typeinfo.CaseClassTypeInfo
import org.apache.flinkx.api.{IntegrationTestSink, StreamExecutionEnvironment}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Eventually.timeout
import org.scalatest.time.{Seconds, Span}
import org.apache.flink.core.execution.SavepointFormatType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/** Restores a savepoint written by a former ADT, through a MiniCluster, with the state descriptor built in `open`.
  *
  * Nothing declares the evolutions here: the companion of the ADT is the only thing carrying them, and the TaskManager
  * initializes it while restoring, which is what makes the shape of the descriptor irrelevant.
  */
class EvolutionRestoreTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val cluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(1).setNumberTaskManagers(1).build()
  )

  override protected def beforeAll(): Unit = cluster.before()
  override protected def afterAll(): Unit  = cluster.after()

  private def environment(checkpoints: Path, restoreFrom: Option[String]): StreamExecutionEnvironment = {
    val configuration = new Configuration()
    configuration.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpoints.toUri.toString)
    configuration.set(
      CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
      ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )
    restoreFrom.foreach(path => configuration.set(StateRecoveryOptions.SAVEPOINT_PATH, path))
    val env = StreamExecutionEnvironment.getExecutionEnvironment(configuration)
    env.setParallelism(1)
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.enableCheckpointing(10)
    env.setRestartStrategy(RestartStrategies.noRestart())
    env
  }

  /** Order as a job at version 0 wrote it, under its former name: only the data of a former version is post-evolved. */
  private def formerOrderInfo: TypeInformation[Order] = {
    val current    = implicitly[TypeInformation[Order]].asInstanceOf[CaseClassTypeInfo[Order]]
    val serializer = current.createSerializer(new SerializerConfigImpl()).asInstanceOf[CaseClassSerializer[Order]]
    val formerName = classOf[Order].getName.replace("Order", "FormerOrder")
    val former     = new CaseClassSerializer[Order](
      Evolutions.get[Order](formerName, 0, getClass.getClassLoader),
      0,
      serializer.isCaseClassImmutable,
      serializer.fieldNames,
      serializer.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )
    val fieldTypes = (0 until current.getArity).map(current.getTypeAt[Any])
    new CaseClassTypeInfo[Order](classOf[Order], fieldTypes, current.fieldNames, former)
  }

  it should "apply the declared evolutions to a state whose descriptor is built on the TaskManager" in {
    val checkpoints = Files.createTempDirectory("flinkx-evolution")

    IntegrationTestSink.values.clear()
    val writing = environment(checkpoints, None)
    writing
      .addSource(new Endless(Seq(Order("a", 1))))
      .keyBy(_.id)
      .process(new KeepLast[Order]()(formerOrderInfo))
      .uid("keep-last")
      .addSink(new IntegrationTestSink[String])
    val written = writing.executeAsync("write")
    eventually(timeout(Span(30, Seconds)))(IntegrationTestSink.values.size should be >= 1)
    // Stopping with a savepoint keeps the state, where a job reaching its end would have it cleaned up
    val savepoint = written.stopWithSavepoint(false, checkpoints.toUri.toString, SavepointFormatType.CANONICAL).get()

    // A fresh TaskManager knows nothing: only the companion of the ADT can declare the evolutions, and only the
    // restore asks for them, long after the job graph was built
    Evolutions.reset()

    IntegrationTestSink.values.clear()
    val restoring = environment(checkpoints, Some(savepoint))
    restoring
      .fromElements(Order("a", 2))
      .keyBy(_.id)
      .process(new KeepLast[Order])
      .uid("keep-last")
      .addSink(new IntegrationTestSink[String])
    restoring.execute("restore")

    withClue("the state must be restored, and read back through the declared postEvolution:")(
      IntegrationTestSink.values.asScala.toList.map(_.toString).head should startWith("Order(a!,1) ->")
    )
    withClue("nothing but the companion could have declared it:")(
      Evolutions.declaredClassNames should contain(classOf[Order].getName)
    )
  }

}
