package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.{
  CheckpointingOptions,
  Configuration,
  ExternalizedCheckpointRetention,
  StateRecoveryOptions
}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flinkx.api.evolution.EvolutionRestoreFixtures._
import org.apache.flinkx.api.serializers._
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
  * Nothing declares the evolutions here: the provider listed in `META-INF/services` is the only thing carrying them,
  * and it is read while the TaskManager restores, which is what makes the shape of the descriptor irrelevant.
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

  it should "apply the declared evolutions to a state whose descriptor is built on the TaskManager" in {
    val checkpoints = Files.createTempDirectory("flinkx-evolution")

    IntegrationTestSink.values.clear()
    val writing = environment(checkpoints, None)
    writing
      .addSource(new Endless(Seq(Order("a", 1))))
      .keyBy(_.id)
      .process(new KeepLast[Order])
      .uid("keep-last")
      .addSink(new IntegrationTestSink[String])
    val written = writing.executeAsync("write")
    eventually(timeout(Span(30, Seconds)))(IntegrationTestSink.values.size should be >= 1)
    // Stopping with a savepoint keeps the state, where a job reaching its end would have it cleaned up
    val savepoint = written.stopWithSavepoint(false, checkpoints.toUri.toString, SavepointFormatType.CANONICAL).get()

    // A fresh TaskManager knows nothing: only the provider of the jar can declare the evolutions, and only the
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

    withClue("the state must be restored, and read back through the declared postDeserialize:")(
      IntegrationTestSink.values.asScala.toList.map(_.toString).head should startWith("Order(a!,1) ->")
    )
    withClue("nothing but the provider could have declared it:")(
      Evolutions.declaredNames(getClass.getClassLoader) should contain(classOf[Order].getName)
    )
  }

}
