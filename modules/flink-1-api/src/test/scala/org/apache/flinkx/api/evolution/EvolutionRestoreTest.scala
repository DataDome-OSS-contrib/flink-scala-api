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
import org.apache.flink.util.StateMigrationException
import org.apache.flinkx.api.evolution.EvolutionRestoreFixtures._
import org.apache.flinkx.api.serializer.CaseClassSerializer
import org.apache.flinkx.api.auto._
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

/** Restores in a MiniCluster a savepoint of a former ADT, with a state descriptor that `open` creates.
  *
  * The job graph contains no evolution: the companion scan finds the evolving companion in the jars of the job.
  *
  * Flink restores a keyed state only with a key serializer that is compatible as is: the key type of a keyed stream
  * cannot evolve, even with the evolution annotations.
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

  /** Order at version 0, under its former class name. The post-evolution mapper applies only to such former data. */
  private def formerOrderInfo: TypeInformation[Order] = {
    val current    = implicitly[TypeInformation[Order]].asInstanceOf[CaseClassTypeInfo[Order]]
    val serializer = current.createSerializer(new SerializerConfigImpl()).asInstanceOf[CaseClassSerializer[Order]]
    val formerName = classOf[Order].getName.replace("Order", "FormerOrder")
    val former     = new CaseClassSerializer[Order](
      Evolutions.get[Order](formerName, 0, getClass.getClassLoader),
      serializer.isCaseClassImmutable,
      serializer.fieldNames,
      serializer.getFieldSerializers.asInstanceOf[Array[TypeSerializer[_]]]
    )
    val fieldTypes = (0 until current.getArity).map(current.getTypeAt[Any])
    new CaseClassTypeInfo[Order](classOf[Order], fieldTypes, current.fieldNames, former)
  }

  /** Visitor at version 0, with its only field `id`. */
  private def formerVisitorInfo: TypeInformation[Visitor] = {
    val current    = implicitly[TypeInformation[Visitor]].asInstanceOf[CaseClassTypeInfo[Visitor]]
    val serializer = current.createSerializer(new SerializerConfigImpl()).asInstanceOf[CaseClassSerializer[Visitor]]
    val former     = new CaseClassSerializer[Visitor](
      Evolutions.get[Visitor](classOf[Visitor].getName, 0, getClass.getClassLoader),
      serializer.isCaseClassImmutable,
      Some(Array("id")),
      serializer.getFieldSerializers.take(1).asInstanceOf[Array[TypeSerializer[_]]]
    )
    new CaseClassTypeInfo[Visitor](classOf[Visitor], Seq(current.getTypeAt[Any](0)), Seq("id"), former)
  }

  /** The exception and its causes. */
  private def causes(failure: Throwable): Seq[Throwable] =
    Iterator.iterate(failure)(_.getCause).takeWhile(_ != null).toSeq

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
    // A stop with savepoint keeps the state. A finished job does not keep it.
    val savepoint = written.stopWithSavepoint(false, checkpoints.toUri.toString, SavepointFormatType.CANONICAL).get()

    // Empty the registry, as on a new TaskManager: only the evolving companion gives the evolutions to the restore
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
  }

  it should "not restore a keyed state whose key type declares an evolution" in {
    val checkpoints = Files.createTempDirectory("flinkx-evolution-key")

    IntegrationTestSink.values.clear()
    val writing = environment(checkpoints, None)
    writing
      .addSource(new Endless(Seq(Visit(Visitor("a"), 1))))
      .keyBy(_.visitor)(formerVisitorInfo)
      .process(new CountVisits)
      .uid("count")
      .addSink(new IntegrationTestSink[String])
    val written = writing.executeAsync("write")
    eventually(timeout(Span(30, Seconds)))(IntegrationTestSink.values.size should be >= 1)
    val savepoint = written.stopWithSavepoint(false, checkpoints.toUri.toString, SavepointFormatType.CANONICAL).get()

    Evolutions.reset()

    val restoring = environment(checkpoints, Some(savepoint))
    restoring
      .fromElements(Visit(Visitor("a"), 2))
      .keyBy(_.visitor)
      .process(new CountVisits)
      .uid("count")
      .addSink(new IntegrationTestSink[String])
    val failure = intercept[Exception](restoring.execute("restore"))

    val migration = causes(failure).collectFirst { case e: StateMigrationException => e }
    withClue(s"the restore must fail on the key serializer, not with ${causes(failure).map(_.getMessage)}:")(
      migration.map(_.getMessage).getOrElse("") should include("key serializer")
    )
  }

}
