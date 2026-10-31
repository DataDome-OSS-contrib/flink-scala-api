package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector

/** An ADT renamed since the savepoint, and the functions of the job that writes and restores the savepoint. */
object EvolutionRestoreFixtures {

  /** A versioned ADT: the restore needs its declaration. Its post-evolution mapper marks each migrated instance. */
  @version(1)
  @renamed(since = 1, "FormerOrder")
  @postEvolution(mark)
  case class Order(id: String, total: Int)
  object Order extends Evolving[Order] { val evolutions = Evolutions[Order] }

  /** Adds `!` to the id of a migrated instance: the result shows that the restore applied the declaration. */
  def mark(version: Int, order: Order): Order =
    if (order.id.endsWith("!")) order else order.copy(id = order.id + "!")

  /** Emits the given values, then runs until cancel, so that the test can take a savepoint of the state. */
  class Endless[T](values: Seq[T]) extends SourceFunction[T] {
    @volatile private var running = true

    override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
      values.foreach(value => ctx.getCheckpointLock.synchronized(ctx.collect(value)))
      while (running) Thread.sleep(10)
    }

    override def cancel(): Unit = running = false
  }

  /** Keeps the last value of each key. It creates its state descriptor in `open`, on the TaskManager. */
  class KeepLast[T: TypeInformation] extends KeyedProcessFunction[String, T, String] {

    @transient private var last: ValueState[T] = _

    override def open(context: OpenContext): Unit = {
      // Created after the serialization of the job graph: only the jar of the job contains the evolutions
      last = getRuntimeContext.getState(new ValueStateDescriptor[T]("last", implicitly[TypeInformation[T]]))
    }

    override def processElement(
        value: T,
        ctx: KeyedProcessFunction[String, T, String]#Context,
        out: Collector[String]
    ): Unit = {
      val previous = last.value()
      last.update(value)
      out.collect(if (previous == null) s"$value" else s"$previous -> $value")
    }
  }

}
