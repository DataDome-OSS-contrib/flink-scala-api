package org.apache.flinkx.api.evolution

import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{postDeserialize, renamed, version}

/** The ADT that wrote the savepoint, and the one restoring it: renamed, with a renamed field. */
object EvolutionRestoreFixtures {

  /** Versioned, so restoring it needs its declaration, and marked on the way back from the state. */
  @version(1)
  @renamed(since = 1, "FormerOrder")
  @postDeserialize(mark)
  case class Order(id: String, total: Int)

  /** Applied to every instance read back, so a restored value tells the declaration was applied. */
  def mark(version: Int, order: Order): Order =
    if (order.id.endsWith("!")) order else order.copy(id = order.id + "!")

  /** Emits the given values, then stays open so a savepoint can be taken while the state is held. */
  class Endless[T](values: Seq[T]) extends SourceFunction[T] {
    @volatile private var running = true

    override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
      values.foreach(value => ctx.getCheckpointLock.synchronized(ctx.collect(value)))
      while (running) Thread.sleep(10)
    }

    override def cancel(): Unit = running = false
  }

  /** Keeps the last value per key, with the state descriptor built in `open`, on the TaskManager. */
  class KeepLast[T: TypeInformation] extends KeyedProcessFunction[String, T, String] {

    @transient private var last: ValueState[T] = _

    override def open(context: OpenContext): Unit = {
      // Built here, long after the job graph was serialized: nothing carries the evolutions but the jar
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
