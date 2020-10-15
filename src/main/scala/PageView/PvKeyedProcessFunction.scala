package PageView

import model.PvCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class PvKeyedProcessFunction extends KeyedProcessFunction[Long, PvCount, PvCount]{
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))

  override def processElement(value: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    val preCount = countState.value()
    countState.update(preCount + value.count)
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, context: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, collect: Collector[PvCount]): Unit = {
    val totalCount = countState.value()
    collect.collect(PvCount(context.getCurrentKey, totalCount))
    countState.clear()
  }
}
