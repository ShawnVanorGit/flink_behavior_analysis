package TempIncreaseAlert

import model.TempReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

class TempKeyedProcessFunction extends KeyedProcessFunction[String, TempReading, String] {

  lazy val preTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("preTempState", classOf[Double]))
  lazy val curTimeStampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("curTimeStampState", classOf[Long]))

  override def processElement(value: TempReading, context: KeyedProcessFunction[String, TempReading, String]#Context, collector: Collector[String]): Unit = {
    val preTemp = preTempState.value()
    preTempState.update(value.temperature)
    val curTimeStamp = curTimeStampState.value()

    if(value.temperature < preTemp || preTemp == 0.0){
      curTimeStampState.clear()
    }else if(curTimeStamp == 0 && value.temperature > preTemp){
      val time = context.timerService().currentProcessingTime() + 100L
      context.timerService().registerProcessingTimeTimer(time)
      curTimeStampState.update(time)
    }
  }

  override def onTimer(timestamp: Long, context: KeyedProcessFunction[String, TempReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    context.output(new OutputTag[String]("alert"), "连续升温警告： " + context.getCurrentKey)
    curTimeStampState.clear()
  }
}