package BrushOrderAlert

import model.{BlackList, UserAdvertClick}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 14:38
 **/
class BrushOrderKeyedProcessFunction(maxCount: Long) extends KeyedProcessFunction[(Long, Long), UserAdvertClick, UserAdvertClick]{

  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("countState", classOf[Long]))
  lazy val isBlackBooleanState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("blackBooleanState", classOf[Boolean]))
  lazy val timeStampState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeStampState", classOf[Long]))

  override def processElement(value: UserAdvertClick, context: KeyedProcessFunction[(Long, Long), UserAdvertClick, UserAdvertClick]#Context, collector: Collector[UserAdvertClick]): Unit = {
      val count = countState.value();
    if(count == 0){
      //注册第二天的0点清空各状态的定时器
      val ts = (context.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000) - 8 * 60 * 60 * 1000
      timeStampState.update(ts)
      context.timerService().registerEventTimeTimer(ts)
    }
    if(count >= maxCount){
      if(!isBlackBooleanState.value()){
        isBlackBooleanState.update(true)
        context.output(new OutputTag[BlackList](id="BlackList"), BlackList(value.userId, value.advertId, "Click ad over " + maxCount + " times today."))
      }
      return
    }
    countState.update(count + 1)
    collector.collect(value)
  }

  override def onTimer(timestamp: Long, context: KeyedProcessFunction[(Long, Long), UserAdvertClick, UserAdvertClick]#OnTimerContext, out: Collector[UserAdvertClick]): Unit = {
    if(timestamp == timeStampState.value()){
      isBlackBooleanState.clear()
      countState.clear()
    }
  }
}
