package OrderTimeoutWithoutCep

import model.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 16:39
 **/
class OrderKeyedProcessFunction extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isCreatedState", classOf[Boolean]))
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isPayedState", classOf[Boolean]))
  lazy val timeStampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeStampState", classOf[Long]))

  override def processElement(value: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {

    val isCreated = isCreatedState.value()
    val isPayed = isPayedState.value()
    val ts = timeStampState.value()

    if(value.eventType == "create"){
      if(isPayed){
        collector.collect(OrderResult(value.orderId, "payed successfully"))
        isCreatedState.clear()
        isPayedState.clear()
        timeStampState.clear()
        context.timerService().deleteEventTimeTimer(ts)
      } else {
        //如果还没pay过，注册定时器，等待15分钟
        val waitToPayTime = value.eventTime * 1000L + 900 * 1000L
        context.timerService().registerEventTimeTimer(waitToPayTime)
        timeStampState.update(waitToPayTime)
        isCreatedState.update(true)

      }
    } else if(value.eventType == "pay"){
      if(isCreated){
        if(value.eventTime * 1000L < ts){
          collector.collect(OrderResult(value.orderId, "payed successfully"))
        } else {
          context.output(new OutputTag[OrderResult]("pay_timeout"), OrderResult(value.orderId, "payed but already timeout"))
        }
        isCreatedState.clear()
        isPayedState.clear()
        timeStampState.clear()
        context.timerService().deleteEventTimeTimer(ts)
      } else {
        context.timerService().registerEventTimeTimer(value.eventTime * 1000L)
        isPayedState.update(true)
      }
    }
  }
  override def onTimer(timestamp: Long, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
    if(isCreatedState.value()){
      context.output(new OutputTag[OrderResult]("pay_timeout"), OrderResult(context.getCurrentKey, "order timeout"))
    }else{
      context.output(new OutputTag[OrderResult]("pay_timeout"), OrderResult(context.getCurrentKey, "payed but not found create log"))
    }
    isCreatedState.clear()
    isPayedState.clear()
    timeStampState.clear()
  }
}
