package ContinuousLoginFailure

import model.{LoginEvent, LoginFailWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class LoginFailKeyedProcessFunction(failCount: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{

  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailList", classOf[LoginEvent]))
  lazy val timeStampState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timeStampState", classOf[Long]))

  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    if(value.eventType == "fail"){
      loginFailListState.add(value)
      //注册一个两秒后的定时器
      val ts = value.timestamp * 1000L + 2000L
      context.timerService().registerEventTimeTimer(ts)
      timeStampState.update(ts)
    } else {
      context.timerService().deleteEventTimeTimer(timeStampState.value())
      loginFailListState.clear()
      timeStampState.clear()
    }
  }

  override def onTimer(timestamp: Long, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    val loginFailList:ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailListState.get().iterator()
    while(iter.hasNext){
      loginFailList += iter.next()
    }
    if(loginFailList.length >= failCount){
      out.collect(LoginFailWarning(loginFailList.head.userId, loginFailList.head.timestamp, loginFailList.last.timestamp, "login fail in 2s for " + loginFailList.length + " times."))
    }
    loginFailListState.clear()
    timeStampState.clear()
  }
}
