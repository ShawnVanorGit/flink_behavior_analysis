package ContinuousLoginFailure

import model.{LoginEvent, LoginFailWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 10:21
 **/
class LoginFailAdvanceKeyedProcessFunction(failCount: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{

  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginFailListState", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, collector: Collector[LoginFailWarning]): Unit = {
    if(value.eventType == "fail"){
      val iter = loginFailListState.get().iterator()
      if(iter.hasNext){
        val firstFailEvent = iter.next()
        if(value.timestamp - firstFailEvent.timestamp < 2){
          collector.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
          loginFailListState.clear()
        }
      }
      loginFailListState.add(value)
    } else {
      loginFailListState.clear()
    }
  }
}
