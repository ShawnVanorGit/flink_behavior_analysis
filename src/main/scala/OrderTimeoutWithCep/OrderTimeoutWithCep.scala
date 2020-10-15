package OrderTimeoutWithCep

import NetWorkFlowAnalysis.NetWorkFlowAnalysis.getClass
import OrderTimeoutWithoutCep.{OrderPayPatternSelectFunction, OrderTimeOutPatternTimeoutFunction}
import model.{OrderEvent, OrderResult}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:00
 **/
object OrderTimeoutWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/OrderLog.csv").getPath
    val inputStream = env.readTextFile(resource)

    val orderEventStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //带时间限制的pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType.equals("create"))
      .followedBy("follow")
        .where(_.eventType.equals("pay"))
          .within(Time.minutes(15))


    //将pattern作用到inputStream上，得到一个patternStream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    val orderTimeoutTag = OutputTag[OrderResult]("orderTimeoutTag")
    //调用select，对超时的做测流输出报警
    val resultStream = patternStream.select(orderTimeoutTag, new OrderTimeOutPatternTimeoutFunction, new OrderPayPatternSelectFunction)

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutTag).print("timeout")

    env.execute()
  }
}
