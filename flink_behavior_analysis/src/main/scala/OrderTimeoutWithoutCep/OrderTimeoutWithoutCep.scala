package OrderTimeoutWithoutCep

import OrderTimeoutWithCep.OrderTimeoutWithCep.getClass
import model.{OrderEvent, OrderResult}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 16:36
 **/
object OrderTimeoutWithoutCep {
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
      .process(new OrderKeyedProcessFunction)

    orderEventStream.print("normal_order")
    orderEventStream.getSideOutput(new OutputTag[OrderResult]("pay_timeout")).print("timeout_order")

    env.execute()
  }
}
