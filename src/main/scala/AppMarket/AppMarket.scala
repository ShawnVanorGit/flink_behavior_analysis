package AppMarket

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:36
 **/
object AppMarket {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val sourceStream = env.addSource(new SimulatedSourceFunction).assignAscendingTimestamps(_.timestamp)

    val processStream = sourceStream
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.days(1), Time.seconds(5))
      .process(new AppMarketProcessWindowFunction)
      .print()

    env.execute()
  }
}
