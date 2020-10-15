package NetWorkFlowAnalysis

import java.text.SimpleDateFormat

import ContinuousLoginFailure.ContinuousLoginFailure.getClass
import model.ApacheLogEvent
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object NetWorkFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val resource = getClass.getResource("/apache.log").getPath
    val inputStream = env.readTextFile(resource)

    val dataStream  = inputStream
      .map(data => {
        val arr = data.split(" ")
        ApacheLogEvent(arr(0), arr(1), new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(arr(3)).getTime, arr(5), arr(6))
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = t.timestamp
      })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new NetWorkFlowAggregateFunction, new NetWorkFlowWindowFunction)
      .keyBy(_.windowEnd)
      .process(new NetWorkFlowKeyedProcessFunction(3))
      .print()

    env.execute("NetWorkFlowAnalysis")
  }
}
