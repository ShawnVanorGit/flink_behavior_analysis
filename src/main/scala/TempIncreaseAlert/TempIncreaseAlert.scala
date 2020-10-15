package TempIncreaseAlert

import model.TempReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic


/**
 * 1s之内温度连续上升就报警
 */
object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val socketStream = env.socketTextStream("localhost", 9998)
    val dataStream = socketStream
      .map(data => {
        val arr = data.split(",")
        TempReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[TempReading](Time.seconds(1)) {
        override def extractTimestamp(t: TempReading): Long = {t.timestamp + 1000L}
      })

    val processStream = dataStream
      .keyBy(_.id)
      .process(new TempKeyedProcessFunction)


    dataStream.print("data")
    processStream.getSideOutput(new OutputTag[String]("alert")).print("output")
    env.execute()
  }
}
