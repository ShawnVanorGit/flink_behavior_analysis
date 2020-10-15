package BrushOrderAlert

import model.{BlackList, UserAdvertClick}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @Author Natasha
 * @Description 对有刷单行为的用户报警
 * @Date 2020/10/14 14:05
 **/

object BrushOrderAlert {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/AdClickLog.csv").getPath
    val socketStream = env.readTextFile(resource)
    val dataStream = socketStream
      .map(data => {
        val arr = data.split(",")
        UserAdvertClick(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //将有刷单行为的用户输出到侧输出流（黑名单报警）
    val filterStream = dataStream
      .keyBy(data=>(data.userId, data.advertId))
      .process(new BrushOrderKeyedProcessFunction(100))

    val aggProviceStream = filterStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new BrushOrderAggregateFunction, new BrushOrderWindowFunction)

    aggProviceStream.print("各省份下单详情")
    filterStream.getSideOutput(new OutputTag[BlackList]("BlackList")).print("恶意刷单详情")

    env.execute()
  }


}
