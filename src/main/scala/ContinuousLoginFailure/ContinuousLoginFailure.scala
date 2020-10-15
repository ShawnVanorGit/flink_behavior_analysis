package ContinuousLoginFailure

import BrushOrderAlert.BrushOrderAlert.getClass
import model.LoginEvent
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @Author Natasha
 * @Description 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
 * @Date 2020/10/14 10:30
 **/

object ContinuousLoginFailure {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/LoginLog.csv").getPath
    val socketStream = env.readTextFile(resource)
    val dataStream = socketStream
      .map(data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(t: LoginEvent): Long = t.timestamp * 1000L
      })

    val loginStream = dataStream
      .keyBy(_.userId)
      .process(new LoginFailAdvanceKeyedProcessFunction(2))

    dataStream.print("LoginEvent");
    loginStream.print("LoginFailWarning")

    env.execute()
  }
}
