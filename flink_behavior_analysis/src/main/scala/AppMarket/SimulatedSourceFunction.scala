package AppMarket

import java.util.UUID

import model.MarketUserBehavior
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:37
 **/
class SimulatedSourceFunction extends SourceFunction[MarketUserBehavior]{

  var running = true
  val rand: Random = Random
  val behaviorSet: Seq[String] = Seq("view", "download", "install", "uninstall")
  val appSet: Seq[String] = Seq("appstore", "weibo", "wechat", "tieba")

  override def run(sourceContext: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    val maxCount = Long.MaxValue
    var count = 0L
    while(running && count < maxCount){
      val userId = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val app = appSet(rand.nextInt(appSet.size))
      val timeStream = System.currentTimeMillis()
      sourceContext.collect(MarketUserBehavior(userId, behavior, app, timeStream))
      count += 1
      Thread.sleep(50L)
    }
  }

  override def cancel(): Unit = running = false
}
