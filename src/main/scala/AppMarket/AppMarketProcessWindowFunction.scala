package AppMarket

import java.sql.Timestamp

import model.{MarketUserBehavior, MarketViewCount}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:53
 **/
class AppMarketProcessWindowFunction extends ProcessWindowFunction[MarketUserBehavior, MarketViewCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val app = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketViewCount(start, end, app, behavior, count))
  }
}
