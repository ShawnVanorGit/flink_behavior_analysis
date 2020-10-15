package NetWorkFlowAnalysis

import java.sql.Timestamp

import model.NetWorkFlowUrlCount
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class NetWorkFlowKeyedProcessFunction(topSize: Int) extends KeyedProcessFunction[Long, NetWorkFlowUrlCount, String]{

  lazy val urlCountMapState: MapState[String, Int] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Int]("urlCountMapState", classOf[String], classOf[Int]))

  override def processElement(value: NetWorkFlowUrlCount, ctx: KeyedProcessFunction[Long, NetWorkFlowUrlCount, String]#Context, out: Collector[String]): Unit = {
    urlCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, NetWorkFlowUrlCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val urlList: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)];
    val iter = urlCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      urlList += ((entry.getKey, entry.getValue))
    }
    urlCountMapState.clear();
    val sortedList = urlList.sortWith(_._2 > _._2).take(topSize)

    val result: StringBuilder = new StringBuilder
    result.append("windowEnd: ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedList.indices){
      val currentItem = sortedList(i)
      result
        .append("No.").append(i + 1)
        .append(" (点击率：").append(currentItem._2).append(")\t")
        .append("url= ").append(currentItem._1).append("\n")
    }
    result.append("------------------------------------------\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}