package NetWorkFlowAnalysis

import model.NetWorkFlowUrlCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class NetWorkFlowWindowFunction extends WindowFunction[Int, NetWorkFlowUrlCount, String, TimeWindow]{
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[Int],
                     out: Collector[NetWorkFlowUrlCount]): Unit = {
    out.collect(NetWorkFlowUrlCount(key, window.getEnd, input.iterator.next()))
  }
}
