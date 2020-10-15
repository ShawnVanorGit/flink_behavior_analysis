package BrushOrderAlert

import model.ProvinceAdvertClick
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 15:16
 **/
class BrushOrderWindowFunction extends WindowFunction[Long, ProvinceAdvertClick, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[ProvinceAdvertClick]): Unit = {
    out.collect(ProvinceAdvertClick(key,input.head,window.getEnd.toString))
  }
}
