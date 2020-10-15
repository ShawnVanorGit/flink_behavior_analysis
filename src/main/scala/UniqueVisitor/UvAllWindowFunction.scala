package UniqueVisitor

import model.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UvAllWindowFunction extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    var userSet = Set[Long]()
    for(i <- input){
      userSet += i.userId
    }
    out.collect(UvCount(window.getEnd, userSet.size))
  }
}
