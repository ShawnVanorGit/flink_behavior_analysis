package NetWorkFlowAnalysis

import model.ApacheLogEvent
import org.apache.flink.api.common.functions.AggregateFunction

class NetWorkFlowAggregateFunction extends AggregateFunction[ApacheLogEvent, Int, Int]{
  override def createAccumulator(): Int = {
    0
  }

  override def add(in: ApacheLogEvent, acc: Int): Int = {
    acc + 1
  }

  override def getResult(acc: Int): Int = {
    acc
  }

  override def merge(acc: Int, acc1: Int): Int = {
    acc + acc1
  }
}
