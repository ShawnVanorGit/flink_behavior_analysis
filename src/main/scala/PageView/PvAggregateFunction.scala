package PageView

import org.apache.flink.api.common.functions.AggregateFunction

class PvAggregateFunction extends AggregateFunction[(String, Int), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: (String, Int), acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
