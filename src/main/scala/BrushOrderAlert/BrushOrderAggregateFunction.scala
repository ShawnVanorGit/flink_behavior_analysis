package BrushOrderAlert

import model.UserAdvertClick
import org.apache.flink.api.common.functions.AggregateFunction

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 15:12
 **/
class BrushOrderAggregateFunction extends AggregateFunction[UserAdvertClick, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserAdvertClick, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
