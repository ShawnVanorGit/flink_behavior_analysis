package OrderTimeoutWithoutCep

import java.util

import model.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternSelectFunction

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:13
 **/
class OrderPayPatternSelectFunction extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payOrderId = map.getOrDefault("follow", null).iterator().next().orderId
    OrderResult(payOrderId, " pay successfully")
  }
}
