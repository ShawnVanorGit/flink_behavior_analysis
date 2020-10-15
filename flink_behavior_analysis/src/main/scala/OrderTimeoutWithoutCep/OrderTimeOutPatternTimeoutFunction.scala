package OrderTimeoutWithoutCep

import java.util

import model.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternTimeoutFunction

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:10
 **/
class OrderTimeOutPatternTimeoutFunction extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val orderTimeoutOrderId = pattern.getOrDefault("begin", null).iterator().next().orderId
    OrderResult(orderTimeoutOrderId, " time out " + timeoutTimestamp +"s")
  }
}
