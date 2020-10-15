package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/14 16:37
 **/
case class OrderEvent(
                       orderId: Long, eventType: String, txId: String, eventTime: Long
                     )
