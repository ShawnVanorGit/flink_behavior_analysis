package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:29
 **/
case class MarketUserBehavior(
                               userId: String,
                               behavior: String,
                               channel: String,
                               timestamp: Long
                             )
