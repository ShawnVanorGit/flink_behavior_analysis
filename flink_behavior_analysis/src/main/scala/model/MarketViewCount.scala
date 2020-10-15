package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 9:35
 **/

case class MarketViewCount(
                            windowStart: String,
                            windowEnd: String,
                            channel: String,
                            behavior: String,
                            count: Long
                          )
