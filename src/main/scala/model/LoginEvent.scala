package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 10:08
 **/
case class LoginEvent(
                       userId: Long,
                       ip: String,
                       eventType: String,
                       timestamp: Long
                     )
