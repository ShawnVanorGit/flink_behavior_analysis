package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 10:08
 **/
case class LoginFailWarning(
                             userId: Long,
                             firstFailTime: Long,
                             lastFailTime: Long,
                             waringMsg: String
                           )
