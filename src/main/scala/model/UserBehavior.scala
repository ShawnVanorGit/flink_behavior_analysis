package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 10:08
 **/
case class UserBehavior(
                         userId: Long,
                         itemId: Long,
                         categoryId: Int,
                         behavior: String,
                         timestamp: Long
                       )
