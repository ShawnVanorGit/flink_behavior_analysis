package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 10:08
 **/
case class ApacheLogEvent(
                         ip:String,
                         userId:String,
                         timestamp:Long,
                         method:String,
                         url:String
                         )
