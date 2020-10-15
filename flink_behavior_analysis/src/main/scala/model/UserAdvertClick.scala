package model

/**
 * @Author Natasha
 * @Description
 * @Date 2020/10/15 10:07
 **/
case class UserAdvertClick(
                        userId: Long,
                        advertId: Long,
                        province: String,
                        city: String,
                        timestamp: Long
                      )
