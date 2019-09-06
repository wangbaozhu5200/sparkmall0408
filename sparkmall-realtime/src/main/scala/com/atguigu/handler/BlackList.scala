package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackList {

  //时间转换类
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  //定义黑名单RedisKey
  private val blackList: String = "BlackList"

  /**
    * 校验数据集
    *
    * @param adsLogDStream 过滤后的数据集
    */
  def checkDateTOBlackList(adsLogDStream: DStream[AdsLog]) = {
    //1.得到单个数据集每个人点击每个广告的次数
    val dateUserAdToCount: DStream[((String, String, String), Long)] = adsLogDStream.map(adsLog => {

      //获取年月日
      val date: String = sdf.format(new Date(adsLog.timestamp))
      ((date, adsLog.userid, adsLog.adid), 1L)

    }).reduceByKey(_ + _)

    //将点击次数数据结合Redis中现存色数据统计总数（20+5）
    dateUserAdToCount.foreachRDD(
      rdd => {
        rdd.foreachPartition(items => {
          //获取redis客户端
          val jedis: Jedis = RedisUtil.getJedisClient

          items.foreach {
            case (((date, userid, adid), count)) => {
              //拼接RedisKey
              val redisKey = s"date_user_ad_$date"
              //拼接HasKey
              val hasKey = s"$userid:$adid"
              //将数据进行汇总
              jedis.hincrBy(redisKey, hasKey, count)
              if (jedis.hget(redisKey, hasKey).toLong >= 100L) {
                //将满足条件的用户加入黑名单
                jedis.sadd(blackList, userid)
              }
            }
          }
          //关闭redis
          jedis.close()
        })
      }
    )


    //校验点击次数超过100，则将该用户加入黑名单

  }

  def filterDataByBlackList(sc: SparkContext, adsLogDStream: DStream[AdsLog]) = {
    adsLogDStream.transform(rdd => {
      //      rdd.foreachPartition(items=>{
      //        //获取redis连接（操作redis连接太频繁）
      //        val jedis: Jedis = RedisUtil.getJedisClient
      //        //货期黑名单
      //        items.filter(x=>(jedis.sismember(blackList,x.userid)))
      //        //关闭连接
      //        jedis.close()
      //      })
      //获取redis连接
      val jedis: Jedis = RedisUtil.getJedisClient
      //获取黑名单
      val userIds: util.Set[String] = jedis.smembers(blackList)
      //关闭连接
      jedis.close()
      //将获取的黑名单进行广播
      val userIdsBC: Broadcast[util.Set[String]] = sc.broadcast(userIds)
      //过滤广播中的黑名单中的用户id
      rdd.filter(adsLog => !userIdsBC.value.contains(adsLog.userid))
    })
  }

}
