package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date
import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.immutable

object DateAreaCityCountHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getDateCitycounnt(filterAdsLogDStream: DStream[AdsLog]) = {
    //规整数据,转换格式
    val dateAreaCityAdToOne: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(adsLog => {
      val date: String = sdf.format(new Date(adsLog.timestamp))
      ((date, adsLog.area, adsLog.city, adsLog.adid), 1L)
    })
    //
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = dateAreaCityAdToOne.updateStateByKey((seq: Seq[Long], status: Option[Long]) => {
      //同合计当前批次的总数
      val sum: Long = seq.sum
      //将当前批次与之前状态中的数据结合
      Some(sum + status.getOrElse(0L))
    })
    //返回
    dateAreaCityAdToCount
  }

  /**
    * 统计后的单日大区城市广告总数写入redis
    *
    * @param dateAreaCityAdToCount 单日大区城市广告总数
    */
  def saveDateAreaCityAdcountToRedis(dateAreaCityAdToCount: DStream[((String, String, String, String), Long)]) = {
    dateAreaCityAdToCount.foreachRDD(rdd => {
      rdd.foreachPartition(items => {
        val jedis: Jedis = RedisUtil.getJedisClient

        //方案一：单条数据插入
        //        items.foreach{
        //          case ((date,area,city,adid),count)=>
        //            val redisKey: String = s"date_area_city_ad_$date"
        //            val hashKey = s"$area:$city:$adid"
        //            jedis.hset(redisKey,hashKey,count.toString)
        //        }

        //将 items 转化程map在进行groupBy
        // val stringToStringToTuple: Map[String, Map[String, (String, String, String, Long)]] = items.map { case ((date, area, city, adid), count) => (date, (area, city, adid, count)) }.toMap.groupBy(_._1)
        //        stringToStringToTuple

        val stringToStringToLong: Map[String, Map[String, String]] = items.toMap.groupBy(_._1._1).map {
          case (date, items) =>
            val stringToLong: Map[String, String] = items.map {
              case ((date1, area, city, ad), count) => (s"$area:$city:$ad", count.toString)
            }
            (date, stringToLong)
        }

        stringToStringToLong.foreach {
          case (date, map) => {
            val redisKey = s"date_area_city_ad_$date"
            //导入隐式转换
            import scala.collection.JavaConversions._
            jedis.hmset(redisKey, map)
          }
        }
        jedis.close()
      })
    })
  }
}
