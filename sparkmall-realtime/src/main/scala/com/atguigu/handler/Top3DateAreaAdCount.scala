package com.atguigu.handler

import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Top3DateAreaAdCount {
  /**
    * t统计每天各区域广告点击次数
    *
    * @param dateAreaCityAdToCount
    */
  def statAreaCityAdsPerDay(dateAreaCityAdToCount: DStream[((String, String, String, String), Long)]) = {
    //1.map=>((date,area,ad),count)
    val dateAreaAdToCount: DStream[((String, String, String), Long)] = dateAreaCityAdToCount.map {
      case ((date, area, city, ad), count) => ((date, area, ad), count)
    }.reduceByKey(_ + _)

    //map=>((date,area),(ad,count))并按照Key进行分组
    val dateAreaToAdCountlist: DStream[((String, String), Iterable[(String, Long)])] = dateAreaAdToCount.map {
      case ((date, area, ad), count) => ((date, area), (ad, count))
    }.groupByKey()

    //mapvalus=es=>((date,area),Iter(ad,count))
    val dateAreaToAdCountlistTop3: DStream[((String, String), List[(String, Long)])] = dateAreaToAdCountlist.mapValues(items => {
      items.toList.sortWith(_._2 > _._2).take(3)
    })

    //将钱3名点击次数的数据集变成json格式
    val dateAreaTOJson: DStream[((String, String), String)] = dateAreaToAdCountlistTop3.mapValues(items => {
      import org.json4s.JsonDSL._
      JsonMethods.compact(items)
    })

    //将Json文件写入Redis
    dateAreaTOJson.foreachRDD(rdd => {
      rdd.foreachPartition(items => {
        //获取Redis连接
        val jedis: Jedis = RedisUtil.getJedisClient

        //        //方案一:单条数据添加到Redis
        //        items.foreach {
        //          case ((date, area), jsonstr) =>
        //            val redisKey: String = s"top3_ads_per_day:$date"
        //            jedis.hset(redisKey, area, jsonstr)
        //        }

        //方案二:多条数据添加到Redis
        val dateTodateAreaJson: Map[String, Map[(String, String), String]] = items.toMap.groupBy(_._1._1)

        val dateToAreaJson: Map[String, Map[String, String]] = dateTodateAreaJson.mapValues(items => {
          items.map { case ((date, area), jsonstr) => (area, jsonstr)
          }
        })
        dateToAreaJson.foreach {
          case (date, map) =>
            val redisKey: String = s"top3_ads_per_day:$date"
            import scala.collection.JavaConversions._
            jedis.hmset(redisKey, map)
        }

        //关闭Redis连接
        jedis.close()

      })
    })

  }
}
