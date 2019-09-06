package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.AdsLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

object LastHourAdsHandler {
  def statLastHourAds(filterAdsLogDStream: DStream[AdsLog]) = {
    //获取时间格式
    val dateFormatter = new SimpleDateFormat("HH:mm")

    //
    val UseidTimeToOne: DStream[((String, String), Long)] = filterAdsLogDStream.map(adsinfo => {
      ((adsinfo.adid, dateFormatter.format(new Date(adsinfo.timestamp))), 1L)
    })

    val UseidToTimeCount: DStream[(String, (String, Long))] = UseidTimeToOne.reduceByKeyAndWindow((x: Long, y: Long) => x + y, Minutes(2)).map { case ((adid, ts), count) => (adid, (ts, count)) }

    //    val UseidToTimeCount: DStream[(String, (String, Long))] = UseidTimeToOne.reduceByKey(_ + _).map {
    //      case ((adid, ts), count) => (adid, (ts, count))
    //    }

    val adsIdHourMinterJson: DStream[(String, String)] = UseidToTimeCount.groupByKey().mapValues { items => {
      import org.json4s.JsonDSL._
      JsonMethods.compact(JsonMethods.render(items))
    }
    }

    //写入redis
    adsIdHourMinterJson.foreachRDD(rdd => {
      val jedis: Jedis = RedisUtil.getJedisClient
      if (jedis.exists("last:hour:ads:click")) {
        jedis.del("last:hour:ads:click")
      }
      jedis.close()

      rdd.foreachPartition(items => {
        if (items.nonEmpty) {
          val jedis: Jedis = RedisUtil.getJedisClient
          //导入将scala的map转化为Java的map的包
          import scala.collection.JavaConversions._
          jedis.hmset("last:hour:ads:click", items.toMap)
          jedis.close()
        }
      })
    })
  }
}
