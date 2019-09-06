package com.atguigu.app

import com.atguigu.bean.AdsLog
import com.atguigu.handler.{BlackList, DateAreaCityCountHandler, LastHourAdsHandler, Top3DateAreaAdCount}
import com.atguigu.utils.MyKafkaUntil
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealTimeApp {
  def main(args: Array[String]): Unit = {
    //1.创建SarkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    //设置检查点
    ssc.sparkContext.setCheckpointDir("./dateAreaCityck")

    //读取kafka数据
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUntil.getKafkaStream(ssc, "ads_log")

    //将数据集内容转化为样例类对象
    val adsLogDStream: DStream[AdsLog] = kafkaDStream.map { case (key, value) =>
      val splits: Array[String] = value.split(" ")
      AdsLog(splits(0).toLong, splits(1), splits(2), splits(3), splits(4))
    }

    //需求五:根据黑名单过滤数据集
    val filterAdsLogDStream: DStream[AdsLog] = BlackList.filterDataByBlackList(ssc.sparkContext, adsLogDStream)

    filterAdsLogDStream.cache()
    //需求五：校验点击次数，超过100加入黑名单
    BlackList.checkDateTOBlackList(filterAdsLogDStream)

    //需求六：广告点击量的实时统计（采用保存状态的转换覆盖到redis中）
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = DateAreaCityCountHandler.getDateCitycounnt(filterAdsLogDStream)

    dateAreaCityAdToCount.cache()
    //需求六:将返回数据写入Rides
    DateAreaCityCountHandler.saveDateAreaCityAdcountToRedis(dateAreaCityAdToCount)

    //需求七：每天各地区 top3 热门广告
    Top3DateAreaAdCount.statAreaCityAdsPerDay(dateAreaCityAdToCount)
    //需求八:统计各广告最近 1 小时内的点击量

    LastHourAdsHandler.statLastHourAds(filterAdsLogDStream)

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
