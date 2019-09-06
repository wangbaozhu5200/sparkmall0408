package com.atguigu.test

import com.atguigu.utils.MyKafkaUntil
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object testApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestApp")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(3))
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUntil.getKafkaStream(ssc, "ads_log")

    //打印数据
    kafkaDStream.map(_._2).print()

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
