package com.atguigu

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReceiveAPI {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")
    val sc = new SparkContext(conf)

    //获取ssc
    val ssc = new StreamingContext(sc,Seconds(3))

    val kafkapara = Map(
      "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181",
      "group.id" -> "bigitdata01"
    )
    val topics = Map("test"->1)
    //基于Receiver消费的kafka数据
    val kafkaDSstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, topics, StorageLevel.MEMORY_ONLY)

    //打印数据
    kafkaDSstream.map(_._1).print()
    println("**************")
    println()
    kafkaDSstream.map(_._2).print()

    //启动
    ssc.start()
    ssc.awaitTermination()

  }
}
