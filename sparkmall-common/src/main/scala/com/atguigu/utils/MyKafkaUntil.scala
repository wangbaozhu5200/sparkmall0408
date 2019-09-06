package com.atguigu.utils

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MyKafkaUntil {
  def getKafkaStream(ssc: StreamingContext, topic: String) = {
    val properties: Properties = PropertiesUtil.load("config.properties")

    val kafkapara = Map(
      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
      "group.id" -> "bigitdata03"
    )
    //基于Direct消费的kafka数据
    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, Set(topic))
    //返回
    kafkaDstream
  }
}
