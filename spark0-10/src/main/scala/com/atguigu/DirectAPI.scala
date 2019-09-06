package com.atguigu

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DirectAPI {
  //基于Direct消费的0.10版本 的kafka 自动消费
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")
    val sc = new SparkContext(conf)

    //获取ssc
    val ssc = new StreamingContext(sc, Seconds(3))

    val kafkapara = Map(
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181",
      "group.id" -> "bigitdata02",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"

    )

    //基于Receiver消费的kafka数据
    //    val kafkaDSstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, topics, StorageLevel.MEMORY_ONLY)

    //基于Direct消费的kafka数据
    // val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, Set("test"))

    val kafkaDStrean: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array("test"),
        kafkapara)
    )

    kafkaDStrean.map(recode => recode.value()).print()

    //启动
    ssc.start()
    ssc.awaitTermination()

  }
}
