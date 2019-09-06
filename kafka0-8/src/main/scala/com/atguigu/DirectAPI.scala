package com.atguigu

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object DirectAPI {
  //基于Direct消费 自动维护
  def main(args: Array[String]): Unit = {
    val ck = "./ck"

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate(ck, () => getStreamingContext(ck))

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

  def getStreamingContext(ck: String): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")
    val sc = new SparkContext(conf)

    //获取ssc
    val ssc = new StreamingContext(sc, Seconds(3))

    ssc.checkpoint(ck)
    val kafkapara = Map(
      "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
      "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181",
      "group.id" -> "bigitdata02"
    )
    //基于Receiver消费的kafka数据
    //    val kafkaDSstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, topics, StorageLevel.MEMORY_ONLY)

    //基于Direct消费的kafka数据
    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, Set("test"))

    kafkaDstream.map(_._2).print()

    //返回ssc
    ssc
  }
}
