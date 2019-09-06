package com.atguigu

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

object DirectApiHandle {
  //基于Direct消费 手动维护
//  def main(args: Array[String]): Unit = {
//    def getStreamingContext(ck: String): StreamingContext = {
//      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiveAPI")
//      val sc = new SparkContext(conf)
//
//      //获取ssc
//      val ssc = new StreamingContext(sc, Seconds(3))
//
//      ssc.checkpoint("./ck")
//      val kafkapara = Map(
//        "bootstrap.servers" -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
//        "zookeeper.connect" -> "hadoop101:2181,hadoop102:2181,hadoop103:2181",
//        "group.id" -> "bigitdata02"
//      )
//
//      // getFromOffset(jdbc)
//
//      //基于Receiver消费的kafka数据
//      //    val kafkaDSstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, topics, StorageLevel.MEMORY_ONLY)
//      //基于Direct消费的kafka数据
//      //      val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkapara, Set("test"))
//
//      val offsetRanges = Array.empty[OffsetRange]
//
//      val partitionToLong: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
//      val topicAnd = TopicAndPartition("test", 1)
//
//      partitionToLong + (topicAnd -> 1000L)
//
//      def messageHandler(m: MessageAndMetadata[String, String]) = {
//        m.message()
//      }
//
//
//      val kafkaDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
//        ssc,
//        kafkapara,
//        partitionToLong,
//        messageHandler
//      )
//
//      val kafkaDstream2: DStream[String] = kafkaDstream.transform {
//        rdd => {
//          val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        }
//          rdd
//      }
//      kafkaDstream2.foreachRDD(rdd => {
//        for (elem <- offsetRanges) {
//          val offset: Long = elem.untilOffset
//          val topic: String = elem.topic
//          val partition: Int = elem.partition
//          jdbc
//        }
//      })
//
//      //启动
//      ssc.start()
//      ssc.awaitTermination()
//    }
//  }
}



