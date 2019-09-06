package com.atguigu.app


import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.Utils.JdbcUtil
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handle.SingleJumpHandle
import com.atguigu.utils.PropertiesUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SingleJumpApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SingleJumpApp").enableHiveSupport().getOrCreate()

    import spark.implicits._

    //读取Hive的数据
    val userVisitActionRDD: RDD[UserVisitAction] = spark.sql("select * from user_visit_action").as[UserVisitAction].rdd

    //.获取配置信息（获取session的范围）
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val jsonStr: String = properties.getProperty("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)
    val targetPageFlow: String = jsonObj.getString("targetPageFlow")

    val targetPageFlowArray: Array[String] = targetPageFlow.split(",")

    //去尾
    val topages: Array[String] = targetPageFlowArray.dropRight(1)
    //去头
    val frompages: Array[String] = targetPageFlowArray.drop(1)
    //拉链操作
    val targetJumPage: Array[String] = topages.zip(frompages).map { case (to, from) => s"$to-$from" }


    val singleCount: RDD[(String, Long)] = SingleJumpHandle.getSinglePagesCount(userVisitActionRDD, topages)

    val singleJumpCount: RDD[(String, Int)] = SingleJumpHandle.getSingleJumpCount(userVisitActionRDD, targetJumPage)

    //拉渠道Drivr计算跳转率
    val singleCountmap: Map[String, Long] = singleCount.collect().toMap

    val singleJumpCountArr: Array[(String, Int)] = singleJumpCount.collect()

    val result: Array[Array[Any]] = singleJumpCountArr.map {
      case (singleJump, count) => {
        val singleJumpRatio: Double = count.toDouble / singleCountmap.getOrElse(singleJump.split("-")(0), 1L)
        Array(s"Three-${System.currentTimeMillis()}",
          singleJump,
          singleJumpRatio)
      }
    }

    //写入MYSQL
    JdbcUtil.executeBatchUpdate("insert into jump_page_ratio values(?,?,?)", result)

    spark.stop()

  }
}
