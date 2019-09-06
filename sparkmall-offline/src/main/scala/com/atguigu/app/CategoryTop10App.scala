package com.atguigu.app

import java.util.Properties
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.Utils.JdbcUtil
import com.atguigu.accu.CategoryAccu
import com.atguigu.datamode.UserVisitAction
import com.atguigu.handle.CategaryTop10Handler
import com.atguigu.utils.PropertiesUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object CategoryTop10App {
  def main(args: Array[String]): Unit = {
    //1.获取sparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CategoryTop10App")

    //2.创建sparkSesssion
    val sparksess: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //3.获取配置信息（获取session的范围）
    val properties: Properties = PropertiesUtil.load("conditions.properties")
    val jsonStr: String = properties.getProperty("condition.params.json")
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)

    //4.按照要求读取数据
    val userVisitActionRDD: RDD[UserVisitAction] = CategaryTop10Handler.readHiveData(sparksess, jsonObj)

    //缓存一下数据，用来需求二进行复用
    userVisitActionRDD.cache()

    //4.按照要求打印结果
    //    userVisitActionRDD.foreach(userVisitAction => println(s"${userVisitAction.user_id}--aaa"))

    //5.创建累加器对象
    val accu = new CategoryAccu

    //6.注册累加器
    sparksess.sparkContext.register(accu, "categoryCount")

    //7.统计各个品类的点击，订单，支付数据
    userVisitActionRDD.foreach(UserVisitAction => {
      //a.判断是否是点击日志
      if (UserVisitAction.click_category_id != -1) {
        accu.add(s"click_${UserVisitAction.click_category_id}")
        //b.判断是否是订单日志
      } else if (UserVisitAction.order_category_ids != null) {
        //c.遍历订单日志中的 category 的个数
        UserVisitAction.order_category_ids.split(",").foreach(category => accu.add(s"order_$category"))
        //判断是否是支付日志
      } else if (UserVisitAction.pay_category_ids != null) {
        //c.遍历订单日志中的 category 的个数
        UserVisitAction.pay_category_ids.split(",").foreach(category => accu.add(s"pay_$category"))
      }
    })

    //8.获取累加器中的数据
    val categorySum: mutable.HashMap[String, Long] = accu.value

    //9.规整数据
    //categorySum.groupBy{case (c1, c2) => c1.split("_")(1)}
    val categorTocategorSum: Map[String, mutable.HashMap[String, Long]] = categorySum.groupBy(_._1.split("_")(1))


    //10.排序
    val categroyTop10: List[(String, mutable.HashMap[String, Long])] = categorTocategorSum.toList.sortWith { case (c1, c2) => {
      if (c1._2.getOrElse(s"pay_${c1._1}", 0L) > c2._2.getOrElse(s"pay_${c2._1}", 0L)) {
        true
      } else if (c1._2.getOrElse(s"pay_${c1._1}", 0L) == c2._2.getOrElse(s"pay_${c2._1}", 0L)) {
        //c.再比较订单次数
        if (c1._2.getOrElse(s"order_${c1._1}", 0L) > c2._2.getOrElse(s"order_${c2._1}", 0L)) {
          true
        } else if (c1._2.getOrElse(s"order_${c1._1}", 0L) > c2._2.getOrElse(s"order_${c2._1}", 0L)) {
          //d.最后比较点击次数
          c1._2.getOrElse(s"click_${c1._1}", 0L) > c2._2.getOrElse(s"click_${c2._1}", 0L)
        } else false
      } else false
    }
    }.take(10)

    //需求一结果写入MYSQL
    val categroyTop10list: List[Array[Any]] = categroyTop10.map {
      case (category, categorymap) => {
        Array(s"One-${System.currentTimeMillis()}",
          category,
          categorymap.getOrElse(s"click_$category", 0L),
          categorymap.getOrElse(s"order_$category", 0L),
          categorymap.getOrElse(s"pay_$category", 0L)
        )
      }
    }
    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", categroyTop10list)

    //********************************
    //第二个需求：
    val categaryToSession: RDD[(String, String, Long)] = CategaryTop10Handler.rederhivesession(userVisitActionRDD, categroyTop10)

    //转换格式
    val categaryToSessionAny: RDD[Array[Any]] = categaryToSession.map { case (categary, session, sum) => (
      Array(s"Two-${System.currentTimeMillis()}", categary, session, sum))
    }
    val categaryToSessionList: Array[Array[Any]] = categaryToSessionAny.collect()

    JdbcUtil.executeBatchUpdate("insert into category_session_top10 values(?,?,?,?)", categaryToSessionList)



    //关闭资源
    sparksess.close()
  }
}
