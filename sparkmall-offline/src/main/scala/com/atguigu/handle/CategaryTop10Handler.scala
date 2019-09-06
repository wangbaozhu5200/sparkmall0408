package com.atguigu.handle

import com.alibaba.fastjson.JSONObject
import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

object CategaryTop10Handler {
  def rederhivesession(userVisitActionRDD: RDD[UserVisitAction], categroyTop10: List[(String, mutable.HashMap[String, Long])]) = {
    //获取前十的品类id
    val categorytop10: List[String] = categroyTop10.map(_._1)
    val categoryRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(UserVisitAction => categorytop10.contains(UserVisitAction.click_category_id.toString))

    //转换格式
    val categorytoOne: RDD[((String, String), Long)] = categoryRDD.map(userVisitAction => ((userVisitAction.click_category_id.toString, userVisitAction.session_id), 1L))

    //求和
    val categorytoSum: RDD[((String, String), Long)] = categorytoOne.reduceByKey(_ + _)

    //转换维度
    val categorytoSessionAndSum: RDD[(String, (String, Long))] = categorytoSum.map { case ((category, session), sum) => (category, (session, sum)) }
    //分组
    val categoryToSessionAndSumItr: RDD[(String, Iterable[(String, Long)])] = categorytoSessionAndSum.groupByKey()
    //排名取前十
    val sortcategoryToSessionSum: RDD[(String, List[(String, Long)])] = categoryToSessionAndSumItr.mapValues(itr => (itr.toList.sortWith(_._2 > _._2)).take(10))
    //压平操作
    val result: RDD[(String, String, Long)] = sortcategoryToSessionSum.flatMap {
      case (category, itr) => {
        itr.map { case (session, sum) => (category, session, sum) }
      }
    }
    //返回
    result
  }


  /**
    * 读取Hive数据
    *
    * @param sparksess SparkSession
    * @param jsonObj   过滤条件
    * @return
    */
  def readHiveData(sparksess: SparkSession, jsonObj: JSONObject): _root_.org.apache.spark.rdd.RDD[_root_.com.atguigu.datamode.UserVisitAction] = {
    //0.隐式转换
    import sparksess.implicits._

    //1.准备过滤条件
    val startDate: String = jsonObj.getString("startDate")
    val endDate: String = jsonObj.getString("endDate")
    val startAge: String = jsonObj.getString("startAge")
    val endAge: String = jsonObj.getString("endAge")
    //2.封装SQL语句
    val sql = new StringBuilder("select ac.* from user_visit_action ac join user_info u on ac.user_id=u.user_id where 1=1")

    //3.拼接过滤条件
    if (startDate != null) {
      sql.append(s" and date>='$startDate'")
    }
    if (endDate != null) {
      sql.append(s" and date<='$endDate'")
    }
    if (startAge != null) {
      sql.append(s" and age>=$startAge")
    }
    if (endAge != null) {
      sql.append(s" and age<=$endAge")
    }
    //打印sql
    println(sql.toString())

    //读取数据
    val df: DataFrame = sparksess.sql(sql.toString())

    //转化为RDD[UserVisitAction]
    val rdd: RDD[UserVisitAction] = df.as[UserVisitAction].rdd

    //返回结果
    rdd

  }

}
