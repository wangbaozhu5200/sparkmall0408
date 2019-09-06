package com.atguigu.handle

import com.atguigu.datamode.UserVisitAction
import org.apache.spark.rdd.RDD

object SingleJumpHandle {
  def getSingleJumpCount(userVisitActionRDD: RDD[UserVisitAction], targetJumPage: Array[String]) = {
    //按照session进行分区
    val sessionToUserActionRDD: RDD[(String, Iterable[(String, String)])] = userVisitActionRDD.map(userVisitAction => (userVisitAction.session_id, (userVisitAction.action_time, userVisitAction.page_id.toString))).groupByKey()

    //按照时间进行排序
    val jumpPageAndOne: RDD[(String, List[(String, String)])] = sessionToUserActionRDD.mapValues(items => {
      items.toList.sortWith(_._1 < _._1)
    })

    //过滤
    val filterSessionPagesItr: RDD[(String, List[String])] = jumpPageAndOne.mapValues(items => {
      //a.去头
      val frompages: List[String] = items.map(_._2).drop(1)
      //b.去尾
      val topages: List[String] = items.map(_._2).dropRight(1)
      //拼接
      val jumpPages: List[String] = frompages.zip(topages).map {
        case (from, to) => s"$from-$to"
      }
      //过滤
      jumpPages.filter(targetJumPage.contains)
    })
    //压平操作
    val singleJumpCount: RDD[(String, Int)] = filterSessionPagesItr.flatMap(_._2).map((_, 1)).reduceByKey(_ + _)

    //返回类型
    singleJumpCount
  }

    def getSinglePagesCount(userVisitActionRDD: RDD[UserVisitAction], topages: Array[String]) = {
      //过滤出所需要的单页数据
      val filterPageRDD: RDD[UserVisitAction] = userVisitActionRDD.filter(userVisitAction => (topages.contains(userVisitAction.page_id.toString)))

      //计算每个页面的点击次数
      val singlePageCount: RDD[(String, Long)] = filterPageRDD.map(x => (x.page_id.toString, 1L)).reduceByKey(_ + _)

      //返回
      singlePageCount
    }
  }
