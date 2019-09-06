package com.atguigu.accu

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAccu extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  private var categoryCount = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = categoryCount.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accu = new CategoryAccu
    accu.categoryCount ++= this.categoryCount
    accu
  }

  override def reset(): Unit = categoryCount.clear()

  override def add(v: String): Unit = {
    categoryCount(v) = categoryCount.getOrElse(v, 0L) + 1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //获取other的数据
    val othercaCount: mutable.HashMap[String, Long] = other.value

    //遍历赋值
    othercaCount.foreach {
      case (category, count) =>
        this.categoryCount(category) = categoryCount.getOrElse(category, 0L) + count
    }
  }

  override def value: mutable.HashMap[String, Long] = categoryCount
}
