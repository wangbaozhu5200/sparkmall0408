package com.atguigu.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class CityRatioUDAF extends UserDefinedAggregateFunction {
  //输入数据类型
  override def inputSchema: StructType = StructType(StructField("city", StringType) :: Nil)

  //中间数据类型
  override def bufferSchema: StructType = StructType(StructField("cityCount", MapType(StringType, LongType)) :: StructField("totalcount", LongType) :: Nil)

  //输出数据类型
  override def dataType: DataType = StringType

  //函数稳定性
  override def deterministic: Boolean = true

  //初始化缓存数据类型
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[String, Long]()
    buffer(1) = 0L
  }

  //区内聚合
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //去除buffer中的数据
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    //赋值
    val city: String = input.getString(0)
    buffer(0) = cityCount + (city -> (cityCount.getOrElse(city, 0L) + 1L))
    buffer(1) = totalCount + 1L

  }

  //区间聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //去除buffer数据
    val cityCount1: Map[String, Long] = buffer1.getAs[Map[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)

    val cityCount2: Map[String, Long] = buffer2.getAs[Map[String, Long]](0)
    val totalCount2: Long = buffer2.getLong(1)

    //赋值

    buffer1(0) = cityCount1.foldLeft(cityCount2) {
      case (map, (city1, count)) => map + (city1 -> (map.getOrElse(city1, 0L) + count))
    }
    buffer1(1) = totalCount1 + totalCount2
  }

  //计算最终结果
  override def evaluate(buffer: Row): String = {
    //取值
    val cityCount: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)

    //对城市点击次数进行排序
    val sortedCityCount: List[(String, Long)] = cityCount.toList.sortWith(_._2 > _._2).take(2)
    var otherRatio = 100D

    //计算城市比率
    val cityRation: List[String] = sortedCityCount.map {
      case (city, count) =>
        val ratio: Double = Math.round(count.toDouble * 1000 / totalCount) / 10D
        otherRatio -= ratio
        s"$city:$ratio%"
    }
    //其他城市
    val reust: List[String] = cityRation :+ s"其他：${Math.round(otherRatio*10)/10D}%"
    //转换为字符串
    reust.mkString(",")
  }
}
