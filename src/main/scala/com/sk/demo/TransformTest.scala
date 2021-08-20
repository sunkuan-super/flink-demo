package com.sk.demo

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
 * @Title: TransformTest
 * @Package: com.sk.demo
 * @Description:
 * @Author: sk
 * @Date: 2021/8/19 - 20:39
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream.map(data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val aggStream = dataStream
      .keyBy("id")
      .min("temperature")

    val aggStream2 = dataStream
      .keyBy("id")
      .minBy("temperature")

    aggStream2.print()

    env.execute()
  }
}
