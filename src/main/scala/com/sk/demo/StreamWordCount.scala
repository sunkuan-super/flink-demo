package com.sk.demo

import org.apache.flink.streaming.api.scala._


object StreamWordCount {
  def main(args: Array[String]): Unit = {

    // 创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收一个socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 9999)
    // 进行转化处理统计
    val resultDataStream:DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    env.execute("stream word count")
  }
}
