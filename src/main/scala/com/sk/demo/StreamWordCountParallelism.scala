package com.sk.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


object StreamWordCountParallelism {
  def main(args: Array[String]): Unit = {

    // 创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 全局设置并行度
    // env.setParallelism(4)
    // 从外部命令中提取参数，作为socket主机名和端口号
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")
    println(s"host = ${host}")
    println(s"port = ${port}")

    // 接收一个socket文本流
    // val inputDataStream: DataStream[String] = env.socketTextStream("localhost", 9999)
    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)

    // 进行转化处理统计
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1) //.setParallelism(2) // 算子也可以设置并行度

    resultDataStream.print().setParallelism(1) // 打印并行度

    env.execute("stream word count")
  }
}
