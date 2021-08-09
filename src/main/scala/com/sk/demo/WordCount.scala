package com.sk.demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    println("start...")
    // 创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 对数据进行转换处理、统计。先分词，再按照word进行分组，最后进行聚合统计
    val resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 以第一个元素作为Key进行统计
      .sum(1) //对所有数据的第二个元素求和

    resultDataSet.print()


  }

}
