package com.sk.demo

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.{Properties, Random}

/**
 * @Title: SourceTest
 * @Package: com.sk.demo
 * @Description:
 * @Author: sk
 * @Date: 2021/8/19 - 16:46
 */
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从集合中读取数据
    val dataList = List(
      SensorReading("_1", 20151111, 35.8),
      SensorReading("_2", 20152222, 36.8),
      SensorReading("_3", 20153333, 37.8),
      SensorReading("_4", 20153334, 38.8)
    )

    // 1、 从集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(dataList)
    // 2、 从集合中读取数据， 这个参数可以是任意类型
    // env.fromElements(1.0, 35 , "hello")
    // 3、 从文本中读取
    // env.readTextFile("/home/pg/aa.txt")

    // 4、从Kafka中读取数据
    // val prop = new Properties()
    // prop.setProperty("bootstrap.servers", "localhost:9092")
    // prop.setProperty("group.id", "consumer-group")
    // val streamKafka = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), prop))

    // 5、自定义Source
    val stream4 = env.addSource(new MySensorSourceFunction())
    stream4.print()

    // 执行
    env.execute("source test")
  }


}

// 自定义的SourceFunction
class MySensorSourceFunction() extends SourceFunction[SensorReading] {
  // 定义一个标识位flag,用来表示数据源是否正常运行发出数据
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义一个随机数发生器
    val rand = new Random()


    // 随机生成一组（10个）传感器的其实温度：(id, temp)
    var currTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))
    // 定义无限循环，不停地产生数据，除非被cancel
    while (running) {
      // 在上次数据基础上微调，更新温度值
      currTemp = currTemp.map(
        // 微调高斯分布
        data => (data._1, data._2 + rand.nextGaussian())
      )

      val currTime = System.currentTimeMillis()
      currTemp.foreach(data => ctx.collect(SensorReading(data._1, currTime, data._2)))

      Thread.sleep(500)
    }
  }


}

