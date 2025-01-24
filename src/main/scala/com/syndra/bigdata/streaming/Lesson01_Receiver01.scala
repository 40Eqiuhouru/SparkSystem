package com.syndra.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming Receiver
 */
object Lesson01_Receiver01 {
  def main(args: Array[String]): Unit = {
    /*
    * local[n] 2个就够了 : 1个给 reveiverjob 的 Task
    * 另一个给 beatch计算的job(只不过如果batch比较大, 期望 n > 2, 另外多出来的线程可以跑并行的 batch@job@task)
    * */
    val conf: SparkConf = new SparkConf().setAppName("Spark_Streaming").setMaster("local[9]")
    // 微批的流式计算, 时间去定义批次(while --> 时间间隔触发 Job)
    val sc = new StreamingContext(conf, Seconds(5))
    sc.sparkContext.setLogLevel("ERROR")

    // 接收数据源地址
    val dataDStream: ReceiverInputDStream[String] = sc.socketTextStream("localhost", 8889)
    //    // 相当于 hello world
    //    val flatDStream: DStream[String] = dataDStream.flatMap(_.split(" "))
    //    val resDStream: DStream[(String, Int)] = flatDStream.map((_, 1)).reduceByKey(_ + _)
    //    // 输出算子
    //    resDStream.print()

    val res: DStream[(String, String)] = dataDStream.map(_.split(" ")).map(vars => {
      Thread.sleep(20000)
      (vars(0), vars(1))}
    )
    res.print()
    // 执行算子
    sc.start()
    // 阻塞程序
    sc.awaitTermination()
  }
}
