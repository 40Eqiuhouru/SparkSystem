package com.syndra.bigdfata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 闭包
 */
object Lesson08_Other {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    var n = 0
    // 累加器
    val testAc: LongAccumulator = sc.longAccumulator("testAc")
    val data: RDD[Int] = sc.parallelize(1 to 10, 2)

    val count: Long = data.map(x => {
      if (x % 2 == 0) testAc.add(1) else testAc.add(100)
      n += 1
      println(s"Executor:n $n")
      x
    }).count()

    println(s"count : $count")
    println(s"Driver:n: $n")
    println(s"Driver:testAc: ${testAc}")
    println(s"Driver:testAc.avg: ${testAc.avg}")
    println(s"Driver:testAc.sum: ${testAc.sum}")
    println(s"Driver:testAc.count: ${testAc.count}")

    //    val data: RDD[String] = sc.parallelize(List(
    //      "hello Syndra",
    //      "hello Spark",
    //      "hi Syndra",
    //      "hello WangZX",
    //      "hello Syndra",
    //      "hello Hadoop"
    //    ))

    //    val data1: RDD[String] = data.flatMap(_.split(" "))
    //
    //    // 推送出去到 executor 执行, 然后结果回收回 driver端
    //    val list: Array[String] = data1.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).keys.take(2)
    //
    //
    //    //    val list = List("hello", "Syndra") // 有的时候是未知的 ?
    //
    //    val blist: Broadcast[Array[String]] = sc.broadcast(list) // 第一次见到 broadcast 是什么时候
    //    val res: RDD[String] = data1.filter(x => blist.value.contains(x)) // 闭包(发生在 driver, 执行发送到 executor) ← 想闭包进函数, 必须实现了序列化
    //    res.foreach(println)
  }
}
