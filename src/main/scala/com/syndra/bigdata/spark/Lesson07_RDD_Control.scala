package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD Control Operations
 */
object Lesson07_RDD_Control {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    //    sc.setCheckpointDir("hdfs://mycluster/checkpoint")
    sc.setCheckpointDir("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\ckp")

    // 贴源数据 RDD
    val data: RDD[Int] = sc.parallelize(1 to 10)

    // 转换加工的 RDD
    val d2rdd: RDD[(String, Int)] = data.map(e => if (e % 2 == 0) ("A", e) else ("B", e))

    // 调优点 : 只有那些重复使用的 RDD 适合调优 : 缓存结果数据
    //    d2rdd.cache()
    //    d2rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    //    d2rdd.persist(StorageLevel.MEMORY_AND_DISK)
    // 思路上会出现 bug

    d2rdd.persist(StorageLevel.MEMORY_AND_DISK)
    // 触发 Job
    d2rdd.checkpoint() // 第二个作业 Job02

    // 思路再前进一步 : 权衡 : 调优 : 可靠性和速度

    // 奇偶分组
    val group: RDD[(String, Iterable[Int])] = d2rdd.groupByKey()
    group.foreach(println) // 第一个作业 Job01

    // 奇偶统计
    val kv1: RDD[(String, Int)] = d2rdd.mapValues(e => 1)
    val reduce: RDD[(String, Int)] = kv1.reduceByKey(_ + _)
    reduce.foreach(println) // 第三个作业 Job03

    val res: RDD[(String, String)] = reduce.mapValues(e => e + "Syndra")
    res.foreach(println) // 第四个作业 Job04

//    while (true) {
//    }
  }
}
