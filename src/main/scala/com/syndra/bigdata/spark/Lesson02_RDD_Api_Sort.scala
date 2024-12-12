package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD API Sort Operations
 */
object Lesson02_RDD_Api_Sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // PV, UV
    // 需求 : 根据数据计算各网站的 PV, UV 同时只显示 top5
    // 解题 : 要按 PV 值, 或 UV 值排序, 取前 5 名
    val file: RDD[String] = sc.textFile("data/pvuvdata", 5)
    // PV :
    // 187.144.73.116	浙江	2018-11-12	1542011090255	3079709729743411785	www.jd.com	Comment
    println("----------- PV -----------")

    // line 代表一行数据, split() 切割成一个数组, 标 1 键值对输出
    val pair: RDD[(String, Int)] = file.map(line => (line.split("\t")(5), 1))

    val reduce: RDD[(String, Int)] = pair.reduceByKey(_ + _)
    // 翻转 KV
    val map: RDD[(Int, String)] = reduce.map(_.swap)
    val sorted: RDD[(Int, String)] = map.sortByKey(false)
    val res: RDD[(String, Int)] = sorted.map(_.swap)
    val pv: Array[(String, Int)] = res.take(5)
    pv.foreach(println)

    println("----------- UV -----------")

    // 187.144.73.116	浙江	2018-11-12	1542011090255	3079709729743411785	www.jd.com	Comment

    val keys: RDD[(String, String)] = file.map(
      line => {
        val strs: Array[String] = line.split("\t")
        (strs(5), strs(0))
      }
    )
    val key: RDD[(String, String)] = keys.distinct()
    val pairUV: RDD[(String, Int)] = key.map(k => (k._1, 1))
    val uvReduce: RDD[(String, Int)] = pairUV.reduceByKey(_ + _)
    val unSorted: RDD[(String, Int)] = uvReduce.sortBy(_._2, false)
    val uv: Array[(String, Int)] = unSorted.take(5)
    uv.foreach(println)

    while (true) {
    }
  }
}
