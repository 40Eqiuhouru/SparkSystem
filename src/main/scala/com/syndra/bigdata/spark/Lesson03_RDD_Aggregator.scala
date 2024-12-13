package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD Aggregator Compute
 */
object Lesson03_RDD_Aggregator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[(String, Int)] = sc.parallelize(List(
      ("Syndra", 123),
      ("Syndra", 456),
      ("Syndra", 789),
      ("NingHY", 321),
      ("NingHY", 654),
      ("NingHY", 987),
      ("WangZX", 135),
      ("WangZX", 246),
      ("WangZX", 357)
    ))

    // 当有 key 的情况下, 相同的 value -> 一组
    // 只有键值对的数据集上才有 groupByKey()
    // 行 -> 列
    val group: RDD[(String, Iterable[Int])] = data.groupByKey()
    group.foreach(println)

    println("--------------------")

    // 行列转换
    // 列 -> 行 第一种方式
    // map() 是一进一出, flatMap() 是一进多出
    // flatMap(e => : (String, Iterable[Int]) => Unit)
    val res01: RDD[(String, Int)] = group.flatMap(e => e._2.map(x => (e._1, x)).iterator)
    res01.foreach(println)

    println("--------------------")

    // flatMapValues(f : Iterable[Int] => Unit) : 前面是扁平化, 后面是只会把 value 传到函数中
    // 列 -> 行 第二种方式
    group.flatMapValues(e => e.iterator).foreach(println)
    println("--------------------")

    // 取出每组中排序后的前 2 的数据
    group.mapValues(e => e.toList.sorted.take(2)).foreach(println)
    println("--------------------")

    group.flatMapValues(e => e.toList.sorted.take(2).iterator).foreach(println)
    println("--------------------")

    // 在行列转换时, 数据的体积会发生变化吗 ?
    // 数据的体积其实没有发生太大的变化, 因为 value 并没有约少, 更想表示的是
    // 像 group 这类操作, 尤其是 groupByKey() 这种操作, 每人有三行 -> 每人一行, 但它的列数会变多, 数据其实没有相应减少的
    // 后续调优时, 再进行相应进阶

    println("---------- sum, count, max, min, avg ----------")

    val sum: RDD[(String, Int)] = data.reduceByKey(_ + _)
    val max: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov > nv) ov else nv)
    val min: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov < nv) ov else nv)
    // 曾经的数据集, value 已经没有意义了, 因为要做 count
    // 要把它变成 ("Syndra", 1) 且 key 不变
    // 而且每条记录进来还是只输出一条记录一对一的输出
    val count: RDD[(String, Int)] = data.mapValues(e => 1).reduceByKey(_ + _)
    // 计算平均值
    // 用 sum / count, 这是语义上的, 怎么实现 ?
    // sum 要和 count 的值两个值相遇到内存里然后才能进到一个函数中计算
    // 相遇要写一个独立的函数实现
    // 那么现在是 sum 是一个独立的数据集, count 是一个独立的数据集, 两个独立的数据集怎么相遇呢 ?
    // 恰巧它们具有相同的 key, 这其实就是一个关联操作, 但是用 union() 还是 join() ?
    // union() 是把一个数据集垂直拼进去, join() 是按照相同的 key 把不同的字段拼成一行
    val tmp: RDD[(String, (Int, Int))] = sum.join(count)
    val avg: RDD[(String, Int)] = tmp.mapValues(e => e._1 / e._2)

    println("---------- sum ----------")
    sum.foreach(println)

    println("---------- max ----------")
    max.foreach(println)

    println("---------- min ----------")
    min.foreach(println)

    println("---------- count ----------")
    count.foreach(println)

    println("---------- avg ----------")
    avg.foreach(println)

    // 其实分布式计算时都有这么一个调优的过程
    // 如果能在前面的 map 端把数据量压小的话, 后续 shuffle 时 IO 量也会变小
    // Spark 最重要的算子就是 combineByKey(), 面试也是聊这个
    // 一次计算完成求 avg, 缺点是比较繁琐
    val tmpx: RDD[(String, (Int, Int))] = data.combineByKey(
      //      createCombiner: V => C, 第一条记录的 value 怎么放入 hashmap
      (value: Int) => (value, 1),
      //      mergeValue: (C, V) => C, 如果有第二条记录, 第二条以及以后的它们的 value 怎么放到 hashmap 中
      (oldValue: (Int, Int), newValue: Int) => (oldValue._1 + newValue, oldValue._2 + 1),
      //      mergeCombiners: (C, C) => C 合并溢写结果的函数
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)
    )
    tmpx.mapValues(e => e._1 / e._2).foreach(println)

    while (true) {
    }
  }
}
