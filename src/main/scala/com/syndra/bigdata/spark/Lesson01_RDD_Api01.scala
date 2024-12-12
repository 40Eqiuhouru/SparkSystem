package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD API<br>
 * <p>DataSet-Oriented Operations :
 * <ol>
 *   <li>A Single Element : union(), cartesion(). no function compute.</li>
 *   <li>KV Element : cogroup(), join(). no function compute.</li>
 *   <li>Sort.</li>
 *   <li>Aggregate compute : reduceByKey(), combinerByKey(). Have function.</li>
 *   <li>Non-aggregates with functions : map(), flatMap().</li>
 * </ol>
 * <p>CoGroup
 * <p>combinerByKey()
 */
object Lesson01_RDD_Api01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    // 在内存中产生数据集
    val dataRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))

    //    dataRDD.map()
    //    dataRDD.flatMap()
    //    dataRDD.filter((x: Int) => {
    //      x > 3
    //    })
    // 精简写法
    // 打印数据集中大于 3 的元素
    val filterRDD: RDD[Int] = dataRDD.filter(_ > 3)
    val res01: Array[Int] = filterRDD.collect()
    res01.foreach(println)

    println("--------------------")

    // 数据集元素去重
    val res: RDD[Int] = dataRDD.map((_, 1)).reduceByKey(_ + _).map(_._1)
    res.foreach(println)

    // Api 完全去重
    val resx: RDD[Int] = dataRDD.distinct()
    resx.foreach(println)

    println("--------------------")

    // 面向数据集开发  面向数据集的 API  1.基础 API 2.复合 API
    // RDD(HadoopRDD, MapPartitionsRDD, ShuffledRDD...)
    // map, flatMap, filter
    // distinct
    // reduceByKey : 复合 -> combineByKey()

    // 面向数据集 : 交并差 关联 笛卡尔积

    // 面向数据集 : 元素 -> 单元素, KV 元素 -> 机构化, 并非结构化

    // Spark 很人性, 面向数据集提供了不同的方法的封装, 且方法已经经过经验, 常识, 推算出自己的实现的方式
    // 人不需要干预(会有一个算子)
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(List(3, 4, 5, 6, 7))

    // 差集 : 提供了一个方法, 有方向的
    //    val subtract: RDD[Int] = rdd1.subtract(rdd2)
    //    subtract.foreach(println)

    // 交集
    //    val intersection: RDD[Int] = rdd1.intersection(rdd2)
    //    intersection.foreach(println)

    // 笛卡尔积
    // rdd1 的每一个元素会和 rdd2 的每一个元素 拼接一次
    // 如果数据不需要区分每一条记录归属于哪个分区, 间接的, 这样的数据不需要 partitioner, 不需要 shuffle
    // 因为 shuffle 的语义 : 洗牌 -> 面向每一条记录计算他的分区号
    // 如果有行为, 不需要区分记录, 本地 IO 拉取数据, 那么这种直接 IO 一定比先 partitioner -> 计算 -> shuffle 落文件, 最后再 IO 拉取速度快
    //    val cartesian: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    //    cartesian.foreach(println)

    //    println(rdd1.partitions.size)
    //    println(rdd2.partitions.size)
    // 思考为什么没有产生 Shuffle ?
    // 两个独立的数据集最终变成了一个数据集,
    // 这个数据有没有发生移动 ?
    // union() 不会传递数据集怎么加工, 每个元素应该怎么变
    // 只是抽象出有一个映射关系
    // 应用场景
    // 公司数据的加工, 可能有 50 张表, 这 50 张表前面可能每张表就是一个案例
    // 有 50 个案例, 各自调用不同的计算, 最终数据都长的一样了, 未来可能都使用一个过滤逻辑
    // 这样就可以使用 union() 合并, 然后点一个 filter() 就可以了
    //    val unitRDD: RDD[Int] = rdd1.union(rdd2)
    //    println(unitRDD.partitions.size)
    //    unitRDD.foreach(println)

    // 并集
    val kv1: RDD[(String, Int)] = sc.parallelize(List(
      ("Syndra", 11),
      ("Syndra", 12),
      ("WangZX", 13),
      ("AiSiDS", 14)
    ))
    val kv2: RDD[(String, Int)] = sc.parallelize(List(
      ("Syndra", 21),
      ("Syndra", 22),
      ("WangZX", 23),
      ("HanY", 28)
    ))

    val cogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = kv1.cogroup(kv2)
    cogroup.foreach(println)

    //    val join: RDD[(String, (Int, Int))] = kv1.join(kv2)
    //    join.foreach(println)
    //
    //    println("--------------------")
    //
    //    // 左关联 : 在 join 基础上, 过滤了左边有的, 右边没有的
    //    val left: RDD[(String, (Int, Option[Int]))] = kv1.leftOuterJoin(kv2)
    //    left.foreach(println)
    //
    //    println("--------------------")
    //
    //    // 右关联 : 在 join 基础上, 过滤了右边有的, 左边没有的
    //    val right: RDD[(String, (Option[Int], Int))] = kv1.rightOuterJoin(kv2)
    //    right.foreach(println)
    //
    //    println("--------------------")
    //
    //    // 全关联 : 在 join 基础上, 过滤了左边右边都有的
    //    val full: RDD[(String, (Option[Int], Option[Int]))] = kv1.fullOuterJoin(kv2)
    //    full.foreach(println)
    //
    //    println("--------------------")

    // 阻塞等待, 方便源码分析
    while (true) {
    }
  }
}
