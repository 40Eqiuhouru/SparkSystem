package com.syndra.bigdfata.Lesson08_Other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark_RDD_算子综合应用 :<br>
 * <ul>
 * <li>分组取 TopN</li>
 * </ul>
 */
object Lesson06_RDD_Over {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    println("---------- value 放大 10 倍 ----------")
    val data: RDD[String] = sc.parallelize(List(
      "hello world",
      "hello spark",
      "hello world",
      "hello Syndra",
      "hello NingHY",
      "hello hadoop",
      "hello Scala",
      "hello world"
    ))
    val words: RDD[String] = data.flatMap(_.split(" "))
    val kv: RDD[(String, Int)] = words.map((_, 1))
    val res: RDD[(String, Int)] = kv.reduceByKey(_ + _)
    // 如果想让 value 放大 10 倍, 怎么实现 ?
    // 用另外一种写法
    //    val res01: RDD[(String, Int)] = res.map(x => (x._1, x._2 * 10))
    // 推荐写法
    val res01: RDD[(String, Int)] = res.mapValues(x => x * 10)
    // 只要触发 groupByKey(), 就触发了 shuffle 操作, 所以会产生 shuffle 过程
    val res02: RDD[(String, Iterable[Int])] = res01.groupByKey()
    res02.foreach(println)

    //    // TopN, 分组取 TopN, 其中会牵扯到二次排序的概念
    //    // 2019-6-1	39
    //    // 需求 : 同月份中, 温度最高的两天
    //    val file: RDD[String] = sc.textFile("data/tqdata")
    //
    //    // 隐式转换 Ordering, 用于排序
    //    implicit var ordered: Ordering[(Int, Int)] = new Ordering[(Int, Int)] {
    //      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compare(x._2)
    //    }

    println("---------- 分组取 TopN-V1 ----------")
    //    val data: RDD[(Int, Int, Int, Int)] = file.map(line => line.split("\t")).map(arr => {
    //      val arrs: Array[String] = arr(0).split("-")
    //      // (year, month, day, wd)
    //      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    //    })

    //    // 分组取 TopN V1
    //    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    //    val res: RDD[((Int, Int), List[(Int, Int)])] = grouped.mapValues(arr => {
    //      // 关注点 : 相同年月中, 日期和温度有什么特征
    //      // 19年三条数据, 如果让你处理的话, 如果变成一个数组, 你应该关注哪些问题 ?
    //      // 1.同一月份中相同天中过滤掉那些低温只留一个最高温, 作为这一天的代表
    //      // 2.如果数据去重后, 把剩下的数据做个排序, 写成倒序就可以了
    //      // 有了 map 后, 让你进入这个数组相同的 19年6月 这个数组中所有的每天的温度
    //      // 这一堆数据要遍历放进 map
    //      val map = new mutable.HashMap[Int, Int]()
    //      arr.foreach(x => {
    //        if (map.get(x._1).getOrElse(0) < x._2) map.put(x._1, x._2)
    //      })
    //      map.toList.sorted(new Ordering[(Int, Int)] {
    //        // 用温度排序, 且是 y : x, 这样返回就是倒序
    //        override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compare(x._2)
    //      })
    //    })
    //    res.foreach(println)
    //    println("--------------------")

    // 思考 : 以上代码中, 有没有什么潜在风险或问题 ?
    // 1.groupByKey(), 比如 value 整个 6月份的数据达到了 10T, 那么它就溢出了
    // 因为最终会发现它的 value 是要拼成一个迭代器的数据集, 真正计算时, 是要占用内存的
    // groupByKey() 在工作中不建议使用, 除非 key 的 value 数量很少, 那么可以用
    // 2.map 中要开辟对象, 在这承载所有数据, 最终再把它输出, 因此内存利用率上有可能出现 OOM


    println("---------- 分组取 TopN-V2 ----------")

    // 分组取 TopN V2
    // 有个排序的过程
    // 此时其实还可以拿着这个数据集怎么去做, 也可以实现 TopN 的结果
    // 这里面还要关注数据集的特征, 06-01 重复的数据先要去重, 然后相同的 key 划分到一块, 然后再排序
    // 此前是把去重和排序放到了一个算子中去做, 有可能会产生溢出
    // 那么是否可以用什么换什么 ? 拿时间拿速度来换内存不溢出的方案
    // 取相同的年月日中的 max
    //    val reduced: RDD[((Int, Int, Int), Int)] = data.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x: Int, y: Int) => if (y > x) y else x)
    //    // t2._1._1 年, t2._1._2 月, t2._1._3 日, t2._2 值
    //    val maped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
    //    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = maped.groupByKey()
    //    // 取 Top2
    //    grouped.mapValues(arr => arr.toList.sorted.take(2)).foreach(println)


    println("---------- 分组取 TopN-V3 ----------")

    // 分组取 TopN V3
    // 还有没有其他写法 ?
    // 因为它是一个单元素四个, 然后有 sortBy(), 向其中传函数, 函数中是要从其中拿哪些是你排序的依据
    // 排序的依据可以是参考年月温度, 且温度倒序, 将数据集做全排序
    // 全排序
    //    val sorted: RDD[(Int, Int, Int, Int)] = data.sortBy(t4 => (t4._1, t4._2, t4._4), false)
    //    // 去重
    //    val reduced: RDD[((Int, Int, Int), Int)] = sorted.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x: Int, y: Int) => if (y > x) y else x)
    //    // 转 KV
    //    val maped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
    //    // 分组且因为前面已经全排序, 所以此时已经有序
    //    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = maped.groupByKey()
    //    // 取 Top2
    //    grouped.foreach(println)


    println("---------- 分组取 TopN-V4 ----------")
    //    // 分组取 TopN V4
    //    // 全排序
    //    val sorted: RDD[(Int, Int, Int, Int)] = data.sortBy(t4 => (t4._1, t4._2, t4._4), false)
    //    // 有序数据集 -> KV
    //    // 再分组
    //    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = sorted.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    //    grouped.foreach(println)


    println("---------- 分组取 TopN-V5 ----------")

    //    // 分组取 TopN V5
    //    // 通过 groupByKey() 底层的 combineByKey() 压缩
    //    val kv: RDD[((Int, Int), (Int, Int))] = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4)))
    //    val res: RDD[((Int, Int), Array[(Int, Int)])] = kv.combineByKey(
    //      // 1.第一条记录怎么放
    //      (v1: (Int, Int)) => {
    //        Array(v1, (0, 0), (0, 0))
    //      },
    //      // 第二条, 以及后续的怎么放
    //      (oldV: Array[(Int, Int)], newV: (Int, Int)) => {
    //        // 去重, 排序
    //        var flg = 0 // 0, 1, 2 新进来的元素特征 : 日期 a)相同 1) 温度大 2) 温度小  日期 b)不同
    //        for (i <- 0 until oldV.length) {
    //          // 老的日期是否等于新的日期
    //          if (oldV(i)._1 == newV._1) {
    //            // 老的温度是否小于新的温度
    //            if (oldV(i)._2 < newV._2) {
    //              flg = 1
    //              // 新的值覆盖
    //              oldV(i) = newV
    //            } else {
    //              flg = 2
    //            }
    //          }
    //        }
    //        // 新纪录放到第三个格子中
    //        if (flg == 0) {
    //          oldV(oldV.length - 1) = newV
    //        }
    //        //        oldV.sorted
    //        // 基于数据源的排序
    //        scala.util.Sorting.quickSort(oldV)
    //        oldV
    //      },
    //      // 此时已经有序
    //      (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
    //        // 这其中既包含了先前的日期温度
    //        // 去重
    //        val union: Array[(Int, Int)] = v1.union(v2)
    //        union.sorted.take(2)
    //      }
    //    )
    //    res.map(x => (x._1, x._2.toList)).foreach(println)

    while (true) {
    }
  }
}
