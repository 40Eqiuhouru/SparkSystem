package com.syndra.bigdfata.Lesson08_Other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Spark RDD Oriented Partitions Operations<br>
 * {{{
 *   // 迭代器的正确使用方式
 *     val res03: RDD[String] = data.mapPartitionsWithIndex(
 *       (pIndex, pIter) => {
 *         new Iterator[String] {
 *           println(s"----$pIndex--conn--MySQL----")
 *
 *           override def hasNext = if (pIter.hasNext == false) {
 *             println(s"----$pIndex--close--MySQL----")
 *             false
 *           } else true
 *
 *           override def next(): String = {
 *             val value = pIter.next()
 *             println(s"----$pIndex--SELECT $value----")
 *             value + "SELECTED"
 *           }
 *         }
 *       }
 *     )
 *     res03.foreach(println)
 * }}}
 */
object Lesson04_RDD_Partitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("partitions")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10, 2)

    // 外关联 SQL 查询
    val res01: RDD[String] = data.map(
      (value: Int) => {
        println("----conn--MySQL----")
        println(s"----SELECT $value----")
        println("close--MySQL----")
        value + "SELECTED"
      }
    )
    res01.foreach(println)

    println("--------------------")

    val res02: RDD[String] = data.mapPartitionsWithIndex(
      (pIndex, pIter) => {
        val lb = new ListBuffer[String] // 致命的!!! 根据之前的源码发现, Spark 是一个 Pipline, 迭代器嵌套的模式
        // 数据不会在内存积压
        println(s"----$pIndex--conn--MySQL----")
        while (pIter.hasNext) {
          val value: Int = pIter.next()
          println(s"----$pIndex--SELECT $value----")
          lb.+=(value + "SELECTED")
        }
        println("close--MySQL----")
        lb.iterator
      }
    )
    res02.foreach(println)

    println("---------- Iterator Optimize -----------")

    // 如果不是一对一的处理, 如果 new 了一个迭代器, 被调用了后
    // 后续调的时候不是调一次从父级取一次, 也就是说
    // 调了 hasNext() 后, 可能拿了一个字符串, 然后后续再调的时候, 可能要把一个一个单词返回
    // 然后此时又伴随数据库的初始化, 连接的初始化, 就必须自行重写迭代器
    // 那怎么能满足一对多的输出 ?
    // 迭代器的正确使用方式
    val res03: RDD[String] = data.mapPartitionsWithIndex(
      (pIndex, pIter) => {
        //        pIter.flatMap()
        //        pIter.map()

        new Iterator[String] {
          println(s"----$pIndex--conn--MySQL----")

          override def hasNext = if (pIter.hasNext == false) {
            println(s"----$pIndex--close--MySQL----")
            false
          } else true

          override def next(): String = {
            val value = pIter.next()
            println(s"----$pIndex--SELECT $value----")
            value + "SELECTED"
          }
        }
      }
    )
    res03.foreach(println)
  }
}
