package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * <h1>WordCountScala</h1>
 * <p>
 * This is a Scala implementation of WordCount program using Spark.
 */
object WordCountScala {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("wordcount")
    // 资源调度的 Master
    conf.setMaster("local") // 单机本地运行

    val sc = new SparkContext(conf)
    // 单词统计
    // DATASET
    // fileRDD : 数据集
    val fileRDD: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\testdata.txt")
    // hello world
    // flatMap() : 扁平化处理, 进来一个元素, 扁平化成多个元素
    fileRDD.flatMap(_.split(" ")
    ).map((_, 1)
    ).reduceByKey(_ + _).foreach(println)


    //    val fileRDD: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\testdata.txt")
    //    // hello world
    //    // flatMap() : 扁平化处理, 进来一个元素, 扁平化成多个元素
    //    val words: RDD[String] = fileRDD.flatMap((x: String) => {
    //      x.split(" ")
    //    })
    //    // hello
    //    // world
    //    // 单元素的 words 转成一个 tuple 键值对的元素
    //    val pairword: RDD[(String, Int)] = words.map((x: String) => {
    //      new Tuple2(x, 1)
    //    })
    //    // (hello,1)
    //    // (hello,1)
    //    // (world,1)
    //    // key 相同的元素进行 reduceByKey() 聚合
    //    val res: RDD[(String, Int)] = pairword.reduceByKey((x: Int, y: Int) => {
    //      x + y
    //    })
    //    // x:oldvalue, y:value
    //    // (hello,2)
    //    // (world,1)
    //    // 打印结果, 执行算子
    //    // 以上代码不会发生计算, 有一种描述是 RDD 是惰性执行的,
    //    // 也就是它并不会去真正的执行, 什么是执行 ?
    //    // 它要遇到个 x 算子, 就是执行算子, 在遇到 foreach() 并且要打印其中的值时,
    //    // 只有遇到它有执行的, 最终要拿着数据集干点什么事的时候, 才会真正发生计算,
    //    // 如果不写这行, 以上代码根本不会执行.
    //    res.foreach(println)
  }

}
