package com.syndra.bigdata.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * DStream API
 */
object Lesson03_DStream_API {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TEST_DStream_API").setMaster("local[8]")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    //    sc.setCheckpointDir("D:\\ideaProject\\bigdata\\bigdata-spark\\status")
    /*
    * Spark Streaming 100ms batch 1ms
    * Low Level API
    * */
    val ssc: StreamingContext = new StreamingContext(sc, Duration(1000)) // 最小粒度 约等于 : win大小为 1000, slide 滑动距离也是 1000

    // 有状态计算
    /*
    * 状态 ← 历史数据
    * join, 关联 历史的计算要存下来, 当前的计算最后还要合并到历史数据中
    * 持久化下来, 历史的数据状态
    * */

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)
    val mapData: DStream[(String, Int)] = data.map(_.split(" ")).map(x => (x(0), 1))

    mapData.mapWithState(StateSpec.function(
      (k: String, nv: Option[Int], ov: State[Int]) => {
        println(s"=====K: $k, NV: ${nv.getOrElse()}, OV: ${ov.getOption().getOrElse(0)}=====")
        // K, V
        (k, nv.getOrElse(0) + ov.getOption.getOrElse(0))
      }
    ))

    /*
    //    mapData.reduceByKey(_ + _)
    val res: DStream[(String, Int)] = mapData.updateStateByKey((nv: Seq[Int], ov: Option[Int]) => {
      // 每个批次的 Job 中, 对着 nv 求和
      val cnt: Int = nv.count(_ > 0)
      val oldVal: Int = ov.getOrElse(0)
      Some(cnt + oldVal)
    })
    res.print()*/

    /*
    val rdd: RDD[Int] = sc.parallelize(1 to 10)

    while (true) {
      rdd.map(x => {
        println("Fucking!!!")
        x
      })
      Thread.sleep(1000)
    }*/

    /*
    * 1.需求 : 将计算延缓
    * 2.一个数据源, 要保证 1m 级的数据频率和 5m 级的输出频率
    * 3.而且, 是在数据输出的时候计算输出时间点的历史数据
    *
    * 数据源是 1s 中一个 hello 2个 hi
    * */

    /*
    // 数据源的粗粒度 : 1s 来自于 StreamContext
    val resource: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8889)

    //    val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x => (x(0), x(1).toInt))
    val format: DStream[(String, Int)] = resource.map(_.split(" ")).map(x => (x(0), 1))*/

    /* =============== DStream 转换到 RDD 级别的作用域  =============== */
    /*
    * 作用域的三个级别
    * Application : 应用级别, 整个应用的生命周期
    * Job : 作业级别, 一个作业的生命周期
    * RDD : Task : 任务级别, 一个任务的生命周期
    *
    * RDD 是一个单向链表
    * DStream 也是一个单向链表
    * 如果把最后一个 DStream 给 SCC, 那么 SCC 可以启动一个独立线程无 while (true) {最后一个 DStream 遍历}
    * */

    // 广播变量
    /*
    format.print()
    //    val bc: Broadcast[List[Int]] = sc.broadcast((1 to 5).toList)
    var bc: Broadcast[List[Int]] = null
    // 怎么能令 jobNum 的值随着 Job 的提交执行, 递增
    var jobNum = 0
    //    val res: DStream[(String, Int)] = format.filter(x => {
    //      bc.value.contains(x._2)
    //    })
    println("Son of a bitch Syndra") // Application 级别
    val res: DStream[(String, Int)] = format.transform(
      rdd => {
        // 每 Job 级别递增, 是在 ssc 的另一个 while(true) 线程中, Driver 端执行的
        jobNum += 1
        println(s"jobNum : $jobNum")
        if (jobNum <= 1) {
          bc = sc.broadcast((1 to 5).toList)
        } else {
          bc = sc.broadcast((6 to 15).toList)
        }
        // 无论多少次 Job 的运行都是相同的 bc, 只有 rdd 接受的函数, 才是 Executor 端的 才是 Task 端的
        rdd.filter(x => {
          bc.value.contains(x._2)
        })
      }
    )
    res.print()
    //    val res: DStream[(String, Int)] = format.transform( // 每 Job 调用一次
    //      rdd => {
    //        // 函数每 Job 级别
    //        println("Fucking Syndra!!!") // Job 级别
    //        rdd.map(x => {
    //          println("You go sell it, Syndra makes money to support me") // Task 级别
    //          x
    //        })
    //      }
    //    )
    //    res.print()*/

    /*
    * 转换到 RDD 的操作
    * 有两种途径
    * 重点是作用域
    * */

    /*
    // 2.foreachRDD : 直接对 RDD 进行操作, 末端处理
    format.foreachRDD( // StreamingContext 有一个独立的线程执行 while(true), 在主线程的代码是放到执行线程去执行
      rdd => {
        rdd.foreach(print)
        // x...to Redis
        // to MySQL
        // call WebService
      }
    )*/

    /*
    // 1.transform : 先转换成 RDD, 然后再进行操作
    val res: DStream[(String, Int)] = format.transform( // 硬性要求 : 返回值是 RDD
      rdd => {
        rdd.foreach(println) // 产生 Job
        val rddRes: RDD[(String, Int)] = rdd.map(x => (x._1, x._2 * 10)) // 只是在转换
        rddRes
      }
    )
    res.print()*/

    /*
    * hello 1
    * hi 1
    * hi 1
    * hello 2
    * hi 2
    * hi 2
    * */

    /* =============== window API  =============== */
    // 每秒中看到历史 5s 的统计
    /*
    val reduce: DStream[(String, Int)] = format.reduceByKey(_ + _) // 窗口是 1000, Slide 是 1000
    val res: DStream[(String, Int)] = reduce.window(Duration(5000))

    val win: DStream[(String, Int)] = format.window(Duration(5000)) // 先调整量
    val res1: DStream[(String, Int)] = win.reduceByKey(_ + _) // 在基于上一步的量上整体发生计算

    //    val res: DStream[(String, Int)] = format.reduceByKeyAndWindow(_ + _, Duration(5000))
    // 调优 :
    val res: DStream[(String, Int)] = format.reduceByKeyAndWindow(
      // 计算新进入的 batch 的数据
      (ov: Int, nv: Int) => {
        // 所以 hello 并不会调这个函数, 只有 hi 才会
        // 因为 hi 有两个所以会调起这个函数
        println("被调起...")
        ov + nv
      },
      // 挤出去的 batch 的数据
      (ov: Int, oov: Int) => {
        ov - oov
      },
      Duration(5000), Duration(1000))
    res.print()*/

    /*
    val res: DStream[(String, Int)] = format.window(Duration(5000)).reduceByKey(_ + _)
    res.print()*/

    // 每秒统计
    /*
    val res: DStream[(String, Int)] = format.reduceByKey(_ + _)
    res.print()*/

    /*
    val res1s1Batch: DStream[(String, Int)] = format.reduceByKey(_ + _)
    res1s1Batch.mapPartitions(iter => {
      /* 打印频率 : 1s 打印 1次 */
      println("1s");
      iter
    }).print()

    // 5s 窗口
    val newDS: DStream[(String, Int)] = format.window(Duration(5000), Duration(5000))

    // 5个 batch
    val res5s5Batch: DStream[(String, Int)] = newDS.reduceByKey(_ + _)
    res5s5Batch.mapPartitions(iter => {
      /* 打印频率 : 5s 打印 1次...1s 打印 1次 */
      println("5s");
      iter
    }).print()*/

    ssc.start()
    ssc.awaitTermination()
  }
}
