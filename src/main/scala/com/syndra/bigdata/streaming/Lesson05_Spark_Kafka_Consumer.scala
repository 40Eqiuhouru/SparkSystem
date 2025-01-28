package com.syndra.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Duration, StreamingContext}

import java.util

/**
 * 在Kafka数据源上做Streaming计算
 */
object Lesson05_Spark_Kafka_Consumer {
  def main(args: Array[String]): Unit = {
    // 写一个Spark-StreamingOnKafka
    val conf: SparkConf = new SparkConf().setMaster("local[10]").setAppName("StreamingOnKafka")
    conf.set("spark.streaming.backpressure.enabled", "true")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "1") // 运行时状态
    conf.set("spark.streaming.backpressure.initialRate", "5") // 起步状态
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc: StreamingContext = new StreamingContext(conf, Duration(1000))
    ssc.sparkContext.setLogLevel("ERROR")

    // 如何得到Kafka的DStream?
    val map: Map[String, Object] = Map[String, Object](
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop04:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer]),
      (ConsumerConfig.GROUP_ID_CONFIG, "newT2"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE")
      //      (ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    )

    val kafka: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List("test"), map)
    )

    // 1.offset怎么来?
    // 2.对着kafka提交offset的API哪里来?
    // 罪魁祸首就是第一个通过KafkaUtils创建的DStream, 它自己提供的提交API, 它内部包含的RDD提供了offset
    val dStream: DStream[(String, (String, String, Int, Long))] = kafka.map(
      record => {
        val t: String = record.topic()
        val p: Int = record.partition()
        val o: Long = record.offset()
        val k: String = record.key()
        val v: String = record.value()
        (k, (v, t, p, o))
      }
    )
    dStream.print()

    /*
    * 维护offset是为了什么? 哪个时间点用起你维护的offset? 是在Application重启时, Driver重启时
    * 维护offset的另一个语义是什么 : 持久化
    * */

    // 完成业务代码后_V3
    var ranges: Array[OffsetRange] = null
    kafka.foreachRDD(
      rdd => {
        // Driver端可以拿到offset
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 闭包, 通过KafkaUtils得到的第一个DStream向上转型, 提交offset
        kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges, new OffsetCommitCallback {
          // 异步提交offset, 调完后回来可能成功也可能失败
          // callback有一个缺陷 : 异步提交的方式略微有延迟
          // 1.维护/持久化offset到kafka
          override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
            if (offsets != null) {
              ranges.foreach(println)
              println("----------")
              val iter: util.Iterator[TopicPartition] = offsets.keySet().iterator()
              while (iter.hasNext) {
                val k: TopicPartition = iter.next()
                val v: OffsetAndMetadata = offsets.get(k)
                println(s"${k.partition()}...${v.offset()}")
              }
            }
          }
        })
        // 2.维护/持久化到MySQL异步维护或同步
        // 同步
        val local: Array[(String, String)] = rdd.map(r => (r.key(), r.value())).reduceByKey(_ + _).collect()
        // 开启事务, 提交数据, 提交offset, commit
      }
    )

    /*
    // 完成业务代码后_V2
    var ranges: Array[OffsetRange] = null
    // 正确的, 将提交offset的代码放到DStream对象的接收函数中, 那么未来再调度线程中, 这个函数每个Job有机会调用一次, 伴随着提交offset
    kafka.foreachRDD(
      rdd => {
        // Driver端可以拿到offset
        ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 闭包, 通过KafkaUtils得到的第一个DStream向上转型, 提交offset
        //        kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      }
    )
    kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges)*/

    /*
    // 完成业务代码后_V1
    // 因为提交offset代码写到了main线程中, 其实没有起到作用
    kafka.foreachRDD(
      rdd => {
        // Driver端可以拿到offset
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 闭包, 通过KafkaUtils得到的第一个DStream向上转型, 提交offset
        kafka.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
      }
    )*/

    /*
    // 通过KafkaUtils得到的第一个DStream要先去转换下, 其实这个DStream就是consumer@poll回来的records
    // 将record转换成业务逻辑的元素 : 只提取key, value
    val dStream: DStream[(String, (String, String, Int, Long))] = kafka.map(
      record => {
        val t: String = record.topic()
        val p: Int = record.partition()
        val o: Long = record.offset()
        val k: String = record.key()
        val v: String = record.value()
        (k, (v, t, p, o))
      }
    )
    dStream.print()*/

    ssc.start()
    ssc.awaitTermination()
  }
}
