package com.syndra.bigdata.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Receiver Custom
 */
object Lesson02_Receiver02_Custom {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Receiver_Custom").setMaster("local[9]")
    val sc: StreamingContext = new StreamingContext(conf, Seconds(5))
    sc.sparkContext.setLogLevel("ERROR")

    val dStream: ReceiverInputDStream[String] = sc.receiverStream(new CustomReceiver("localhost", 8889))
    dStream.print()

    sc.start()
    sc.awaitTermination()
  }
}
