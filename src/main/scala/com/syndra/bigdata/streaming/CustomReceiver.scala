package com.syndra.bigdata.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket



/**
 * 接收Socket数据的
 * 那么receiver具备连接Socket的能力 : host, port
 */
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.DISK_ONLY) {

  override def onStart(): Unit = {
    new Thread{
      override def run(): Unit = {
        make()
      }
    }.start()
  }

  private def make(): Unit = {
    val server = new Socket(host, port)

    val reader = new BufferedReader(new InputStreamReader(server.getInputStream))
    var line: String = reader.readLine()
    while (!isStopped() && line!= null) {
      store(line)
      line = reader.readLine()
    }
  }

  override def onStop(): Unit = ???
}
