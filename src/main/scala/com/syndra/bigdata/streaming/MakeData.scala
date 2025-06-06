package com.syndra.bigdata.streaming

import java.io.{OutputStream, PrintStream}
import java.net.{ServerSocket, Socket}

object MakeData {
  def main(args: Array[String]): Unit = {
    val listen = new ServerSocket(8889)

    print("Make Data started")
    while (true) {
      val client: Socket = listen.accept()

      new Thread() {
        override def run(): Unit = {
          var num = 0
          if (client.isConnected) {
            val out: OutputStream = client.getOutputStream
            val printer = new PrintStream(out)
            while (client.isConnected) {
              num += 1
              printer.println(s"hello ${num}")
              // 扩展
              printer.println(s"hi ${num}")
              printer.println(s"hi ${num}")
              Thread.sleep(1000)
            }
          }
        }
      }.start()
    }
  }
}
