package com.syndra.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util
import java.util.Properties
import java.util.regex.Pattern

/**
 * Kafka Consumer
 */
object Lesson04_Kafka_Consumer {
  def main(args: Array[String]): Unit = {
    val pros = new Properties()
    pros.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop04:9092")
    pros.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    pros.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])

    // earliest 按 CURRENT-OFFSET 特殊状态 group第一创建时, 0
    // latest 按 LOG-END-OFFSET
    pros.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "FALSE")
    pros.put(ConsumerConfig.GROUP_ID_CONFIG, "t2")

    val consumer = new KafkaConsumer[String, String](pros)

    consumer.subscribe(Pattern.compile("test"), new ConsumerRebalanceListener {
      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
        println("onPartitionsRevoked")
        val iter: util.Iterator[TopicPartition] = partitions.iterator()
        while (iter.hasNext) {
          println(iter.next())
        }
      }

      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
        println(s"onPartitionsAssigned")
        val iter: util.Iterator[TopicPartition] = partitions.iterator()
        while (iter.hasNext) {
          println(iter.next())
        }
        consumer.seek(new TopicPartition("test", 1), 22)
        Thread.sleep(5000)
      }
    })

    var record: ConsumerRecord[String, String] = null
    val offMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(0).toMillis)

      if (!records.isEmpty) {
        println(s"----------${records.count()}----------")
        val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while (iter.hasNext) {
          record = iter.next()
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()

          val key: String = record.key()
          val value: String = record.value()
          println(s"key: $key value: $value partition: $partition offset: $offset")
        }
        val partition = new TopicPartition("test", record.partition())
        val offset = new OffsetAndMetadata(record.offset())
        offMap.put(partition, offset)
        consumer.commitAsync(offMap, null)
        /*
        * 如果在运行时 : 手动提交offset到MySQL → 那么
        * 一旦程序重启, 可以通过ConsumerRebalanceListener协商后获得的分区, 去MySQL查询该分区上次消费记录的位置
        * */
      }
    }
  }
}
