# `Spark—Streaming`源码分析

------

## 章节`37`：`Spark—Streaming`，源码分析，流式微批任务的调度原理

------

### 一、`Spark—StreamingAndKafka`

#### 1.1————规划

1. `hadoop01`：均不部署
2. `hadoop02`：`ZK`，`broker`
3. `hadoop03`：`ZK`，`broker`
4. `hadoop04`：`ZK`，`broker`

##### 1.1.1—————修改`server.properties`

```shell
# 切换到config目录
cd Kafka
cd config
vi server.properties
# 每台机器的id必须不同
broker.id=0
# Kafka的日志存放位置
log.dirs=/var/bigdata/kafka
# Zookeeper的连接配置
# 因为Zookeeper是公用的,如果未来要维护offset,它默认放在根目录下,会被打散
# 因此要给出一个目录 /kafka
zookeeper.connect=hadoop02:2181,hadoop03:2181,hadoop04:2181/kafka
```

##### 1.1.2—————启动`kafka-server`

```shell
# 在config目录下使用配置文件后台运行
kafka-server-start.sh -daemon ./server.properties
```

如下图

![hadoop02后台启动kafka](D:\ideaProject\bigdata\bigdata-spark\image\hadoop02后台启动kafka.png)

`Zookeeper`中的`kafka`目录，如下图

![ZK中的kafka目录](D:\ideaProject\bigdata\bigdata-spark\image\Zookeeper中的kafka目录.png)

#### 1.2————`kafka`基础操作

##### 1.2.1—————创建存储数据隔离的`topic`

```shell
kafka-topics.sh
# 会给出很多选项
# --zookeeper : 和zk建立连接
# --create --topic : 创建topic
# --partitions 3 : 最终分布式时会有多少个分区
# --replication-factor 2 : 副本因子
kafka-topics.sh --zookeeper hadoop02:2181,hadoop03:2181,hadoop04:2181/kafka --create --topic "test" --partitions 3 --replication-factor 2
```

基于`AKF`原则，数据光分治的话，其中的某一个分区就是`1/N`的数据。那么这个分区如果挂掉的话，其实就会丢失`1/N`的数据，所以一般分布式情况下还强调一个可用性。所以会有`--replication-factor`副本因子。

##### 1.2.2—————查看创建的`topic`

```shell
kafka-topics.sh --zookeeper hadoop02:2181,hadoop03:2181,hadoop04:2181/kafka --list
```

##### 1.2.3—————查看`topic`详情

```shell
kafka-topics.sh --zookeeper hadoop02:2181,hadoop03:2181,hadoop04:2181/kafka --describe --topic test
Topic:test      PartitionCount:3        ReplicationFactor:2     Configs:
        Topic: test     Partition: 0    Leader: 2       Replicas: 2,3   Isr: 2,3
        Topic: test     Partition: 1    Leader: 3       Replicas: 3,1   Isr: 3,1
        Topic: test     Partition: 2    Leader: 1       Replicas: 1,2   Isr: 1,2
```

##### 1.2.4—————`producer`写操作

```shell
# 也可以写多个zk地址 : hadoop02:9092,hadoop03:9092,hadoop04:9092
kafka-console-producer.sh --broker-list hadoop04:9092 --topic test
```

会有很多提示可以输入的东西，但是看不到`Zookeeper`，上边会有一个`--broker-list `。也就是`broker`端不是再去通过找`Zookeeper`注册发现了，想连那个`broker`手动给出即可。减少对`Zookeeper`端的压力，因为以及最后的所有元数据的访问交互的东西都在这个

```mermaid
%%{ init: {'flowchart':{'curve':'basis'}}}%%
graph LR
	A((producer)) --> b1
	B((consumer)) --> b1
	B --> ZK[ZK]

	subgraph hadoop02-broker-3
		b3[test-1]
	end
	
	subgraph hadoop02-broker-2
		b2[test-0]
	end
	
	subgraph hadoop02-broker-1
		b1[test-2]
	end
```

层次完成，就不要再去依赖`Zookeeper`。这是它的`API`以及框架改变的一个特点。

以上命令行输入`Enter`会阻塞住。

##### 1.2.5—————`consumer`读操作

```shell
kafka-console-consumer.sh --bootstrap-server hadoop02:9092 --topic test
```

此处会出现，之前生产的消息没有被消费到。

```shell
# 查看刚才命令行抓取的数据的consumer
kafka-consumer-groups.sh --bootstrap-server hadoop03:9092 --list
console-consumer-35704
# 查看详情
kafka-consumer-groups.sh --bootstrap-server hadoop03:9092 --describe --group 'console-consumer-35704'
TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID                                     HOST            CLIENT-ID
test            0          -               0               -               consumer-1-650b193f-79be-461b-8106-ed5e26b0ddcd /192.168.93.147 consumer-1
test            1          -               1               -               consumer-1-650b193f-79be-461b-8106-ed5e26b0ddcd /192.168.93.147 consumer-1
test            2          -               1               -               consumer-1-650b193f-79be-461b-8106-ed5e26b0ddcd /192.168.93.147 consumer-1

```

##### 1.2.6—————查看`kafka`元数据

```shell
kafka-topics.sh --zookeeper hadoop02:2181/kafka --list\
__consumer_offsets
test
```

其中`__consumer_offsets`就是`kafka`的元数据`MetaData`。

```shell
# 查看MetaData详情
kafka-topics.sh --zookeeper hadoop02:2181/kafka --describe --topic '__consumer_offsets'
Topic:__consumer_offsets        PartitionCount:50       ReplicationFactor:1     Configs:segment.bytes=104857600,cleanup.policy=compact,compression.type=producer
        Topic: __consumer_offsets       Partition: 0    Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 1    Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 2    Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 3    Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 4    Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 5    Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 6    Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 7    Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 8    Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 9    Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 10   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 11   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 12   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 13   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 14   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 15   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 16   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 17   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 18   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 19   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 20   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 21   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 22   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 23   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 24   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 25   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 26   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 27   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 28   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 29   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 30   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 31   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 32   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 33   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 34   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 35   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 36   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 37   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 38   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 39   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 40   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 41   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 42   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 43   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 44   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 45   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 46   Leader: 3       Replicas: 3     Isr: 3
        Topic: __consumer_offsets       Partition: 47   Leader: 1       Replicas: 1     Isr: 1
        Topic: __consumer_offsets       Partition: 48   Leader: 2       Replicas: 2     Isr: 2
        Topic: __consumer_offsets       Partition: 49   Leader: 3       Replicas: 3     Isr: 3
```

至此，`kafka`搭建成功。

------

### 二、`kafka-API`

```xml
<!-- https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10 -->
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming-kafka-0-10_2.11</artifactId>
  <version>2.3.4</version>
</dependency>
```

`Producer`端

```scala
package com.syndra.bigdata.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.concurrent.Future

/**
 * Kafka Producer
 */
object Lesson04_Kafka_Producer {
  def main(args: Array[String]): Unit = {
    val pros = new Properties()
    pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop02:9092,hadoop03:9092,hadoop04:9092")
    pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    val producer = new KafkaProducer[String, String](pros)

    while (true) {
      for (i <- 1 to 3; j <- 1 to 3) {
        val record = new ProducerRecord[String, String]("test", s"item$j", s"action$i")
        val records: Future[RecordMetadata] = producer.send(record)
        val metadata: RecordMetadata = records.get()
        val partition: Int = metadata.partition()
        val offset: Long = metadata.offset()
        println(s"item$j action$i partition: $partition, offset: $offset")
      }
//      Thread.sleep(1000)
    }
    producer.close()
  }
}

```

`Consumer`端

```scala
package com.syndra.bigdata.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRebalanceListener, ConsumerRecord, ConsumerRecords, KafkaConsumer}
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
    pros.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE")
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
        // 调用了一个数据库
        consumer.seek(new TopicPartition("test", 1), 22)
        Thread.sleep(5000)
      }
    })

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(0).toMillis)

      if (!records.isEmpty) {
        println(s"----------${records.count()}----------")
        val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
        while (iter.hasNext) {
          val record: ConsumerRecord[String, String] = iter.next()
          val topic: String = record.topic()
          val partition: Int = record.partition()
          val offset: Long = record.offset()

          val key: String = record.key()
          val value: String = record.value()
          println(s"key: $key value: $value partition: $partition offset: $offset")
        }
      }
    }
  }
}

```

`offset`无论是存到`ZK`还是`kafka`还是数据库，那个叫持久化。换言之`consumer`客户端在运行时，它在自己的进程中会维护自己消费到什么位置。其实维不维护都是一回事，它其实就是开启了线程循环要消息。持久化是解决程序重启后的问题。

我们在哪个点上用我们所谓的自定义`offset`是在程序启动时

```scala
// 调用了一个数据库
consumer.seek(new TopicPartition("test", 1), 22)
Thread.sleep(5000)
```

代码在以上位置执行，负载均衡或故障转移时，是由先前记录消费到的位置，后来者由此参考继续在正确位置消费，而没有重复消费的概念。

#### 2.1————维护`offset`

手动维护`offset`有**两类地方**

1. **手动维护到`kafka`**
2. **其他的任何地方：`ZK`，`MySQL`**

防止丢失数据！

#### 2.2————总结`kafka—Consumer`

1. 自动维护`offset`：`ENABLE_AUTO_COMMIT_CONFIG true`，`poll`数据后先去写`offset`，再去计算，会有丢失数据
2. 手动维护`offset`：`ENABLE_AUTO_COMMIT_CONFIG false`
   1. 维护到`kafka`自己的`__consumer_offsets`这个`topic`中，且还能通过`kafka-consumer-groups.sh`查看
   2. 维护到其他的位置：`MySQL`，`ZK`
      1. 牵扯到通过：`ConsumerRebalanceListener`的`def onPartitionsAssigned(partitions: util.Collection[TopicPartition])`回调后，自己`seek`到查询的位置
3. `AUTO_OFFSET_RESET_CONFIG`：自适应的，且必须参考`__consumer_offsets`维护的数据
   1. `earliest`：按`CURRENT-OFFSET`特殊状态`group`第一创建时，0
   2. `latest`：按`LOG-END-OFFSET`
4. `seek`：覆盖性最强，即便使用自适应维护，但是一旦出现`seek`一定会覆盖之前所有的配置。来自于人工指令要么来自曾经维护到外界的参考持久化的位置

------

### 三、总结

`producer`关注的是`key`的设计，如果想保证商品的事务完成性，那么一定用商品`ID`作为`key`。如果是用人的身份证`ID`号作为`key`那么要维护的是关于这一个人的东西的事务完成性。并按顺序打入到指定分区中。

`consumer`只要消费分区数据一定是按照顺序的，而且`key`只能出现在一个分区中不能出现在其他分区中，同理之前分析过的`Shuffle`的概念。只不过`consumer`在消费时，第一步先参考`offset`，`offset`可以来自于`__consumer_offsets`，也可以来自于第三方。

注意，如果想使用第三方就要用`seek`，且只在程序启动时会有一个所谓的参考方向。`consumer`会自行维护在内存中运行时消费位置`offset`。

一个`partition`在一个`group`中只能对应一个`consumer`，一个`consumer`在一个`group`中可以对应多个`partition`。

------

