# Spark-Cluster

------

## 章节10：Spark-CORE，集群框架图解，角色功能介绍，官网学习，搭建

### 1.安装 Spark （单机）

#### 1.1 下载 spark-2.3.4-bin-hadoop2.6.tgz 安装包。

[Spark官网下载](https://archive.apache.org/dist/spark/spark-2.3.4/)

#### 1.2 解压上传 Linux。

```shell
Windows cmd
sftp root@x.x.x.x
输入密码
连接
put spark-2.3.4-bin-hadoop2.6.tgz
tar -zxvf spark-2.3.4-bin-hadoop2.6.tgz /opt/bigdata
```



#### 1.3 配置环境变量 **(非必须)**

```shell
export SPARK_HOME=/opt/bigdata/spark-2.3.4-bin-hadoop2.6
export PATH=$PATH:$SPARK_HOME/bin
```



#### 1.4 安装成功的 **spark-shell** 界面

![./spark-shell](D:\ideaProject\bigdata\bigdata-spark\image\Spark-2.3.4_hadoop2.6-安装成功.png)

#### 1.5 Cluster Framework Graph（集群框架图解）

![Cluster Framework Graph]()

### 2.尝试运行 **wordCount**

#### 2.1 在 **tmp** 目录下写入一个 **data.txt**

```shell
cd tmp
vi data.txt
hello world
hello spark
hello Syndra
```

#### 2.2 在 **spark-shell** 中输入代码

```scala
sc.textFile("/tmp/data.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)
```

#### 2.3 **Enter** 运行

![Spark-worldCount运行成功](D:\ideaProject\bigdata\bigdata-spark\image\Spark-worldCount-运行成功.png)

**以上则表示运行成功**。

### 3.**standalone** 模式

#### 3.1 基础设施

1. **JDK：1.8.xxx**
2. **节点配置**
3. **部署 Hadoop：HDFS，Zookeeper**
4. **免密必须做**

#### 3.2 Linux 下载 Spark 软件包

```shell
# 第二种方式下载 Spark 安装包
wget spark-2.3.4-bin-hadoop2.6.tgz
tar -zxvf spark-2.3.4-bin-hadoop2.6.tgz
mv spark-2.3.4-bin-hadoop2.6 /opt/bigdata
```

#### 3.3 部署细节

##### 3.3.1 官方文档

###### 1.1 [Spark 官方文档网址](https://archive.apache.org/dist/spark/docs/2.3.4/)

###### 1.2 配置

```shell
vi /etc/profile
$SPARK_HOME/conf
```

1. **slaves**

   **hadoop02**

   **hadoop03**

   **hadoop04**

2. **spark-env.sh**

   ```shell
   export HADOOP_CONF_DIR=/opt/bigdata/hadoop-2.6.5/etc/hadoop
   # 每台 Master 改成自己的主机名
   export SPARK_MASTER_HOST=hadoop01
   export SPARK_MASTER_PORT=7077
   export SPARK_MASTER_WEBUI_PORT=8080
   export SPARK_WORKER_CORS=2
   export SPARK_WORKER_MEMORY=4g
   ```

3. **先启动 Zookeeper**

4. **再启动 HDFS**

5. **最后启动 Spark（资源管理层）**

   **master，workers**

##### 3.3.2 部署成功结果图

1. hadoop01 启动 HDFS，如下图：

   ![start-dfs.sh](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01-start-dfs.png)

2. 访问 HDFS-WebUI，如下图：

   ![HDFS-WebUI](D:\ideaProject\bigdata\bigdata-spark\image\HDFS-WebUI.png)

3. hadoop01 启动 ./start-all.sh，如下图：

   ![Spark ./start-all.sh](D:\ideaProject\bigdata\bigdata-spark\image\Spark-start-all-sh.png)

4. 访问 Spark-WebUI，如下图：

   ![Spark-WebUI](D:\ideaProject\bigdata\bigdata-spark\image\Spark-WebUI.png)

5. spark-shell 默认 local，如果想分布式计算程序，就必须加 --master，如下图：

   ![spark-shell_Master](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell_Master.png)

6. Spark-WebUI 会有一个 Application，Spark shell 运行，如下图：

   ![Spark-WebUI_Application](D:\ideaProject\bigdata\bigdata-spark\image\Spark-WebUI_Application.png)

   ![Application_Spark-shell](D:\ideaProject\bigdata\bigdata-spark\image\Application_Spark_shell.png)

#### 3.4 尝试在 standalone 模式下运行 wordCount

![standalone_wordCount](D:\ideaProject\bigdata\bigdata-spark\image\standalone_wordCount.png)

##### 3.4.1 执行完后，没有结果输出，为什么？

- 一定明白，**Spark 是一个分布式计算程序**，计算逻辑最终会发送到集群中，**foreach** 这个算子最终会作用在它的每一个分区上，这个数据虽然很小只有一个分区，但它不在主机，它在后面的某一个 **DataNode** 中，这段逻辑是发送到了某一个 **DataNode** 上执行了，并没有在本地节点打印，所以 Spark 为什么还有一个 **collect() 回收** 这么一个算子。
- **collect()** 算子代表把发出去前面这段逻辑在某一个节点那个分区块执行，但是一到 **collect()** 算子时，把数据回收到本地的 **Driver**，这就是 **Driver** 为什么要在 **Spark** 需要交互的时候它只能是 **Clinet** 模式，主要局限在当前这个进程，然后接一个 **foreach(println)**。

#### 3.5 尝试在集群模式下运行 wordCount

![Cluster_wordCount](D:\ideaProject\bigdata\bigdata-spark\image\Cluster_wordCount.png)

**会发现这个速度很快，因为它自己有一个存储层**，这个数据曾经被缓存过，也就是曾经在看 **DAG** 时，它是一个 **Stage** 跳过了。

##### 3.5.1 Spark-WebUI 的 Job 执行结果

其实执行过了两次程序了，**第一次在集群但外边打印**，**第二次在集群被回收了**，所以在整个生命周期之内有过两次 **Job** 执行，如下图：

![两次 JOb](D:\ideaProject\bigdata\bigdata-spark\image\两次Job.png)

###### 1.1 思考：

后续是不是可以再写 10次，100次，1000次 不同代码执行不同作业，也就是这个程序不停，**Application** 一直在，它不知道未来会有多少个 **Job**，你也不知道未来会有多少个 **Job**，因为你可以一直写下去，所以它要先申请一批 **JVM，Executor**，且 **spark-shell** 不能关也**不能复用**，**一关就什么都没有了**，这些 **JVM，Driver，Client，Executor 进程全部销毁，所有中间数据都没了**。

其实 **Spark 程序的逻辑是未知的，数量不一的**。

------

## 章节11：Spark-CORE，history 服务，standaloneHA，资源调度参数

### 1.根据官方文档配置 基于 Zookeeper HA

1. [Spark 官方文档配置 standaloneHA](https://archive.apache.org/dist/spark/docs/2.3.4/spark-standalone.html)

2. [具体修改配置文件](https://archive.apache.org/dist/spark/docs/2.3.4/configuration.html#deploy)：

3. **同步到其它节点**：

   ```shell
   cd $SPARK_HOME
   cd conf
   # 备份 spark-defaults.conf.template
   cp spark-defaults.conf.template spark-defaults.conf
   vi spark-defaults.conf
   spark.deploy.recoveryMode	ZOOKEEPER
   spark.deploy.zookeeper.url	hadoop02:2181,hadoop03:2181,hadoop04:2181
   spark.deploy.zookeeper.dir	/syndraspark
   ```

4. **小细节**：

   ```shell
   scp spark-defaults.conf hadoop02:`pwd`
   scp spark-defaults.conf hadoop03:`pwd`
   scp spark-defaults.conf hadoop04:`pwd`
   ```

   ```shell
   # 在第一个配置文件中, 已经给出了 Master 是谁,
   # 现在写的是 hadoop01, 那么未来谁还是 Master 呢?
   # 如果想让第二台起个 Master, 需要在第二台中, 手动修改配置文件
   vi spark-defaults.conf
   # 自己的配置文件要写自己的主机名,
   # Hadoop 这点做的非常好, Mycluster 映射成两个主机名,
   # 这两个 NameNode 谁是主, 找你访问 Mycluster 拿到配置文件,
   # 你可以自动连接,
   # 在这使用的时候, 不是自动的, 它是有一个硬性的要求,
   # 在使用时, 手动给出.
   export SPARK_MASTER_HOST=hadoop02
   ```

### 2.重启

#### 2.1 hadoop01 重启

```shell
cd ..
cd sbin
# 全部停止
./stop-all.sh
# 全都启动
./start-all.sh
```

![hadoop01 重启](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01重启.png)

##### 2.1.1 hadoop02 启动 Master

```shell
cd $SPARK_HOME
cd sbin
./start-master.sh
```

![hadoop02启动 Master](D:\ideaProject\bigdata\bigdata-spark\image\hadoop02启动Master.png)

##### 2.1.2 查看 zkCli 查看对应信息

当所有 Master，Worker 运行成功后，会有两个 Master，谁是主呢？

去 hadoop04 的 zkCli 查看 spark 的对应选主信息：

```shell
zkCli.sh
# 再配置好 Spark HA 的配置文件后, 且并没有重启前
# zkCli 是没有 Spark 对应目录的
ls /
# 只有在重启后对应目录才会在 zkCli 中创建
ls /
ls /syndraspark
get /syndraspark/master_status
```

![查看 zkCli 选主信息](D:\ideaProject\bigdata\bigdata-spark\image\查看zkCli选主信息.png)

*如果 hadoop01 挂掉，会自动切换到 hadoop02。*

###### 2.1 SparkWebUI 的节点状态

**hadoop01，如下图：**:

![hadoop01-spark-master_Alive](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01-spark-master_Alive.png)

**hadoop02，如下图：**：

![hadoop02-spark-master_Standby](D:\ideaProject\bigdata\bigdata-spark\image\hadoop02-spark-master_Standby.png)

**Spark 可以借助 Zookeepr 完成 Alive Standby 的状态切换，可以做到一致性。**

### 3.集群中运行一个计算程序

#### 3.1 计算程序运行额外参数

**万一 Master 挂了怎么办，它能不能切换到另一个 Master 继续连接？**

**原有的 spark-shell 直接运行的话是单机模式，如果在 Spark 集群下运行计算程序，必须告诉它 Master 是谁。**

```shell 
# 切换到 hadoop01 的 spark /bin
# 它不像 Hadoop 那么友好, 需要自己手动写集群模式下运行参数
# 因为 hadoop01，hadoop02 都有 Master，谁可用就让它连接谁
./spark-shell --master spark://hadoop01:7077,hadoop02:7077
```

![spark-shell_Cluster](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell_Master.png)

#### 3.2 wordCount 运行计算

##### 3.2.1 spark-shell_Cluster

```shell
# spark-shell cluster 运行计算
./spark-shell --master spark://hadoop01:7077,hadoop02:7077
```

```scala
// Spark Scala wordCount
sc.textFile("hdfs://mycluster/sparktest/data.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
```

![spark-shell_Cluster-wordCount](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell_Cluster-wordCount.png)

##### 3.2.2 SparkWebUI_Cluster

spark-shell --master Cluster 额外参数运行后，SparkWebUI 的运行结果，如下图：

![SparkWebUI_Cluster](D:\ideaProject\bigdata\bigdata-spark\image\SparkWebUI_Cluster-wordCount.png)

此时 wordCount 已经运行成功，这个 Job 已经提交到 Cluster 中运行。当 Spark 在 Cluster 中运行的时候，因为 Master 是 HA 的，其中一个挂了之后，会自行等待一会，因为它不可能像那种身上有数据的，不可能立刻就做切换，因为有可能是网络波动，所有它有一个延迟（重试次数），当达到这个阈值时，它才会真正去 Standby 抢 Alive。或者有可能是认为这个东西可以恢复一下。

###### 2.1 一个小概念（HA 的知识点）

- 它有一个资源层，然后有一个计算层
- 资源层中只是一个对资源的统计和分配时一个需要互动的过程。
- 计算层如果有一个 Driver 启动时， 代码还没有被执行，它的第一步是要先申请一堆的 Executor，这些都是独立的 JVM 进程
- 当这些进程有了后，其实是和它建立了连接
- 如果资源层出现了挂机，短时间内其实你的计算层是不需要使用资源层的
- 如果没有 Master，后续的所有监控，关于资源方面，如果要启动动态分配的话，反而之后就不能完成
- 因为 JVM 再想申请或回收一堆新的 Executor，就达不到那种效果
- 但是无论如何，它的资源层为我们提供了一个可靠的保障，而且如果前面的程序，计算层只申请了一半的资源，那么它里面统计剩下的一半
- 未来别人提交的时候，想提交一个新的计算程序时，这个 Master 如果是 HA 的话，给它两个地址，它其实可以连重新活着的那个

### 4.History Service（知识点）

除了上一个运行的程序，但是重启后之前运行过的程序没了，如果在公司，真正的生产系统中，肯定趋向于一件事，我可以分析过去的跑过 Job 的一些细节和性能，可以对后续的部署进行调优。

#### 4.1 此时需要一个东西叫做历史记录服务，其实 yarn 中也有此功能。

[所以要补全 Spark Cluster 的持久化 Monitoring](https://archive.apache.org/dist/spark/docs/2.3.4/monitoring.html)

```shell
# 可以查看历史记录
./sbin/start-history-server.sh
```

##### 4.1.1 但是这其中有一处细节：谁参与了日志的记录，谁参与了历史的展示？（关于历史记录方面会有两个方向）

*有这么两个维度，就像学 Spring 一样。学 Spring 时都知道它是一个容器，但是一提到容器你明白它有两个方向：*

- **第一是谁把对象扔进去了？对象如何放进去？那个实例对象得扔进去**

  通过启动一个进程，这个进程会从一个所谓的历史当中取出。

  计算层自己，不是 Master，就是计算层自己要做的一件事，每个计算程序自己记自己的事。

- **第二是哪些位置会用到这个地方？**

  Driver

###### 1.1 记录（hadoop01）

```shell
# 开启日志的记录
# 日志写到哪去?
# 一般推荐使用 HDFS
# 计算端开启写日志
# 这是往里放的
# 一方向其中记录
# 计算层会自己将自己的计算日志存入 HDFS
spark.eventLog.enabled	true
spark.eventLog.dir	hdfs://mycluster/spark_log
```

###### 1.2 取出

```shell
# 怎么取出来?
# Spark 历史服务器读取哪里的东西可以读取到曾经的日志
# 一方向其中取出
spark.history.fs.logDirectory	hdfs://mycluster/spark_log
```

###### 1.3 创建相应的 HDFS 的 spark_log 目录

```shell
hdfs dfs -mkdir -p /spark_log
```

###### 1.4 修改 hadoop01 配置文件

```shell
vi spark-defaults.conf
spark.eventLog.enabled	true
spark.eventLog.dir	hdfs://mycluster/spark_log
spark.history.fs.logDirectory	hdfs://mycluster/spark_log
```

###### 1.5 分发配置

```shell
scp spark-defaults.conf hadopo02:`pwd`
scp spark-defaults.conf hadopo03:`pwd`
scp spark-defaults.conf hadopo04:`pwd`
```

###### 1.6 重启服务

```shell
# hadoop01 的 Spark /sbin
./stop-all.sh
./start-all.sh
```

###### 1.7 运行程序

```shell
# hadoop01 spark bin
./spark-shell --master spark://hadoop01:7077,hadoop02:7077
```

```scala
sc.textFile("hdfs://mycluster/sparktest/data.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect().foreach(println)
```

###### 1.8 退出 shell 重启 Cluster 且 启动 HistoryServer

1. **执行完程序且退出 spark-shell，如下图：**

   ![运行完程序退出 shell](D:\ideaProject\bigdata\bigdata-spark\image\执行完程序并且退出shell.png)

2. **WebUI 已经执行完，如下图：**

   ![SparkWebUI执行完](D:\ideaProject\bigdata\bigdata-spark\image\SparkWebUI-执行完.png)

3. **停止集群运行，停止 hadoop02 Master**

   ```shell
   # 停止集群
   ./stop-all.sh
   # 停止 hadoop02 Master sbin
   ./stop-master.sh
   ```

4. **hadoop01 停止集群，如下图：**

   ![hadoop01_stop-all.sh](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01_stop-all.png)

5. **hadoop02 停止 master，如下图：**

   ![hadoop02_stop-master.sh](D:\ideaProject\bigdata\bigdata-spark\image\hadoop02_stop-master.png)

6. **再次运行 hadoop01 集群，如下图：**

   ![hadoop01_start-all.sh](D:\ideaProject\bigdata\bigdata-spark\image\再次运行hadoop01_start-all.png)

7. **再次启动 hadoop02_start-master，如下图：**

   ![hadoop02_start-master.sh](D:\ideaProject\bigdata\bigdata-spark\image\再次启动hadoop02_start-master.png)

8. **再次访问 hadoop01:7077 发现并没有历史服务，如下图**

   ![hadoop01:7077](D:\ideaProject\bigdata\bigdata-spark\image\再次访问hadoop01-7077.png)

9. **需要访问另一个端口号，日志已经记录，如下图**

   ![hadoop01:50070 的 spark_log](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01-50070_spark_log.png)

10. **为什么在 Spark 中看不到结果？其实日志已经放进去了，那么怎么取出来？需要一个进程：HistoryServer，可以代替你去解析配置文件看到结果。**

    ```shell
    # hadoop02 sbin 中手动启动
    # 可以在任何有配置的节点中运行
    ./start-history-server.sh
    ```

    ![启动 hadoop02 的 start-history-server.sh](D:\ideaProject\bigdata\bigdata-spark\image\启动hadoop02_start-history-server-sh.png)

11. **访问启动有 HistroyServer 主机的主机名，默认端口号18080，如下图：**

    ![HistoryServerWebUI](D:\ideaProject\bigdata\bigdata-spark\image\HistoryServerWebUI.png)

12. **执行程序的所有细节，如下图：**

    ![执行细节](D:\ideaProject\bigdata\bigdata-spark\image\执行程序的所有细节.png)



#### 4.2 Spark Submitting Applications（提交程序）

Spark 更倾向于使用 spark-submit 来提交开发的程序，[官网指导链接](https://archive.apache.org/dist/spark/docs/2.3.4/submitting-applications.html)。

##### 4.2.1 命令的语法格式

比较像 Hadoop，只要想提交相应的程序，必然有相应的 jar 包。

```shell
./bin/spark-submit \ # "\" 表示命令后必须立刻跟一个换行符
  --class <main-class> \ # jar 包属于哪个主类
  --master <master-url> \ # Spark 特有的, 给出计算程序要访问哪个资源层的哪个 Master
  --deploy-mode <deploy-mode> \ # 部署模式 : Client/Classe, 这两中模型不是 Clinet 独立跑, 是说在 Client 进程中或 Driver 执行器在 Cluster 的某台机器中
  --conf <key>=<value> \ # key : spark-defaults.conf 中相应的 key, 可以变为参数在程序启动时传递给它
  ... # other options
  <application-jar> \
  [application-arguments] # 参数, 需要传递给主方法
```

###### 1.1 Example

在学习真正提交 Jar 包前，我们有一个 jar 包，spark-submit 可以提交 jar 包，那么这个 jar 包从哪来？

**所有计算框架基本都会有一个 Example （案例），或者官方的例子，**[GitHub Spark Example 地址](https://github.com/apache/spark/blob/v2.3.4/examples/src/main/scala/org/apache/spark/examples/SparkPi.scala)。

**wordCount 是大部分分布式计算框架的 Example，但是在 Spark 中更推荐使用 SparkPi。**

```scala
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import scala.math.random

import org.apache.spark.sql.SparkSession

/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    // 此处和之前学习写 Spark Progarm 的方式不一样
    // 可以认为是 Spark Context
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    // 参数 > 0, 取自己第一个参数, 没有的话取一个数字 2
    val slices = if (args.length > 0) args(0).toInt else 2
    // n 的组成是传递进来的值被放大了 10W 倍
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    // count 其实是一个 RDD, 此处创建一个 RDD, 数据集是传递的值 - 1
    // 最关键的是后面的分区数量是传递进来的数字
    // 也就是一个分区中有 10W 条记录
    // i 的作用是为了调起 map()
    // 这个 map() 会点 100W 次
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    // 概率事件, 它们或多或少的趋向于 3.1415926
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}
// scalastyle:on println
```

1. **运行 Spark 自带的 example，如下图：**

   ```shell
   # 切换到 jars 目录下
   ../../bin/spark-submit --master spark://hadoop01:7077,hadoop02:7077 --class org.apache.spark.examples.SparkPi ./spark-examples_2.11-2.3.4.jar 10
   # 以上分区数量运行过程较短, 不利于观察运行过程, 遂扩大至 10W
   ../../bin/spark-submit --master spark://hadoop01:7077,hadoop02:7077 --class org.apache.spark.examples.SparkPi ./spark-examples_2.11-2.3.4.jar 100000
   ```

   ![使用 spark-submit 运行 Spark jars 目录下的 examples](D:\ideaProject\bigdata\bigdata-spark\image\Spark-jars_examples-jar.png)

2. **运行结果，如下图：**

   ![examples运行结果](D:\ideaProject\bigdata\bigdata-spark\image\examples运行成功.png)

3. **访问 hadoop01:8080 查看，原有 10 基础上扩大 10000 倍，方便查看运行过程，如下图：**

   ![hadoop01:8080 运行结果](D:\ideaProject\bigdata\bigdata-spark\image\10W个分区执行中的任务.png)

   ![10W个分区已经执行完的任务](D:\ideaProject\bigdata\bigdata-spark\image\10W个分区中已经完成的任务.png)

4. **执行完成，如下图：**

   ![10W个分区执行完成](D:\ideaProject\bigdata\bigdata-spark\image\10W个分区执行完成.png)

5. **在企业中一般会写一个所谓的脚本：**

   ```sh
   # 在根目录下
   $SPARK_HOME/bin/spark-submit \
   --master spark://hadoop01:7077,hadoop02:7077 \
   --class org.apache.spark.examples.SparkPi \
   $SPARK_HOME/examples/jars/spark-examples_2.11-2.3.4.jar \
   1000
   ```

   ```sh
   # 运行脚本
   # 但这只是静态文件
   . submit.sh
   ```

6. **执行结果，如下图：**

   ![submit.sh 执行结果](D:\ideaProject\bigdata\bigdata-spark\image\submit-sh执行结果.png)

7. **动态文件，如下图：**

   ```sh
   # 在根目录下
   class=org.apache.spark.examples.SparkPi
   jar=$SPARK_HOME/examples/jars/spark-examples_2.11-2.3.4.jar
   $SPARK_HOME/bin/spark-submit \
   --master spark://hadoop01:7077,hadoop02:7077 \
   --class  $class \
   $jar \
   1000
   ```

   ```sh
   # 再次 执行
   . submit.sh
   ```

   ![动态文件依然可以执行](D:\ideaProject\bigdata\bigdata-spark\image\动态文件依然可执行.png)

8. **传参的方式执行脚本，这种方式是企业中的提交方式，会让工作效率提升且可嵌入到调度系统中，调度系统只需要知道有一个脚本，我们只需要传递参数即可，如下图：**

   ```sh
   # 在根目录下
   class=$1
   jar=$2
   $SPARK_HOME/bin/spark-submit \
   --master spark://hadoop01:7077,hadoop02:7077 \
   --class  $class \
   $jar \
   1000
   ```

   ```sh
   # 固定不变的参数用 '', 会变的参数用 ""
   . submit.sh 'org.apache.spark.examples.SparkPi' "$SPARK_HOME/examples/jars/spark-examples_2.11-2.3.4.jar"
   ```

   ![传参方式执行结果](D:\ideaProject\bigdata\bigdata-spark\image\传参执行脚本.png)

##### 4.2.2 调度

切换到 spark/bin

```shell
./spark-submit --help
# 会打印很多帮助提示的输出
Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
Usage: spark-submit --kill [submission ID] --master [spark://...]
Usage: spark-submit --status [submission ID] --master [spark://...]
Usage: spark-submit run-example [options] example-class [example args]

Options:
  # 给出不同的地址
  --master MASTER_URL         spark://host:port, mesos://host:port, yarn,
                              k8s://https://host:port, or local (Default: local[*]).
  # 部署, 之前一直没有使用过它
  # 它使用的是 Client 模式
  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
                              on one of the worker machines inside the cluster ("cluster")
                              (Default: client).
  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
  # 给它起个名字
  --name NAME                 A name of your application.
  # 可以用逗号分割的一个列表, 把一一些 jar 定义到其中, 最终会放到 executor classpaths
  --jars JARS                 Comma-separated list of jars to include on the driver
                              and executor classpaths.
  --packages                  Comma-separated list of maven coordinates of jars to include
                              on the driver and executor classpaths. Will search the local
                              maven repo, then maven central and any additional remote
                              repositories given by --repositories. The format for the
                              coordinates should be groupId:artifactId:version.
  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
                              resolving the dependencies provided in --packages to avoid
                              dependency conflicts.
  --repositories              Comma-separated list of additional remote repositories to
                              search for the maven coordinates given with --packages.
  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
                              on the PYTHONPATH for Python apps.
  --files FILES               Comma-separated list of files to be placed in the working
                              directory of each executor. File paths of these files
                              in executors can be accessed via SparkFiles.get(fileName).

  --conf PROP=VALUE           Arbitrary Spark configuration property.
  --properties-file FILE      Path to a file from which to load extra properties. If not
                              specified, this will look for conf/spark-defaults.conf.
  # 一些细节
  # Driver 有能力拉取计算结果回自己的 JVM
  # 整个计算层中, Driver, Executor JVM 会存储中间计算过程的数据的元数据
  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 1024M).
  # 加载哪些类
  # 在 Driver 启动前给出定义
  --driver-java-options       Extra Java options to pass to the driver.
  --driver-library-path       Extra library path entries to pass to the driver.
  --driver-class-path         Extra class path entries to pass to the driver. Note that
                              jars added with --jars are automatically included in the
                              classpath.
  # 未来所跑任务的 Executor 进程内存期望大小
  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).

  --proxy-user NAME           User to impersonate when submitting the application.
                              This argument does not work with --principal / --keytab.

  --help, -h                  Show this help message and exit.
  --verbose, -v               Print additional debug output.
  --version,                  Print the version of current Spark.

 Cluster deploy mode only:
  # Driver 启动的核心数, 只能是 Cluster Mode
  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
                              (Default: 1).

 # 只有 standalone 或 mesos 可以接收的参数 
 Spark standalone or Mesos with cluster deploy mode only:
  --supervise                 If given, restarts the driver on failure.
  --kill SUBMISSION_ID        If given, kills the driver specified.
  --status SUBMISSION_ID      If given, requests the status of the driver specified.

 Spark standalone and Mesos only:
  # 这个程序一提交, 在整个的 Master 资源管理层中, 总共要申请多少个 COORE
  # 在 Spark 中, 如果给出了总核心数, 默认情况下, 每节点第一个 Executor 且 这个 Executor 会尽量申请这台节点中的核心数
  # 但更倾向于水平申请
  # 一般会将 --total-executor-cores --executor-cores 参数整合使用
  # --total-executor-cores >= --executor-cores
  --total-executor-cores NUM  Total cores for all executors.

 Spark standalone and YARN only:
  # 每 Executor 是 1 CORE
  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
                              or all available cores on the worker in standalone mode)

 YARN-only:
  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
  --num-executors NUM         Number of executors to launch (Default: 2).
                              If dynamic allocation is enabled, the initial number of
                              executors will be at least NUM.
  --archives ARCHIVES         Comma separated list of archives to be extracted into the
                              working directory of each executor.
  --principal PRINCIPAL       Principal to be used to login to KDC, while running on
                              secure HDFS.
  --keytab KEYTAB             The full path to the file that contains the keytab for the
                              principal specified above. This keytab will be copied to
                              the node running the Application Master via the Secure
                              Distributed Cache, for renewing the login tickets and the
                              delegation tokens periodically.
```

![./spark-submit 帮助提示输出](D:\ideaProject\bigdata\bigdata-spark\image\spark-submit--help.png)

去 hadoop01:8080 会发现曾经跑的程序中只有这么几个 Workers，没有所谓的 Driver 环节，如下图：

![没有 Driver 的环节](D:\ideaProject\bigdata\bigdata-spark\image\只有几个Wokers没有Driver的环节.png)

只需要在 submit.sh 中加一个选项：

```sh
# 在根目录下
class=org.apache.spark.examples.SparkPi
jar=$SPARK_HOME/examples/jars/spark-examples_2.11-2.3.4.jar
$SPARK_HOME/bin/spark-submit \
--master spark://hadoop01:7077,hadoop02:7077 \
--deploy-mode cluster \
--class  $class \
$jar \
1000
```

执行脚本，如下图：

```sh
. submit.sh
```

![submit.sh](D:\ideaProject\bigdata\bigdata-spark\image\执行submit的Cluster模式.png)

Drivers 的运行状态，如下图：

![hadoop01:8080 的 Drivers](D:\ideaProject\bigdata\bigdata-spark\image\Drivers的运行状态.png)

Drivers 运行完成，如下图：

![Drivers 运行完成](D:\ideaProject\bigdata\bigdata-spark\image\Drivers运行完成.png)

###### 2.1 思考：刚才的过程 Client 起到了什么效果？

*刚才 Client 只是和 Master 说，你先帮我创建一个 Drivers，然后其实加载的是我们的逻辑，然后再给 Executor 执行，人家热火朝天呢，Client 其实挂就挂掉了，Client 退出其实不会影响到 Driver 这一侧已有的 Cluster 调度任务的过程，这就是所谓的 Client Mode。*

*因为你的 Driver 是在 Cluster 中，曾经跑一批任务的时候，是 Driver 在 Client 中，所以 Driver 回收的 3.14 也能看见，但 Cluster Mode 下，Driver 把 Executor 结果拉取回来打印，但是 Client 看不到，现在是记录到了 Driver 中，那么 3.14 到底在哪台机器？*

*找那台机器，然后看那个进程的标准输出即可，它应该记录到日志里，因为有正常记录和错误记录。*

*因为是 Cluster Mode 了，再也不是 Client Mode，你的 Driver 跑集群中了，它把任务发下去了，跑完后它把结果回收了，但是它并没有在 Client，所以 Client 刚开始没有看到这些输出，此时你想要它输出怎么办？*

它又要打印，那么就找到这个 Driver，曾经它所在的节点 Worker，进入 Worker，如下图：

![进入 Worker](D:\ideaProject\bigdata\bigdata-spark\image\进入Worker.png)

Driver 的日志标准输出，如下图：

![Driver stdout](D:\ideaProject\bigdata\bigdata-spark\image\Driver的日志标准输出.png)

**但是其实在生产中，是不会使用 println 打印算子的。**

**未来可能有一种场景：**

**目录很多，文件很多，文件体积很小，要写一个 Spark 计算程序，其实这些数据小文件加起来总共的数据量还没有机器内存大，每次都跑异常，就是因为文件的元数据太多了，溢出了 JVM 的内存大小，所以要关注此项参数。**

```shell
# 调整 Driver 的 JVM 内存大小
--driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 1024M).
```

###### 2.2 调整 Executor CORE 的使用

```shell
class=org.apache.spark.examples.SparkPi
jar=$SPARK_HOME/examples/jars/spark-examples_2.11-2.3.4.jar
$SPARK_HOME/bin/spark-submit \
--master spark://hadoop01:7077,hadoop02:7077 \
--total-executor-cores 6 \
--executor-cores 1 \
--class  $class \
$jar \
1000
```

```shell
. submit.sh
```

调整 Executor CORE 的运行结果，如下图：

![调整 CORE 的执行结果](D:\ideaProject\bigdata\bigdata-spark\image\调整CORE的分配.png)

**每个 Executor 只会有1 个核心，1 <= Executor Core <= total。**

```shell
# 未来在公司中用的最多最核心的三个参数
--total-executor-cores
--executor-cores
--executor-memory
```

会根据实际的作业的数据特征是大数据，小文件，还是 IO 比较多，还是并行度比较多，根据各种情况调整各自的值。

------

## 章节12：Spark-CORE，基于 yarn 的集群搭建、配置，资源调度参数，优化 jars

在企业中，更多的资源层是 yarn，公司的多台机器中，他不应该其中只跑一种计算框架，应该是各种类型计算特征的框架，都部署在其中，想用哪种类型的时候，随即调用执行。

### 1.YARN

**Spark On Yarn**

**Kylin -> 麒麟：这套系统，它只是你去页面走一些流程，定义一些数据的维度，数据集之类的，然后最终提交，它会 hold 住下边整个大数据集群中部署的那些东西。比如：Hive，HBase，Spark，你是看不到的，而你部署的时候可能连 Spark 都不用部署，因为 Kylin 中可能集成了 Spark 安装包，而且它不需要每个节点都去部署。**

### 2.部署

1. 停止 Spark 的 Master，Worker，HistoryServer：

   ```shell
   # 切换到 hadoop01 sbin 目录下
   ./stop-all.sh
   # 切换到 hadoop02 sbin 目录下
   ./stop-history-server.sh
   ./stop-master.sh
   ```

2. Spark On Yarn：不需要 Master，Worker 的配置，Yarn 不需要这两个角色启动：

   ```sh
   # 去 hadoop01 Spark conf 目录下改 spark-env.sh
   cd $SPARK_HOME
   cd conf
   vi spark-env.sh
   # 注释掉 MASTER 配置
   # export SPARK_MASTER_HOST=hadoop01
   # export SPARK_MASTER_PORT=7077
   # export SPARK_MASTER_WEBUI_PORT=8080
   #
   # 注释掉 WORKER 配置
   # export SPARK_WORKER_CORES=2
   # export SPARK_WORKER_MEMORY=4g
   ```

3. 此时主从已经没有了，配置也没了，slaves 文件也可以不用了：

   ```shell
   # 切换到 hadoop01 Spark conf 目录下
   # 将 slaves 重命名为 slaves.bak
   mv slaves slaves.bak
   ```

4. 只需要启动 yarn 的角色。

### 3.配置

1. 修改 hadoop01 的 spark-env.sh 配置文件：

   ```shell
   # 切换到 hadoop01 Spark conf 目录下 
   # 只剩下了一个配置
   # 为什么只留这个配置, 因为要关注的是 Spark On Yarn 是一个维度
   # 这个配置目录中会有 Yarn 的配置文件, 会通过读取这个配置文件
   # 会知道 ResourceManager, 且它不只是 Spark On Yarn
   # 还会 On HDFS, 通过其中也可以拿到 NameNode, DataNode 相关的配置信息
   export HADOOP_CONF_DIR=/opt/bigdata/hadoop-2.6.5/etc/hadoop
   ```

2. 修改 spark-defaults.conf：

   ```shell
   # 切换到 hadoop01 Spark conf 目录下
   # 注释到 Zookeeper 做 HA 的配置
   # 它是做的 Master 的 HA, 这三个配置已经没有意义了
   # 一定要清楚哪些东西是在哪个模式下使用的
   # 以下配置
   # spark.deploy.recoveryMode       ZOOKEEPER
   # spark.deploy.zookeeper.url      hadoop02:2181,hadoop03:2181,hadoop04:2181
   # spark.deploy.zookeeper.dir      /syndraspark
   #
   # 以下配置是计算层程序会把计算的日志写到 HDFS 并会启动一个不是资源层的角色
   # 启动的是计算层的一个历史记录的服务器, 它可以拿到曾经的日志并给你展示历史状态
   spark.eventLog.enabled  true
   spark.eventLog.dir      hdfs://mycluster/spark_log
   spark.history.fs.logDirectory   hdfs://mycluster/spark_log
   ```

3. 分发修改后的配置文件：

   ```shell
   scp spark-env.sh spark-defaults.conf hadoop02:`pwd`
   scp spark-env.sh spark-defaults.conf hadoop03:`pwd`
   scp spark-env.sh spark-defaults.conf hadoop04:`pwd`
   ```

#### 3.1 Hadoop

首先在 Yarn 中添加几个配置项，修改的是 nodemanager，然后将资源中的 memory 的每一个 nodemanager 改成 4096（也就是 4G），其实框架访问资源层看到的内存和核心数量它并不是物理的，是我们在资源层可以虚构配置出来的，还有一个配置项是虚拟内存检查，这些知识和开发没有任何关系，都是运维去做（没有运维除外，需要自己做）。

##### 3.1.1 切换到 Hadoop etc/hadoop 的目录

1. 修改 yarn-site.xml：

   ```shell
   # 切换到 hadoop01 /etc/hadoop 目录
   cd $HADOOP_HOME
   vi yarn-site.xml
   ```

   ```xml
   <!-- 添加以下配置 -->
   <property>
       <name>yarn.nodemanager.resource.memory-mb</name>
       <value>4096</value>
   </property>
   <!-- 核心的数量, 和 standaloneHA 的资源层类似 -->
   <property>
       <name>yarn.nodemanager.resource-cpu-vcores</name>
       <value>4</value>
   </property>
   <!-- 虚拟内存检查, 需要关闭 -->
   <property>
       <name>yarn.nodemanager.vmem-check-enabled</name>
       <value>false</value>
   </property>
   ```

2. 修改 mapred-site.xml：

   ```shell
   # 它也是计算层, 每个计算层都有自己的历史服务器
   # 修改 mapred-site.xml
   vi mapred-site.xml
   ```

   ```xml
   <!-- 添加以下配置 -->
   <!-- 开启历史服务器功能 -->
   <property>
       <name>mapred.job.history.server.embedded</name>
       <value>true</value>
   </property>
   <!-- 对应的地址 -->
   <property>
       <name>mapreduce.jobhistory.address</name>
       <value>hadoop03:10020</value>
   </property>
   <!-- 对应的 WebUI 地址 -->
   <property>
       <name>mapreduce.jobhository.webapp.address</name>
       <value>hadoop03:50060</value>
   </property>
   <property>
       <name>mapreduce.jobhistory.intermediate-done-dir</name>
       <value>/work/mr_history_tmp</value>
   </property>
   <property>
       <name>mapreduce.jobhistory.done-dir</name>
       <value>/work/mr-history_done</value>
   </property>
   ```

3. 分发修改后的配置文件：

   ```shell
   scp yarn-site.xml mapred-site.xml hadoop02:`pwd`
   scp yarn-site.xml mapred-site.xml hadoop03:`pwd`
   scp yarn-site.xml mapred-site.xml hadoop04:`pwd`
   ```

### 4.启动

#### 4.1 切换到 hadoop01

1. 启动 yarn，如下图：

   ```shell
   # 切换到 hadoop01 /etc/hadoop
   cd $HADOOP_HOME
   cd etc
   cd hadoop
   # 这个脚本会代替我们把 NodeManager 运行起来
   # 但是 ResourceManager 运行不起来, 定义在了 hadoop03, hadoop04
   start-yarn.sh
   ```

   ![hadoop01 启动 yarn 脚本 start-yarn.sh](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01_start-yarn-sh.png)

2. 手动启动 hadoop03，hadoop04 的 ResourceManager，如下图：

   ```shell
   # 切换到 hadoop03, hadoop04
   # 分别手动启动 resourcemanager
   yarn-daemon.sh start resourcemanager
   ```

   ![hadoop03 手动启动 ResourceManager](D:\ideaProject\bigdata\bigdata-spark\image\hadoop03_start-resourcemanager.png)

   ![hadoop04 手动启动 ResourceManager](D:\ideaProject\bigdata\bigdata-spark\image\hadoop04_start-resourcemanager.png)

3. 访问 hadoop03 HadoopWebUI，如下图：

   ![hadoop03 的 HadoopWebUI](D:\ideaProject\bigdata\bigdata-spark\image\访问HadoopWebUI.png)

4. 此时跑一个 wordcount 的程序，它的历史记录会被记录下来吗？如下图：

   ```shell
   # 切换到 hadoop01 的 share/hadoop/mapreduce
   cd $HADOOP_HOME
   cd share/hadoop/mapreduce
   # 运行 hadoop examples
   hadoop jar hadoop-mapreduce-examples-2.6.5.jar wordcount /sparktest/data.txt /fuck
   ```

   ![hadoop01 wordcount运行结果](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01_examples-wordcount.png)

   ![HadoopWebUI 运行 wordcount 结果](D:\ideaProject\bigdata\bigdata-spark\image\HadoopWebUI_wordcount运行成功.png)

5. 进入 History，会发现看不到，如下图：

   ![无法访问](D:\ideaProject\bigdata\bigdata-spark\image\jobhistory.png)

6. hadoop03 启动 mapreduce 的 jobhistory service，如下图：

   ```shell
   # hadoop03
   mr-jobhistory-daemon.sh start historyserver
   ```

   ![hadoop03 启动 mr-jobhistory](D:\ideaProject\bigdata\bigdata-spark\image\hadoop03_mr-jobhistory-historyserver.png)

   ![HDFS 中会有 mr-history_done, mr_history_tmp 两个目录](D:\ideaProject\bigdata\bigdata-spark\image\mr_history_done_tmp两个目录.png)

   ![再次访问 History](D:\ideaProject\bigdata\bigdata-spark\image\再次访问History.png)

7. Counters 会罗列出所有维度，曾经在执行这个作业结果时，做很多统计，在 hadoop01 shell 中也能追溯回来，所有的数据都在这统计，做调优或对比以及 Cluster状态，都可以在此处看，如下图：

   ![HadoopWebUI Counters](D:\ideaProject\bigdata\bigdata-spark\image\Counters.png)

   ![hadoop01_shell-Counters](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01_shell-Counters.png)

#### 4.2 但这是 MapReduce，跟 Spark 完全没关系，Spark 有自己的历史记录服务器

#### 4.3 以上 YARN Cluster 搭建成功

#### 4.4 启动 Spark

1. **切换到 hadoop01 Spark 目录下**

   ```shell
   # 切换到 hadoop01 Spark 目录下
   cd $SPARK_HOME
   # 此时应该启动什么呢 ? 在只有以下进程的情况下
   [root@hadoop01 spark-2.3.4-bin-hadoop2.6]# jps
   3009 DFSZKFailoverController
   2722 JournalNode
   5076 Jps
   2446 NameNode
   # 还是去 Spark sbin 或 bin 目录下去执行什么东西 ?
   # 一定要记住, 有了 Yarn 后不需要再去启动 Spark 它的什么服务了
   # 直接启动 spark-shell 就可以了
   
   # 此处还有一个小知识点
   # 无论是 spark-shell, submit 后边会接一个 --master 它后边有如下几种
   # spark://host:port : 可以跑在资源层为 standalone 的 Spark 自己的
   # mesos://host:port : 可以是一个第三方的 mesos
   # yarn : 可以是一个第三方的 yarn
   # k8s://https://host:port : 可以是一个第三方的 k8s
   # 以上都是属于分布式, 在此模式下会有 deploy-mode, 它的 Driver 分为 Client/Cluster
   # local (Default: local[*]) : 或者是多少个 local 线程并行度(这个叫做单机的)
   --master MASTER_URL         spark://host:port, mesos://host:port, yarn,
                               k8s://https://host:port, or local (Default: local[*]).
   --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
                               on one of the worker machines inside the cluster ("cluster")
                               (Default: client).
   # 启动 Spark Yarn 资源层
   ./spark-shell --master yarn
   ```

2. **资源层换了，其他东西没变，计算层只有资源层联系的映射处换了，其他没有发生变化，会卡很长时间，它会去找配置项，没有配置就开始 uploading 上边的东西了，如下图：**

   ![spark-shell yarn会卡很长时间](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell_yarn会启动的特别慢.png)

3. **spark-shell yarn 启动成功，如下图：**

   ![spark-shell yarn 启动成功](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell_yarn成功启动.png)

4. **访问 8088，会有一个 Application SPARK，如下图：**

   ![访问 8088 HadoopWebUI](D:\ideaProject\bigdata\bigdata-spark\image\访问HadoopWebUI有Application_Spark.png)

5. **启动 Spark 独立的 History：**

   ```shell
   # 切换到 hadoop01 Spark sbin 目录下
   cd $SPARK_HOME
   cd sbin
   ./start-history-server.sh
   ```

6. **hadoop01 启动 spark-shell --master yarn，hadoop01 会有一个 SparkSubmit 的进程，如下图：**

   ![SparkSubmit](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell-master-yarn-SparkSubmit.png)

7. **hadoop02 会有 ExecutorLauncher 进程，如下图：**

   ![ExecutorLauncher](D:\ideaProject\bigdata\bigdata-spark\image\hadoop02_ExecutorLauncher.png)

8. **hadoop03 会有 CoarseGrainedExecutorBackend 进程，如下图：**

   ![hadoop03_CoarseGrainedExecutorBackend](D:\ideaProject\bigdata\bigdata-spark\image\hadoop03_CoarseGrainedExecutorBackend.png)

9. **hadoop04 也会有 CoarseGrainedExecutorBackend 进程，如下图：**

   ![hadoop04_CoarseGrainedExecutorBackend](D:\ideaProject\bigdata\bigdata-spark\image\hadoop04_CoarseGrainedExecutorBackend.png)

10. **Spark 支持 Clien Mode，强制改 Cluster Mode 会报错：**

    ```shell
    ./spark-shell --master yarn --deploy-mode cluster
    # 以下是报错信息
    Error: Cluster deploy mode is not applicable to Spark shells.
    Run with --help for usage help or --verbose for debug output
    ```

11. **所以 Spark 只支持 Client Mode，因为要回收数据。此时就凸显一个概念，修改 submit.sh：**

    ```sh
    #--total-executor-cores 6 \
    #--executor-cores 1 \
    class=org.apache.spark.examples.SparkPi
    jar=$SPARK_HOME/examples/jars/spark-examples_2.11-2.3.4.jar
    #master=spark://hadoop01:7077,hadoop02:7077
    master=yarn
    $SPARK_HOME/bin/spark-submit \
    --master $master \
    --deploy-mode cluster \
    --class  $class \
    $jar \
    1000
    ```

*运行 submit.sh，Cluster Mode 是要把 Driver 推到集群中放到 ApplicationMaster 身上，客户端其实可有可无已经可以把它挂掉了，人家那边还能继续调度。*

##### 4.4.1 总结

***Driver 在哪，其实就是 ApplicationMater，回顾 ApplicationMaster 是怎么诞生的？***

***也是 Yarn 挑了一台不忙的机器，启动了一个 ApplicationMaster。***

**Client Mode Driver 一定会在 Client 进程中，其实 Driver 会跑到集群启动的 ApplicatonMaster，由它去把这个 Driver 变成一个对象，然后它自行执行资源的调度，仅此一个差异。**

#### 4.4 两种 Mode 的差异：

| Mode / API | ExecutorLauncher | ApplicationMaster |
| :--------: | :--------------: | :---------------: |
|   Client   |      **√**       |         ×         |
|  Cluster   |        ×         |       **√**       |

|   Spark / Mode    | Client | Client |
| :---------------: | :----: | :----: |
|       shell       | **√**  |   ×    |
| submit （非repl） | **√**  | **√**  |

### 5.它为什么会慢呢？

在起步阶段，会发现它会变的很慢，它有一系列的 Uploading libraries，生成库的事。

[官网查看研究](https://archive.apache.org/dist/spark/docs/2.3.4/running-on-yarn.html)

调优

```shell
# 切换到 Spark conf 目录下
cd $SPARK_HOME
cd conf
# 指向所有的 jar 包
vi spark-defaults.conf
spark.yarn.jars hdfs://mycluster/work/spark_lib/jars/*
# 创建目录
hdfs dfs -mkdir -p /work/spark_lib/jars
# 切换到 jars 目录下
cd ../jars
# 把所有 jar 包上传至 HDFS
hdfs dfs -put ./* /work/spark_lib/jars
```

所有 jar 包上传至 HDFS，如下图：

![hdfs put jars](D:\ideaProject\bigdata\bigdata-spark\image\jars上传至HDFS.png)



再次进入 spark-shell master yarn，会发现没有 Uploading libraries 的提示，如下图：

![没有 Uploading libraries相关提示](D:\ideaProject\bigdata\bigdata-spark\image\没有了Uploadinglibraries的相关提示.png)

会多出压缩包，如下图：

![.sparkStaging](D:\ideaProject\bigdata\bigdata-spark\image\sparkStaging.png)

### 6.调度

#### 6.1 shell 有， submit 也有，效果都一样

```shell
# standalone HA
--driver-memory MEM
--executor-memory MEM
# 期望消耗的内存, 核心数量
--executor-cores NUM
# 在 YARN 中换了另外一个维度
# 总共有多少个 executor
--num-executors NUM
```

#### 6.2 Yarn 是资源层，历史服务器是计算层 MapReduce

------

