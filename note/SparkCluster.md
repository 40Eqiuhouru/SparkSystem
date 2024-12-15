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

1. hadoop01 启动 HDFS，如下图：

   ![start-dfs.sh](D:\ideaProject\bigdata\bigdata-spark\image\hadoop01-start-dfs.png)

2. 访问 HDFS-WebUI，如下图：

   ![HDFS-WebUI](D:\ideaProject\bigdata\bigdata-spark\image\HDFS-WebUI.png)

3. hadoop01 启动 ./start-all.sh，如下图：

   ![Spark ./start-all.sh](D:\ideaProject\bigdata\bigdata-spark\image\Spark-start-all-sh.png)

4. 访问 Spark-WebUI，如下图：

   ![Spark-WebUI](D:\ideaProject\bigdata\bigdata-spark\image\Spark-WebUI.png)

5. spark-shell 默认 local，如果想分布式计算程序，就必须加 --master：

   ![spark-shell_Master](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell_Master.png)

6. Spark-WebUI 会有一个 Application，Spark shell 运行：

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