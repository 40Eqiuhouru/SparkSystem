# Spark 框架

## 章节5 : Spark-CORE，复习 Hadoop 生态，梳理术语， HadoopRDD 源码解析

已经执行的任务 RDD，如果别的任务有服用则直接使用

```scala
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
    //    fileRDD.flatMap(_.split(" ")
    //    ).map((_, 1)
    //    ).reduceByKey(_ + _).foreach(println)


    val words: RDD[String] = fileRDD.flatMap((x: String) => {
      x.split(" ")
    })
    // hello
    // world
    // 单元素的 words 转成一个 tuple 键值对的元素
    val pairword: RDD[(String, Int)] = words.map((x: String) => {
      new Tuple2(x, 1)
    })
    // (hello,1)
    // (hello,1)
    // (world,1)
    // key 相同的元素进行 reduceByKey() 聚合
    val res: RDD[(String, Int)] = pairword.reduceByKey((x: Int, y: Int) => {
      x + y
    })
    // x:oldvalue, y:value
    // (hello,2) -> (2, 1)
    // (world,1) -> (1, 1)
    // 打印结果, 执行算子
    // 以上代码不会发生计算, 有一种描述是 RDD 是惰性执行的,
    // 也就是它并不会去真正的执行, 什么是执行 ?
    // 它要遇到个 x 算子, 就是执行算子, 在遇到 foreach() 并且要打印其中的值时,
    // 只有遇到它有执行的, 最终要拿着数据集干点什么事的时候, 才会真正发生计算,
    // 如果不写这行, 以上代码根本不会执行.

    // wordCountCount : 统计出现指定次数的单词有几个
    // (hello,2) -> (2, 1)
    val fanzhuan: RDD[(Int, Int)] = res.map((x) => {
      (x._2, 1)
    })
    // 基于 数字几个的 进行统计
    val resOver:RDD[(Int, Int)] = fanzhuan.reduceByKey(_ + _)
    // 第一个 Job
    res.foreach(println)
    // 进行打印
    // 第二个 Job
    resOver.foreach(println)
    Thread.sleep(Long.MaxValue)

  }

}

```

localhost:40404 是 Spark UI。

- A Resilient Distributed Dataset

  ###### 一个弹性的分布式数据集。

- A list of partitions SPLITS

  ###### 一个 RDD 有几个分区

- A function for computing each split

- A list of dependencies on other RDDs

  ###### 一个 RDD 可能依赖了多个 RDD 得到，而且和父级 RDD 具有一系列不同的关系。

- a Partitioner for key-value RDDs、

  ###### RDD 不止可以存单元素也可以存 KV，但此处强调的是分区器，通过 K 做分区计算拿 K 最简单的哈希取模。

- a list of preferred locations to compute each split

  

------

### Spark WordCount 源码分析——上

#### 第一个问题 : 第一个 RDD 是怎么得来的 ?

```scala
// 单词统计
// DATASET
// fileRDD : 数据集
// 1.进入 textFile()
// val fileRDD: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\testdata.txt", 6)
// 最后有几个分区, 取决于二者谁的并行度最高
// fileRDD 返回的就是 HadoopRDD
// 如果文件 8G, 16个块, 每块 512M, 期望 32个切片, 32个分区, 期望的切片大小是 256M, 256和一个块比, 取的就是 256 了
val fileRDD: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\testdata.txt")
```

minPartitions ： 最小分区数量，如果计算的文件是分布式文件，它有 10 个 Block 块，那么关键就是如果在传第二个参数时，这个数值有可能 </=/> 分区数 10，那么三个值代表什么意思 ?

如果传了一个 12(最少分 12 个)，虽然可能达到 12 个并行度，但最少也得满足 12 个分区，看谁大。

```scala
  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   * @param path path to the text file on a supported file system
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of lines of the text file
   */
  def textFile(
      // 传了一个文件路径, 可以传本地的也可以传 HDFS 这种分布式文件
      // 代表文件并行度, 就是块的数量和希望传最小值的数量
      // 两者取最大值
      path: String,
      // 并行度最高优先
      minPartitions: Int = defaultMinPartitions): RDD[String] = withScope {
    assertNotStopped()
      // TextInputFormat 有两个功能: 1.算出这个数据有多少个切片, 2.可以拿到对每一个切片的输入格式化的输入记录读取器 RecordReader
      // LongWritable 这个和曾经的 MR 设置输入输出格式化的 job.setInputFormatClass() 一样
      // 2.进入 hadoopFile()
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
  }

/** Get an RDD for a Hadoop file with an arbitrary InputFormat
   *
   * @note Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD or directly passing it to an aggregation or shuffle
   * operation will create many references to the same object.
   * If you plan to directly cache, sort, or aggregate Hadoop writable objects, you should first
   * copy them using a `map` function.
   * @param path directory to the input data files, the path can be comma separated paths
   * as a list of inputs
   * @param inputFormatClass storage format of the data to be read
   * @param keyClass `Class` of the key associated with the `inputFormatClass` parameter
   * @param valueClass `Class` of the value associated with the `inputFormatClass` parameter
   * @param minPartitions suggested minimum number of partitions for the resulting RDD
   * @return RDD of tuples of key and corresponding value
   */
  def hadoopFile[K, V](
      path: String,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int = defaultMinPartitions): RDD[(K, V)] = withScope {
    assertNotStopped()

    // This is a hack to enforce loading hdfs-site.xml.
    // See SPARK-11227 for details.
    FileSystem.getLocal(hadoopConfiguration)

    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(new SerializableConfiguration(hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
      // 3.进入 HadoopRDD()
    new HadoopRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

// RDD 的第一个子类实现
@DeveloperApi
class HadoopRDD[K, V](
    sc: SparkContext,
    broadcastedConf: Broadcast[SerializableConfiguration],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int)
  extends RDD[(K, V)](sc, Nil) with Logging
```

```scala
// HadoopRDD 中的方法
// 怎么得到所有分区
override def getPartitions: Array[Partition] = {
    val jobConf = getJobConf()
    // add the credentials here as this can be called before SparkContext initialized
    SparkHadoopUtil.get.addCredentials(jobConf)
    // 输入格式化类有两个功能, 一个是拿, 一个是记录器
    // 此处出现了切片计算, 从而可以得出其实面向 Hadoop 文件操作时, 分区应该就是切片这个维度
    // paration 其实是等于 split 的
    // 4.进入 getSplits()
    // 如果想知道 RDD 有多少个分区, 最核心的就是此处的输入格式化类
    // 用当前文件计算出有多少个切片
    // 默认切片数量 = RDD 分区数量
    val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, minPartitions)
    val inputSplits = if (ignoreEmptySplits) {
      allInputSplits.filter(_.getLength > 0)
    } else {
      allInputSplits
    }
    val array = new Array[Partition](inputSplits.size)
    for (i <- 0 until inputSplits.size) {
      array(i) = new HadoopPartition(id, i, inputSplits(i))
    }
    array
  }
```

```java
  /** 
   * Logically split the set of input files for the job.  
   * 
   * <p>Each {@link InputSplit} is then assigned to an individual {@link Mapper}
   * for processing.</p>
   *
   * <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
   * input files are not physically split into chunks. For e.g. a split could
   * be <i>&lt;input-file-path, start, offset&gt;</i> tuple.
   * 
   * @param job job configuration.
   * @param numSplits the desired number of splits, a hint.
   * @return an array of {@link InputSplit}s for the job.
   */
  // 5.是一个接口, 查看子类实现 org.apache.hadoop.mapred.FileInputFormat
  InputSplit[] getSplits(JobConf job, int numSplits) throws IOException;
```

```java
  /** Splits files returned by {@link #listStatus(JobConf)} when
   * they're too big.*/ 
  // 切片计算的逻辑
  // 文件路径, 期望的最小分区数
  // getSplits() : 算出当前文件最终可以切成多少个切片, 且每一个切片的元数据要登记出来
  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException {
    Stopwatch sw = new Stopwatch().start();
    FileStatus[] files = listStatus(job);
    
    // Save the number of input files for metrics/loadgen
    job.setLong(NUM_INPUT_FILES, files.length);
    long totalSize = 0;                           // compute total size  
    for (FileStatus file: files) {                // check we have valid files
      if (file.isDirectory()) {
        throw new IOException("Not a file: "+ file.getPath());
      }
      totalSize += file.getLen();
    }

    // 所有文件的大小 / 传入的期望分区数 = 期望切片大小   
    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    // 如果认为不干预, 默认为 1  
    long minSize = Math.max(job.getLong(org.apache.hadoop.mapreduce.lib.input.
      FileInputFormat.SPLIT_MINSIZE, 1), minSplitSize);

    // generate splits
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    // 拿到文件
    for (FileStatus file: files) {
      // 拿到文件路径  
      Path path = file.getPath();
      // 拿到文件大小  
      long length = file.getLen();
      if (length != 0) {
        FileSystem fs = path.getFileSystem(job);
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          // 得到文件的所有块的列表, 这是一个数组
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(fs, path)) {
          // 拿到文件的块的大小  
          long blockSize = file.getBlockSize();
          // goalSize : 期望切片大小
          // 6.进入 computeSplitSize()
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          // 首先将文件大小赋给一个值, 这个值会一直变化, 尤其在后续每次减一个切片大小
          // 每循环一次, 是否还能满足一次切片  
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            // 得到切片的主机位
            // 7.进入 getSplitHostsAndCachedHosts()
            // 取此切片偏移量, 这个切片属于哪个块
            // 拿到块后, 其实就可以取出这个块对应的主机
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                // 每次相减的就是 offset 的值                                                
                length-bytesRemaining, splitSize, clusterMap);
            // 面对文件的所有切片
            // 这个切片列表中会添加创建切片
            // 切片是由归属哪个文件, 切片的偏移量, 切片大小, 切片对应的块在哪个主机
            // 登记切片元数据
            // 最核心的四个维度 : 
            // path : 当前切片归属哪个文件.
            // length-bytesRemaining : 切片的起始偏移量面向的文件.
            // splitSize : 切片大小.
            // splitHosts[0] : 切片可以向哪台主机移动.
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                splitHosts[0], splitHosts[1]));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                - bytesRemaining, bytesRemaining, clusterMap);
            splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                splitHosts[0], splitHosts[1]));
          }
        } else {
          String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,0,length,clusterMap);
          splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.elapsedMillis());
    }
    return splits.toArray(new FileSplit[splits.size()]);
  }
```

```java
// 很简单的一个判断关系
protected long computeSplitSize(long goalSize, long minSize,
                                       long blockSize) {
    // 先是小块中取最小值, 是你期望那个切片的大小和块的大小取最小值
    // 如果文件是 8G, 期望 8 个分区, 那么切 8 个片, 一个切片 1G, 期望值就是 1G
    // 但这个文件可能 16块, 这才 512M, 那么取最小值取的是 1块 的大小
    // 块大小取出后, 和最小值 minSize 比一个最大值, 那么取的还是块
    // 换言之, 如果文件的块数量是 16个, 期望分 8个
    // 那么块的大小一定是小于期望值, 块的数量大于期望的数量
    // 所以最终取成块的数量, 也就是俗称的默认切片等于块的大小
    return Math.max(minSize, Math.min(goalSize, blockSize));
}
```

```java
  /** 
   * This function identifies and returns the hosts that contribute 
   * most for a given split. For calculating the contribution, rack
   * locality is treated on par with host locality, so hosts from racks
   * that contribute the most are preferred over hosts on racks that 
   * contribute less
   * @param blkLocations The list of block locations
   * @param offset 
   * @param splitSize 
   * @return two arrays - one of hosts that contribute most to this split, and
   *    one of hosts that contribute most to this split that have the data
   *    cached on them
   * @throws IOException
   */
  private String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations, 
      long offset, long splitSize, NetworkTopology clusterMap)
  throws IOException {
    // 新老版本有一个差异
    // 新版本此方法被提出去了
    // 最终执行的此方法
    // 8.进入 getBlockIndex()
    int startIndex = getBlockIndex(blkLocations, offset);

    long bytesInThisBlock = blkLocations[startIndex].getOffset() + 
                          blkLocations[startIndex].getLength() - offset;

    //If this is the only block, just return
    if (bytesInThisBlock >= splitSize) {
      return new String[][] { blkLocations[startIndex].getHosts(),
          blkLocations[startIndex].getCachedHosts() };
    }

    long bytesInFirstBlock = bytesInThisBlock;
    int index = startIndex + 1;
    splitSize -= bytesInThisBlock;

    while (splitSize > 0) {
      bytesInThisBlock =
        Math.min(splitSize, blkLocations[index++].getLength());
      splitSize -= bytesInThisBlock;
    }

    long bytesInLastBlock = bytesInThisBlock;
    int endIndex = index - 1;
    
    Map <Node,NodeInfo> hostsMap = new IdentityHashMap<Node,NodeInfo>();
    Map <Node,NodeInfo> racksMap = new IdentityHashMap<Node,NodeInfo>();
    String [] allTopos = new String[0];

    // Build the hierarchy and aggregate the contribution of 
    // bytes at each level. See TestGetSplitHosts.java 

    for (index = startIndex; index <= endIndex; index++) {

      // Establish the bytes in this block
      if (index == startIndex) {
        bytesInThisBlock = bytesInFirstBlock;
      }
      else if (index == endIndex) {
        bytesInThisBlock = bytesInLastBlock;
      }
      else {
        bytesInThisBlock = blkLocations[index].getLength();
      }
      
      allTopos = blkLocations[index].getTopologyPaths();

      // If no topology information is available, just
      // prefix a fakeRack
      if (allTopos.length == 0) {
        allTopos = fakeRacks(blkLocations, index);
      }

      // NOTE: This code currently works only for one level of
      // hierarchy (rack/host). However, it is relatively easy
      // to extend this to support aggregation at different
      // levels 
      
      for (String topo: allTopos) {

        Node node, parentNode;
        NodeInfo nodeInfo, parentNodeInfo;

        node = clusterMap.getNode(topo);

        if (node == null) {
          node = new NodeBase(topo);
          clusterMap.add(node);
        }
        
        nodeInfo = hostsMap.get(node);
        
        if (nodeInfo == null) {
          nodeInfo = new NodeInfo(node);
          hostsMap.put(node,nodeInfo);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
          if (parentNodeInfo == null) {
            parentNodeInfo = new NodeInfo(parentNode);
            racksMap.put(parentNode,parentNodeInfo);
          }
          parentNodeInfo.addLeaf(nodeInfo);
        }
        else {
          nodeInfo = hostsMap.get(node);
          parentNode = node.getParent();
          parentNodeInfo = racksMap.get(parentNode);
        }

        nodeInfo.addValue(index, bytesInThisBlock);
        parentNodeInfo.addValue(index, bytesInThisBlock);

      } // for all topos
    
    } // for all indices

    // We don't yet support cached hosts when bytesInThisBlock > splitSize
    return new String[][] { identifyHosts(allTopos.length, racksMap),
        new String[0]};
  }
```

```java
protected int getBlockIndex(BlockLocation[] blkLocations, 
                              long offset) {
    // 遍历文件的物理的每个块, 先取出第一个块
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      // 拿出块的偏移量
      // 切片的偏移量要 > 块的起始位置, 同时切片的偏移量要 < 块的结束
      // 也就是这个切片就在这个块中, 被块包含
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        // 所以返回这个块的索引
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length -1];
    long fileLength = last.getOffset() + last.getLength() -1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }
```

**以上流程即第一个特性，通过 getPartitions() 会知道 RDD 映射成数据源有几个分区。**

#### 第二个问题 : 未来数据怎么进行计算的 ?

```scala
// 会传递一个参数 Partition 在哪个分区
// 会返回一个迭代器, 其实此方法中并没有发生计算, 但拿着迭代器只有在被调用才会发生计算
// sc.textFile(file) 怼的是一个文件, 这文件怎么就变成一个迭代器返回了 ?
override def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[(K, V)] = {
    // new 了一个迭代器
    // 1.进入 NextIterator[(K, V)]
    // 回过头看为什么 new NextIterator, 然后后续实现
    // 其实此处即是包装了对文件的 IO, 并包装成了一个迭代器
    val iter = new NextIterator[(K, V)] {

      private val split = theSplit.asInstanceOf[HadoopPartition]
      logInfo("Input split: " + split.inputSplit)
      private val jobConf = getJobConf()

      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets InputFileBlockHolder for the file block's information
      split.inputSplit.value match {
        case fs: FileSplit =>
          InputFileBlockHolder.set(fs.getPath.toString, fs.getStart, fs.getLength)
        case _ =>
          InputFileBlockHolder.unset()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] = split.inputSplit.value match {
        case _: FileSplit | _: CombineFileSplit =>
          Some(SparkHadoopUtil.get.getFSBytesReadOnThreadCallback())
        case _ => None
      }

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      // 记录读取器, 它是什么值呢 ?
      private var reader: RecordReader[K, V] = null
      private val inputFormat = getInputFormat(jobConf)
      HadoopRDD.addLocalConfiguration(
        new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(createTime),
        context.stageId, theSplit.index, context.attemptNumber, jobConf)

      // 还是通过 inputFormat
      reader =
        try {
          // 记录读取器
          // 而且在 textFile() 中默认返回的是 LineRecordReader
          inputFormat.getRecordReader(split.inputSplit.value, jobConf, Reporter.NULL)
        } catch {
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
            null
        }
      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener { context =>
        // Update the bytes read before closing is to make sure lingering bytesRead statistics in
        // this thread get correctly added.
        updateBytesRead()
        closeIfNeeded()
      }

      private val key: K = if (reader == null) null.asInstanceOf[K] else reader.createKey()
      private val value: V = if (reader == null) null.asInstanceOf[V] else reader.createValue()

      // 最重要的实现 getNext()
      // 只有实现 getNext, NextIterator.hasNext() 才能被成功调起
      override def getNext(): (K, V) = {
        try {
          // 逆推, 往上找
          // 预加载了一条记录到 KV 中, 并更新了 finished
          finished = !reader.next(key, value)
        } catch {
          case e: IOException if ignoreCorruptFiles =>
            logWarning(s"Skipped the rest content in the corrupted file: ${split.inputSplit}", e)
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        // 并把两条记录返回
        (key, value)
      }

      override def close(): Unit = {
        if (reader != null) {
          InputFileBlockHolder.unset()
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.inputSplit.value.isInstanceOf[FileSplit] ||
                     split.inputSplit.value.isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(split.inputSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning("Unable to get input size to set InputMetrics for task", e)
            }
          }
        }
      }
    }
    // 把迭代器进行包装
    // 整个 compute 是 new 而非执行
    // 在经过 HadoopRDD 时, 没有产生计算
    new InterruptibleIterator[(K, V)](context, iter)
  }
```

```scala
package org.apache.spark.util

/** Provides a basic/boilerplate Iterator implementation. */
private[spark] abstract class NextIterator[U] extends Iterator[U] {

  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false

  /**
   * Method for subclasses to implement to provide the next element.
   *
   * If no next element is available, the subclass should set `finished`
   * to `true` and may return any value (it will be ignored).
   *
   * This convention is required because `null` may be a valid value,
   * and using `Option` seems like it might create unnecessary Some/None
   * instances, given some iterators might be called in a tight loop.
   *
   * @return U, or set 'finished' when done
   */
  // 但自己的 getNext() 是空的
  protected def getNext(): U

  /**
   * Method for subclasses to implement when all elements have been successfully
   * iterated, and the iteration is done.
   *
   * <b>Note:</b> `NextIterator` cannot guarantee that `close` will be
   * called because it has no control over what happens when an exception
   * happens in the user code that is calling hasNext/next.
   *
   * Ideally you should have another try/catch, as in HadoopRDD, that
   * ensures any resources are closed should iteration fail.
   */
  protected def close()

  /**
   * Calls the subclass-defined close method, but only once.
   *
   * Usually calling `close` multiple times should be fine, but historically
   * there have been issues with some InputFormats throwing exceptions.
   */
  def closeIfNeeded() {
    if (!closed) {
      // Note: it's important that we set closed = true before calling close(), since setting it
      // afterwards would permit us to call close() multiple times if close() threw an exception.
      closed = true
      close()
    }
  }

  // 需要调用 hasNext()
  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        // 关注 getNext()
        // 如果调 getNext() 返回了数据, 里边 finished = false, 有数据了, 没有结束
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    // 此处为 false, 取反为 true, 代表有数据
    !finished
  }

  // 有数据就会调 next()
  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    // 反馈就是 hasNext() 准备的 nextValue 的 KV, 数据就被拿走了
    nextValue
  }
}
```

RDD 并不存数据。

------

**HBase 中用到了在微服务架构中的什么 ? 客户端获取数据时经过了一个什么过程 ?**

Zookeeper 注册发现，尤其元数据的 root 信息要写到 Zookeeepr 中, 客户端是从获得一个简单的 root meta 元数据表的位置和 RegionServer 进行通信，所以这以过程其实就是利用的注册发现。

### Spark WordCount 源码分析——下

```scala
// 1.进入 flatMap()
val words: RDD[String] = fileRDD.flatMap((x: String) => {
      x.split(" ")
})
```

```scala
  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = withScope {
    // Spark 是一个分布式计算, 很多函数要分发出去在不同节点, 此时会牵扯到一个东西 --- 闭包
    // clean() 就是检查函数中所有的逻辑当中用到的资源对象能否被序列化
    val cleanF = sc.clean(f)
    // new 了一个 MapPartitionsRDD, 且没有被执行
    // 2.进入 MapPartitionsRDD
    // 未来这个 (context, pid, iter) => iter.flatMap(cleanF) 函数传进去后
    // 如果用这个函数的时, 需要传三个参数, 这个函数被传进入后, 不一定在哪执行
    // 一旦函数被使用, 需要把三个参数传进去, 会用这个迭代器的 flatMap()
    // 本来是想在 RDD 上用 flatMap(), 最终底层干活时, 调了一个迭代器
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
  }
```

```scala
package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 *
 * @param prev the parent RDD.
 * @param f The function used to map a tuple of (TaskContext, partition index, input iterator) to
 *          an output iterator.
 * @param preservesPartitioning Whether the input function preserves the partitioner, which should
 *                              be `false` unless `prev` is a pair RDD and the input function
 *                              doesn't modify the keys.
 * @param isOrderSensitive whether or not the function is order-sensitive. If it's order
 *                         sensitive, it may return totally different result when the input order
 *                         is changed. Mostly stateful functions are order-sensitive.
 */
// 父类一定又是一个 RDD, 且是 RDD 的子类
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    // 前面的 RDD 作为参数传给父级
    // prev 存的是前一个 RDD 的地址, 有点像单向链表
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isOrderSensitive: Boolean = false)
  // 3.进入 RDD 单参构造
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  // 思考一个问题 : 通过 faltMap() 得到了一个新的 RDD, 二者间虽然有些对象引用,
  // 但数据是怎么过来的 ?
  // 最重要的方法
  // 想一件事 : 这个方法被调起才会返回迭代器, 那么是哪调的前面的 compute(P): Iterator ?
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    // 调用的是前一个 RDD 的迭代器
    // 4.HadoopRDD 并没有 Iterator(), 但其父类有
    f(context, split.index, firstParent[T].iterator(split, context))

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }

  override protected def getOutputDeterministicLevel = {
    if (isOrderSensitive && prev.outputDeterministicLevel == DeterministicLevel.UNORDERED) {
      DeterministicLevel.INDETERMINATE
    } else {
      super.getOutputDeterministicLevel
    }
  }
}
```

```scala
  /** Construct an RDD with just a one-to-one dependency on one parent */
  // 4.进入 RDD
  def this(@transient oneParent: RDD[_]) =
    // 调用自己的另一个构造方法
    // 前面有几个分区, 此处就有几个分区一一对应
    this(oneParent.context, List(new OneToOneDependency(oneParent)))
```

```scala
// 两个参数
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,
    // 固定的一比一关系
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging 
```

```scala
  /**
   * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
   * This should ''not'' be called by users directly, but is available for implementors of custom
   * subclasses of RDD.
   */
  // 最终得到一个概论, 只要外界调了 RDD 的一个Iterator, 如果内存缓存和持久化都找不到的话
  // 它一定会调自己的 compute()
  final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      // 获取数据
      getOrCompute(split, context)
    } else {
      // 从持久化中读取
      // 进入
      computeOrReadCheckpoint(split, context)
    }
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD is checkpointing.
   */
  // 无论进入哪个, 都会有 compute()
  private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointedAndMaterialized) {
      firstParent[T].iterator(split, context)
    } else {
      compute(split, context)
    }
  }
```

然后在 words 基础上调 map()

```scala
    // hello
    // world
    // 单元素的 words 转成一个 tuple 键值对的元素
    // 5.进入 map()
    // 无论是 flatMap(), 还是 map() 都是针对的一条记录的转化操作, 
    // 并不关心操作的一条记录之外的其他记录
    val pairword: RDD[(String, Int)] = words.map((x: String) => {
      new Tuple2(x, 1)
    })
```

```scala
  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  // 和之前的环节相同
  def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
```

```scala
    // (hello,1)
    // (hello,1)
    // (world,1)
    // key 相同的元素进行 reduceByKey() 聚合
    // 它关注的是相同 key 的一组数据
    // 老值 + 新值
    // 6.进入 reduceByKey()
    val res: RDD[(String, Int)] = pairword.reduceByKey((x: Int, y: Int) => {
      x + y
    })

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce.
   */
  // (oldValue, Value) => x + y
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // combine 压缩减少 IO
    // (value1: V) => value1 : 进来一个 value1, 返回一个 value1
    // 第二三个参数 : 取老值 + 新值
    // 8.进入 combineByKeyWithClassTag()
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): RDD[(K, V)] = self.withScope {
    // 哈希分区器
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  // 只传了一个参数, 一个匿名函数
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = self.withScope {
    // 默认分区器
    // 7.进入 defaultPartitioner()
    reduceByKey(defaultPartitioner(self), func)
  }
```

```scala
    
    // If the existing max partitioner is an eligible one, or its partitions number is larger
    // than the default number of partitions, use the existing partitioner.
    if (hasMaxPartitioner.nonEmpty && (isEligiblePartitioner(hasMaxPartitioner.get, rdds) ||
        defaultNumPartitions < hasMaxPartitioner.get.getNumPartitions)) {
      hasMaxPartitioner.get.partitioner.get
    } else {
      // reduce 在做统计时, 默认分区器即哈希分区器      
      new HashPartitioner(defaultNumPartitions)
    }
```

```scala
  /**
   * :: Experimental ::
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   *
   * Users provide three functions:
   *
   *  - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   *  - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   *  - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   *
   * @note V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]).
   */
  @Experimental
  def combineByKeyWithClassTag[C](
      // 初始化
      createCombiner: V => C,
      // (_ + _)
      mergeValue: (C, V) => C,
      // 溢写的数据合并
      mergeCombiners: (C, C) => C,
      // 分区器
      partitioner: Partitioner,
      // 在前面的 map 环境中要触发 combine, 把这个 1 先加起来
      // 变成一条记录再输出, 而且默认值为 true
      // 即触发 map 合并 combine 的过程
      mapSideCombine: Boolean = true,
      // 序列化
      serializer: Serializer = null)(implicit ct: ClassTag[C]): RDD[(K, C)] = self.withScope {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("HashPartitioner cannot partition array keys.")
      }
    }
    val aggregator = new Aggregator[K, V, C](
      self.context.clean(createCombiner),
      self.context.clean(mergeValue),
      self.context.clean(mergeCombiners))
    if (self.partitioner == Some(partitioner)) {
      self.mapPartitions(iter => {
        val context = TaskContext.get()
        new InterruptibleIterator(context, aggregator.combineValuesByKey(iter, context))
      }, preservesPartitioning = true)
    } else {
      // 9.进入 ShuffledRDD
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer) // 序列化
        .setAggregator(aggregator) // 聚合操作
        .setMapSideCombine(mapSideCombine) // 是否要触发 map 端汇聚
    }
  }
```

(hello, 1)

(hello, 1)

(hello, 1)

一堆 (单词，1) 是怎么在分布式情况下经过 Shuffle 变成 (单词，n) ?

用了一个 reduceByKey()，传了一个函数进去，老值 + 新值 一个匿名函数，但是这个 reduceByKey() 中调的时候，会先分配一个分区器，因为它一定会触发 Shuffle，Shuffle 一定会使用分区器 partitioner，让相同的 key 得到一个相同的分区号，而且默认是 HashPartition，最终调的是 combineByKeyWithClassTag()，其实 Spark 中替我们做了些优化，自带的 combine 回去数据减少网络 IO，如果想完成 combine，数据被汇聚聚合压缩也需要三步：1.记录怎么进，2.后续 value 怎么求和，3.多次溢写怎么整合汇聚。

所以此时 combineByKeyWithClassTag 有三个参数

```scala
package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index: Int = idx
}

/**
 * :: DeveloperApi ::
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */
// TODO: Make this return RDD[Product2[K, C]] or have some way to configure mutable pairs
@DeveloperApi
class ShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    // 禁止序列化, reduce 不用再跑一遍 map
    @transient var prev: RDD[_ <: Product2[K, V]],
    part: Partitioner)
  // 10.进入 RDD
  extends RDD[(K, C)](prev.context, Nil) {

  private var userSpecifiedSerializer: Option[Serializer] = None

  private var keyOrdering: Option[Ordering[K]] = None

  private var aggregator: Option[Aggregator[K, V, C]] = None

  private var mapSideCombine: Boolean = false

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, C] = {
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  /** Set key ordering for RDD's shuffle. */
  def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  /** Set aggregator for RDD's shuffle. */
  def setAggregator(aggregator: Aggregator[K, V, C]): ShuffledRDD[K, V, C] = {
    this.aggregator = Option(aggregator)
    this
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  def setMapSideCombine(mapSideCombine: Boolean): ShuffledRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val serializer = userSpecifiedSerializer.getOrElse {
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override protected def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
      .read()
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
```

```scala
/**
* Implemented by subclasses to return how this RDD depends on parent RDDs. This method will only
* be called once, so it is safe to implement a time-consuming computation in it.
*/
// 父类 RDD 有一个 getDependencies() 可以被重写
protected def getDependencies: Seq[Dependency[_]] = deps  

override def getDependencies: Seq[Dependency[_]] = {
    val serializer = userSpecifiedSerializer.getOrElse {
      val serializerManager = SparkEnv.get.serializerManager
      if (mapSideCombine) {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[C]])
      } else {
        serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
      }
    }
    List(new ShuffleDependency(prev, part, serializer, keyOrdering, aggregator, mapSideCombine))
  }
```

------

## 章节6 : Spark-CORE，wordcount 案例源码分析，图解

图解详见 Idea 工程中 image 目录。

------



## 章节7：Spark-CORE，集合操作 API，pvuv 分析，RDD 源码分析

**1.Spark_RDD_API_Example**

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD API
 */
object Lesson01_RDD_Api01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("test01")
    val sc = new SparkContext(conf)

    // 在内存中产生数据集
    val dataRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 4, 3, 2, 1))

    //    dataRDD.map()
    //    dataRDD.flatMap()
    //    dataRDD.filter((x: Int) => {
    //      x > 3
    //    })
    // 精简写法
    // 打印数据集中大于 3 的元素
    val filterRDD: RDD[Int] = dataRDD.filter(_ > 3)
    val res01: Array[Int] = filterRDD.collect()
    res01.foreach(println)

    println("--------------------")

    // 数据集元素去重
    val res: RDD[Int] = dataRDD.map((_, 1)).reduceByKey(_ + _).map(_._1)
    res.foreach(println)

    // Api 完全去重
    val resx: RDD[Int] = dataRDD.distinct()
    resx.foreach(println)

    println("--------------------")

    // 面向数据集开发  面向数据集的 API  1.基础 API 2.复合 API
    // RDD(HadoopRDD, MapPartitionsRDD, ShuffledRDD...)
    // map, flatMap, filter
    // distinct
    // reduceByKey : 复合 -> combineByKey()

    // 面向数据集 : 交并差 关联 笛卡尔积

    // 面向数据集 : 元素 -> 单元素, KV 元素 -> 机构化, 并非结构化

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5))
    val rdd2: RDD[Int] = sc.parallelize(List(3, 4, 5, 6, 7))
    println(rdd1.partitions.size)
    println(rdd2.partitions.size)
    // 思考为什么没有产生 Shuffle ?
    // 两个独立的数据集最终变成了一个数据集,
    // 这个数据有没有发生移动 ?
    // union() 不会传递数据集怎么加工, 每个元素应该怎么变
    // 只是抽象出有一个映射关系
    // 应用场景
    // 公司数据的加工, 可能有 50 张表, 这 50 张表前面可能每张表就是一个案例
    // 有 50 个案例, 各自调用不同的计算, 最终数据都长的一样了, 未来可能都使用一个过滤逻辑
    // 这样就可以使用 union() 合并, 然后点一个 filter() 就可以了
    // 1.进入 union()
    val unitRDD: RDD[Int] = rdd1.union(rdd2)
    println(unitRDD.partitions.size)
    unitRDD.foreach(println)

    // 阻塞等待, 方便源码分析
    while (true) {
    }
  }
}
```

```scala
/**
 * Return the union of this RDD and another one. Any identical elements will appear multiple
 * times (use `.distinct()` to eliminate them).
 */
def union(other: RDD[T]): RDD[T] = withScope {
  // SparkContext 的方法
  // 2.进入 union()
  sc.union(this, other)
}

/** Build the union of a list of RDDs. */
def union[T: ClassTag](rdds: Seq[RDD[T]]): RDD[T] = withScope {
  val partitioners = rdds.flatMap(_.partitioner).toSet
  if (rdds.forall(_.partitioner.isDefined) && partitioners.size == 1) {
    new PartitionerAwareUnionRDD(this, rdds)
  } else {
    // 3.进入 UnionRDD()
    new UnionRDD(this, rdds)
  }
}

@DeveloperApi
class UnionRDD[T: ClassTag](
    sc: SparkContext,
    // 只有自己的成员属性, 里边放的是父级的 RDD
    var rdds: Seq[RDD[T]])
  extends RDD[T](sc, Nil) {  // Nil since we implement getDependencies

  // visible for testing
  private[spark] val isPartitionListingParallel: Boolean =
    rdds.length > conf.getInt("spark.rdd.parallelListingThreshold", 10)

  // 根据之前学的理论, RDD 体系中, 只要是 RDD 继承 RDD
  // 其实每个子类都必须实现分区 getPartitions, 然后实现数据如何计算 compute
  // 数据分区
  override def getPartitions: Array[Partition] = {
    val parRDDs = if (isPartitionListingParallel) {
      val parArray = rdds.par
      parArray.tasksupport = UnionRDD.partitionEvalTaskSupport
      parArray
    } else {
      rdds
    }
    // 此处的 parRDDs 就是前面的两个分区
    // 用 map() 不断拿出 RDD, 每拿出一个 RDD 就调用自己的 partitions.length 拿到大小
    // 经过 map() 转换后其实都变成了一个有两个 1 的集合
    val array = new Array[Partition](parRDDs.map(_.partitions.length).seq.sum)
    var pos = 0
    // 此处分为两个循环
    // 第一个循环 zipWithIndex 算子会变成一个两个元素的方法, (a, b) -> (a, 0), (b, 1)
    // 带序号的键值对
    // 第二个嵌套循环, 用前面的 RDD 的分区数是几就会循环几次
    // 拿到前面的 rdd1 快速的把它所有分区遍历一次
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      // 目标结果的 pos 号分区对应的是哪个 RDD(0 号分区对应父级 RDD)
      // rddIndex : 是第几号
      // split.index : 分区在这个 RDD 中是第几个分区
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      // 目标结果的 RDD 的下一个分区指向其他的 RDD 的哪个分区
      pos += 1
    }
    // 最终返回一个 Array
    array
  }

  // 依赖关系
  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      // 是一个区间性关系
      // 当前 RDD 并不是 oneToOne 的关系, 是由多个 RDD 变成了一个
      deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
      pos += rdd.partitions.length
    }
    deps
  }

  // 数据计算
  // 传递的其实是某一个分区
  // 拿到了前面的真正的分区迭代器传递到来, 再向后传递
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
```

**2.cogroup 底层实现：**

```scala
// 1.进入 cogroup()
val cogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = kv1.cogroup(kv2)
cogroup.foreach(println)
```

```scala
/**
 * For each key k in `this` or `other1` or `other2`, return a resulting RDD that contains a
 * tuple with the list of values for that key in `this`, `other1` and `other2`.
 */
def cogroup[W1, W2](other1: RDD[(K, W1)], other2: RDD[(K, W2)])
    : RDD[(K, (Iterable[V], Iterable[W1], Iterable[W2]))] = self.withScope {
  // 2.进入 cogroup()
  cogroup(other1, other2, defaultPartitioner(self, other1, other2))
}
```

```scala
/**
 * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
 * list of values for that key in `this` as well as `other`.
 */
def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
    : RDD[(K, (Iterable[V], Iterable[W]))] = self.withScope {
  if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
    throw new SparkException("HashPartitioner cannot partition array keys.")
  }
  // 3.进入 CoGroupRDD[]()
  val cg = new CoGroupedRDD[K](Seq(self, other), partitioner)
  cg.mapValues { case Array(vs, w1s) =>
    (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
  }
}
```

```scala
package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{CompactBuffer, ExternalAppendOnlyMap}

/**
 * The references to rdd and splitIndex are transient because redundant information is stored
 * in the CoGroupedRDD object.  Because CoGroupedRDD is serialized separately from
 * CoGroupPartition, if rdd and splitIndex aren't transient, they'll be included twice in the
 * task closure.
 */
// CoGroupRDD 是来自于两个 RDD
private[spark] case class NarrowCoGroupSplitDep(
    @transient rdd: RDD[_],
    @transient splitIndex: Int,
    var split: Partition
  ) extends Serializable {

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    split = rdd.partitions(splitIndex)
    oos.defaultWriteObject()
  }
}

/**
 * Stores information about the narrow dependencies used by a CoGroupedRdd.
 *
 * @param narrowDeps maps to the dependencies variable in the parent RDD: for each one to one
 *                   dependency in dependencies, narrowDeps has a NarrowCoGroupSplitDep (describing
 *                   the partition for that dependency) at the corresponding index. The size of
 *                   narrowDeps should always be equal to the number of parents.
 */
private[spark] class CoGroupPartition(
    override val index: Int, val narrowDeps: Array[Option[NarrowCoGroupSplitDep]])
  extends Partition with Serializable {
  override def hashCode(): Int = index
  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * :: DeveloperApi ::
 * An RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * @param rdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 *
 * @note This is an internal API. We recommend users use RDD.cogroup(...) instead of
 * instantiating this directly.
 */
@DeveloperApi
class CoGroupedRDD[K: ClassTag](
    @transient var rdds: Seq[RDD[_ <: Product2[K, _]]],
    part: Partitioner)
  extends RDD[(K, Array[Iterable[_]])](rdds.head.context, Nil) {

  // For example, `(k, a) cogroup (k, b)` produces k -> Array(ArrayBuffer as, ArrayBuffer bs).
  // Each ArrayBuffer is represented as a CoGroup, and the resulting Array as a CoGroupCombiner.
  // CoGroupValue is the intermediate state of each value before being merged in compute.
  private type CoGroup = CompactBuffer[Any]
  private type CoGroupValue = (Any, Int)  // Int is dependency number
  private type CoGroupCombiner = Array[CoGroup]

  private var serializer: Serializer = SparkEnv.get.serializer

  /** Set a serializer for this RDD's shuffle, or null to use the default (spark.serializer) */
  def setSerializer(serializer: Serializer): CoGroupedRDD[K] = {
    this.serializer = serializer
    this
  }

  // 最终结果和前面的每一个 rdd 可能存在着不同的依赖关系
  override def getDependencies: Seq[Dependency[_]] = {
    // rdds 代表结果的 rdd 和前面父级 rdd, 有两个父级
    // 这两个 rdd 是一个数据集调用 map
    // 也就是说每一个 rdd 会进此处的方法体中
    rdds.map { rdd: RDD[_] =>
      if (rdd.partitioner == Some(part)) {
        logDebug("Adding one-to-one dependency with " + rdd)
        new OneToOneDependency(rdd)
      } else {
        // 有可能后续根本就没有产生 shuffle(后续会涉及这个点)
        // 全部都有可能是 shuffle 的
        logDebug("Adding shuffle dependency with " + rdd)
        new ShuffleDependency[K, Any, CoGroupCombiner](
          rdd.asInstanceOf[RDD[_ <: Product2[K, _]]], part, serializer)
      }
    }
  }

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](part.numPartitions)
    for (i <- 0 until array.length) {
      // Each CoGroupPartition will have a dependency per contributing RDD
      array(i) = new CoGroupPartition(i, rdds.zipWithIndex.map { case (rdd, j) =>
        // Assume each RDD contributed a single dependency, and get it
        dependencies(j) match {
          case s: ShuffleDependency[_, _, _] =>
            None
          case _ =>
            Some(new NarrowCoGroupSplitDep(rdd, i, rdd.partitions(i)))
        }
      }.toArray)
    }
    array
  }

  override val partitioner: Some[Partitioner] = Some(part)

  // 计算
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = dependencies.length

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    // numRdds 可能和前面的两个 rdd 有窄依赖/宽依赖/shuffle 依赖
    // 不管怎么样, 有两种依赖关系, 可能相同可能不同, 依赖集和数据集要对应
    for ((dep, depNum) <- dependencies.zipWithIndex) dep match {
      case oneToOneDependency: OneToOneDependency[Product2[K, Any]] @unchecked =>
        val dependencyPartition = split.narrowDeps(depNum).get.split
        // Read them from the parent
        val it = oneToOneDependency.rdd.iterator(dependencyPartition, context)
        rddIterators += ((it, depNum))

      // 如果是 shuffle 依赖关系
      case shuffleDependency: ShuffleDependency[_, _, _] =>
        // Read map outputs of shuffle
        val it = SparkEnv.get.shuffleManager
          .getReader(shuffleDependency.shuffleHandle, split.index, split.index + 1, context)
          .read() // 就会产生一个 shuffle 读的过程, read() 会返回一个迭代器
        // 拿到迭代器后, 把对应的分区号对应上
        // 最终会完成 : 目标的 rdd 中 compute 计算时
        // 拿到前面每个父级的数据迭代器
        rddIterators += ((it, depNum))
    }

    // 粗略看作为 HashMap
    val map = createExternalMap(numRdds)
    // 先拿左边的迭代器, 再拿右边的迭代器
    for ((it, depNum) <- rddIterators) {
      // 插入所有数据
      // 第一次循环拿到左边的父级迭代器
      // 第二次循环拿到右边的父级迭代器
      map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
    }
    context.taskMetrics().incMemoryBytesSpilled(map.memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(map.diskBytesSpilled)
    context.taskMetrics().incPeakExecutionMemory(map.peakMemoryUsedBytes)
    new InterruptibleIterator(context,
      map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
  }

  private def createExternalMap(numRdds: Int)
    : ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner] = {

    val createCombiner: (CoGroupValue => CoGroupCombiner) = value => {
      val newCombiner = Array.fill(numRdds)(new CoGroup)
      newCombiner(value._2) += value._1
      newCombiner
    }
    val mergeValue: (CoGroupCombiner, CoGroupValue) => CoGroupCombiner =
      (combiner, value) => {
      combiner(value._2) += value._1
      combiner
    }
    val mergeCombiners: (CoGroupCombiner, CoGroupCombiner) => CoGroupCombiner =
      (combiner1, combiner2) => {
        var depNum = 0
        while (depNum < numRdds) {
          combiner1(depNum) ++= combiner2(depNum)
          depNum += 1
        }
        combiner1
      }
    new ExternalAppendOnlyMap[K, CoGroupValue, CoGroupCombiner](
      createCombiner, mergeValue, mergeCombiners)
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
```

**3.RDD_API_Sort_Operations**

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD API Sort Operations
 */
object Lesson02_RDD_Api_Sort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sort")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // PV, UV
    // 需求 : 根据数据计算各网站的 PV, UV 同时只显示 top5
    // 解题 : 要按 PV 值, 或 UV 值排序, 取前 5 名
    val file: RDD[String] = sc.textFile("data/pvuvdata", 5)
    // PV :
    // 187.144.73.116	浙江	2018-11-12	1542011090255	3079709729743411785	www.jd.com	Comment
    println("----------- PV -----------")

    // line 代表一行数据, split() 切割成一个数组, 标 1 键值对输出
    val pair: RDD[(String, Int)] = file.map(line => (line.split("\t")(5), 1))

    val reduce: RDD[(String, Int)] = pair.reduceByKey(_ + _)
    // 翻转 KV
    val map: RDD[(Int, String)] = reduce.map(_.swap)
    val sorted: RDD[(Int, String)] = map.sortByKey(false)
    val res: RDD[(String, Int)] = sorted.map(_.swap)
    val pv: Array[(String, Int)] = res.take(5)
    pv.foreach(println)

    println("----------- UV -----------")

    // 187.144.73.116	浙江	2018-11-12	1542011090255	3079709729743411785	www.jd.com	Comment

    val keys: RDD[(String, String)] = file.map(
      line => {
        val strs: Array[String] = line.split("\t")
        (strs(5), strs(0))
      }
    )
    val key: RDD[(String, String)] = keys.distinct()
    val pairUV: RDD[(String, Int)] = key.map(k => (k._1, 1))
    val uvReduce: RDD[(String, Int)] = pairUV.reduceByKey(_ + _)
    val unSorted: RDD[(String, Int)] = uvReduce.sortBy(_._2, false)
    val uv: Array[(String, Int)] = unSorted.take(5)
    uv.foreach(println)

    while (true) {
    }
  }
}
```

请问以上作业有几个 Job ?

PV Compute Level：

```scala
// 从 textFile() 算起
val file: RDD[String] = sc.textFile("data/pvuvdata", 5) // 创建算子
val pair: RDD[(String, Int)] = file.map(line => (line.split("\t")(5), 1)) // 转换算子
val reduce: RDD[(String, Int)] = pair.reduceByKey(_ + _) // 转换算子
val map: RDD[(Int, String)] = reduce.map(_.swap) // 转换算子
val sorted: RDD[(Int, String)] = map.sortByKey(false) // 转换算子
val res: RDD[(String, Int)] = sorted.map(_.swap) // 转换算子
val pv: Array[(String, Int)] = res.take(5) // 执行算子, 且只有这一步没有返回 RDD, 上面都在返回 RDD
// 以上会产生一个 Job
```

UV Compute Level：

```scala
// 还是从 file 算起
val keys: RDD[(String, String)] = file.map(
  line => {
    val strs: Array[String] = line.split("\t")
    (strs(5), strs(0))
  }
) // 转换算子
val key: RDD[(String, String)] = keys.distinct() // 转换算子
val pairUV: RDD[(String, Int)] = key.map(k => (k._1, 1)) // 转换算子
val uvReduce: RDD[(String, Int)] = pairUV.reduceByKey(_ + _) // 转换算子
val unSorted: RDD[(String, Int)] = uvReduce.sortBy(_._2, false) // 转换算子
val uv: Array[(String, Int)] = unSorted.take(5) // 执行算子
// 貌似看上去, 这个作业会有两个 Job
// 但事实是这样吗 ?
```

SparkUI 运行结果：

![SparkUI](D:\ideaProject\bigdata\bigdata-spark\image\Lesson02_Spark_RDD_API_Sort.png)

Idea 运行结果：

![Idea_Result](D:\ideaProject\bigdata\bigdata-spark\image\需求_根据数据计算各网站的 PV, UV 同时只显示 top5_result.png)

一共 6 个 Job，会发现 sortByKey() 算子触发了一个 Job。

###### 第一个问题：但是从代码中，根据之前的理论，sortByKey() 它不是一个转换算子吗？怎么能触发 Job 呢？并且 PV，UV 的 sortByKey() 都触发了一个Job。

###### 第二个问题：take() 是执行算子，take() 了两次，但是它返回的数据没错，就是五条，但是应该是两个 Job，为什么最终 6 个 Job？。

**sortByKey() 不做计算，会先跑一次对数据的抽样，样本抽出来后，先划分格子，把各自准备好后，才能真正去跑抽取所有数据并放到正确的格子中，这个结果才是全排序。**

------

## 章节8：Spark-CORE，聚合计算 API，combineByKey()，分区调优

**1.Spark RDD Aggregator Compute**

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD Aggregator Compute
 */
object Lesson03_RDD_Aggregator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[(String, Int)] = sc.parallelize(List(
      ("Syndra", 123),
      ("Syndra", 456),
      ("Syndra", 789),
      ("NingHY", 321),
      ("NingHY", 654),
      ("NingHY", 987),
      ("WangZX", 135),
      ("WangZX", 246),
      ("WangZX", 357)
    ))

    // 当有 key 的情况下, 相同的 value -> 一组
    // 只有键值对的数据集上才有 groupByKey()
    // 行 -> 列
    val group: RDD[(String, Iterable[Int])] = data.groupByKey()
    group.foreach(println)

    println("--------------------")

    // 行列转换
    // 列 -> 行 第一种方式
    // map() 是一进一出, flatMap() 是一进多出
    // flatMap(e => : (String, Iterable[Int]) => Unit)
    val res01: RDD[(String, Int)] = group.flatMap(e => e._2.map(x => (e._1, x)).iterator)
    res01.foreach(println)

    println("--------------------")

    // flatMapValues(f : Iterable[Int] => Unit) : 前面是扁平化, 后面是只会把 value 传到函数中
    // 列 -> 行 第二种方式
    group.flatMapValues(e => e.iterator).foreach(println)
    println("--------------------")

    // 取出每组中排序后的前 2 的数据
    group.mapValues(e => e.toList.sorted.take(2)).foreach(println)
    println("--------------------")

    group.flatMapValues(e => e.toList.sorted.take(2).iterator).foreach(println)
    println("--------------------")

    // 在行列转换时, 数据的体积会发生变化吗 ?
    // 数据的体积其实没有发生太大的变化, 因为 value 并没有约少, 更想表示的是
    // 像 group 这类操作, 尤其是 groupByKey() 这种操作, 每人有三行 -> 每人一行, 但它的列数会变多, 数据其实没有相应减少的
    // 后续调优时, 再进行相应进阶

    println("---------- sum, count, max, min, avg ----------")

    val sum: RDD[(String, Int)] = data.reduceByKey(_ + _)
    val max: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov > nv) ov else nv)
    val min: RDD[(String, Int)] = data.reduceByKey((ov, nv) => if (ov < nv) ov else nv)
    // 曾经的数据集, value 已经没有意义了, 因为要做 count
    // 要把它变成 ("Syndra", 1) 且 key 不变
    // 而且每条记录进来还是只输出一条记录一对一的输出
    val count: RDD[(String, Int)] = data.mapValues(e => 1).reduceByKey(_ + _)
    // 计算平均值
    // 用 sum / count, 这是语义上的, 怎么实现 ?
    // sum 要和 count 的值两个值相遇到内存里然后才能进到一个函数中计算
    // 相遇要写一个独立的函数实现
    // 那么现在是 sum 是一个独立的数据集, count 是一个独立的数据集, 两个独立的数据集怎么相遇呢 ?
    // 恰巧它们具有相同的 key, 这其实就是一个关联操作, 但是用 union() 还是 join() ?
    // union() 是把一个数据集垂直拼进去, join() 是按照相同的 key 把不同的字段拼成一行
    val tmp: RDD[(String, (Int, Int))] = sum.join(count)
    val avg: RDD[(String, Int)] = tmp.mapValues(e => e._1 / e._2)

    println("---------- sum ----------")
    sum.foreach(println)

    println("---------- max ----------")
    max.foreach(println)

    println("---------- min ----------")
    min.foreach(println)

    println("---------- count ----------")
    count.foreach(println)

    println("---------- avg ----------")
    avg.foreach(println)

    // 其实分布式计算时都有这么一个调优的过程
    // 如果能在前面的 map 端把数据量压小的话, 后续 shuffle 时 IO 量也会变小
    // 一次计算完成求 avg, 缺点是比较繁琐
    val tmpx: RDD[(String, (Int, Int))] = data.combineByKey(
      //      createCombiner: V => C, 第一条记录的 value 怎么放入 hashmap
      (value: Int) => (value, 1),
      //      mergeValue: (C, V) => C, 如果有第二条记录, 第二条以及以后的它们的 value 怎么放到 hashmap 中
      (oldValue: (Int, Int), newValue: Int) => (oldValue._1 + newValue, oldValue._2 + 1),
      //      mergeCombiners: (C, C) => C 合并溢写结果的函数
      (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)
    )
    tmpx.mapValues(e => e._1 / e._2).foreach(println)

    while (true) {
    }
  }
}
```

```scala
/**
 * Group the values for each key in the RDD into a single sequence. Hash-partitions the
 * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
 * within each group is not guaranteed, and may even differ each time the resulting RDD is
 * evaluated.
 *
 * @note This operation may be very expensive. If you are grouping in order to perform an
 * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
 * or `PairRDDFunctions.reduceByKey` will provide much better performance.
 */
def groupByKey(): RDD[(K, Iterable[V])] = self.withScope {
  // 2.进入 groupKey()
  groupByKey(defaultPartitioner(self))
}

/**
 * Group the values for each key in the RDD into a single sequence. Allows controlling the
 * partitioning of the resulting key-value pair RDD by passing a Partitioner.
 * The ordering of elements within each group is not guaranteed, and may even differ
 * each time the resulting RDD is evaluated.
 *
 * @note This operation may be very expensive. If you are grouping in order to perform an
 * aggregation (such as a sum or average) over each key, using `PairRDDFunctions.aggregateByKey`
 * or `PairRDDFunctions.reduceByKey` will provide much better performance.
 *
 * @note As currently implemented, groupByKey must be able to hold all the key-value pairs for any
 * key in memory. If a key has too many values, it can result in an `OutOfMemoryError`.
 */
def groupByKey(partitioner: Partitioner): RDD[(K, Iterable[V])] = self.withScope {
  // groupByKey shouldn't use map side combine because map side combine does not
  // reduce the amount of data shuffled and requires all map side data be inserted
  // into a hash table, leading to more objects in the old gen.
  val createCombiner = (v: V) => CompactBuffer(v)
  val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
  val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
  // 由此可见, groupKey() 的底层实现也是基于 combineByKeyWothClassTag[]()
  val bufs = combineByKeyWithClassTag[CompactBuffer[V]](
    createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
  bufs.asInstanceOf[RDD[(K, Iterable[V])]]
}
```

Avg 的 Job 复杂度：

![Avg的Job复杂度](D:\ideaProject\bigdata\bigdata-spark\image\Avg求Job的复杂度.png)

这张图好像很乱，因为有两次各自的 shuffle，但是这个来自的数据集是同一个数据集，曾经数据集只有一批数据，但是这一批数据先要计算通过 shuffle 拉取到一台机器，然后相同的数据集又要参与一次计算，再通过网络 shuffle 再发给一台机器，有一台机器收了他们两个结果之后，内存相遇后才能拿出计算的方法求出平均值。

###### 如果你有两个数据集：

- ###### 第一步：解决相遇的问题；

- ###### 第二步：才是解决计算逻辑的问题；

**那么如何优化**？

**首先要找到问题：数据被拉起独立计算不同的逻辑两次，，再把这句话减少，什么是优化？**

**数据被拉起两次计算，完成两个独立的结果集，如果想优化，最本质的思想就是：数据能不能拉起一次内存计算就会产生两个结果集。**

**接下来就是如何实现的问题。**

------

一次计算完成求 Avg：

![OnceComputeFinishAvg](D:\ideaProject\bigdata\bigdata-spark\image\OnceComputeFinishAvg.png)

------

**2.Spark RDD Oriented Partitions Operations**

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * Spark RDD Oriented Partitions Operations<br>
 * {{{
 *   // 迭代器的正确使用方式
 *     val res03: RDD[String] = data.mapPartitionsWithIndex(
 *       (pIndex, pIter) => {
 *         new Iterator[String] {
 *           println(s"----$pIndex--conn--MySQL----")
 *
 *           override def hasNext = if (pIter.hasNext == false) {
 *             println(s"----$pIndex--close--MySQL----")
 *             false
 *           } else true
 *
 *           override def next(): String = {
 *             val value = pIter.next()
 *             println(s"----$pIndex--SELECT $value----")
 *             value + "SELECTED"
 *           }
 *         }
 *       }
 *     )
 *     res03.foreach(println)
 * }}}
 */
object Lesson04_RDD_Partitions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("partitions")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10, 2)

    // 外关联 SQL 查询
    val res01: RDD[String] = data.map(
      (value: Int) => {
        println("----conn--MySQL----")
        println(s"----SELECT $value----")
        println("close--MySQL----")
        value + "SELECTED"
      }
    )
    res01.foreach(println)

    println("--------------------")

    val res02: RDD[String] = data.mapPartitionsWithIndex(
      (pIndex, pIter) => {
        val lb = new ListBuffer[String] // 致命的!!! 根据之前的源码发现, Spark 是一个 Pipline, 迭代器嵌套的模式
        // 数据不会在内存积压
        println(s"----$pIndex--conn--MySQL----")
        while (pIter.hasNext) {
          val value: Int = pIter.next()
          println(s"----$pIndex--SELECT $value----")
          lb.+=(value + "SELECTED")
        }
        println("close--MySQL----")
        lb.iterator
      }
    )
    res02.foreach(println)

    println("---------- Iterator Optimize -----------")

    // 如果不是一对一的处理, 如果 new 了一个迭代器, 被调用了后
    // 后续调的时候不是调一次从父级取一次, 也就是说
    // 调了 hasNext() 后, 可能拿了一个字符串, 然后后续再调的时候, 可能要把一个一个单词返回
    // 然后此时又伴随数据库的初始化, 连接的初始化, 就必须自行重写迭代器
    // 那怎么能满足一对多的输出 ?
    // 迭代器的正确使用方式
    val res03: RDD[String] = data.mapPartitionsWithIndex(
      (pIndex, pIter) => {
        //        pIter.flatMap()
        //        pIter.map()

        new Iterator[String] {
          println(s"----$pIndex--conn--MySQL----")

          override def hasNext = if (pIter.hasNext == false) {
            println(s"----$pIndex--close--MySQL----")
            false
          } else true

          override def next(): String = {
            val value = pIter.next()
            println(s"----$pIndex--SELECT $value----")
            value + "SELECTED"
          }
        }
      }
    )
    res03.foreach(println)
  }
}
```

分区操作中，迭代器的进阶使用方式。

------

**3.Spark RDD High Level**

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD High-Level Operations
 */
object Lesson05_RDD_HighLevel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10, 5)

    // 数据抽样
    // 种子 : 一般抽样都会涉及到种子的概念, 如果给出种子, 相同的种子多少次抽到的都是相同的那批数据
    // sample(是否允许重新抽, 抽取占比, 种子)
    // 一种场景
    // A, B 在同一个项目组, 数据其中一批在集群中, 然后 A, B 共同开发
    // 二人约定种子都是 22, A 在本地拿 22 抽出一批数据, B 在另外一台机器中也用 22 抽出一批数据
    // 然后 A, B 抽到的数据相同, 如果有时项目中刻意的有相同代码
    // 要基于相同的样本去开发业务逻辑时, 那就用 22 约定一个种子就可以了
    //    data.sample(false, 0.1, 222).foreach(println)
    //    println("--------------------")
    //    data.sample(false, 0.1, 222).foreach(println)
    //    println("--------------------")
    //    data.sample(false, 0.1, 221).foreach(println)
    //    println("--------------------")

    // 大数据中, 并行度是计算提速的一个方式
    // 但是并行度绝对不是说越高越好, 要保证一个并行度粒度中的数据量
    // 即符合 IO 又符合内存的消耗, 因为创建进程的时间也有损耗
    // 如果只有 3台 机器, 每个两个核心的话, 理论上最多的并行度是 6 个 JVM
    // 一个 JVM 消耗一个 CPU, 6 个就够了, 但是有可能几十个 RDD 关联后, 它的分区变的好多, 并行度就上去了
    // 那么此时其实同意时间只能有 6 个进程的话, 如果有 60 个分区, 那就要分十次跑完
    // 那还不如就把它变成 6 个分区, 每个分区数量大一点, 所以调整分区是一定会在未来的工作
    // 在业务层面之外调优不可避免, 而同时是面试环节必须要聊的一件事, 含金量很高
    // 这其中细活很多!!!
    println(s"data:${data.getNumPartitions}")

    println("--------------------")

    // 为每条记录打印标识
    val data1: RDD[(Int, Int)] = data.mapPartitionsWithIndex(
      (pI, pT) => {
        pT.map(e => (pI, e))
      }
    )
    data1.foreach(println)

    println("--------------------")

    // 1.调整分区量
    // 1.1.进入 repartition()
    val repartition = data1.repartition(4)

    // 转换后的数据分区
    val res: RDD[(Int, (Int, Int))] = repartition.mapPartitionsWithIndex(
      (pI, pT) => {
        // 曾经是哪个分区的哪条数据
        pT.map(e => (pI, e))
      }
    )

    println(s"data:${res.getNumPartitions}")
    res.foreach(println)

    while (true) {
    }
  }
}
```

在调整分区的过程中会不会产生 shuffle？

![调整分区是否会产生shuffle](D:\ideaProject\bigdata\bigdata-spark\image\调整分区数量的过程会不会产生shuffle.png)

repartition 的过程一定会产生一个 shuffle，因为如果没有这个过程，是不能够把一个分区的数据从一台机器移动到另一台机器的，所以 repartition 是依靠 shuffle 的方式，让记录再重新计算它的分区号，然后再通过 shuffle-write，shuffle-read 拉取数据几个计算机数据洗牌的过程。

```scala
/**
 * Return a new RDD that has exactly numPartitions partitions.
 *
 * Can increase or decrease the level of parallelism in this RDD. Internally, this uses
 * a shuffle to redistribute data.
 *
 * If you are decreasing the number of partitions in this RDD, consider using `coalesce`,
 * which can avoid performing a shuffle.
 *
 * TODO Fix the Shuffle+Repartition data loss issue described in SPARK-23207.
 */
def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
  // 调用 coalesce() 算子, 即传递了要缩减/放大的分区数量, 同时固定的传递了第二个参数为 true
  // 就是要触发 shuffle, 其实干活的是 coalesce(), 它可以产生 shuffle 也可以不产生 shuffle
  coalesce(numPartitions, shuffle = true)
}
```

------

## 章节9：Spark-CORE，二次排序，分组取TopN，算子综合应用

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark_RDD_算子综合应用 :<br>
 * <ul>
 * <li>分组取 TopN</li>
 * </ul>
 */
object Lesson06_RDD_Over {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // TopN, 分组取 TopN, 其中会牵扯到二次排序的概念
    // 2019-6-1	39
    // 需求 : 同月份中, 温度最高的两天
    val file: RDD[String] = sc.textFile("data/tqdata")

    // 隐式转换 Ordering, 用于排序
    implicit var ordered = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compare(x._2)
    }

    println("---------- 分组取 TopN-V1 ----------")
    val data: RDD[(Int, Int, Int, Int)] = file.map(line => line.split("\t")).map(arr => {
      val arrs: Array[String] = arr(0).split("-")
      // (year, month, day, wd)
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    })

    //    // 分组取 TopN V1
    //    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = data.map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
    //    val res: RDD[((Int, Int), List[(Int, Int)])] = grouped.mapValues(arr => {
    //      // 关注点 : 相同年月中, 日期和温度有什么特征
    //      // 19年三条数据, 如果让你处理的话, 如果变成一个数组, 你应该关注哪些问题 ?
    //      // 1.同一月份中相同天中过滤掉那些低温只留一个最高温, 作为这一天的代表
    //      // 2.如果数据去重后, 把剩下的数据做个排序, 写成倒序就可以了
    //      // 有了 map 后, 让你进入这个数组相同的 19年6月 这个数组中所有的每天的温度
    //      // 这一堆数据要遍历放进 map
    //      val map = new mutable.HashMap[Int, Int]()
    //      arr.foreach(x => {
    //        if (map.get(x._1).getOrElse(0) < x._2) map.put(x._1, x._2)
    //      })
    //      map.toList.sorted(new Ordering[(Int, Int)] {
    //        // 用温度排序, 且是 y : x, 这样返回就是倒序
    //        override def compare(x: (Int, Int), y: (Int, Int)): Int = y._2.compare(x._2)
    //      })
    //    })
    //    res.foreach(println)
    //    println("--------------------")

    // 思考 : 以上代码中, 有没有什么潜在风险或问题 ?
    // 1.groupByKey(), 比如 value 整个 6月份的数据达到了 10T, 那么它就溢出了
    // 因为最终会发现它的 value 是要拼成一个迭代器的数据集, 真正计算时, 是要占用内存的
    // groupByKey() 在工作中不建议使用, 除非 key 的 value 数量很少, 那么可以用
    // 2.map 中要开辟对象, 在这承载所有数据, 最终再把它输出, 因此内存利用率上有可能出现 OOM

    println("---------- 分组取 TopN-V2 ----------")

    // 分组取 TopN V2
    // 有个排序的过程
    // 此时其实还可以拿着这个数据集怎么去做, 也可以实现 TopN 的结果
    // 这里面还要关注数据集的特征, 06-01 重复的数据先要去重, 然后相同的 key 划分到一块, 然后再排序
    // 此前是把去重和排序放到了一个算子中去做, 有可能会产生溢出
    // 那么是否可以用什么换什么 ? 拿时间拿速度来换内存不溢出的方案
    // 取相同的年月日中的 max
    val reduced: RDD[((Int, Int, Int), Int)] = data.map(t4 => ((t4._1, t4._2, t4._3), t4._4)).reduceByKey((x: Int, y: Int) => if (y > x) y else x)
    // t2._1._1 年, t2._1._2 月, t2._1._3 日, t2._2 值
    val maped: RDD[((Int, Int), (Int, Int))] = reduced.map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
    val grouped: RDD[((Int, Int), Iterable[(Int, Int)])] = maped.groupByKey()
    // 取 Top2
    grouped.mapValues(arr => arr.toList.sorted.take(2)).foreach(println)

    while (true) {
    }
  }
}
```

### **第一个 Job** 的执行成本，如下图所示：

![分组取TopN-V1](D:\ideaProject\bigdata\bigdata-spark\image\分组取TopN-V1.png)

它有两个 **Stage** 也就是两步，Stage 之间的曲线代表发生了一次 **shuffle**，底层的 **groupNyKey()** 要走 **shuffle** 拉取它相同 **key** 为一组的数据拉过来才能发生后续的计算。

###### 问题：用了 **groupByKey()** 容易 **OOM**，且自己的算子实现了函数：**去重，排序**。

### **第二个 Job**，如下图所示：

![分组取TopN-V2](D:\ideaProject\bigdata\bigdata-spark\image\分组取TopN-V2.png)

拿了一步 **shuffle**，先解决了去重，然后再最终取得这个分组，但是这其中依然用到了 **groupByKey()** 有可能会 **OOM**。

同样的需求，不同的写法，成本和风险是不一样的。

###### 问题：用了 **groupByKey()**，容易 **OOM**，取巧 **Spark RDD reduceByKey()** 的取 **max** 间接达到去重，让自己的算子变动简单点。

### **第三个 Job**，如下图：

![分组取TopN-V3](D:\ideaProject\bigdata\bigdata-spark\image\分组取TopN-V3.png)

比第二个 **Job** 又复杂了些，因为使用了很多的算子，基于 **shuffle** 也是基于划分 **key** 的分区的过程，关键是 **sortBy()** 是按年月温度排序，**reduceByKey()** 是按年月日，主要是按年月排序。

第一次 **shuffle** 拉起数据时，计算每一个数据，它应该是哪个分区，应该是谁和谁排在一起，第一步排序时，按照**年月温度**这三个维度，在 **reduceByKey()** 去重时，是按照年月日，那么此时有个问题：

**年月温度**和**年月日**它们之间有可能会产生错乱，找年月温度时，它们按照大小排在一起就可以了，但是 **reduceNyKey()** 这种行为的话，它一定是按照哈希取模的方式，而哈希取模一定是按照年月日而没有按照年月温度，年月温度的它的哈希一定是找年月温度都相同的一定在一起，然后后续做 **groupByKey() **分组时，是按照年月分组，而且没有按照年月日分组或者按照年月温度去分组，所以这几步 **shuffle**太多，其实数据特征曾经排序已经洗乱了，因为后面的 **shuffle** 让数据重新划分分组，已经可能扰乱了数据的顺序，虽然先前是一个全排序。

**sortBy()** 一定是一个处理全排序的，但这是一步，最好把它理解成一步之内的事，如果后续再有 **shuffle** 的话，这全排序的事就不要再想了，保守的时候，如果后续想要全排序时再重新排序，这是一个法则。

###### 问题：用了 **groupByKey()**，容易 **OOM**，取巧 **Spark 的 RDD 的 reduceByKey()** 去重，用了 **sortByKey()** 排序， **注意多级 shuffle** 关注，后续 **shuffle** 的 **key** 一定得是前置 **rdd key** 的子集。

### **第四个 Job**，如下图：

![分组取TopN-V4](D:\ideaProject\bigdata\bigdata-spark\image\分组取TopN-V4.png)

其实是这样的，**全排序**的结果后，按照的是**年月温度**，但是分组时按的是**年月**，也就是说，后续 **shuffle** 如果没有从左向右使用先前的 **key** 的子集，先前**年月温度是一个三个维度的全集**，但**分组是年月**，**相同年月还排在一起**，它们的**哈希值还是一样的**，**取模值也是一样的**，从有序中每个转过来后，它们依然还是**有序的**，但如果下一步使用的不是**当前的子集**，先前的**年月温度**，后续 **shuffle** 使用的**年月日**，此时使得**年月日都去重了**，**年月日和先前就不是同一类了**。

**因为先前年月的一定排在一起了，但是年月日加了一个日期，在哈希取模的顺序就乱了，因为日期会让年月的顺序发生混乱，这才是根本。**

其实这个知识点在学 **MapReduce** 时讲过二次排序的坑，在 **MapReduce** 中有两个阶段，**Map** 阶段中一定会排序，相当于它们可以分到一起，**Reduce** 阶段可以写一个 **groupComparator()** 分组比较器，但是**分组比较器**在比较时只能比较 **Map** 排序 **key** 的**子集**，如果先前按照**年月温度**排序，后续就不能按照**年月天气**分组，只能按照**年月**一个**小的集**去分组，**这才叫真正的二次排序**。

但是以上的写法，其实它们或多或少都存在了很多问题，要么 **OOM**，要么 **shuffle** 的问题，有没有一个方法可以既让 **shuffle** 数量表少，且基本没有 **OOM** 的风险，速度或多或少相对变快。

###### 问题：用了 **groupByKey()**，容易 **OOM**，取巧 **Spark 的 RDD 的  sortByKey()** 排序，**没有破坏多级 shuffle 的 key 的子集关系**。

### **第五个 Job**，如下图：

![分组取TopN-V5](D:\ideaProject\bigdata\bigdata-spark\image\分组取TopN-V5.png)

首先 **shuffle** 只会有一次，**combineByKey()** 基本不会产生 **OOM**，且**内存占用也是最少的**，所以最后这个是最优方式。

#### 问题：

##### 分布式计算的核心思想：

###### 调优天下无敌：**combineByKey()** 。

###### 分布式是并行的，离线批量计算有个特征就是后续步骤 **(Stage) **依赖其一步骤 **(Stage)**，如果先前的步骤 **(Stage)** 能够加上正确的**combineByKey()**，我们自定义的 **combineByKey()** 的函数尽量压缩内存中的数据。

### value 放大 10 倍，如下图：

![value放大10倍](D:\ideaProject\bigdata\bigdata-spark\image\value放大10倍.png)

#### 另一种写法(也是推荐写法)，如下图：

![mapValues()](D:\ideaProject\bigdata\bigdata-spark\image\推荐写法mapValues.png)

##### **那么问题来了，谁的成本相对低一些？性能好一些？**

为什么？真的有没有懂 **shuffle** 的概念？它俩差在哪了？

###### 就差在一个算子上，要么是 **map()**，要么是 **mapValues()**，先来看 **map()** 点开：

```scala
/**
 * Return a new RDD by applying a function to all elements of this RDD.
 */
// 然后参数中无法就是前置的 rdd 引用, 传一个匿名函数进去
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
  val cleanF = sc.clean(f)
  // 会 new 一个 MapPartitionsRDD
  // map 拿着我们的函数传参, 先把迭代器中的 KV 扔给 cleanF
  // cleanF 接收的是迭代器的 KV
  new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
}

// map 只传递两个参数
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    // map 不传递这个分区参数, 所以默认 false
    preservesPartitioning: Boolean = false,
    isOrderSensitive: Boolean = false)
```

再来看 **mapValues()** 点开：

```scala
/**
 * Pass each value in the key-value pair RDD through a map function without changing the keys;
 * this also retains the original RDD's partitioning.
 */
def mapValues[U](f: V => U): RDD[(K, U)] = self.withScope {
  // 传递的函数 f, 先清理成 cleanF
  val cleanF = self.context.clean(f)
  new MapPartitionsRDD[(K, U), (K, V)](self,
    // 拿出父级迭代器
    // 如果是 KV, 它拼了一个返回 KV, 就是 rdd 的 KV 已经处理好了
    // 拿我们的函数只处理 value, 这就是为什么调用时只会收到一个函数
    (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) },
    preservesPartitioning = true)
}

// mapValues 传递三个参数
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    // mapValues 传递这个参数且为 true
    preservesPartitioning: Boolean = false,
    isOrderSensitive: Boolean = false)
```

这个属性：

```scala
preservesPartitioning: Boolean = false
```

决定了 **map()** 和 **mapVaules()** 的差异。

###### 那么这个属性决定了什么道理？

- 当 **reducebyKey()** 时，是按照单词，然后调了 **map**，**map** 结束后，**key** 还是单词，但是 **key** 变了吗？并没有，那之后再调 **groupByKey()**，**key 没有发生变化，分区器没有发生变化，分区数没有发生变化，且是 KV，那么建议：mapValues()，flatMapValues()。**，就是把 **shuffle 依赖调整成 窄依赖** 做一次优化。
- **map()** 没有分区器，**mapValues()** 有分区器，取决于属性是否为真，确定后续是否有分区器。

```scala
// 如果有分区器且前后分区数量相等, 那么可以不发生 shuffle
override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None
```

------

## 章节10：Spark-CORE，集群框架图解，角色功能介绍，官网学习，搭建
