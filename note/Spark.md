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

### Spark WordCount 源码分析

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

```scala
// 进入 flatMap()
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
    val cleanF = sc.clean(f)
    // new 了一个 MapPartitionsRDD, 且没有被执行
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(cleanF))
  }
```

```scala
// 父类一定又是一个 RDD
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false,
    isOrderSensitive: Boolean = false)
extends RDD[U](prev)
```

