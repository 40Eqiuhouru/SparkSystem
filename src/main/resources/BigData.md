怎么知道 RM 在哪呢 ? 

在项目架构中的 yarn-site 配置文件中会给出相应的配置信息

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.resourcemanager.ha.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.resourcemanager.zk-address</name>
        <value>node02:2181,node03:2181,node04:2181</value>
    </property>
    <property>
        <name>yarn.resourcemanager.cluster-id</name>
        <value>test</value>
    </property>
    <property>
        <name>yarn.resourcemanager.ha.rm-ids</name>
        <value>rm1,rm2</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname.rm1</name>
        <value>master1</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname.rm2</name>
        <value>master2</value>
    </property>
</configuration>
```

其实在 Configuration new 出来的时候加载完配置文件, 然后给到 job, 可以通过配置信息知道那些东西在哪了.

还有一个非常重要的配置, mapred-site 中的 MapReduce 平台名称, 如果是 yarn 它才是集群的模式

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <!-- 集群模式 -->
        <value>yarn</value>
        <!-- 单机模式 -->
        <value>local</value>
    </property>
</configuration>
```

去找 RM.

```java
// 让框架知道是 windows 异构平台运行
conf.set("mapreduce.app-submission.cross-platform", "true");
```

裸执行代码的时候，需要添加设置，括号内写已经编译好的 jar包路径

```java
// 未来裸执行代码的时候, 应该上传哪个 jar 包
job.setJar("D:\\ideaProject\\bigdata\\synhadoop\\target\\hadoop-hdfs-1.0-1.0.jar");
```

运行模式可以被覆盖：

```java
// 让框架知道是 windows 异构平台运行
conf.set("mapreduce.app-submission.cross-platform", "true");
// 覆盖运行模式
conf.set("mapreduce.framework.name", "local");
```

本地单机的时候是不需要打 jar包的。

```java
// bin/hadoop command [genericOptions] [commandOptions]
// hadoop jar xxx.jar xxx -D xxx inpath outpath
// 解析命令行参数
// 工具类帮我们把 -D 等等的属性直接 set 到 conf 中, 会留下 commandOptions
GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
//        Path inFile = new Path("/data/wc/input");
Path inFile = new Path(remainingArgs[0]);
//        Path outFile = new Path("/data/wc/output");
Path outFile = new Path(remainingArgs[1]);
```

框架提供的以上方式使得路径没有写死，可以通过解析参数获得。

为什么有些参数要带 -D，而有些参数却不需要带，不需要带的是 [commandOptions] 程序自己调的参数，那些带  -D 的参数给 [genericOptions] 填充的。

```shell
-D mapreduce.job.reduces=2 /data/test/input /data/test/outputargs
```

以上为不用硬编码，完全通过 Edit Configurations 配置参数个性化。

------

Hadoop 源码分析：

​	目的：更好的理解所学技术的细节以及原理。

​	资源层 YARN先不分析。

​	what?why?：通过源码及一些接口API或文档尽快理解的东西，尤其源码很容易把这两个问题解开。

​	how?：前两个非常理解后，如何使用？如何正确使用？如何高效使用？才可以被实现

MapReduce有三个环节 <- 分布式计算 <- 追求：

- 计算向数据移动
- 并行度、分治
- 数据本地化读取

三个环节：

Client：

- 没有计算发生
- 很重要：支撑了计算向数据移动和计算的并行度

```java
public boolean waitForCompletion(boolean verbose
                                   ) throws IOException, InterruptedException,
                                            ClassNotFoundException {                                        
    if (state == JobState.DEFINE) {
      // 异步执行 
      // 最重要的是提交环节做了哪些事  
      submit();
    }
    if (verbose) {
      // 开始监控  
      monitorAndPrintJob();
    } else {
      // get the completion poll interval from the client.
      int completionPollIntervalMillis = 
        Job.getCompletionPollInterval(cluster.getConf());
      while (!isComplete()) {
        try {
          Thread.sleep(completionPollIntervalMillis);
        } catch (InterruptedException ie) {
        }
      }
    }
    return isSuccessful();
  }
```

```java
public void submit() 
         throws IOException, InterruptedException, ClassNotFoundException {
    ensureState(JobState.DEFINE);
    // 1.x -> 2.x 的过渡, 新旧 API 有一个变化
    setUseNewAPI();
    // 和集群连接
    connect();
    final JobSubmitter submitter = 
        getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
    status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
      public JobStatus run() throws IOException, InterruptedException, 
      ClassNotFoundException {
        // submitJobInternal 一共要做五件事
        // Checking the input and output specifications of the job. split
	    // Computing the InputSplits for the job.
	    // Setup the requisite accounting information for the DistributedCache of the job, if necessary.
        // Copying the job's jar and configuration to the map-reduce system directory on the distributed file-system.
        // Submitting the job to the JobTracker and optionally monitoring it's status.  
        return submitter.submitJobInternal(Job.this, cluster);
      }
    });
    state = JobState.RUNNING;
    LOG.info("The url to track the job: " + getTrackingURL());
   }
```

MR 框架默认的输入格式化类：

```java
TestInputFormat < FileInputFormat < InputFormat
 					getSplits()
// 在用户没有配置的情况下, 默认值    
minSize = 1;
maxSize = Long.Max;   
blockSize = file;
// 默认 split大小 = block大小
// split 是一个窗口机制 : 调大 split 改小, 调小 split 改大
splitsSize = Math.max(minSize, Math.min(maxSize, blockSize));
// 人为干预最小输入分片大小
FileInputFormat.setMinInputSplitSize(job, 40);    
```

```java
// 切片的起始位置被一个块包含
if ((blkLocations[i].getOffset() <= offset < blkLocations[i].getOffset() + blkLocations[i].getLength()))
```

split 的四个重要属性：解耦 存储层和计算层

1. file
2. offset
3. length
4. hosts 支撑的计算向数据移动

```java
// 会添加所有文件的所有的切片, 那么 List 的大小就是切片的数量也就是 Map 的数量
// 此时会发现还没做计算呢, Client 一启动就知道这次计算有多少个并行度且各自的移动去向都搞定了
List<InputSplit> splits = new ArrayList<InputSplit>();
```

```java
// 作业的切片写会把相关资源传进去
// Client 在传递资源的时候, 会把 jar包、配置信息、切片清单都放到了 HDFS
// 最终 AppMaster 才能拿着清单才能和资源层申请要哪些节点上跑 Container 跑任务
JobSplitWriter.createSplitFiles(jobSubmitDir, conf, 
        jobSubmitDir.getFileSystem(conf), array);
```



$$
\frac{\partial KL}{\partial W} = \frac{1}{a} \sum_{i=1}^{a} (-(1-f_i)) \cdot x + \frac{1}{b} \sum_{i=1}^{b} f \cdot x
$$


MapTask：

ReduceTask：