# `Spark-CORE` 源码

------

## 章节`13`：`Spark-CORE` 源码，`RpcEnv`，`StandaloneMaster`启动分析

------

### 1.下载源码

1. [GitHub  官方下载地址](https://github.com/apache/spark/tree/v2.3.4)

2. 导入到 IDEA 中，如下图：

   ![导入 Spark-2.3.4_源码](D:\ideaProject\sourcecodelearn\spark-2.3.4\image\IDEA导入Spark-2.3.4_源码.png)

### 2.区分层次

#### 2.1 资源层

***也就是 `Spark Standalone Mode` 中的 `Master`，`Worker` 这是它的资源层进程。***

##### 2.1.1 几个概念

###### 1.1 第一步，从资源层开始分析：

`Master` 其实是一个 `JVM`，然后集群的资源层中还有一系列的 `Worker`，也是 `JVM`。

并且 `Worker` 的数量还不是一个可能有很多个，且如果它们都是独立的话，根本没有层这个概念，因为它们就是单体了，它们是一个主从的集群，是所有的 `Worker` 单向和 `Master` 通信，`Master` 要和所有 Worker 通信。

所以最终它们组建了一个集群，一个分布式集群，整个的集群这些角色，它们统一完成资源调度的事，所以才有资源层的层的概念。

假如是 `Client Mode`。

如果有一个 `Client JVM`，它要访问资源层，也就是访问 `Master`，然后由资源层再制造出计算层。因为计算层中刚好 `Client`，可能还需要一个理解并解析我们写的代码并提交代码逻辑到分布式计算的一个进程。

这个进程如果是 `Cluster Mode` 的话，这个东西叫 `Driver`，它也是个 `JVM`， 它就是 `SparkContext`。

`Driver` 在理解我们的代码前，它要去和资源层申请一批 `Executor`，和另外一个角色 `EP`，理解代码的是一个角色，但是代码的并行分布式计算时，会放到很多的 `Executor` 中。

最核心的源码就是在计算层，要分析的非常清楚，资源层了解了解即可。因为不怎么用 `Standalone`，而是用 `YARN`，其实资源层的逻辑是相似的。

###### 1.2 通信

层次划分出后，无论哪个角色，它们都是分布式的，这些角色它们都是独立的 `JVM`。它们会跑在不同的节点上，所以在分布式中如果想分析源码，首先要去理解一个概念：

角色间的通信：它们会互相调用对方的功能完成整体的功能，所以一定会涉及到 `RPC`。

也就是浏览器远程的调用服务器的分发，而且有时可能会给返回值，可能不给返回值。

所以 `RPC` 要明白一件事，它并不是一个固定的技术，它是一个宏观的事实的一个描述，就是远程调用。

它抽象出以下大致流程：

最少两个 实例/实体，它们会有三种情况：

1. 同一个进程中；
2. 不同的进程中，同一个主机中；
3. 不同的进程中，不同主机中（最复杂的场景）；

如果消息向外发送的话，肯定会牵扯到传输层，传输层发送给另外一方，也有一个传输层。

传输层中有一个技术是 `Netty`，它对传输层进行了包装，`Netty` 向外向上给我们的程序提供了一套完整的面向通信传输的一套接口，`Netty` 向下可以是 `BIO` 也可以是 `NIO`。

它自己不是传输层，`Socket` 可能是 `BIO` 也有可能是 `NIO`，但它提供了包装，无论是什么 `IO`，都提供了统一的编程接口，让编程人员忽略了复杂的技术。

如果在多实例的场景下，发送的数据包应该给谁呢？

因此在实例与传输层间，加入分发，在分发与传输间往往会加队列，因为主机的进程能拿到的物理机的线程数量是有限的，加入队列来弥补线程问题。

分析 `sbin/start-all.sh`脚本，

```sh
if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# Load the Spark configuration
. "${SPARK_HOME}/sbin/spark-config.sh"

# Start Master
"${SPARK_HOME}/sbin"/start-master.sh

# Start Workers
"${SPARK_HOME}/sbin"/start-slaves.sh
```

先看 `sbin/start-master.sh`，

```sh
# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.deploy.master.Master"
```

由此找到启动类路径`org.apache.spark.deploy.master.Master`，`Master`是一个伴生关系，先走`object`主方法，然后还有一个`class`。

它的核心方法为：

```scala
def main(argStrings: Array[String]) {
  Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
    exitOnUncaughtException = false))
  Utils.initDaemon(log)
  val conf = new SparkConf
  // 参数赋给 args
  val args = new MasterArguments(argStrings, conf)
  // 启动 Master 前, 第一件事先要启动 RPC 环境
  // 核心方法 : startRpcEnvAndEndpoint
  val (rpcEnv, _, _) = startRpcEnvAndEndpoint(args.host, args.port, args.webUiPort, conf)
  // 启动完后, 还会让它在此等待, 一直不退出, 那么 Master 才能一直运行
  rpcEnv.awaitTermination()
}
```

进入 `startRpcEnvAndEndpoint()`，

```scala
/**
 * Start the Master and return a three tuple of:
 *   (1) The Master RpcEnv
 *   (2) The web UI bound port
 *   (3) The REST server bound port, if any
 */
def startRpcEnvAndEndpoint(
    host: String,
    port: Int,
    webUiPort: Int,
    // 最终要返回一个 RpcEnv 类型
    conf: SparkConf): (RpcEnv, Int, Option[Int]) = {
  val securityMgr = new SecurityManager(conf)
  // 调用 RpcEnv.create 创建一个 RpcEnv
  val rpcEnv = RpcEnv.create(SYSTEM_NAME, host, port, conf, securityMgr)
  // 在 RpcEnv 设置 Endpoint, 也成为端点或实例
  // 把实例注册到 RpcEnv 中
  // 且注册的是 Master 自己
  // 把 Master 对象注册进了 EpcEnv 中
  val masterEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME,
    // new 的是 伴生关系的 class Master
    // 它会被异步执行
    new Master(rpcEnv, rpcEnv.address, webUiPort, securityMgr, conf))
  val portsResponse = masterEndpoint.askSync[BoundPortsResponse](BoundPortsRequest)
  (rpcEnv, portsResponse.webUIPort, portsResponse.restPort)
}
```

此方法会把`Master`注册到`RpcEnv`中，且会被异步执行。进入到`RpcEnv.create()`，

```scala
def create(
    name: String,
    host: String,
    port: Int,
    conf: SparkConf,
    securityManager: SecurityManager,
    clientMode: Boolean = false): RpcEnv = {
  // 又调了一个 create()
  create(name, host, host, port, conf, securityManager, 0, clientMode)
}

def create(
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    conf: SparkConf,
    securityManager: SecurityManager,
    numUsableCores: Int,
    clientMode: Boolean): RpcEnv = {
  val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
    numUsableCores, clientMode)
  // 再次强调一点, Netty 是传输层技术, 它不是 RPC 技术
  // Spark-2.3.4 中依赖了 Netty 作为传输通信
  // 最终看 create() 的过程
  new NettyRpcEnvFactory().create(config)
}
```

可以看到嵌套调用的`create()`，且`Spark`的底层传输是依赖于`Netty`。最终调用的`create()`，

```scala
// 最终调用的 create()
def create(config: RpcEnvConfig): RpcEnv = {
  val sparkConf = config.conf
  // Use JavaSerializerInstance in multiple threads is safe. However, if we plan to support
  // KryoSerializer in future, we have to use ThreadLocal to store SerializerInstance
  // 使用的 Java 序列化在线程安全的角度是安全的
  val javaSerializerInstance =
    new JavaSerializer(sparkConf).newInstance().asInstanceOf[JavaSerializerInstance]
  // 最终要 new 一个 NettyRpcEnv 工厂, 这么一个环境
  // 最终 NettyRpcEnv 会运行起来
  val nettyEnv =
    new NettyRpcEnv(sparkConf, javaSerializerInstance, config.advertiseAddress,
      config.securityManager, config.numUsableCores)
  if (!config.clientMode) {
    // 变量引用了一个函数
    // 最终会调用 startServer()
    val startNettyRpcEnv: Int => (NettyRpcEnv, Int) = { actualPort =>
      nettyEnv.startServer(config.bindAddress, actualPort)
      (nettyEnv, nettyEnv.address.port) // 但它并没有被执行
     }
    try {
      // 在此处执行, 在某一个端口上启动了一个服务
      // 此时会把 startNettyRpcEnv 传进来
      Utils.startServiceOnPort(config.port, startNettyRpcEnv, sparkConf, config.name)._1
    } catch {
      case NonFatal(e) =>
        nettyEnv.shutdown()
        throw e
    }
  }
  nettyEnv
}
```

使用 `Java`的序列化保证线程安全，最终会调用`startServer()`并运行`NettyPrcEnv()`。进入`NettyRpcEnv`，

```scala
// 分发器
// 在 RPC 环境中为什么会有这个东西 ?
// 实体/实例 在传输时, 必不可少的就是分发
private val dispatcher: Dispatcher = new Dispatcher(this, numUsableCores)
```

会有一个分发器，不同实例/实体间的通信必然会有分发，进入`startServer()`，

```scala
// 此处启动传输层服务
// 在 Netty 中除了 Rpc 外还应该有传输服务
def startServer(bindAddress: String, port: Int): Unit = {
  val bootstraps: java.util.List[TransportServerBootstrap] =
    if (securityManager.isAuthenticationEnabled()) {
      java.util.Arrays.asList(new AuthServerBootstrap(transportConf, securityManager))
    } else {
      java.util.Collections.emptyList()
    }
  // 传输使用 Netty 实现
  // 这其中也把分发器传进去了
  // 还要创建一个 Netty 的传输服务
  // 其实就是得到在 Netty 的 RpcEnv 中刚完成一个 server 这一层的事
  server = transportContext.createServer(bindAddress, port, bootstraps)
  dispatcher.registerRpcEndpoint(
    RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher))
}
```

进入 `createServer()`，会发现 `new` 一个`TS`，进入`TS`其中会进行相应的初始化`init()`

```scala
/** Create a server which will attempt to bind to a specific host and port. */
public TransportServer createServer(
    String host, int port, List<TransportServerBootstrap> bootstraps) {
  // new 一个 TransportServer
  return new TransportServer(this, host, port, rpcHandler, bootstraps);
}
```

进入`init()`，其中会实现趋向于`Netty`的底层代码，

```java
try {
  // 初始化
  init(hostToBind, portToBind);
} catch (RuntimeException e) {
  JavaUtils.closeQuietly(this);
  throw e;
}
```

进入`initializePipeline`

```java
// 趋向于 Netty 的代码
private void init(String hostToBind, int portToBind) {

  IOMode ioMode = IOMode.valueOf(conf.ioMode());
  // 一般 Loop 会放到线程上执行
  // 思考一个问题 : 如果 new 了一个线程, 然后把 value 东西放进去
  // 它里面一定会 run() 调起, 这个 run() 行为分为两种 : 1.Loop 行为 2.非 Loop 行为
  // 如果用 Java new 了一个线程, 这个线程会执行 run(), 这个 run() 有没有 while (true) 就决定了它是不是一个 Loop
  // 通俗来说, 如果在开发项目时, 需要一个线程一直处理不定时要做的事, 因为比如说像 Netty 它不可能这个线程一直用
  // 用它的时候, 它其中得有一个死循环卡住线程不让其退出, 所以为什么会有 EventLoop, MessageLoop
  EventLoopGroup bossGroup =
    NettyUtils.createEventLoop(ioMode, conf.serverThreads(), conf.getModuleName() + "-server");
  EventLoopGroup workerGroup = bossGroup;

  PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
    conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());

  // 一般有老板, 组织, 干活的还有个 bootstrap
  // 其实 NIO 有 Reactor 模型, 然后 Netty 对这种 NIO 的 Reactor 模型进行了升级
  // 它是一种主从的 Selector 接收, 然后后续还有一些 Worker 线程去执行
  // 这个 bootstrap 就是一个向导, 其中会有一系列未来的 group, 它要做绑定
  bootstrap = new ServerBootstrap()
     // bossGroup 就是 acceptor 接收, 因为在通信 IO 中,
     // 一定是 Listener 了 8080 来了一个请求会产生一个事件叫 accept
     // 一定要接收完之后才能得到一个新的 Socket
     // 新的 Socket 是由 workerGroup 去处理的
    .group(bossGroup, workerGroup)
     // NIO 中的 channel
    .channel(NettyUtils.getServerChannelClass(ioMode))
    .option(ChannelOption.ALLOCATOR, allocator)
    .childOption(ChannelOption.ALLOCATOR, allocator);
    
// 最终要的是 handler
bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
  @Override
  protected void initChannel(SocketChannel ch) {
    RpcHandler rpcHandler = appRpcHandler;
    for (TransportServerBootstrap bootstrap : bootstraps) {
      rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
    }
    // 以及 Pipeline
    // 其实就是 Dispatcher 的 transportContext
    // context 包含了 Dispatcher
    // 因为 Netty 是一个基于事件的异步过程
    context.initializePipeline(ch, rpcHandler);
  }
}
                           
InetSocketAddress address = hostToBind == null ?
        new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
// 最终要通过绑定得到, 这个请求才能真正过来
// 到此, Netty 的传输层服务才刚刚运行起来
channelFuture = bootstrap.bind(address);
channelFuture.syncUninterruptibly();
```

创建一个`createChannelHandler`

```java
/**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      // 非常趋向于 Netty 的底层源码
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }
```

进入`createChannelHandler`会发现，创建两个处理器

```java
/**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    // 发送处理器
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    // 请求处理器
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred());
    // 传递处理器, 以及 client
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }
```

进入`TransportChannelHandler`，有一个

```java
// 将获取到的请求消息做匹配
@Override
public void channelRead(ChannelHandlerContext ctx, Object request) throws Exception {
  if (request instanceof RequestMessage) {
    requestHandler.handle((RequestMessage) request);
  } else if (request instanceof ResponseMessage) {
    responseHandler.handle((ResponseMessage) request);
  } else {
    ctx.fireChannelRead(request);   
  }
}
```

比如匹配的是`request`，进入相应的`handle`

```java
  @Override
  public void handle(RequestMessage request) {
    if (request instanceof ChunkFetchRequest) {
      processFetchRequest((ChunkFetchRequest) request);
    } else if (request instanceof RpcRequest) {
      processRpcRequest((RpcRequest) request);
    } else if (request instanceof OneWayMessage) {
      // 比较高级的流式通信
      processOneWayMessage((OneWayMessage) request);
    } else if (request instanceof StreamRequest) {
      processStreamRequest((StreamRequest) request);
    } else {
      throw new IllegalArgumentException("Unknown request type: " + request);
    }
  }
```

最终会到`dispatcher`上

```scala
override def receive(
    client: TransportClient,
    message: ByteBuffer): Unit = {
  val messageToDispatch = internalReceive(client, message)
  // 最终 Netty 的方法调到了分发器上
  dispatcher.postOneWayMessage(messageToDispatch)
}
```

最终会把消息推送到`inbox`

```scala
  /**
   * Posts a message to a specific endpoint.
   * 也是 Dispatcher, 它要把这条消息推送给 inbox
   *
   * @param endpointName name of the endpoint.
   * @param message the message to post
   * @param callbackIfStopped callback function if the endpoint is stopped.
   */
  private def postMessage(
      endpointName: String,
      message: InboxMessage,
      callbackIfStopped: (Exception) => Unit): Unit = {
    val error = synchronized {
      val data = endpoints.get(endpointName)
      if (stopped) {
        Some(new RpcEnvStoppedException())
      } else if (data == null) {
        Some(new SparkException(s"Could not find $endpointName."))
      } else {
        // 收件箱 inbox
        data.inbox.post(message)
        receivers.offer(data)
        None
      }
    }
    // We don't need to call `onStop` in the `synchronized` block
    error.foreach(callbackIfStopped)
  }
```

```scala
// 传输服务器
@volatile private var server: TransportServer = _
```

因为`Netty`是一个基于事件的异步的方式，刚才的环节是向其中注入的过程，最终要通过`bind`才能真正启动的过程。

`Dispatcher`可以理解成承上启下的作用。

在`Spark`框架中，`Master`，`Worker`，`Driver`，`Client`其实都是分布式角色，独立的`JVM`，它们其实都是`Endpoint`，都会有`receive`，`receiveAndReply`。且某些角色，比如`Worker`中一定会有`Master`的引用，它一定会发一些`send`，`ask`，去向它注册资源等等，你的`Client`一定会有`Master`的引用，因为你最终会和`Master`申请一个`Driver`而且`Driver`有没有成功要告知我。

为什么`OnStart()`要被异步调起？因为有可能此时的`JVM`中有多个不同实例，可能如果都是不用异步执行的话，有一个实例`.OnStart()`时，可能要执行数个小时或数十分钟，那么后续代码就被阻塞住了，但如果有几个线程，这几个实例可以并行的`OnStart()`，所以可以充分利用资源。其实本身可以是线性执行。

#### 2.2 计算层

***无论资源层是 `Standalaone` 还是 `YARN` 亦或是 `Mesos`，它的计算层一成不变，都是 `SparkSubmit` 提交的那个过程。***

***也就是说，其实要分析的是整套的 `Spark` 源码，以及后续的 `Spark SQL`，``Spark Streaming` 都要看源码，要完整的分析。***

***其中最核心的就是`Submit`的过程。***

------

## 章节`14`：`Spark-CORE` 源码，`Worker` 启动，`SparkSubmit` 提交，`Driver` 启动分析

------

### 一、`Worker`的启动流程

用同样的方法在启动脚本中找到`Worker`的启动类。

`Worker`也是一个分布式进程，要启动相应的`Worker`类，其实`RpcEnv`应该是分布式环境中的基础设施，如果一台机器中没有`RpcEnv`基础层的话，那么它是不能同其它机器通信的。

**`Worker`在`OnStart()`时，`Worker`的`OnStart()`回调一定是给`Master`发送一个消息`Register()`开始注册`Worker`，回调`Master`的`receive()`，之后`Master`向`Worker`发送信息，`Registered()`注册成功的信息，因为`send()`一定会回调`Worker`的`receive()`，然后两个达成通信的事情，而且通讯的过程分为`注册`和`响应`。**

`Worker`向`Master`发送心跳

```scala
forwordMessageScheduler.scheduleAtFixedRate(new Runnable {
    override def run(): Unit = Utils.tryLogNonFatalError {
         // 只有 Worker 在收到 Master 的注册成功信息后, 才会发送心跳事件
         self.send(SendHeartbeat)
       }
}, 0, HEARTBEAT_MILLIS, TimeUnit.MILLISECONDS)
```

`Master`接收`Worker`发送的心跳，以此判断集群的健康状态

```scala
// 心跳
case Heartbeat(workerId, worker) =>
  idToWorker.get(workerId) match {
    case Some(workerInfo) =>
      workerInfo.lastHeartbeat = System.currentTimeMillis()
    case None =>
      if (workers.map(_.id).contains(workerId)) {
        logWarning(s"Got heartbeat from unregistered worker $workerId." +
          " Asking it to re-register.")
        worker.send(ReconnectWorker(masterUrl))
      } else {
        logWarning(s"Got heartbeat from unregistered worker $workerId." +
          " This worker was never registered, so ignoring the heartbeat.")
      }
  }
```

至此，`Master`，`Worker`的资源层启动的粗粒度过程已经完成。

剩下的事情就是有计算层来使用资源层。**其实资源层并不重要，最最重要的就是计算层的`Submit`。**

### 二、`Submit`的提交过程

在`spark-submit.sh`脚本中找到启动类

```sh
# 脚本会替换成 SparkSubmit 的类
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```

`SparkSubmit`是整个计算层被激活的第一个类。

不带任何参数执行`spark-submit.sh`，会默认匹配

```scala
// 匹配提交方法
case SparkSubmitAction.SUBMIT => submit(appArgs, uninitLog)
```

进入到`submit()`会看到核心方法`doRunMain()`

```scala
def doRunMain(): Unit = {
    if (args.proxyUser != null) {
      val proxyUser = UserGroupInformation.createProxyUser(args.proxyUser,
        UserGroupInformation.getCurrentUser())
      try {
        proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            runMain(args, uninitLog)
          }
        })
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          if (e.getStackTrace().length == 0) {
            // scalastyle:off println
            printStream.println(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
            // scalastyle:on println
            exitFn(1)
          } else {
            throw e
          }
      }
    } else {
      // 启动主程序
      runMain(args, uninitLog)
    }
}
```

此时要启动主程序`runMain()`，进入后

```scala
// 这行很重要, 检查启动环境, 传参
val (childArgs, childClasspath, sparkConf, childMainClass) = prepareSubmitEnvironment(args)
```

返回会调用`start()`，进入`start()`

```scala
try {
  // 在集群模式运行会调用 start()
  app.start(childArgs.toArray, sparkConf)
} catch {
  case t: Throwable =>
    findCause(t) match {
      case SparkUserAppException(exitCode) =>
        System.exit(exitCode)
      case t: Throwable =>
        throw t
    }
}
```

```scala
private[spark] class ClientApp extends SparkApplication {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val driverArgs = new ClientArguments(args)

    if (!conf.contains("spark.rpc.askTimeout")) {
      conf.set("spark.rpc.askTimeout", "10s")
    }
    Logger.getRootLogger.setLevel(driverArgs.logLevel)

    val rpcEnv =
      RpcEnv.create("driverClient", Utils.localHostName(), 0, conf, new SecurityManager(conf))

    val masterEndpoints = driverArgs.masters.map(RpcAddress.fromSparkURL).
      map(rpcEnv.setupEndpointRef(_, Master.ENDPOINT_NAME))
    rpcEnv.setupEndpoint("client", new ClientEndpoint(rpcEnv, driverArgs, masterEndpoints, conf))

    rpcEnv.awaitTermination()
  }

}
```

最终通信的话，所谓的`客户端`需要去和资源层申请一个地方启动`driver`，因为现在是集群模式，所以在`start()`后必须要有一个能够启动`Rpc`层和`Master`通信的一个端点`ClientEndpoint`。任何的分布式在`Spark`环境中，只要是分布式角色想和别人通信，它就一定是一个端点。

`Client`有了之后，下一步希望得到一个`Driver`，进入`ClientEndpoint()`，会有一个`onStart()`

```scala
  override def onStart(): Unit = {
    driverArgs.cmd match {
      case "launch" =>
        // TODO: We could add an env variable here and intercept it in `sc.addJar` that would
        //       truncate filesystem paths similar to what YARN does. For now, we just require
        //       people call `addJar` assuming the jar is in the same directory.
        // 找下一个类执行
        val mainClass = "org.apache.spark.deploy.worker.DriverWrapper"

        val classPathConf = "spark.driver.extraClassPath"
        val classPathEntries = getProperty(classPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val libraryPathConf = "spark.driver.extraLibraryPath"
        val libraryPathEntries = getProperty(libraryPathConf, conf).toSeq.flatMap { cp =>
          cp.split(java.io.File.pathSeparator)
        }

        val extraJavaOptsConf = "spark.driver.extraJavaOptions"
        val extraJavaOpts = getProperty(extraJavaOptsConf, conf)
          .map(Utils.splitCommandString).getOrElse(Seq.empty)

        val sparkJavaOpts = Utils.sparkJavaOpts(conf)
        val javaOpts = sparkJavaOpts ++ extraJavaOpts
        val command = new Command(mainClass,
          Seq("{{WORKER_URL}}", "{{USER_JAR}}", driverArgs.mainClass) ++ driverArgs.driverOptions,
          sys.env, classPathEntries, libraryPathEntries, javaOpts)

        val driverDescription = new DriverDescription(
          driverArgs.jarUrl,
          driverArgs.memory,
          driverArgs.cores,
          driverArgs.supervise,
          command)
        asyncSendToMasterAndForwardReply[SubmitDriverResponse](
          RequestSubmitDriver(driverDescription))

      case "kill" =>
        val driverId = driverArgs.driverId
        asyncSendToMasterAndForwardReply[KillDriverResponse](RequestKillDriver(driverId))
    }
  }
```

所以在`SparkSubmit`的`JVM`中，最终要和自己的`JVM`中有一个`ClientEndpoint`。然后`onStart()`会准备一个参数`org.apache.spark.deploy.worker.DriverWrapper`，但是这个`Driver`并不在集群中。

`schedule()`在之前`RegisterApplication()`，`RegisterWorker()`，`receiveAndReply()`中均被调起，首先要明白`schedule()`在`Master`中会在多种通信的情况下被调起。有些状态变化时都会调起。

```scala
/**
   * Schedule the currently available resources among waiting apps. This method will be called
   * every time a new app joins or resource availability changes.
   * <br>
   * for 中没有人等待被提交的话, 那么就不会执行, 然后可能是有一个 driver 起来后可能需要申请 Executor
   * 所以 schedule() 可能有多种被调起的特征, 要么 for 不被执行只执行 startExecutorsOnWorkers() 分配器
   * 要么只执行 for 因为是刚要申请一个 driver, driver 还没跑起来了不可能有人去申请 Executor
   * 还有一种可能是集群状态发生变化, 既有 driver 被提交又有些刚才的 driver 要申请的 Executor
   * 两个方法逻辑都会被执行, 所以为什么 schedule() 会被多个不同的通信消息被调起
   * 因为它有不同的特征
   * 要么调度 driver, 要么调度 Executor, 要么同时调度这两个
   */
  private def schedule(): Unit = {
    if (state != RecoveryState.ALIVE) {
      return
    }
    // Drivers take strict precedence over executors
    val shuffledAliveWorkers = Random.shuffle(workers.toSeq.filter(_.state == WorkerState.ALIVE))
    val numWorkersAlive = shuffledAliveWorkers.size
    var curPos = 0
    // for 的第一件是判断 driver 哪些事等着被调度
    // 第二个是在一些 worker 上要启动一些执行器
    for (driver <- waitingDrivers.toList) { // iterate over a copy of waitingDrivers
      // We assign workers to each waiting driver in a round-robin fashion. For each driver, we
      // start from the last worker that was assigned a driver, and continue onwards until we have
      // explored all alive workers.
      var launched = false
      var numWorkersVisited = 0
      while (numWorkersVisited < numWorkersAlive && !launched) {
        // 找到存活的 Worker, 通过每次获得不同的 Worker 就能把 Driver 分配到不同的 Worker 上
        val worker = shuffledAliveWorkers(curPos)
        numWorkersVisited += 1
        if (worker.memoryFree >= driver.desc.mem && worker.coresFree >= driver.desc.cores) {
          // 以上条件都满足的情况下, 启动 Driver
          launchDriver(worker, driver)
          waitingDrivers -= driver
          launched = true
        }
        curPos = (curPos + 1) % numWorkersAlive
      }
    }
    startExecutorsOnWorkers()
  }
```

进入到`launchDriver()`，会启动`Driver`

```scala
  private def launchDriver(worker: WorkerInfo, driver: DriverInfo) {
    logInfo("Launching driver " + driver.id + " on worker " + worker.id)
    worker.addDriver(driver)
    driver.worker = Some(worker)
    // 最终会拿到 Worker 的 endpoint 的引用
    // 然后给他发一个消息, LaunchDriver 一个样例类
    // 给 Worker 发送消息, 启动 Driver
    worker.endpoint.send(LaunchDriver(driver.id, driver.desc))
    // 把 Driver 的状态设置为 running
    driver.state = DriverState.RUNNING
  }
```

也就是说，`Client`说你给我启动一个`Driver`，然后告诉`Master`，`Master`洗牌`Worker`挑了一个之后给他发了一个消息。然后进入到`Worker`的`receive()`。

一旦某一个主机中的`Worker`的`JVM`进程收到了要启动一个`Driver`，那么`Worker``new`了一个`Driver`其中包装了很多东西，最重要的是有一个`command`，`command`中有`DriverWrapper`还有我们自己的两个那个类的全限定名，有了`Driver`后还要启动`driver.start()`。

`Spark`的作业代码叫线性执行，先执行一行再执行一行，尤其执行到`new SparkContext()`时后续的代码是不能被执行的。只有得到了`sc`后才能继续执行，所以先分析`val sc = new SparkContext(conf)`。

由`sc.textFile()`调用`hadoopRDD.flatMap()`调用`MapPartitionsRDD.runJob()`调回到`sc.textFile()`，最终它是怎么把我们的这些逻辑变成一些分布式的又通过`Worker`请求其他进程的`Executor`在别处运行呢？最终要的就是`SparkContext()`。

分析`SparkContext()`，从目前来看，所谓的`Driver`其实就是`SparkContext()`。

之前有`RpcEnv`是远程调用，`SparkEnv`是基础设施，在计算层而计算层在`Rpc`上又有一个基础设施是`Spark`计算环境的基础设施。整个在计算层决策，尤其在`Driver调度器`，`Executor执行器`中除了有`RpcEnv`还有`SparkEnv`。

#### 2.1 进入 `SparkContext.createTaskScheduler()`

进入`createTaskScheduler()`

```scala
// 拿 Master 资源层是什么类型做匹配
master match {
    case "local" =>
      val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
      val backend = new LocalSchedulerBackend(sc.getConf, scheduler, 1)
      scheduler.initialize(backend)
      (backend, scheduler)

    case LOCAL_N_REGEX(threads) =>
      def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
      // local[*] estimates the number of cores on the machine; local[N] uses exactly N threads.
      val threadCount = if (threads == "*") localCpuCount else threads.toInt
      if (threadCount <= 0) {
        throw new SparkException(s"Asked to run locally with $threadCount threads")
      }
      val scheduler = new TaskSchedulerImpl(sc, MAX_LOCAL_TASK_FAILURES, isLocal = true)
      val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
      scheduler.initialize(backend)
      (backend, scheduler)

    case LOCAL_N_FAILURES_REGEX(threads, maxFailures) =>
      def localCpuCount: Int = Runtime.getRuntime.availableProcessors()
      // local[*, M] means the number of cores on the computer with M failures
      // local[N, M] means exactly N threads with M failures
      val threadCount = if (threads == "*") localCpuCount else threads.toInt
      val scheduler = new TaskSchedulerImpl(sc, maxFailures.toInt, isLocal = true)
      val backend = new LocalSchedulerBackend(sc.getConf, scheduler, threadCount)
      scheduler.initialize(backend)
      (backend, scheduler)

    // spark:// 匹配的就是这个 case
    case SPARK_REGEX(sparkUrl) =>
      val scheduler = new TaskSchedulerImpl(sc)
      val masterUrls = sparkUrl.split(",").map("spark://" + _)
      val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
      scheduler.initialize(backend)
      (backend, scheduler)

    case LOCAL_CLUSTER_REGEX(numSlaves, coresPerSlave, memoryPerSlave) =>
      // Check to make sure memory requested <= memoryPerSlave. Otherwise Spark will just hang.
      val memoryPerSlaveInt = memoryPerSlave.toInt
      if (sc.executorMemory > memoryPerSlaveInt) {
        throw new SparkException(
          "Asked to launch cluster with %d MB RAM / worker but requested %d MB/worker".format(
            memoryPerSlaveInt, sc.executorMemory))
      }

      val scheduler = new TaskSchedulerImpl(sc)
      val localCluster = new LocalSparkCluster(
        numSlaves.toInt, coresPerSlave.toInt, memoryPerSlaveInt, sc.conf)
      val masterUrls = localCluster.start()
      val backend = new StandaloneSchedulerBackend(scheduler, sc, masterUrls)
      scheduler.initialize(backend)
      backend.shutdownCallback = (backend: StandaloneSchedulerBackend) => {
        localCluster.stop()
      }
      (backend, scheduler)

    case masterUrl =>
      val cm = getClusterManager(masterUrl) match {
        case Some(clusterMgr) => clusterMgr
        case None => throw new SparkException("Could not parse Master URL: '" + master + "'")
      }
      try {
        val scheduler = cm.createTaskScheduler(sc, masterUrl)
        val backend = cm.createSchedulerBackend(sc, masterUrl, scheduler)
        cm.initialize(scheduler, backend)
        (backend, scheduler)
      } catch {
        case se: SparkException => throw se
        case NonFatal(e) =>
          throw new SparkException("External scheduler cannot be instantiated", e)
      }
  }
}
```

如果是`spark://`形式那么会走`case SPARK_REGEX()`，`StandaloneSchedulerBackend`的父类是`CoarseGrainedSchedulerBackend`。

------

## 章节`15`：`Spark-CORE` 源码，`Application` 注册，`Executor` 资源申请分析

------

### 1.对三层循环的逆推

```scala
/** Return whether the specified worker can launch an executor for this app. */
// 是否可以启动一个 Executor, pos : 某个偏移量下标
def canLaunchExecutor(pos: Int): Boolean = {
  val keepScheduling = coresToAssign >= minCoresPerExecutor
  val enoughCores = usableWorkers(pos).coresFree - assignedCores(pos) >= minCoresPerExecutor

  // If we allow multiple executors per worker, then we can always launch new executors.
  // Otherwise, if there is already an executor on this worker, just give it more cores.
  val launchingNewExecutor = !oneExecutorPerWorker || assignedExecutors(pos) == 0
  if (launchingNewExecutor) {
    val assignedMemory = assignedExecutors(pos) * memoryPerExecutor
    val enoughMemory = usableWorkers(pos).memoryFree - assignedMemory >= memoryPerExecutor
    val underLimit = assignedExecutors.sum + app.executors.size < app.executorLimit
    keepScheduling && enoughCores && enoughMemory && underLimit
  } else {
    // We're adding cores to an existing executor, so no need
    // to check memory and executor limits
    keepScheduling && enoughCores
  }
}
```

```scala
// Keep launching executors until no more workers can accommodate any
// more executors, or if we have reached this application's limits
// 把每个下标对应一个 Worker, 先判断它是否能够启动 Executor 得到一个结果集(有多少台 Worker 可以分配资源)
var freeWorkers = (0 until numUsable).filter(canLaunchExecutor)
// 是否还能分配 Worker
while (freeWorkers.nonEmpty) {
  // 迭代结果集
  freeWorkers.foreach { pos =>
    var keepScheduling = true
    // 集群中的 Worker 是否还能分配 Executor 资源的结果集
    while (keepScheduling && canLaunchExecutor(pos)) {
      coresToAssign -= minCoresPerExecutor
      assignedCores(pos) += minCoresPerExecutor

      // If we are launching one executor per worker, then every iteration assigns 1 core
      // to the executor. Otherwise, every iteration assigns cores to a new executor.
      if (oneExecutorPerWorker) {
        assignedExecutors(pos) = 1
      } else {
        assignedExecutors(pos) += 1
      }

      // Spreading out an application means spreading out its executors across as
      // many workers as possible. If we are not spreading out, then we should keep
      // scheduling executors on this worker until we use all of its resources.
      // Otherwise, just move on to the next worker.
      if (spreadOutApps) {
        keepScheduling = false
      }
     }
    }
    freeWorkers = freeWorkers.filter(canLaunchExecutor)
   }
   assignedCores
  }
```

假设三台资源都够，至少每个能分`1~2`个，那么资源都足够时，`freeWorkers.nonEmpty`肯定为`true`能够循环，当它循环之后，`freeWorkers.foreach`知道第一个`pos`下标是`0`，进入第二个循环后，`while (keepScheduling && canLaunchExecutor(pos))`会对着`0`号下标的`Wroker`开始做`while`，它是否可以循环多次取决于`keepScheduling`和`canLaunchExecutor(pos)`是否满足条件，第一次进入时`keepScheduling`为`true`，在`canLaunchExecutor()`中它拿着`pos=0`在`usableWorkers()`中用`pos`获取`coresFree`剩余的核心数`-assignedCores`当前的`Worker`分配了多少，未来分配时分出几个核心了，再用`usableWorkers`剩余的核心数再减去它`assignedCores`可能得出另外一个值。目前是`0`，那么只要它`> minCoresPerExecutor`就为`true`。那么`enoughCores`这个`Worker`还能再分配。最终只要都为`true`，那么`canLaunchExecutor()`就会返回`true`。

在资源都足够的情况下，进入`while (keepScheduling && canLaunchExecutor(pos))`，当前`minCoresPerExecutor = 3`，如果在开始我们需要`10`个，那么`coresToAssign -= minCoresPerExecutor`就等于`10 - 3 = 7`，未来再申请`7`个就可以了。因为已经从资源池`minCoresPerExecutor`中拿出了`3`个核心，且最重要的是它`assignedCores(pos)`数组身上`+ 3`，其实三层循环的作用就是为每个数组规划资源的分配。

如果一个`Worker`上只能启动一个`Executor`也就是`if (oneExecutorPerWorker)`为`true`，`if (spreadOutApps)`默认为`true`，`keepScheduling = false`会把一个东西强行改成`false`，换而言之，如果给`spreadOutApps`在配置文件中改成`false`的话，那么它就不会进来，这个循环`while (keepScheduling && canLaunchExecutor(pos))`就会继续执行，因为这其中并没有把它改成`false`，只要这个`Wroker`上还有资源可以继续分配。

那么`if (spreadOutApps)`为`true | false`，这个循环是否要继续循环会有一个特征，要注意，`while (keepScheduling && canLaunchExecutor(pos))`是嵌套在`freeWorkers.foreach `中的它有两个结果：

1. 要么`pos = 0`时在`assignedCores`和`assignedExecutors`中走一次分配，然后就退出循环，那么外层循环肯定走下一个`Worker`，然后继续从另一个`Worker`去分配资源。
2. 如果`spreadOutApps`为`false`的话，也就代表优先把当前一台的资源消耗干净。

可以理解为：水平优先申请资源，垂直优先申请资源，这完全取决于你且默认水平优先，因为你将所有的打散在集群的每一个节点上，可能更容易造成`计算向数据移动的可能性`。如果你优先在一个节点上申请`Executor`，有可能你申请三个，它上面的资源都够，结果三个都在`0`号的`Worker`上了，其它上面是没有`Executor`的，那么计算只能向第一台机器移动。那么`DataNode`数据的`Block`块有可能在其他节点上，那么数据就要发生移动`IO`拉取了。***它等于是利用循环遍历的方式，打散均匀的去分配所有资源，尽量水平分配。***

这是资源分配的环节。在资源环节完成后，最终返回的是`assignedCores[]`。

`Driver`和`Executor`都是独立的`JVM`，它们都是通过`Worker JVM`然后`fork`出的。关键在于`Executor JVM`要调起哪个主方法？

`send()`对应要看`receive()`，`ask`对应`reply`。

为什么有了一个对象，它是端点对象，它可以和`rpc`通讯层，别人给它发消息都能调用它的方法？

为什么还要在堆中再准备一批对象且再准备线程池？

因为通信分发是由端点完成的，但是这个执行器是`Executor JVM`，它就必须要有一个执行器找一批线程。因为你发来不同的任务，如果`Task`发来后，最终`Task`是交给堆中的另外一个对象的一些线程来执行的。它们是这样一个关系：通信是通信，执行是执行，执行任务是扔在线程池中执行的，所以真正的执行器是`Executor`对象，`CoarseGrainedExecutorBackend`只是`Endpoint`端点。

### 2.总结

以上基本是所有`计算层`和`资源层`的角色都已经跑通，它们的通信间也有往返，那么还有的就是分析代码该执行了。因为到`Executor`起来了，都注册成功了，代表`SparkContext()`已经创建完成。

整个`Application`其实注册它的过程就是一个启动申请`Executor`的过程，它完成注册后，其实`SparkContext`就完事了。那么再往下分析就该`创建算子, RDD`以及`Action 算子`开始调度任务，重点是`DAGScheduler`有向无环图的计算过程。以及拿到`Stage`然后`Stage`变成`Task`，`Task`再发出去的过程。也就是`任务调度的环节`，资源调度后即开始任务调度。

资源层如果是`yarn`，比如跑的模型分为两种，`Client`或`Cluster`。

1. 例如`Client`你的`SparkSubmit`，这是`Client JVM`，它其中包含`Driver`以及`自己的代码`，运行起来后`SparkSubmit`会和`yarn`通信，`yarn`会产生一个角色进程`ExecutorLauncher`约等于曾经的`ApplicationMaster`。`yarn`是一种通用的资源层，可以支持很多种计算框架，但是每种计算层对资源的描述，对话消息不同，所以其实`Driver`是和`ExecutorLauncher`通信的。

   所以`launcher`向上能和`yarn`通信，向下可以不修改`Driver`的通信语言也就是`Driver`的所有通信端点，实现类等等不用发生变化，`Driver`不需要变，因为`yarn`中肯定没有`Master`的通信风格，只有`yarn`的通信风格。所以中间加入`ExecutorLauncher`代替了`Driver`把它`Spark`西澳西资源申请的过程翻译成`yarn`能看懂的。最终由`yarn`申请完资源后会得到一批`Executor JVM`，然后`Executor JVM`创建完成后会反向注册`SparkSubmit Driver`

2. 例如`Cluster`，也有个`SparkSubmit`，只不过`Driver`不在这。此时曾经`SparkSubmit`要和`yarn`申请一个`ExecutorLauncher`，那么现在`SparkSubmit`和`yarn`申请一个`Spark`版的`ApplicationMaster JVM`包含了`ExecutorLauncher`和`Driver`，还是和`yarn`通信，再让`yarn`申请若干`Executor`，`Executor`和`ApplicationMaster`中的`Driver`通信并反向注册。

------

## 章节`16`：`Spark-CORE` 源码，`SparkContext`，`DAGScheduler`，`Stage`划分分析

------

### 一、术语

- **核心：数据期望分布式计算**

- **相干计算**：理想的相干（男女过滤的`fliter`），

  这样一系列不相干的计算如果它们串联在一起，它们可以在一台主机上的一个`Block`块上发生计算。不需要说我在一台主机把它先变成只有男人的数据，然后再发给另外一台主机，然后再去把每一个男变成男人。它们最终能被封装成一个计算组然后打包并最终坐拥在一个块上。

  什么是分布式并行计算？

  其实不相干计算它自己的一个块的每条记录它都不参与，那么其它块的记录它也不在乎。所以一个逻辑计算代码可以分发给这三个块的主机，同时拉起同时计算，线性每个块`1min`，那么就是三分钟处理完，这就是分布式并行计算的优点。

- **不相干计算**：不相干计算（把一条记录放大`10`倍 | 把某个元素在中间分割变成`KV`，一进一出 | 一进多出`map`，`flatMap`）

  期望的是把男人的数量统计出来。

  `Stage`中应该放一列不相干的操作。

面向编程人员，灵活度应该怎么操作？

所以此时出现了一个新的概念。 `Spark`中提出了一个概念，如果一个数据集，其实可以完全得到每个状态之间的中间状态，这个概念叫做`RDD`。`RDD`并不存数据其中有分区的概念。

`Spark`面向编程人员提供的是`RDD`。为什么`Stage`与`Stage`间需要`shuffle`？

因为如果这个`RDD`它后续的`RDD`依然是窄依赖，它们还可以继续在同一节点继续计算，是不需要`shuffle`的，一个`Stage`的结束边缘一定是后续要发生`shuffle`了，`shuffle`会切割`Stage`变成两个`Stage`，也就是代表着不能在一台就可以计算出结果。它必须是有一个`shuffle`拉取的过程。

### 二、分析`runJob()`

```scala
/**
 * Applies a function f to all elements of this RDD.
 */
def foreach(f: T => Unit): Unit = withScope {
  val cleanF = sc.clean(f)
  // 把自己的 RDD 作为第一个参数
  sc.runJob(this, (iter: Iterator[T]) => iter.foreach(cleanF))
}
```

所以`runJob()`第一个参数是`RDD`

```scala
/**
 * Run a function on a given set of partitions in an RDD and return the results as an array.
 *
 * @param rdd target RDD to run tasks on
 * @param func a function to run on each partition of the RDD
 * @param partitions set of partitions to run on; some jobs may not want to compute on all
 * partitions of the target RDD, e.g. for operations like `first()`
 * @return in-memory collection with a result of the job (each collection element will contain
 * a result from one partition)
 */
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: Iterator[T] => U,
    partitions: Seq[Int]): Array[U] = {
  val cleanedFunc = clean(func)
  // 进入 runJob()
  runJob(rdd, (ctx: TaskContext, it: Iterator[T]) => cleanedFunc(it), partitions)
}

/**
  * Run a function on a given set of partitions in an RDD and pass the results to the given
  * handler function. This is the main entry point for all actions in Spark.
  *
  * @param rdd target RDD to run tasks on
  * @param func a function to run on each partition of the RDD
  * @param partitions set of partitions to run on; some jobs may not want to compute on all
  * partitions of the target RDD, e.g. for operations like `first()`
  * @param resultHandler callback to pass each result to
  */
def runJob[T, U: ClassTag](
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    resultHandler: (Int, U) => Unit): Unit = {
  if (stopped.get()) {
    throw new IllegalStateException("SparkContext has been shutdown")
  }
  val callSite = getCallSite
  val cleanedFunc = clean(func)
  logInfo("Starting job: " + callSite.shortForm)
  if (conf.getBoolean("spark.logLineage", false)) {
    logInfo("RDD's recursive dependencies:\n" + rdd.toDebugString)
  }
  // 一个非常重要的类
  dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)
  progressBar.foreach(_.finishAll())
  rdd.doCheckpoint()
}
```

回想`wordcount`案例中，`res.foreach(println)`的`foreach`算子是把`res`这个`RDD`作为第一个参数，然后传给了`SparkContext()`的`runJob()`,`runJob()`只收到了先前一系列关联的`RDD`的最后一个`RDD`。每一个`RDD`被转换出来时，`RDD`埋了一个最先前的一个引用，它是一个单向链表，从后向前指向。

一个非常重要的类`DAGScheduler`，`submitStage()`通过使用`递归` + `遍历`的方式处理`RDD`。

```scala
/** Submits stage, but first recursively submits any missing parents. */
private def submitStage(stage: Stage) {
  val jobId = activeJobForStage(stage)
  if (jobId.isDefined) {
    logDebug("submitStage(" + stage + ")")
    if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
      // 找到前置 Stage
      val missing = getMissingParentStages(stage).sortBy(_.id)
      logDebug("missing: " + missing)
      if (missing.isEmpty) {
        logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
        // 触底调用, 提交任务
        submitMissingTasks(stage, jobId.get)
      } else {
        // 遍历 missing (HashSet) 中先前的 RDD
        for (parent <- missing) {
          // 递归提交
          submitStage(parent)
        }
        waitingStages += stage
      }
    }
  } else {
    abortStage(stage, "No active job for stage " + stage.id, None)
  }
}
```

------

## 章节`17`：`Spark-CORE` 源码，`TaskScheduler`，`Executor`运行`Task`，`SparkEnv`分析

------

### 一、围绕的问题

**`Task`任务是怎么出现的？**

**一个`Stage`可以最终有多少个`Task`呢？**

**`Task`是怎么分发到分布式进程中的？**

**`Task`有几种？**

**`Task`是如何执行的？**

**计算的逻辑是在`Task`执行的哪个环节中？**

任务步骤划分由`DAGScheduler`完成，任务的调度是由`TaskSchedulerImpl`这个`task`实现的。最终`submitTasks`调的是`backend`，然后一定是由端点给别人发消息才能发出去。

迭代器的数据源分为两种：`HadoopRDD.RecordReader`和`ShuffleRDDReader`。

------

### 二、`Spark`计算框架如何支撑的分布式计算过程，继续源码分析：`[计算框架]`

在分布式框架中，角色即进程，只不过在`Spark`中它的`task`是以线程的方式运行的。

一个线程如果有很多进程的话，这些线程其实它都有自己的输入输出。某一个任务可能和另一个任务有线性的关系。

如果`A`的`executor`的`task`跑完后，它的书写结果是会有别的进程中的`task`拿走去做计算，因此会有一个东西叫做资源管理层，所以它必须有一个计算层。`SparkEnv`会让任务在跑的时候，无需关心数据的来源，无需关心数据怎么才能写出去给别人，会维护很多东西满足内存管理以及数据磁盘管理。

`SparkContext`中就已经包含了`Env`。

`broadcastManager`它是一个广播变量的一个实现，可以由一个点把自己持有的数据广播到集群所有点都可以访问。

`mapOutputTracker`它是`map`输出追踪器，一个文件线性的总会有小文件寻址慢。因为大文件线性读比小文件的随机寻址读肯定速度快很多。

存储层`blockManager`中最重要的是`SortShuffleManager`，整个存储层也是一个基础设施，和很多服务关联起来，框架默认使用动态内存管理器。

最终公司`99%`的数据都可以结构化，结构化的数据就可以确定类型，确定类型的数据就可以直接变成字节数组，然后就可以放到堆外内存。

------

### 三、`MapReduce`与`Spark`的比较

所以未来有两个结果，未来大数据偏向于纯`SQL`可以解决任何问题，然后`SQL`的逻辑代码的中间调优层会倾向于不使用对象表示数据，而是直接使用那些可寻址的序列化后的结构化数据，在堆外内存也就是内核可以直接触碰到的内存寻址空间。所以在`JVM`上跑的大数据上面的东西的速度更倾向于`C`语言的那些程序的速度，但是肯定没有人家快，但它会充分利用堆外内存那块。

`MapReduce`中间有很多冷启动的过程，但`Spark`它只有两次调度，一次调度是资源`Executor`调度，先把`JVM`支起来，二次调度是`task`资源的分配，应该分配到哪个节点。

`MapReduce`只能把中间数据写到`HDFS`并且也写到磁盘，但是`Spark`它因为自己加了自己的`blockManager`而且它还有内存存储空间，所以任务中的数据那种重复使用的数据可以放到内存中，但是千万不要把`Spark`说成是内存计算框架，它还是中间`Shuffle`上可能需要磁盘的一个过程。但是它可以将一些数据缓存到内存中，速度极快，这就是为什么在逻辑回归等这样的一种迭代计算时，它的速度会比`MapReduce`快了`100`倍。不是因为它是内存计算，它是弹性的数据集。中间数据可以弹性存储在内存中。这就是为什么`Spark`比`MapReduce`快。

------

### 四、`Spark`的误区

很多的人以及所谓的机构和老师，说`Spark`是内存计算，这是错误的。

#### 4.1————思考一下，既然`Spark`这么强，为什么还有`Spark On Hive`？

`Hive`中最值钱也是最重要的就是**`MetaStore`元数据管理**，`Hive`可以是，计算的时候元数据其实存的就是表和所谓的逻辑表和数据的位置`Location`的一个应用关系，这个`Location`可以是`HDFS`，也可以是`HBase`等等其他的存储层，但是映射关系`Hive`掌握了，但是计算时可以把它转成`MapReduce`也可以是由`Spark`接管。

而且`Spark`做的更狠，其实`Spark`只是用了它的元数据映射关系，连`SQL`解析都是由`Spark`自己完成的，就是在`Spark`写的时候，`Spark`先解析你给的`SQL`转成它的计算，然后过程当中调了一下`Hive`的元素映射，找到了那个所谓的`table1`字符串，指向的是文件还是`HBase`。把对应的关系拉取过来后，自己生成自己的底层`RDD`，那么这样跑的话，最终跑的是`RDD`，那么`RDD`的跑法一定比`MapReduce`快，而且站在一家公司的角度，你都有一个`Hive`来管理你公司所有的元数据了，你就为了想使用`Spark`写`SQL`，你难道说把这样的元数据让`Spark`重新维护一个`MetaStore`到一个元数据映射层，要重新录入一遍吗？

是不是有点吃饱了撑的？

所以最终一整合，`Hive`中登记表的数据位置和信息，`Spark`只需要专心完成`SQL`解析和执行计划，物理执行计划生成就可以了。两者结合就可以了。

这也就符合了软件工程学。

而且站在公司，其实`SQL`最核心的就是`SELECT`，`FROM`，`WHERE`等多再加上`JOIN`等等这些词。

其实`SQL`的语言方法还可以扩展，那么`Hive`中它有一个`HQL`扩展出了很多东西，是适用于分布式的分区等等。那么此时其实`Spark`可以按照它的抄一遍自己实现，还是说基于它的`SparkSQL`解析直接实现，然后元数据直接映射还是继续使用它的`Hive`库。相信集成`Hive`还是更容易的。

------

## 章节`18`：`Spark-CORE`源码，`MemoryManager`，`BlockManager`分析

------

### 一、为什么`Spark`中有这么多`Manager`？

最终**总用的是`60%`的区域**，**`50% 的 onHeapStorage`是一块被保护的区域**，而且这个区域是一个虚的，曾经它的`meomry`是`static`的，而现在是统一内存管理，差异在于，如果是`static`静态的，那么如果豁出了`50%`的位置，那么左边`60%`就只能是计算的时候一个临时数据，右边就是存储的那些`RDD`数据只能存死，然后空间满了就满了，谁也不能抢谁的。

------

### 二、什么是统一动态内存管理？

就是`50%`的空间可以挪来挪去，如果没有将`RDD`的数据中间`Map`输出的数据缓存到内存的话，那么这个内存中是不是就是`Storage`中的内存就没有被使用。所以计算`Shuffle`时的数据或你给它计算时准备的那些`new`的例子，就可以吃掉`Storage`这部分的内存。

所以它是一个**动态可伸缩的**，设计成一个这样的内存使用方式。

不管是**`on Heap`还是`off Heap`都有`Storage`且都是动态的**，其实这就像是此磁盘的文件系统和磁盘驱动一样，不是给目标用户使用的。往往在它上边会包装一些东西。

`off Heap`是`Java`进程，它的进程的那个虚拟地址空间可寻址的，如果调了内核的读写的话，这个区域很快的。如果把数据写到这`on Heap`其中，那么第一容易产生`GC`，第二如果调用了系统调用，如果这个数据读写的话，其实是要经过`JVM`寻址，然后在最终给你那个`JVM`进程的栈中传参。

所以此时其实使用堆外是目前所有计算框架，更倾向于使用`off Heap`空间。因为速度最快倾向于`C`的速度，`C`是没有虚拟机的虚拟地址的概念，它不需要翻译，`C`的程序就是操作系统的程序，它的地址就是直接可访问地址。

整体来看`MemoryManager`就是玩内存的。

------

### 三、`SparkEnv`做了什么？

首先第一点，`SparkEnv`是在`Driver`端然后是在`Java`端，在`Executor`端，它们都会被执行，也就是说它们都会遇到

```scala
val blockManagerMaster = new BlockManagerMaster(registerOrLookupEndpoint( // 要么是注册 Endpoint, 要么是寻找 Endpoint
  BlockManagerMaster.DRIVER_ENDPOINT_NAME,
  // 所以在真正这步调这个函数时, 这个 new 有真的发生吗 ?
  // 整个 new 的过程被封装成一个匿名函数传进去了
  new BlockManagerMasterEndpoint(rpcEnv, isLocal, conf, listenerBus)),
  conf, isDriver)
```

`blockManagerMaster`这段代码。然后它们各自会`new`出`BlockManagerMaster`对象，然后对象被`new`的时候，其实传进去了`DriverEndpointRef`，最终造成的是`BlockManagerMaster`拿到的是`BlockManagerMasterEndpointRef`。

其实`BlockManager`分析完后，后续的东西就可以进一步分析了，但先要分析`SortShuffleManager`，如果对这个不懂的话，其实像这些传输以及`mapOutputTracker`这些根本无从入手。它们是依托于`SortShuffleManager`注册的。

------

### 四、为什么叫`SorterManager`？

逻辑是最后，因为内存中的数据要写出去，它的`Sort`分区是有序的。所在像拿着`writer`写的时候，它应该是会拿到一个数组才对，然后像下边每一个取出来每一个分区的`Id`时，这个`Id`真正要写的时候时写到一起的。

`BypassMergeSortShuffleWriter`跳过`Merge`，`combiner`行为的。

------

### 五、总结

还是没有脱离`SparkEnv`的环节，其中有很多组件，每个组件都要分析的很透彻。然后稍微把每个组件展开分析，因为它们或多或少都有一些关联性，所以在它关联的点上再去介绍某一个组件。

对`MapReduce`这个老式的计算框架埋了个伏笔，它有哪些问题，假设了下，如果你是一个`Spark`的设计者，你回去怎么去做？对`BLockManager`做展开分析，对`MemoryManager`内存存储这块做展开。

------

## 章节`19`：`Spark-CORE`源码，`Dependency`，`SortShuffleManager`分析

------

### 一、`SorterShuffleManager`组件展开分析

按理来说，其实`SorterShuffleManager`的`Shuffle`这个它是和存储相关的，因为`Shuffle`分为`write`和`read`，所以它肯定会用到存储。但是它又和计算相关，因为`DAG`到时候也是靠`Shuffle`切割，算得上`Spark`源码中比较重要的一个点。

而且一直说`Spark`比`MapReduce`快，其中也有一些方面来自于`ShuffleManager`因为它的`Shuffle`的形式是要比`MapReduce`更灵活，更针对于代码逻辑的执行优化。

#### 1.1————`SortShuffleManager`中有三种`ShuffleHandle`

1. **`BypassMergeSortShuffleHandle`**
2. **`SerializedShuffleHandle`**
3. **`BaseShuffleHandle`**

它们都是在`registerShuffle[]`中。

#### 1.1.1—————那么谁用的注册呢？

**`shuffleHandle`**和**`ShuffleDependency`**。

#### 1.2————`registerShuffle`的联系

在看`Task`任务时，一个任务，无论是`ResultTask or ShuffleTask`外边跑起来的时候都是先拿到`reader()`然后调`writer()`，这个`writer()`中会传入`RDD`迭代器。此时这个任务才能用`Pipline`的方式跑起来。

对`ShuffleRDD`既能支持`group`又能支持`reduce`，其实说白了就是支持了`combiner`又能支持排序。其实这些所有的算子的带来的性能差异其实最终就是那些参数，也就是这些参数最终作用在谁身上，是作用在`Dependencies`上。

而且最终`Dependencies`最终其实是要拿着这些参数去找`registerShuffle`使用的。这其实是一个层级依赖关系。

也就是其实最重要的是它要接收是`dependency`，因为你会发现，最终返回哪种`handler`其实一直在拿着作为参数搞事情。所以此时是在它其中真正调用`registerShuffle`。

一个`shuffle`想使用`shufId`是成本很高的，挺苛刻的事。

#### 1.3————为什么比`MapReduce`快？

最终，所有计算逻辑都可以被`BaseShuffleHandle`处理，也就是它可以处理任何情况，因为它是最后一步的判定结果。

但是在它之前先拦了两把刀，这两把刀是针对某种特殊情况，为特殊调优加速准备，所以这就是为什么`Spark`比`MapReduce`快，从`Handler`定义的顺序就逐步分析出来了。

它会针对不同的`Shuffle`行为定义不同的处理优化。

细活其实挺多的。

`MapReduce`坏处是无论是否需要排序，最终都会排序，好处就是一个任务的输出它只是一个文件中放了很多不同的分区，按顺序放好，这个顺序会让读写速度变快。

#### 1.4————回到代码开始

其实是在`new ShuflleRDD`后，它里边的`dependency`，在`new`的过程中，然后它的一个属性， `shuffleHandler`在初始化的时候，调用了`shuffleManager`的`registerShuffle`，然后取了其中每一个取回了其中的某一种`handler`，回来了`shuffleHandle`，然后且这个东西它其实有归属于`Stage`两个上下游`Stage`中都会有。一个是作为参数传进去一个是下游`RDD`中的一个属性，也就是`ShuffleDependency`既是上游`RDD`的这个`Stage`的一个参数，又是下游那个`RDD`自己的一个属性，所以两个`Stage`中都会有`ShuDependency`。

而又因为`Stage`最终是要退化成`Task`，`Task`跑的时候其实是`ShuffleMapTask`真正的物理级别。并且一定会有一个`runTask`。

`registerShuffle()`是在`new ShuffleRDD`对象时被用到的，`getWriter()`是一个`Task`跑起来后，第一个先要去取到了一个`writer`。

##### 1.4.1—————怎么取呢？

把`shuffleHandler`传了进去，且最主要的是传达的`depshuffleHandle`以及`context`。`reader`是找最开始的`RDD`的`reader`，因为它一定会通过`shuffle`拉取数据。

只要是`RDD`它就一定会有`compute()`

```scala
override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
  val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
  // 通过 SparkEnv 调用 getReader()
  SparkEnv.get.shuffleManager.getReader(dep.shuffleHandle, split.index, split.index + 1, context)
    .read()
    .asInstanceOf[Iterator[(K, C)]]
}
```

也就是说`getReader()`是一个`Task`左边缘，`getWriter()`是右边缘迭代器使用它的`write`，然后这个迭代器会间接的串到它左边缘的那个`RDD`身上的`compute()`中去掉`getReader()`。

#### 1.5————`getWriter()`也有三种

1. **`UnsafeShuffleWriter`**
2. **`BypassMergeSortShuffleWriter`**
3. **`BaseShuffleHandle`**

------

### 二、思考

假设用一个数组实现`HashMap`行不行？

```scala
// 和 MapReduce 的哈希环一个意思
// 存储数据的索引
private var data = new Array[AnyRef](2 * initialCapacity)
```

`AnyRef`是一个引用类型，开辟多少字节空间？

通俗来说是`4`个字节，这个数组中的每一个元素是用来存储未来的每一个对象吗？显然不是它应该是存那些数据的引用。也就是说它存的是数据的索引。

如果遍历数据集取单元素应该是线性步进，如果取`n`个元素就是`n`步进，或者指数步进，且用线性空间存储`KV`的话，这个数组的宽度如果想存储`10`个`KV`其实它的宽度应该是`20`。

所以这也就是为什么要`* 2`以及取元素也是通过这种方式取正确的`Key`的位置。

------

### 三、总结

分析到一个比较核心的位置，就是`Shuffle`这个系统。`SortShuffleManager`其中有`registerShuffle()`，`getReader()`，`getWriter()`。

在`register()`时，会有三个`handle`，`Bypass`是跳过聚合和排序的，它利用的就是直接将每条记录输出到文件，最后再把文件中每个分区的文件合并成一个文件，然后再产生一个所有文件就结束了。

这种行为不会内存积压，然后也没有`CPU`上的一些排序、聚合等等复杂的操作。所以相对比较快，但是它有限定的应用场景，就是约束条件，`Map`端是没有聚合的，因为如果聚合的话，就要在内存开辟一个缓冲区，等待着若干条记录，然后给它做一次合并。如果开启聚合就有这一行为。所以它不能聚合，这样才能支持来记录即刻写文件。

下游分区数量也不能太多，官方默认**`200`**，下游分区数量决定了其中一个`Map`它溢写时写的小文件的数量。如果小文件下游分区数`> 200`，那么等于你默认开启很多小文件。

#### 3.1————开启小文件带来的诸多问题

1. 文件描述符

   需要消耗整个计算机连同系统内核中总文件描述数量，且最终`JVM`上也要开辟对象。

2. 内存开销大

   资源内存消耗较大，`Spark`用折中的方式解决，就是在计算时在这种特殊情况下，既没聚合行为`groupByKey`、`sortByKey`、`combineByKey`算子。所以此时才有可能触发`Bypass`，可以把它想象成`Shuffle`中的整个`Shuffle`所能干的所有事情的一个子集。

#### 3.2————`shuffle`管理器

为什么称为**管理器？**

因为`SparkShuffle`系统很复杂，它的写会有很多中写法，所以需要一个管理器根据代码的调用不同的算子中的`Dependency`，也就是前后依赖`Dependency`的那些属性依赖关系，尤其这种`shuffle`依赖关系其中属性的不同，因为要看的是最终这些`Dependency`中这些上下依赖的关系的属性。

这些**属性的不同**，所以造成的**`shuffle`写的方式不同**。所以它需要有一个**管理器**，这便是管理器的维度。

#### 3.3————`Partitioner`分区器

另外还有一个叫做分区器，它有两种分为：

- **`HashPartitioner`**
- **`RangePartitioner`**

提供排序及区间排序的分区器。

#### 3.4————`SortShuffleManager`分区管理器

**它为什么叫做`SortShuffleManager`？且它的`handle`这种`writer`还叫`BypassMergeSortShuffleWriter`。**

按理说这个`handle`的`writer`它**跳过排序**，它就**不应该归属于`SortShuffleManager`**，但其实你会发现其**物理结果文件是分区有序的**。

所以这种虽然**看似跳过排序**，但因为它**生成的文件**是**线性拼接的有序文件**，它**依然有序**。

其实如果最后不合成一个文件，不是一个全局有序的文件，就是分区文件组中散开小文件，其实就是曾经的**`HashShuffleManager`**。

`Key`是乱序的，但分区是有序的。

------

## 章节`20`：`Spark-CORE`源码，`SortShuffleWriter`，内存缓冲区`buffer`

------

### 一、`BypassMergeSortShuffleWriter`优化`ShuffleWriter`行为

它在代码逻辑中不期望做这种有开销的事情的时候，它就是使用这种最快的方式。但尤其在代码逻辑中，尤其类似按组做聚合计算写计算结果是一个唯一值时。

#### 1.1————第一种不起作用的情况

也就是说，在计算过程中，会有分组计算的行为，且计算的结果的数据量会有变化，像`reduceByKey`或`combineByKey`这种二次均值的操作时。显然`BypassMergeSortShuffleWriter`就不起作用了。

也就是说我们期望聚合，所以它不起作用。

#### 1.2————第二种不起作用的情况

下游分区数量`> 200`时，会有一个弊端，小文件数量太多，`IO`太多，性能和内存反而下降。

`combine`内存不会产生积压，它根本就不能实现`combine`，所以它此时根本不会走。

因此，有没有`combine`是一个区别点。

#### 1.3————如何达成“满足任何算子的计算过程”？

**`BypassMergeSortShuffleManager`可以没有**，**`UnsafeShuffleManagerWriter`也可以没有**，其实`Spark`**只要有一个`SortShuffleWriter`就可以满足任何算子的计算过程**，只不过用它是**必须要开辟内存缓冲区这种计算模型**。

所以会有内存消耗。而**`BypassMergeSortShuffleManager`是不需要开辟内存缓冲区的**。

#### 1.4————`UnsafeShuffleWriter`是基于`BypassMergeSortShuffleManager`和`SortShuffleWriter`之间的优化

------

### 二、`SortShuffle.sorter`分析

#### 2.1————`RDD`中的数据是怎么完成计算和书写？

```scala
// 一个任务它写的数据是怎么来的 ?
// 就是来自于 Iterator(一个任务最后的迭代器)
override def write(records: Iterator[Product2[K, V]]): Unit = {
  sorter = if (dep.mapSideCombine) {
    require(dep.aggregator.isDefined, "Map-side combine without Aggregator specified!")
    new ExternalSorter[K, V, C](
      context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)
  } else {
    // In this case we pass neither an aggregator nor an ordering to the sorter, because we don't
    // care whether the keys get sorted in each partition; that will be done on the reduce side
    // if the operation being run is sortByKey.
    new ExternalSorter[K, V, V](
      context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)
   }
   // 进入 insertAll()
   sorter.insertAll(records)

   // Don't bother including the time to open the merged output file in the shuffle write time,
   // because it just opens a single file, so is typically too fast to measure accurately
   // (see SPARK-3570).
   val output = shuffleBlockResolver.getDataFile(dep.shuffleId, mapId)
   val tmp = Utils.tempFileWith(output)
   try {
     val blockId = ShuffleBlockId(dep.shuffleId, mapId, IndexShuffleBlockResolver.NOOP_REDUCE_ID)
     val partitionLengths = sorter.writePartitionedFile(blockId, tmp)
     shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, tmp)、
     // 更新 mapStatus 状态
     mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths)
   } finally {
     if (tmp.exists() && !tmp.delete()) {
       logError(s"Error while deleting temp file ${tmp.getAbsolutePath}")
     }
   }
}
```

`sorter`有两种可能性，但是`new`的都是同一个`ExternalSorter`，只不过在传参构造上有差异

- `context, dep.aggregator, Some(dep.partitioner), dep.keyOrdering, dep.serializer)`
- `context, aggregator = None, Some(dep.partitioner), ordering = None, dep.serializer)`

最主要的是有没有聚合器，这会最终决定`sorter`的特征。

```scala
def insertAll(records: Iterator[Product2[K, V]]): Unit = {
  // TODO: stop combining if we find that the reduction factor isn't high
  val shouldCombine = aggregator.isDefined

  // 是否需要聚合
  if (shouldCombine) {
    // Combine values in-memory first using our AppendOnlyMap
    val mergeValue = aggregator.get.mergeValue
    val createCombiner = aggregator.get.createCombiner
    var kv: Product2[K, V] = null
    val update = (hadValue: Boolean, oldValue: C) => {
      if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
    }
    while (records.hasNext) {
      addElementsRead()
      kv = records.next()
      // 用 k 计算 k 的分区号, 以及加上 k 再加上需要更新的东西
      map.changeValue((getPartition(kv._1), kv._1), update)
      maybeSpillCollection(usingMap = true)
    }
  } else {
    // Stick values into our buffer
    while (records.hasNext) {
      addElementsRead()
      val kv = records.next()
      // 走一个 buffer
      // 这块类似 Map 执行结果由 KV 变为 KVP
      buffer.insert(getPartition(kv._1), kv._1, kv._2.asInstanceOf[C])
      maybeSpillCollection(usingMap = false)
    }
  }
}
```

在`insertAll()`中，会判断是否需要聚合，且一个走`map`一个走`buffer`。

`buffer.insert()`类似`MapReduce`只不过`MapReduce`它的缓冲区是一个字节数组，它其中放的是数据序列化后，只不过此处会发现它的引用类型的`buffer`他显然还没做序列化这件事。

##### 2.1.1—————数组扩容

```scala
/** Add an element into the buffer */
def insert(partition: Int, key: K, value: V): Unit = {
  if (curSize == capacity) {
    // 数组扩容
    growArray()
  }
  // 分区号, Key
  data(2 * curSize) = (partition, key.asInstanceOf[AnyRef])
  // Value
  data(2 * curSize + 1) = value.asInstanceOf[AnyRef]
  curSize += 1
  afterUpdate()
}

/** Double the size of the array because we've reached capacity */
// 数组扩容
private def growArray(): Unit = {
  if (capacity >= MAXIMUM_CAPACITY) {
    throw new IllegalStateException(s"Can't insert more than ${MAXIMUM_CAPACITY} elements")
  }
  val newCapacity =
    if (capacity * 2 > MAXIMUM_CAPACITY) { // Overflow
      MAXIMUM_CAPACITY
    } else {
      capacity * 2
    }
  val newArray = new Array[AnyRef](2 * newCapacity)
  // JDK 自带的方法
  System.arraycopy(data, 0, newArray, 0, 2 * capacity)
  data = newArray
  capacity = newCapacity
  resetSamples()
}
```

在`growArray()`中其实走了一个`JDK`自带的方法。

##### 2.1.2—————`buffer`的`spill`溢写

其实是拿着排序`CPU`的计算的过程，换取了不开辟更多的小文件。

在输入向内存缓冲区溢写的事完成后，他会有一个合并小文件的过程，并且最终它要完成一个`mapStatus`状态的更新，然后会把状态更新回`Driver`，因为`Driver`中还有`taskScheduler`任务调度器需要知道这个`map`任务跑完后才能调度下边的任务。

**二者的差异：**

- 上边是不在内存缓冲区，到达一个分区就记录直接到达自己的分区文件了，所以每个文件都是独立分区。
- 下边是所有记录先到达内存，那么这其中的分区肯定是乱七八糟的，但是只不过是会有一个排序的过程生成那个文件中将分区与分区分隔开。相同分区是在一起了但是他们不会穿插。

##### 2.1.3—————`map`的`spill`溢写

**会有一个新旧记录和哈希碰撞的解决方案。**

一个是用**数组**引用元素，一个是**数组 + 链表**引用元素。数组中也要有头，数组中的存储的链表也有对象头元素，且元素还有指针，指针也是内存空间`4`字节引用的开辟。

此时发现用数组也能`hold`这些数据，用`HashMap`也能`hold`这些数据，那么无非就是在**快速**和**省内存**中抉择。如果是**`Web`开发不会有大数据**，所以肯定是需要的**速度稍微可能好点**，但如果是**大数据**，那么**数据会冲击内存**，相对来说用**类似数组的`HashMap`反而更省内存**。

##### 2.1.4—————数组是怎么玩的？

```scala
/**
  * Set the value for key to updateFunc(hadValue, oldValue), where oldValue will be the old value
  * for key, if any, or null otherwise. Returns the newly updated value.
  */
def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
  assert(!destroyed, destructionMessage)
  val k = key.asInstanceOf[AnyRef]
  if (k.eq(null)) {
    if (!haveNullValue) {
      incrementSize()
    }
    nullValue = updateFunc(haveNullValue, nullValue)
    haveNullValue = true
    return nullValue
  }
  var pos = rehash(k.hashCode) & mask
  var i = 1
  // 整个 while 的作用类似遍历二叉树并更新节点值
  while (true) {
    // 类似二叉树中的非叶子节点(左右头节点或左右父节点)
    val curKey = data(2 * pos)
    // 尝试在数组中取
    if (curKey.eq(null)) {
      // 曾经没有出现过
      // 进行赋值操作
      val newValue = updateFunc(false, null.asInstanceOf[V])
      data(2 * pos) = k
      data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
      incrementSize()
      return newValue
    } else if (k.eq(curKey) || k.equals(curKey)) {
      // 已经出现过
      // 进行更新操作
      val newValue = updateFunc(true, data(2 * pos + 1).asInstanceOf[V])
      data(2 * pos + 1) = newValue.asInstanceOf[AnyRef]
      return newValue
    } else {
      // 哈希碰撞
      val delta = i
      pos = (pos + delta) & mask
      i += 1
    }
  }
  null.asInstanceOf[V] // Never reached but needed to keep compiler happy
}
```

先用到达的数据算出哈希值以及下标，和所谓的因子数`x 2`，尝试从数组中取，取出的`Key`分为三种情况

1. **`if (k.eq(null))`**
2. **` else if (k.eq(curKey) || k.equals(curKey))`**
3. **哈希碰撞**

##### 2.1.5—————怎么保证一直有坑位？

```scala
/** Double the table's size and re-hash everything */
// 数组扩容
protected def growTable() {
  // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
  val newCapacity = capacity * 2
  require(newCapacity <= MAXIMUM_CAPACITY, s"Can't contain more than ${growThreshold} elements")
  val newData = new Array[AnyRef](2 * newCapacity)
  val newMask = newCapacity - 1
  // Insert all our old values into the new array. Note that because our old keys are
  // unique, there's no need to check for equality here when we insert.
  var oldPos = 0
  while (oldPos < capacity) {
    if (!data(2 * oldPos).eq(null)) {
      val key = data(2 * oldPos)
      val value = data(2 * oldPos + 1)
      var newPos = rehash(key.hashCode) & newMask
      var i = 1
      var keepGoing = true
      while (keepGoing) {
        val curKey = newData(2 * newPos)
        if (curKey.eq(null)) {
          newData(2 * newPos) = key
          newData(2 * newPos + 1) = value
          keepGoing = false
        } else {
          val delta = i
          newPos = (newPos + delta) & newMask
          i += 1
        }
      }
    }
    oldPos += 1
  }
  data = newData
  capacity = newCapacity
  mask = newMask
  growThreshold = (LOAD_FACTOR * newCapacity).toInt
}
```

之所以没有使用先前的`System.arraycopy()`，是因为**每个元素不是线性的**，**无关联的扔在数组中**，它要通过一个**哈希算法放入其中**，所以需要`rehash()`一遍。

所以没有调用`System.arraycopy()`的拷贝过程。以此来保证一直有坑位。

------

### 三、`BypassMergeSortShuffleManager`宏观分析

最终分析完源码后，`BypassMergeSortShuffleManager`是一种无内存缓冲区，无内存开辟，直接动用`IO`生成文件进行拼接，得到的一个结果文件。

如果有聚合或者当`IO`文件可能会有很多影响性能的时候，那么会触发`SortShuffleWriter`跳过这种无内存缓冲的直接动用内存缓冲。但是动用内存缓冲还分为两种性能，内存只要动用内存缓冲就走`ExternalSorter`，然后通用的是`insertAll()`只不过它有一个优化在其中。因为动用内存缓冲分为聚合和不聚合，就是分为有没有`Key`判定寻找或`Value`计算这么一个函数的触发过程。如果只是为了优化上面这种小文件看得太多的情况，就直接走`buffer`没有聚合。那么此时其实动用的也是一维数组引用对象的指针，然后线性顺序堆放其中，最终走的是溢写排序，然后归并得到一个和它一样的全排列文件分区有序。

如果走的是有归并这种`combine`操作，对数据压缩这种行为的话，那就走`map`的`KV`形式。但是`map`底层，因为做的是大数据，尽量减少对象的开辟，尽量减少`GC`的产生，所以他其中也是动用了`data`是一个线性数组。引用所有元素。

二者的差异是一个是线性放入其中一个是哈希算法。哈希算法的实现为循环规避碰撞，线性探测再散列。

------

### 四、`IO`角度对比`Spark`和`MapReduce`

`MapReduce`对于溢写是在数据放入哈希环后触发`combine`进行缩小溢写。

`Spark`对于溢写是在放的时候就开始`combine`聚合数据，所以它更能在内存停留更多的重复数据，因为重复数据已经约成一条了，所以它溢写的次数一定会比`MapReduce`的哈希环次数少。

**能够更好的优化`IO`才是真正的架构师，才是大佬。**

这也就是为什么说`Spark`的性能是`MapReduce`的几倍甚至几十倍几百倍。更多的是充分利用内存以及更强的去压榨内存。

内存的寻址速度是磁盘的十万倍`CPU`中的寄存器中的数据获得的速度要比内存又快了很多。所以此时其实`CPU`算的是很快的，不要认为增加了`CPU`的指令集调用，增加了一些方法它就一定会慢。其实`CPU`多浪费一点算力，相对于`IO`来说的话完全可以忽略不计。

其实越往`CPU`方面去动用损耗反而越甩向外的速度损耗。

------

### 五、总结

上游写的环节，实际上写的时候，它有三个实现

- `BypassMergeSortShuffleManager`
- `SortShuffleWriter`
- `UnsafeShuffleWriter`

他们都是写的环节，`Spark`为什么分为这三个，他是在`shuffle`这个系统中想做一些调优，也就是我们在做数据计算的时候如果数据有节点间的移动的话，其实移动的成本是不一样的根据不同计算。有些就是数据改个位置，所以此时做分组他也没有什么聚合缩减数据的现象，所以`Bypass`就可以了，因为它可以减掉一些内存的积压和`CPU`的排序过程。所以无需`merge`和`sort`，这样是最直接的落地成文件。

- **对象储存大于序列化的结果**
- **`JVM`堆中`> Java`堆（堆外）**
- **堆外只能是字节数**

但是也有一些计算是需要在内存缓冲一会儿，然后做一些基于缓冲的数据做一些聚合、统计等等操作。所以它还有一个保底的`SortShuffleWriter`，这两个基本上可以覆盖所有的调优范围，只不过它还有一个就是`Unsafe`。

字节数组它有一个弊端，序列化。

------

## 章节`21`：`Spark-CORE`源码，`SortShuffleWriter`，内存缓冲区`buffer`

------

### 一、`UnsafeShuffleWriter`的写

`Unsafe`中也有`sorter`，最终`sorter.insertRecord()`插入数据。只不过不是`KV`而是`Buffer`。

有时写文件一定会发生磁盘`IO`，磁盘`IO`是整个计算机的瓶颈。那么此时这个框架如果想运行的快点的话，我不把数据写到磁盘我先写到内存，那么此时可以再准备一个新的`output`。`output`可以指向内存中的一个`ByteArray`。根据字节数组准备一个输出流然后此时再将这个`output`传递给框架，那么框架的数据就不会写磁盘，没有磁盘`IO`就直接写到内存了，再使用字节数组进行后续处理。`file`有一个输出流，字节数组`ByteArray`也有一个输出流。

```java
@Nullable private ShuffleExternalSorter sorter;
private MyByteArrayOutputStream serBuffer;
// 不是 KV 而是 Buffer
sorter.insertRecord(
serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
```

`serializedRecordSize`：实际存放多少数据。

`partitionId`：分区`Id`。

以上都在`JVM`堆中。

内存也牵扯到使用数据页而不是使用低单位存储数据，因为会浪费更多的内存索引空间。磁盘是一个线性地址空间虽然它有磁道，内存也是线性地址空间，网络流也是线性地址空间。只要是二进制的计算机，这种数字信号系统它都可以理解成线性空间。

元数据：可以理解为偏移量。

索引：从二进制`bit`到`Byte`已经可以让数据变得很小了。

------

### 二、`insertRecord()`中的`page`页以及`OS`中的`page`页

```java
/**
 * Write a record to the shuffle sorter.
 */
public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
  throws IOException {

  // for tests
  assert(inMemSorter != null);
  if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
    logger.info("Spilling data because number of spilledRecords crossed the threshold " +
      numElementsForSpillThreshold);
    spill();
  }

  // 扩容
  growPointerArrayIfNecessary();
  // Need 4 bytes to store the record length.
  final int required = length + 4;
  acquireNewPageIfNecessary(required);

  assert(currentPage != null);
  final Object base = currentPage.getBaseObject();
  // 数据寻址位置
  final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
  Platform.putInt(base, pageCursor, length);
  pageCursor += 4;
  // recordBase : 堆中的数组
  // 将数据存入 page 页
  Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
  pageCursor += length;
  // 存储所谓的索引
  inMemSorter.insertRecord(recordAddress, partitionId);
}
```

`currentPage`很复杂。`Byte[]`要放入所谓的`page`中，最终排序排索引即可。因为排索引它对内存整理数据的成本是极低的它的交换中位置没有数据迁移的过程。

##### 2.1————先分析`Executor`

`Executor`有自己的内存管理器`TaskMemroyManager`，在这其中它会把内存划分为两块区域`Execution`，`Storage`。

但是这个计算空间整体可以理解为是所有任务共享的，那么每个任务怎么使用空间？

它其中出现了一个`TaskMemoryManager`，各自通过自己的内存任务管理器和`MemoryManager`进行交涉。

`task.run()`会调用`runTask()`而后子类`ShuffleMapTask`对其进行实现并运行任务。这个逻辑是归属于一个线程中的，且这个线程中专门是有一个维护了一个自己的`context`，其中传参是包含了`taskMemoryManager`。`tungstenMemoryMode`钨斯计划的目的是为`Spark`计算进行提速。

2.1.1—————`MemoryBlock`

```java
// UnsafeMemoryAllocator 会使用 MemoryBlock (堆外)
@Override
public MemoryBlock allocate(long size) throws OutOfMemoryError {
  // 底层动用的是 Unsafe 然后返回 Address
  long address = Platform.allocateMemory(size);
  MemoryBlock memory = new MemoryBlock(null, address, size);
  if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
    memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
  }
  return memory;
}
```

```java
// HeapMemoryAllocator 也会使用 MemoryBlock (堆内)
@Override
public MemoryBlock allocate(long size) throws OutOfMemoryError {
  if (shouldPool(size)) {
    synchronized (this) {
      final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(size);
      if (pool != null) {
        while (!pool.isEmpty()) {
          final WeakReference<long[]> arrayReference = pool.pop();
          // 堆分配开辟一个数组就可以了
          final long[] array = arrayReference.get();
          if (array != null) {
            assert (array.length * 8L >= size);
            MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
            if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
              memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
            }
            return memory;
          }
        }
        bufferPoolsBySize.remove(size);
      }
    }
  }
  // 同理
  long[] array = new long[(int) ((size + 7) / 8)];
  MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
  if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
    memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
  }
  return memory;
}
```

所谓的`page`其实是`MemoryBlock`，它来自于`MemoryLocation`的一个映射，它的数据其实是被分配到`Unsafe`堆外的内存空间中。它其实就是对它持有地址的一个使用。所以它的父类叫做`MemoryLocation`。

以上流程完毕后，便有了`currentPage`对象。

------

### 三、为什么下游分区数是`15777215`而不是`32`或者其他数值？

因为动完之后精度会有损失。

```java
/**
 * Pack a record address and partition id into a single word.
 *
 * @param recordPointer a record pointer encoded by TaskMemoryManager.
 * @param partitionId a shuffle partition id (maximum value of 2^24).
 * @return a packed pointer that can be decoded using the {@link PackedRecordPointer} class.
 */
public static long packPointer(long recordPointer, int partitionId) {
  assert (partitionId <= MAXIMUM_PARTITION_ID);
  // Note that without word alignment we can address 2^27 bytes = 128 megabytes per page.
  // Also note that this relies on some internals of how TaskMemoryManager encodes its addresses.
  // 地址向右移 24 位, 腾出了前面的 24 位
  final long pageNumber = (recordPointer & MASK_LONG_UPPER_13_BITS) >>> 24;
  final long compressedAddress = pageNumber | (recordPointer & MASK_LONG_LOWER_27_BITS);
  // 此处就确定了为什么下游分区数是 16777215
  // 低 24 位移到左边
  // 左边是分区号右边是地址
  return (((long) partitionId) << 40) | compressedAddress;
}
```

------

### 四、总结

**整体的思路：**

主要的目的是想，最终要在堆中维护索引。**那么一条记录要消耗多少空间记录它的索引呢？**

`32位64位128位256位`，其实你可以开辟更多的位数。但是最终如果你选定了一个某些位数的时候，它其中要放两个元素

- 位置
- 分区号

在分析之前的流程的时候，都是带着它的分区号，因为你只有带着分区号未来做输出的时候就可以把相同分区的数据，按照这个索引排在一起，一定会保证相同的分区数据排在一起，生成在一个文件中。生成文件段的时候这个段中的分区记录是排在一起的。所以索引中是一定要带着分区的，而且索引是要索引数据，所以一定要有数据地址。所以大家一块儿放入其中的时候要怎么放要开辟多大的字节数组存储这两个东西。

我们知道分区的分区`ID`号是`int`类型四个字节，四个字节虽然可以表示很大。但是它这个时候有消耗，因为你既有地址又有分区的话，那么分区如果`32`你地址这边只能使用`32位`了，**用`64位`的空间索引固定宽度**，**既描述的分区号又描述的寻址地址**。

这样的话，左边`24位`是分区，右边是地址，拼成了一个64位的。

**最主要的是，它在堆中开辟的索引是指向了堆中那个字节数组的位置，或者堆外那个字节数组的位置，它是一个指向。**

#### 4.1————为什么我们要分析`UnsafeShuffleWriter`？

后续会分析`Spark SQL`，或者结构化数据。其实它有一个东西叫`DataSet`。

`DataSet`分为两类

- 有类型
- 无类型

在`SQL`优化的时候，更倾向于把我们的数据不用`RDD`，而用`DataSet`会直接做序列化减少对象在节点间序列化、反序列化的过程，因为如果你的数据开始在磁盘上，加载到`JVM`中肯定变成对象，在序列化和反序列化之间肯定会有消耗，因此要减少这种消耗。整个框架追求的是内存的最高利用率。

------

## 章节`22`：`Spark_CORE`源码，`UnsafeShuffleWriter`，`Tungsten`，`Unsafe`，堆外

------

### 一、`Unsafe`细节深度分析

**把你想象成一条数据，你这个数据变成数组，你是怎么到的内存里，然后最终怎么到磁盘上。这一系列的细节？**

此时

```java
while (records.hasNext()) {
  insertRecordIntoSorter(records.next()); // object
}
```

这个环节的对象还是`Object`，这个记录拿到后调了一个`insertRecordIntoSorter()`本类的方法。它在

```java
@VisibleForTesting
void insertRecordIntoSorter(Product2<K, V> record) throws IOException {
  assert(sorter != null);
  final K key = record._1();
  final int partitionId = partitioner.getPartition(key);
  // 清空
  serBuffer.reset();
  // 序列化
  serOutputStream.writeKey(key, OBJECT_CLASS_TAG);
  serOutputStream.writeValue(record._2(), OBJECT_CLASS_TAG);
  // 溢写到 Buffer
  serOutputStream.flush();
  // 以上是将记录的对象转换成字节数组, 放到了堆中的字节数组中

  final int serializedRecordSize = serBuffer.size();
  assert (serializedRecordSize > 0);

  // 不是 KV 而是 Buffer
  sorter.insertRecord(
    serBuffer.getBuf(), Platform.BYTE_ARRAY_OFFSET, serializedRecordSize, partitionId);
}
```

它在`record`中取出了`key`和`value`，在这个过程中准备了一个`buffer`，**将它写到`buffer`中，其实这个过程中已经做了一次序列化，也就是由对象变成字节数组，这一步很重要**，然后最核心的步骤，记录会插入到`sorter`中。这个记录其实就是上面`buffer`中的`getBuf()`取出字节数组。

然后最主要的是`sorter`由什么构成？

`ShuffleExternalSorter`中会有一个`insertRecord()`，`buffer`最终变成了字节数。

```java
/**
 * Write a record to the shuffle sorter.
 */
public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
  throws IOException {

  // for tests
  assert(inMemSorter != null);
  if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
    logger.info("Spilling data because number of spilledRecords crossed the threshold " +
      numElementsForSpillThreshold);
    spill();
  }

  // 扩容
  growPointerArrayIfNecessary();
  // Need 4 bytes to store the record length.
  final int required = length + 4;
  // 申请 page
  // 如果先前申请的页面没有满, 第二条记录是不会分配页面的
  acquireNewPageIfNecessary(required);

  assert(currentPage != null);
  // 曾经的 array
  final Object base = currentPage.getBaseObject();
  // 数据寻址位置
  final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
  // 传入 字节数组, 游标, 长度
  Platform.putInt(base, pageCursor, length);
  pageCursor += 4;
  // recordBase : 堆中的数组
  // 将数据存入 page 页
  // 传入 当前 page 页和游标
  Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
  pageCursor += length;
  // 存储所谓的索引
  inMemSorter.insertRecord(recordAddress, partitionId);
}
```

#### 1.1————`length`为什么`+ 4`？

> Need 4 bytes to store the record length.

也就是我们其实这个记录的`Object`，他比如说`500B`，然后再加上`4B`，这`4B`其实未来是描述这个记录的大小存在，即记录大小 + 描述它大小的`4B`那个值就是`require`希望申请的字节数量。

------

### 二、`growPointerArrayIfNecessary()`扩容

均通过`Unsafe.copyMemory()`只不过根据`base`是否为空略微有差异。

------

### 三、`acquireNewPageIfNecessary()`申请`page`

```java
/**
 * Allocates more memory in order to insert an additional record. This will request additional
 * memory from the memory manager and spill if the requested memory can not be obtained.
 *
 * @param required the required space in the data page, in bytes, including space for storing
 *                      the record size. This must be less than or equal to the page size (records
 *                      that exceed the page size are handled via a different code path which uses
 *                      special overflow pages).
 */
private void acquireNewPageIfNecessary(int required) {
  // 要么没有 currentPage 要么 currentPage 写满了
  if (currentPage == null ||
    pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
    // TODO: try to find space in previous pages
    // 分配页面
    currentPage = allocatePage(required);
    // 取第一个对象的游标
    pageCursor = currentPage.getBaseOffset();
    allocatedPages.add(currentPage);
  }
}
```

`currentPage == null ||
    pageCursor + required > currentPage.getBaseOffset() + currentPage.size()`要么是没有 `currentPage`要么是`currentPage`写满了。

然后进行分配页面的流程。

#### 3.1————分配页面

进入到`allocatePage()`

```java
/**
 * Allocate a memory block with at least `required` bytes.
 *
 * @throws OutOfMemoryError
 */
protected MemoryBlock allocatePage(long required) {
  MemoryBlock page = taskMemoryManager.allocatePage(Math.max(pageSize, required), this);
  if (page == null || page.size() < required) {
    throwOom(page, required);
  }
  used += page.size();
  return page;
}
```

这其中就用到了`taskMemoryManager`，任务其实是根据自己任务的`taskMemoryManager`，每个任务一个`Executor`进程中会有很多的线程去跑任务，每个任务其实通过自己的任务内存管理器去申请，申请的时候进入到`allocatePage()`

```java
/**
 * Allocate a block of memory that will be tracked in the MemoryManager's page table; this is
 * intended for allocating large blocks of Tungsten memory that will be shared between operators.
 *
 * Returns `null` if there was not enough memory to allocate the page. May return a page that
 * contains fewer bytes than requested, so callers should verify the size of returned pages.
 * <br>
 * 堆内存的总访问入口
 *
 * @throws TooLargePageException
 */
public MemoryBlock allocatePage(long size, MemoryConsumer consumer) {
  assert(consumer != null);
  assert(consumer.getMode() == tungstenMemoryMode);
  if (size > MAXIMUM_PAGE_SIZE_BYTES) {
    throw new TooLargePageException(size);
  }

  // 是否能申请出内存
  long acquired = acquireExecutionMemory(size, consumer);
  if (acquired <= 0) {
    return null;
  }

  final int pageNumber;
  synchronized (this) {
    // 页的编号
    // 分配的 page
    pageNumber = allocatedPages.nextClearBit(0);
    if (pageNumber >= PAGE_TABLE_SIZE) {
      releaseExecutionMemory(acquired, consumer);
      throw new IllegalStateException(
        "Have already allocated a maximum of " + PAGE_TABLE_SIZE + " pages");
    }
    allocatedPages.set(pageNumber);
  }
  // 数据页的类型是 MemoryBlock
  MemoryBlock page = null;
  try {
    // 做钨斯计划的内存分配
    page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
  } catch (OutOfMemoryError e) {
    logger.warn("Failed to allocate a page ({} bytes), try again.", acquired);
    // there is no enough memory actually, it means the actual free memory is smaller than
    // MemoryManager thought, we should keep the acquired memory.
    synchronized (this) {
      acquiredButNotUsed += acquired;
      allocatedPages.clear(pageNumber);
    }
    // this could trigger spilling to free some pages.
    return allocatePage(size, consumer);
  }
  page.pageNumber = pageNumber;
  pageTable[pageNumber] = page;
  if (logger.isTraceEnabled()) {
    logger.trace("Allocate page number {} ({} bytes)", pageNumber, acquired);
  }
  return page;
}
```

申请一个足够大的页面，它做如下了几步：

##### 3.1.1—————第一步

`acquireExecutionMemory()`是`task`任务内存管理器，会走到总的内存管理器身上。

```java
/**
 * Acquire N bytes of memory for a consumer. If there is no enough memory, it will call
 * spill() of consumers to release more memory.
 *
 * @return number of bytes successfully granted (<= N).
 */
public long acquireExecutionMemory(long required, MemoryConsumer consumer) {
  assert(required >= 0);
  assert(consumer != null);
  MemoryMode mode = consumer.getMode();
  // If we are allocating Tungsten pages off-heap and receive a request to allocate on-heap
  // memory here, then it may not make sense to spill since that would only end up freeing
  // off-heap memory. This is subject to change, though, so it may be risky to make this
  // optimization now in case we forget to undo it late when making changes.
  synchronized (this) {
    long got = memoryManager.acquireExecutionMemory(required, taskAttemptId, mode);

    // Try to release memory from other consumers first, then we can reduce the frequency of
    // spilling, avoid to have too many spilled files.
    if (got < required) {
      // Call spill() on other consumers to release memory
      // Sort the consumers according their memory usage. So we avoid spilling the same consumer
      // which is just spilled in last few times and re-spilling on it will produce many small
      // spill files.
      TreeMap<Long, List<MemoryConsumer>> sortedConsumers = new TreeMap<>();
      for (MemoryConsumer c: consumers) {
        if (c != consumer && c.getUsed() > 0 && c.getMode() == mode) {
          long key = c.getUsed();
          List<MemoryConsumer> list =
              sortedConsumers.computeIfAbsent(key, k -> new ArrayList<>(1));
          list.add(c);
        }
      }
      while (!sortedConsumers.isEmpty()) {
        // Get the consumer using the least memory more than the remaining required memory.
        Map.Entry<Long, List<MemoryConsumer>> currentEntry =
          sortedConsumers.ceilingEntry(required - got);
        // No consumer has used memory more than the remaining required memory.
        // Get the consumer of largest used memory.
        if (currentEntry == null) {
          currentEntry = sortedConsumers.lastEntry();
        }
        List<MemoryConsumer> cList = currentEntry.getValue();
        MemoryConsumer c = cList.remove(cList.size() - 1);
        if (cList.isEmpty()) {
          sortedConsumers.remove(currentEntry.getKey());
        }
        try {
          long released = c.spill(required - got, consumer);
          if (released > 0) {
            logger.debug("Task {} released {} from {} for {}", taskAttemptId,
              Utils.bytesToString(released), c, consumer);
            got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
            if (got >= required) {
              break;
            }
          }
        } catch (ClosedByInterruptException e) {
          // This called by user to kill a task (e.g: speculative task).
          logger.error("error while calling spill() on " + c, e);
          throw new RuntimeException(e.getMessage());
        } catch (IOException e) {
          logger.error("error while calling spill() on " + c, e);
          throw new SparkOutOfMemoryError("error while calling spill() on " + c + " : "
            + e.getMessage());
        }
      }
    }

    // call spill() on itself
    if (got < required) {
      try {
        long released = consumer.spill(required - got, consumer);
        if (released > 0) {
          logger.debug("Task {} released {} from itself ({})", taskAttemptId,
            Utils.bytesToString(released), consumer);
          got += memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);
        }
      } catch (ClosedByInterruptException e) {
        // This called by user to kill a task (e.g: speculative task).
        logger.error("error while calling spill() on " + consumer, e);
        throw new RuntimeException(e.getMessage());
      } catch (IOException e) {
        logger.error("error while calling spill() on " + consumer, e);
        throw new SparkOutOfMemoryError("error while calling spill() on " + consumer + " : "
          + e.getMessage());
      }
    }

    consumers.add(consumer);
    logger.debug("Task {} acquired {} for {}", taskAttemptId, Utils.bytesToString(got), consumer);
    return got;
  }
}
```

去看内存管理器中还能不能申请这样一个大小，如果能申请就申请，如果不能申请，它会先去让别人去看看别的线程能不能把内存空间省出来等等。

此处有一个属性`bitmap`：

```java
/**
 * Bitmap for tracking free pages.
 */
private final BitSet allocatedPages = new BitSet(PAGE_TABLE_SIZE);
```

###### 1.1——————为什么会用`bitmap`？

它会假设你有很多数据，这个数据不应该开辟太多的空间，其实这个`bitmap`它也是做索引用的。所以会发现索引其实都会做一个通用的这么一个架构思想———**在索引上做压缩**。

定长定宽，之前分析的索引都是用字节数组，要么它们都是四个字节、要么它们都是`8B`、要么它们都是一个二进制位来做索引。

如果对哪个数据页做了处理，释放了就把相应的位置改成`0`，然后还可以根据`pageNumber`处理一些其他事情。

###### 1.2——————`page`是怎么来的？

它其实是一个抽象的概念，它的具体类型是`MemoryBlock`，是通过总内存管理器调用了钨斯计划的分配器，在分配的时候根据申请大小去分配

```java
// 做钨斯计划的内存分配
page = memoryManager.tungstenMemoryAllocator().allocate(acquired);
```

分配的时候就是最核心的步骤。那么如何分配？

##### 3.1.2—————如何分配`page`？

它分为堆上分配和堆外分配。

```java
/**
 * Allocates a contiguous block of memory. Note that the allocated memory is not guaranteed
 * to be zeroed out (call `fill(0)` on the result if this is necessary).
 * <br>
 * 堆上分配和堆外分配内存
 */
MemoryBlock allocate(long size) throws OutOfMemoryError;
```

出现了两个分支。

###### 2.1——————堆上分配`HeapMemoryAllocator`

```java
@Override
public MemoryBlock allocate(long size) throws OutOfMemoryError {
  if (shouldPool(size)) {
    synchronized (this) {
      final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(size);
      if (pool != null) {
        while (!pool.isEmpty()) {
          final WeakReference<long[]> arrayReference = pool.pop();
          // 堆分配开辟一个数组就可以了
          final long[] array = arrayReference.get();
          if (array != null) {
             assert (array.length * 8L >= size);
             MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
             if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
               memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
             }
             return memory;
          }
        }
        bufferPoolsBySize.remove(size);
      }
    }
  }
  // 同理
  // 最少分配 1 元素
  long[] array = new long[(int) ((size + 7) / 8)];
  // array 对象的偏移量, 长度
  MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
  if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
    memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
  }
  return memory;
}
```

`array`来自于`new long[]`，每一个元素是`8B`。`Platfrom.java`是对`Unsafe.java`的包装，其中设置了取第一个元素面向对象的`offset`即`LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);`。这个数组它是一个对象，会有`16B`的浪费。通过这个方法可以取到它的偏移量，现在是堆上分配的环节，元素的偏移量是`16B`，还有`size`长度，因此会得到一个`MemoryBlock`。

###### 2.2——————堆外分配`UnsafeMemoryAllocator`

相对于对内分配来说会有些差异

```java
@Override
public MemoryBlock allocate(long size) throws OutOfMemoryError {
  // 底层动用的是 Unsafe 然后返回 Address
  // 调用 Unsafe 分配堆外内存
  long address = Platform.allocateMemory(size);
  MemoryBlock memory = new MemoryBlock(null, address, size);
  if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
    memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
  }
  return memory;
}
```

`Platform.allocateMemory()`会直接分配堆外内存，这个方法有一个特征，它会返回一个`address`。直接在堆外`Java`进程中申请了一个操作系统可以访问的进程空间的地址。一个虚地址。

调用了`MemoryBlock`，其中第一个参数它不是堆中的字节数组所以它为`null`，`address`是曾经调用了堆外的方法后开辟的字节数组而且这个字节数组是完整的没有对象头，说白了就是整个`Java`进程堆中的堆外空间的某一个起始的位置。

```java
pageCursor = currentPage.getBaseOffset();
```

此时游标指向的`getBase`其实就是开始时那个`address`。堆内的游标指向的是`array`刨去头的第一个元素，堆外指的是同一个起始的位置。

此时重点来了

```java
final Object base = currentPage.getBaseObject();
```

**`base`能取出来吗？**

曾经赋的是`null`，所以相同的方法会根据`base`是否为`null`处理不一样。如果`base`为空，游标指向堆外的物理地址，因为是`int`所以是直接向物理地址填充`4B`，游标`+ 4`指向了`4B`的末尾。

当更新完成后，此时` Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);`中的`recordBase + recordOffset`就可以截取数据也就是真正的数据连续的字节数组。

关键是第三个参数`base`为`null`，如果`N`次以后发现它为`null`的话，其实就会根据后续的游标在物理位置的地址上把它拷贝`length`长度的`recordBase`。然后游标继续加`length`游标换成了堆外的另一个物理地址。下一条记录进来又要放一个`length`、下一条记录的大小、下一条记录的字节数，然后每次游标再同步。

**堆外内存地址申请多大？**

`acquired`有一次优化，无论是哪种方式，都会调用分配器分配大小，通过`acquireExecutionMemory(size, consumer)`分配大小。

通过`memoryManager.acquireExecutionMemory(required - got, taskAttemptId, mode);`的`UnifiedMemoryManager`统一内存管理器，申请你需要的字节数量且`got`是返回值

```java
val (executionPool, storagePool, storageRegionSize, maxMemory) = memoryMode match {
  case MemoryMode.ON_HEAP => (
    onHeapExecutionMemoryPool,
    onHeapStorageMemoryPool,
    onHeapStorageRegionSize,
    maxHeapMemory)
  case MemoryMode.OFF_HEAP => (
    offHeapExecutionMemoryPool,
    offHeapStorageMemoryPool,
    offHeapStorageMemory,
    maxOffHeapMemory)
}
```

会根据是堆内还是堆外会初始化不同的`execution`，且会根据你是对外的缓冲还是对内的缓冲再进行初始化。初始完后，第一次申请是有可能取一个最大值`taskMemoryManager.allocatePage(Math.max(pageSize, required), this);`，其中`pageSize`来自于`SortShuffleManager.getWriter()`时要`new UnsafeShuffleWriter`，会有一个`open()`进入`ShuffleExternalSorter`调用父类并传入第二个参数`(int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes())`。在`MemoryConsumer`中也有一个`pageSize`，其中`PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES`的值为`static final int MAXIMUM_PAGE_SIZE_BYTES = 1 << 27;  // 128 megabytes`然后到`MemoryManager`中`pageSizeBytes`取大约`64M`。是根据`val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)`推算出的尽量大的内存大小。

###### 2.3——————堆内与堆外的差异

这其中的差异是，堆内会有一个`array`头的浪费，对外它是一个纯的进程中的线性空间而且是可以访问的。

工作的步骤都一样无非就是通过，**参数`base`是否为`null`**从而有一些差异。因为它们**最终调用的都是`Unsafe.putInt()`**和**`Unsafe.copyMemory()`**。更新了头拷贝了数据的连续字节地址的数据，这是两个核心方法。

##### 3.1.3—————第二步

###### 3.1——————对于堆上分配

曾经分配的时候是分配了一个数组，在`BlockManager`时，有一个游标指向`array`的头过去的元素第一个下标`offset`位置，这是游标`pageCursor`所在的位置，第一部的时候是直接在这个位置插入了一个记录四个字节，等于是把后面的数据填充了，填充后又把游标加`4`。因为游标加完`4`后就是数据`record`放的位置。也就是插入头`4B`的数组跳过`array`的对象头，这个`offset`就是未来要插入数据的位置。然后游标更新后，拷贝进来那条记录的字节数组，起始位置，`array`对象，游标，长度。只有堆外没有太多浪费，只要是堆内它就必然是对象得到的，那么它一定会有一个头的浪费，所以才会有`Unsafe`另外一个方法的堆外分配。如果是堆内的方式，它的头一定是不应该写入的。通过`recordOffset`可以只写入数据而不把头带进去，因此会节省`4B`。然后数据通过游标`cursor`插入`array`。

至此游标已更新。

申请的页面是很大的，一条记录只消耗一点，下一条记录如果再进来还需要申请新的页面吗？

因为刚才的页面并没有用完

```java
if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() )
```

要么为空，要么是游标的大小 `+ required >`起始位置加结束位置，那么页面并没有满，所以不会再去分配页面，继续使用老页面。其实依然是先走`Platform.putInt()`，把新的记录的大小要插进去，再插一个`4B`的`length`，然后再次做数据项的拷贝。

当这个`array`和`page`被使用完后，此时如果再来一条记录它会申请一个新的`page`。一个任务执行的时候它可能会分配多个`page`这取决于数据量对于内存空间的比例。

------

### 四、总结

**有三个环节：**

1. 一个任务的结果用的`BlockManager`的磁盘，也就是最终`Spark`是将结果写到磁盘的，有磁盘`IO`。
2. 那么在任务执行的过程中，它是会用到`MemoryManager`中的`Execution`执行内存区域，来去申请内存的使用。
3. `Storage`什么时候被用起的？有些`RDD`会被重复使用，这样的`RDD`它叫热点`RDD`，他的数据会被利用很多次。

`Spark`中有`Stage`的概念，`Stage`之间是逻辑的，它最终会变成根据最后一个`RDD`分区的数量。变成一些`Task`任务，任务与任务之间的衔接，其实依赖的是`ShuffleDependency`。`Stage`划分也是通过`ShuffleDependency`划分的。`ShuffleDependency`很重要，因为其中有一个东西叫做`handle`。·`handle`有`3`种，可以决定，之前分析的`Shuffle`写的时候，因为`ShuffleDependency`可以通过`registerShuffle()`它有`3`种`handle`。写的时候会决定它有`3`种写法，调用不同的算子决定不同的属性时，它的写的效果性能等等是不一样的。

至此写的已经分析完了，写成文件落地了且其实落地成文件后，它会有一个`shuffleBlockId`，在`BlockManager`中存储。上游一个任务结束之后输出的结果会存在本地的文件系统，并由`Spark`的`BlockManager`管理这些结果。

------

## 章节`23`：`Spark-CORE`源码，`ShuffleReader`，`Tracker`，`Scheduler`完整调度

------

**此节分析：上游第一个任务跑完之后，下游这个任务，它的`ShuffleReader`是怎么把他关心的那些数据拉取回来的？**

**而且这其中一定要明白`Shuffle`有洗牌的意思，如果上游有三个`task`，其实下游如果有四个`task`的话，其中的这一个`task`，它要拉取的是上游每一个`task`中属于它自己的分片数据。相同分区号的数据会汇聚到一个小分区中，那么一个分区中其实是有若干组的。**

现在要分析的是，一个`task`任务，它肯定要么是`Shuffle`写，要么是`Result`写。然后它的读，要么是来自数据源`HadoopRDD`的读，要么是来自于`Shuffle`的读。`HadoopRDD`的读它就是一个对`InputFormat`输入格式化类的一个包装的迭代器。**`shuffle`读也是一个迭代器它最终是怎么变的？**

**而且`shuffle`读的时候肯定是要从别的`shuffle`写中去拉取写完的数据。**

### 一、分析`Shuffle`读的实现过程

首先在`ShuffledRDD.compute()`中取到`dependency`中的`dependencies`。拿到`dependency`之后，通过`SparkEnv`从环境中获得`shuffleManager`。

#### 1.1————`getReader()`

然后通过`shuffleManager.getReader()`取`reader`

```scala
/**
 * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
 * Called on executors by reduce tasks.
 */
override def getReader[K, C](
    handle: ShuffleHandle,
    startPartition: Int,
    endPartition: Int,
    context: TaskContext): ShuffleReader[K, C] = {
  // 只有一个 Reader
  new BlockStoreShuffleReader(
    handle.asInstanceOf[BaseShuffleHandle[K, _, C]], startPartition, endPartition, context)
}
```

曾经去`getWriter`的时候会发现它很复杂，传进来`handler`要匹配`match`，因为它有`3`种`handler`。但是此时`getReader`的时会发现它并没有拿着`handler`去匹配什么。而是它固定`new`了一个`BlockStoreShuffleReader`对象，且`getWriter`会继续调用`read()`。

所以`read()`方法应该是`Shuffle`读之后，会通过`compute()`封装成迭代器。

#### 1.2————`read()`

如何拉取数据以及拉取后怎么进行包装成迭代器返回。

```scala
/** Read the combined key-values for this reduce task */
override def read(): Iterator[Product2[K, C]] = {
  val wrappedStreams = new ShuffleBlockFetcherIterator(
    context,
    blockManager.shuffleClient,
    blockManager,
    mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
    serializerManager.wrapStream,
    // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
    SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
    SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
    SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
    SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
    SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))

  val serializerInstance = dep.serializer.newInstance()

  // Create a key/value iterator for each stream
  val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
    // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
    // NextIterator. The NextIterator makes sure that close() is called on the
    // underlying InputStream when all records have been read.
    serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
  }

  // Update the context task metrics for each record read.
  val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
  val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
    recordIter.map { record =>
      readMetrics.incRecordsRead(1)
      record
    },
    context.taskMetrics().mergeShuffleReadMetrics())

  // An interruptible iterator must be used here in order to support task cancellation
  val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

  val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
    if (dep.mapSideCombine) {
      // We are reading values that are already combined
      val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
      dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
    } else {
      // We don't know the value type, but also don't care -- the dependency *should*
      // have made sure its compatible w/ this aggregator, which will convert the value
      // type to the combined type C
      val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
      dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
    }
  } else {
    require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
    interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
  }

  // Sort the output if there is a sort ordering defined.
  val resultIter = dep.keyOrdering match {
    case Some(keyOrd: Ordering[K]) =>
      // Create an ExternalSorter to sort the data.
      val sorter =
     	  new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
      sorter.insertAll(aggregatedIter)
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
      // Use completion callback to stop sorter if task was finished/cancelled.
      context.addTaskCompletionListener(_ => {
          sorter.stop()
      })
      CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
    case None =>
      aggregatedIter
  }

  resultIter match {
    case _: InterruptibleIterator[Product2[K, C]] => resultIter
    case _ =>
      // Use another interruptible iterator here to support task cancellation as aggregator
      // or(and) sorter may have consumed previous interruptible iterator.
      new InterruptibleIterator[Product2[K, C]](context, resultIter)
  }
}
```

未来节点间拉取数据的时候对应是`Netty`，从`BlockManager`中取。

进入`ShuffleBlockFetcherIterator`，这个对象中

- `private[this] val localBlocks = scala.collection.mutable.LinkedHashSet[BlockId]()`：本地的块，且不需要走`Netty`拉取。
- `private[this] val remoteBlocks = new HashSet[BlockId]()`：远程的块，需要单独处理，通过触发`Netty`通信拉`Shuffle`流进行传输。
- `private[this] val results = new LinkedBlockingQueue[FetchResult]`：迭代器最终迭代的就是`result`。

`mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition)`这是一个`map`输出的追踪器。这是在`reader`和`writer`分析完毕后分析到`scheduler`大流程的时候再去分析追踪。

也就是上游怎么注册下游怎么追踪。

**在`SparkEnv`中曾经分析过**，有一个`map`输出的追踪器，其实上游的输出会向他其中去更新自己的`mapStatus`，然后下游计算的时候其实会通过它下游的任务寻找。而且所有的`executor`当中的任务执行的结果，它们都会向它去汇总，所以从这个管家中可以取到任意的`ShuffleId`、`startPartition`、`endPartition)`。也就是每一个`Shuffle`其实就是代表的是每一个`Stage`的上游。那么上犹中一共有多少个分区？每个分区写了多少块？这个是可以取出来的。

这是一个宏观的`Writer`和`Reader`，写的东西会向其中注册，所有的写每一个任务的输出，它那个块这个输出它是属于哪个`executor`的，在下游的时候只需要去向他询问即可。

在`new ShuffleBlockFetcherIterator()`时，它其中会调用一个初始化。

**初始化的目的是为了让`results`中的所有东西都搞定，因为它的`ShuffleBlockFetcherIterator`是一个迭代器。迭代器在初始化的时候要初始化未来可以迭代的数据是什么，要把数据源获取到。因为获取到后只要初始化的环节完成了，代表着未来在`hasNext()`和`next()`中获取到的数据就是`result`中的数据以供使用。**

在构造的过程中

1. 第一步，区分本地的，当前任务拉取本地的远端块将远端本地的都汇聚到`results`中，那么先把迭代器封装起来。然后`val wrappedStreams = new ShuffleBlockFetcherIterator()`可以访问所有的数据。

##### 1.2.1—————对象初始化`initialize()`

###### 1.1——————区分`splitLocalRemoteBlocks()`

**区分本地`localBlocks`的和远端`remoteBlocks`的块，并得到一个结果集。**

根据取`read()`传入的第六个参数设置的`48M`通过`math.max(maxBytesInFlight / 5, 1L)`计算出拉取的`targetRequestSize`，为什么是`1/5`？因为可以五个请求绑在一块儿，也就是说**同时可以最少拉五个并行**的。

`if (address.executorId == blockManager.blockManagerId.executorId)`登记本地的块，并且向`localBlocks`登记我本地已有的块。

**另外一个分支就是远程的块。**且在`while (iterator.hasNext) {}`中取出每一个块的大小并且跳过空块。

**为什么会产生空块？**

假设有三条记录八个分区，最大的可能这三条记录各去了一个分区，还有五个分区是空的，那五个就会为空。或者某个块中的数据全被`fliter()`过滤掉了根本就没有输出，那么它其实最后也是个空。

还要判断`if (curRequestSize >= targetRequestSize || curBlocks.size >= maxBlocksInFlightPerAddress)`，如果面向某一台机器我要拉取的块加起来已经大于`1/5`了。

```scala
remoteRequests += new FetchRequest(address, curBlocks)
```

它不会对一个节点玩命的去拉所有的块，而是`48M`的`1/5`在一个节点就敲定出一个`remoteRequest`。

**这样有什么好处？**

- 如果未来我要拉两台机器，每台机器都会创建很多`1/5`大小的`FetchRequest()`，那么真正要拉的时候其实只要给它打散，我可以一台机器对着两台机器，每台都去拉取，那么这样的话就会减少对着一台机器的阻塞。我可以一台机器并行拉两台。
- 因为站在下游的角度，我并不知道上游它是不是只对我一个人发送数据。如果只是针对我的话，其实我跟他一对一的拉取是最舒服的。但是上游还给别人拉取数据的话。那么此时其实它那边会有轮询，有些时间片是不给我传数据的。因为那边网卡中数据包交替着发给不同的人。
- 那么这样的话我其实就会有一个带宽的浪费，那我莫不如同时对几个人一块儿拉取。这也是为什么要`/ 5`。也就是它最终有五个机器的并行度去拉取。

**但是其实这其中还有一个好处**

- 如果面向一台机器中，其中有十个块打成一个请求的话，其实这十个块每块不大，我就不用发起十个请求。把十个请求可以压缩成一步请求。

**最终会返回一个`remoteRequests`集合。**

###### `fetchRequests ++= Utils.randomize(remoteRequests)`打散数据

相当于做了一次混洗牌的过程，最好是五`(N)`个节点并行去拉取，一个尽量均衡的过程。

###### 1.2——————拉取`fetchUpToMaxBytes()`

在这个方法的`while (isRemoteBlockFetchable(fetchRequests)) {}`中，`fetchRequests.dequeue()`循环一次取出一个请求`request`。`send(remoteAddress, request)`最终从本地发出。

`send(remoteAddress: BlockManagerId, request: FetchRequest)`中的`sendRequest(request)`的`request`就是曾经填充的`block`块。进入`sendRequest()`。

`sendRequest(req: FetchRequest)`，其中定义了一个监听器`blockFetchingListener`，其实监控器中只有两种状态

1. `onBlockFetchSuccess(blockId: String, buf: ManagedBuffer)`，拉取成功了，会在`result.put()`中登记`new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf, remainingBlocks.isEmpty)`
2. `onBlockFetchFailure(blockId: String, e: Throwable)`，拉取失败了，会在`result.put()`中登记`new FailureFetchResult(BlockId(blockId), address, e)`

通过监听器的结果区分成功与否。

`shuffleClient`是`Netty`传输的`Service`也就是`NettyBlockTransferService`。`shuffleClient`会把刚才的监听器传进去。`fetchBlocks`最终实现是`NettyBlockTransferService`，在实现的`fetchBlocks`中创建并启动`blockFetchStarter.createAndStart(blockIds, listener)`服务。

最终启动的是

```scala
override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
  val client = clientFactory.createClient(host, port)
  new OneForOneBlockFetcher(client, appId, execId, blockIds, listener,
                            transportConf, tempFileManager).start()
}
```

`start()`。在`start()`中会执行`client.sendRpc(openMessage.toByteBuffer(), new RpcResponseCallback() {}`发送本地消息。且`NettyBlockTransferService`在每个节点都有，会形成一个分布式通信的底层架构。

**最终在`start()`会牵扯到拉取数据的过程。**

###### 1.3——————整合`fetchLocalBlocks()`

**整合远端的和本地。`results`的相关操作。**

#### 1.2.2—————`recordIter`转换

在有了能够访问全部数据的迭代器后，`wrappedStreams.flatMap`可以遍历每一个块儿以及流。然后做一次反序列化`deserializeStream()`。其实就是转换的迭代器一步步加工成`recordIter`。

每条记录裸露出来了。

#### 1.2.3—————`metricIter`度量

拿着转换后的记录`recordIter`做了一个度量，从`context.taskMetrics`系统当中封装了一个度量的迭代器，**但此时并没有发生计算**。

#### 1.2.4————`interruptibleIter`推测执行

迭代器放到了一个可以被取消的迭代器中，因为它在任何计算环境中有一个推测执行，如果一个执行的慢的话我可以再发起在不同的节点看谁先跑完。但是跑完后一定会把另一个取消掉。

#### 为什么会有`Shuffle`？

- **改变分区的数量、改变并行度**
- **聚合`aggregated`**
- **全排序`keyOrdering`**

#### 1.2.5————`aggregatedIter`聚合

**首先会判断`if (dep.aggregator.isDefined)`有没有聚合器。**

- **然后会判断是否为`mapSide`。**（聚合对任务的影响，`Reduce`接收的是没有被加工的数据还是处理过的数据）

  - **`map`端有聚合**`combineCombinersByKey()`，会返回一个`ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)`。**只做一些聚合。**

  - **`map`端没有聚合**`combineValuesByKey()`，会返回一个`ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)`

    返回的`ExternalAppendOnlyMap[K, C, C]`参数**差异取决于上游有没有对`map`端做聚合**，**全量聚合。**

- **如果没有使用聚合器**

  `interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]`，就将结果赋值给`interruptibleIter`。**原封不动。**

**`aggregator`最终会被`ShuffledRDD`设置进去，在编程的时候只要触碰了`reduceByKey()`、`groupByKey()`，它最终就是`combineByKey()`。然后它最终会把三个聚合函数放到聚合器中，并放到`ShuffledRDD`中。**

**也就是说会有`3`种情况。**

#### 1.2.6————`resultIter`结果

**在`dependency`中排序`keyOrdering`，分为两种情况**

- 准备`sorter`，最终返回一个`CompletionIterator`。
- 返回一个`aggregatedIter`。

**最后`resultIter`模式匹配**

- `InterruptibleIterator[Product2[K, C]] => resultIter`，如果一直没有发生变化直接返回`resulttIter`。
- `new InterruptibleIterator[Product2[K, C]](context, resultIter)`，包装成可阻断的迭代器类型。

------

### 二、总结

- `getWriter()`比较复杂，但`getReader()`也很复杂。
- 以及**聚合对任务的影响**。

分析了`Shuffle`读取数据的环节，**`reader`也受`dependency`的影响，它的读的行为会有一些变化。**

`ShuffleReader`和`ShuffleWriter`这两个环节，已经分开分析完毕。

------

## 章节`24`：`Spark-CORE`源码，`RDD`持久化，检查点，广播变量，累加器——Ⅰ

------

### 一、整理`Shuffle`的`Writer`及`Reader`两个环节

分开的时候，可能能想起来，但具体整合到一起，它们是什么约束。`Writer`和`Reader`在大数据领域中应该是一种什么形式。

**最终是要以场景驱动。**

`SparkShuffle`系统参考的谁？

------

### 二、`RDD`持久化

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD Control Operations
 */
object Lesson07_RDD_Control {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    // 贴源数据 RDD
    val data: RDD[Int] = sc.parallelize(1 to 10)

    // 转换加工的 RDD
    val d2rdd: RDD[(String, Int)] = data.map(e => if (e % 2 == 0) ("A", e) else ("B", e))

    // 奇偶分组
    val group: RDD[(String, Iterable[Int])] = d2rdd.groupByKey()
    group.foreach(println)

    // 奇偶统计
    val kv1: RDD[(String, Int)] = d2rdd.mapValues(e => 1)
    val reduce: RDD[(String, Int)] = kv1.reduceByKey(_ + _)
    reduce.foreach(println)

    while (true) {
    }
  }
}

```

第一批`Job`如下图：

![第一个 Job](D:\ideaProject\bigdata\bigdata-spark\image\第一个Job.png)

![第二个 Job](D:\ideaProject\bigdata\bigdata-spark\image\第二个Job.png)

那么这其中**谁被用了很多次**，**`d2dd`用了两次**。

**此时要思考一个问题：**这个`d2rdd`如果用了两次的话，分别根据图上描述，其实`d2rdd`的计算都是要从头开始发起计算的。也就是说，其实`d2rdd`代表这个**`data.map(e => if (e % 2 == 0) ("A", e) else ("B", e))`**逻辑是要**重复执行两次**。

`data: RDD[Int]`数据源是一笔数据源是一笔，转换的结果`d2rdd`被`d2rdd.groupByKey()`和`d2rdd.mapValues(e => 1)`使用了两次。

所以代表着`d2rdd`这套逻辑要读取两次`data`这个数据源。因为通过：

1. **`data → d2rdd → group → foreach(println)`是一个`Job`。**
2. **`data → d2rdd → kv1 → reduce → foreach(println)`也是一个`Job`。**

`group`和`kv1`的父`RDD`都是`d2rdd`，等于是一个树状分支每个分支都要单独跑`Job`，所以总的开始的地方`data、d2rdd`要跑两次。所以此处会有一个**调优的点**。

如果`d2rdd`中的数据的逻辑计算完后能够被复用，也就是第二次作业的时候不去再执行`kv1`的计算逻辑，那么性能就会提升。

#### 2.1————调优

**引入`cache()`算子**

```scala
// 调优点 : 只有那些重复使用的 RDD 适合调优 : 缓存结果数据
d2rdd.cache()
```

第二批`Job`如下图：

![持久化算子的第一个 Job](D:\ideaProject\bigdata\bigdata-spark\image\持久化第一个Job.png)

![持久化算子的第二个 Job](D:\ideaProject\bigdata\bigdata-spark\image\持久化第二个Job.png)

第二批的两个`Job`和第一批的两个`Job`的**差别是绿点**，第二批任务执行的时候，只有在第一个`Job`跑的时候

```scala
// 贴源数据 RDD
val data: RDD[Int] = sc.parallelize(1 to 10)

// 转换加工的 RDD
val d2rdd: RDD[(String, Int)] = data.map(e => if (e % 2 == 0) ("A", e) else ("B", e))
```

以上的逻辑**被执行且结果先缓存到内存了**。缓存到内存之后，然后才跑的`groupByKey()`。然后内存中就有`d2rdd`的数据了。

第二个任务`kv1`它在跑的时候发现他的父`d2rdd`已经有数据了所以拿过来直接用就可以，所以前面的都跳过了，就是从那个绿点走起的。

**数据并没有写入磁盘而是在内存中**，如下图

![内存中的数据](D:\ideaProject\bigdata\bigdata-spark\image\数据并没有写入磁盘而在内存中.png)

`cache()`算子底层调用的是`persist()`算子，而`persist()`底层调用的是`persist(StorageLevel.MEMORY_ONLY)`存储级别。

通过`persist()`算子进行序列化`d2rdd.persist(StorageLevel.MEMORY_ONLY_SER)`，如下图

![通过 persist() 算子序列化](D:\ideaProject\bigdata\bigdata-spark\image\persist序列化.png)

可以看出数据大小削减的还是比较多的。是否序列化，它要考虑的体积和资源消耗是不一样的，除了大数据只要碰`Java`序列化与否带来的内存的压力大小是不一样的。无非是在并发系统时，要考虑的是时延及性能的问题，最终折中选出先解决速度还是内存空间的问题。

大数据领域的话，更偏向于序列化，速度可以稍微慢一丢丢，反正不可能是达到毫秒级操作几`T`的数据。莫不如让数据跑在内存中，因为看似序列化损耗了一些序列化的过程，但是放大到整个集群中，减少了磁盘`IO`，因为数据量很大，就会减少`IO`反而磁盘`IO`那个时间他是要比`CPU`计算的时间要慢上很多。未来内存中可能有几个`G`的数据，那么我只需要在用的时候取出一条反序列化一条即可。

`AND`是优先的意思，并不是并集。

#### 2.2————无形中的调优

```scala
val res: RDD[(String, String)] = reduce.mapValues(e => e + "Syndra")
res.foreach(println)
```

第三批`Job`如下图：

![第一个 Job](D:\ideaProject\bigdata\bigdata-spark\image\第三批第一个Job.png)

![第二个 Jon](D:\ideaProject\bigdata\bigdata-spark\image\第三批第二个Job.png)

![第三个 Job](D:\ideaProject\bigdata\bigdata-spark\image\第三批第三个Job.png)

*此处有一个概念*

1. **`RDD`可以调用`persist(级别)`，它可以缓存数据，最好是`task`中间的`RDD`。（可能不是触发`Shuffle-write`）**
2. **`RDD`如果是`task`最后一个，且`Shuffle-read`的结果被复用，可以跳过该`RDD`的执行过程。**

这两个概念很重要，因为在设计代码特征时，如果发现了当前`RDD`是一个边缘`RDD`，它最终其实是后面的那个`RDD`。在以上案例中`d2rdd`及`reduce`均被复用了，其实`Shuffle-read`后，这个`RDD`如果被复用的话，其实是不需要去调优的。因为`Shuffle`系统已经帮你调优了，最好是`task`中间的`RDD`被重复使用这样才是最有意义的。

#### 2.3————关于调优的两方面

也就是说，在`Spark`中关于数据`RDD`**调优方面有两点：**

1. **手动调用，将数据缓存，因为它一直被重复使用。**
2. **首先判断它是否为`Shuffle`完毕的结果集，如果是那么没必要对它做`cache()`。因为它的`Shuffle`系统已经拉回来了，在本地有数据了它直接可以跳过前面的东西。**

------

### 三、`checkpoint`检查点

持久化的东西可能会丢，`checkpoint`是比持久化更高级的一个词，检查点中其实包含了持久。

```scala
sc.setCheckpointDir("./bigdata-spark/data/ckp")
// 设置检查点
d2rdd.checkpoint()
```

`SparkContext`是一个全局性公共的配置，想令哪些数据放进去就把哪些数据调用`d2rdd.checkpoint()`。

设置检查点后运行结果的`Job`会和先前不同，如下图：

![检查点的任务数量](D:\ideaProject\bigdata\bigdata-spark\image\检查点会有四个Job.png)

检查点的四个任务，分别如下图：

![检查点第一个 Job](D:\ideaProject\bigdata\bigdata-spark\image\检查点第一个Job.png)

![检查点第二个 Job](D:\ideaProject\bigdata\bigdata-spark\image\检查点第二个Job.png)

![检查点第三个 Job](D:\ideaProject\bigdata\bigdata-spark\image\检查点第三个Job.png)

![检查点第四个 Job](D:\ideaProject\bigdata\bigdata-spark\image\检查点第四个Job.png)

`checkpoint()`并不会触发`runJob()`，只有`Action`算子自己跑完后捎带脚做一次`checkpoint()`。这就是为什么有一个作业先跑完了`groupByKey()`。

#### 3.1————涉及到`checkpoint()`的性能调优

1. **`checkpoint()`不会立刻触发作业。**

2. **触发机制：`Action`算子触发`runJob`后再触发的`checkpoint()`。**

3. **`checkpoint()`会单独触发`runJob()`运行。（痛点：代码逻辑会重复一次）**

4. **一条不成文的规定：我们在使用`checkpoint()`的时候要结合`persist()`算子。**

5. **因为： `persist()`是优先任务执行，然而` checkpoint()`是滞后任务、`Job`的。**

6. ```scala
   val d2rdd
   d2rdd.persist(StorageLevel.MEMORY_AND_DISK)
   d2rdd.checkpoint()
   ```

7. 无论`persist`、`checkpoint`：都有一个共通点：那些被重复利用的数据。

#### 3.2————一些`RDD`概念

- `RDD`：

  弹性的分布式数据集。

- 分布式：

  `partition`。

- 弹性的：

  - 某一个分区：

    可能是`memory`、`disk`、`HDFS`。

- `MapReduce`中：

  - 如果一个转换的数据很值钱：

    单独跑一个`Map`的`MR`程序，`Map`的结果只能存在`HDFS`，后续可以跑几百个不同逻辑的计算面向这个结果。

------

### 四、`RDD`的`iterator()`分析`persist()`及`checkpoint()`

只要是个`RDD`别人要我的数据时肯定是通过我的`iterator()`要走这个迭代器包装的那个迭代器。从迭代器中拿走数据，关键是迭代器做包装的这个数据是来自于跟先前去要的走`compute()`还是自行从`persist()`中`get()`还是从一个`checkpoint()`中`get()`。

其实这个迭代器有三个位置可以去计算

1. **`BlockManager`**
2. **`checkpoint()`**
3. **`compute()`**

#### 4.1—————`iterator()`

```scala
/**
 * Internal method to this RDD; will read from cache if applicable, or otherwise compute it.
 * This should ''not'' be called by users directly, but is available for implementors of custom
 * subclasses of RDD.
 */
final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
  // 优先去找 persist
  if (storageLevel != StorageLevel.NONE) {
    getOrCompute(split, context)
  } else {
    // 然后去找 compute
    // 最后去找 checkpoint
    computeOrReadCheckpoint(split, context)
  }
}
```

当这个`RDD`是整套作业第一次执行到这个`RDD`时，会第一次或者是这个`RDD`被复用的时，有两种情况。

所以`getOrCompute()`时，如果做`StorageLevel`的话，还需要做

```scala
SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
  readCachedBlock = false
  computeOrReadCheckpoint(partition, context)
}
```

会发现还会调用`computeOrReadCheckpoint`，而在该方法中会判断直接拿还是发生计算。

所以整个的顺序为

1. **`persist()`**
2. **`checkpoint()`**
3. **`compute()`**

##### 4.1.1—————`getOrElseUpdate`取缓存数据

首先会获取`RDDBlockId(id, partition.index)`，`blockId`就是一个字符串，调用`get`()

- **有可能取回来**

  ```scala
  return Left(block)
  ```

- **有可能没取回来**，没有逻辑就往下走开始发生计算。

###### 1.1——————`get()`

```scala
/**
 * Get a block from the block manager (either local or remote).
 *
 * This acquires a read lock on the block if the block was stored locally and does not acquire
 * any locks if the block was fetched from a remote block manager. The read lock will
 * automatically be freed once the result's `data` iterator is fully consumed.
 */
def get[T: ClassTag](blockId: BlockId): Option[BlockResult] = {
  val local = getLocalValues(blockId)
  // 有可能上一个任务跑完直接放本地了
  if (local.isDefined) {
    logInfo(s"Found block $blockId locally")
    return local
  }
  val remote = getRemoteValues[T](blockId)
  // 也有可能上一个任务跑完没有放本地
  if (remote.isDefined) {
    logInfo(s"Found block $blockId remotely")
    return remote
  }
  None
}
```

在`getLocalValues(blockId)`中，会判断

- `if (level.useMemory && memoryStore.contains(blockId))`
- true：
  - `memoryStore.getValues(blockId).get`
- false：
- `else if (level.useDisk && diskStore.contains(blockId))`
- `else `

优先尝试从本地取数据，如果曾经有的话。第一次取不到数据，会继续往下执行。

###### 1.2——————`doPutIterator()`

`doPut()`一个柯里化方法，`(
    blockId: BlockId,
    level: StorageLevel,
    classTag: ClassTag[_],
    tellMaster: Boolean,
    keepReadLock: Boolean)(putBody: BlockInfo => Option[T])`，最终干活的是第二个括号。

- `if (level.useMemory)`

  - `if (level.deserialized)`

    - 没序列化把对象放进去`putIteratorAsValues()`

      - `while (values.hasNext && keepUnrolling)`使用先前的迭代器读取数据

        `reserveUnrollMemoryForThisTask(blockId, amountToRequest, MemoryMode.ON_HEAP)`中，会调用`memoryManager.acquireUnrollMemory(blockId, memory, memoryMode)`。`memoryStore`和`ExecutionMemory`执行内存和存储内存他们其实都是对`MemoryStore`统一内存管理器的一个上层包装

    - 直接放序列化的结果`putIteratorAsBytes()`

      - `while (values.hasNext && keepUnrolling)`依然会看有没有

        并序列化成什么东西`serializationStream.writeObject(values.next())(classTag)`。最终是以` ChunkedByteBufferOutputStream`字节数组写入。

- `else if (level.useDisk)`

  从磁盘拿文件并包装成`Channel`，输出的文件用迭代器拿过来`serializerManager.dataSerializeStream(blockId, out, iterator())(classTag)`，最终会在`writeAll()`中取数据。

#### 4.2————`persist()`核心

`persist()`最核心的就是

- 它会阻断`RDD`的`Pipline`，它自己这个位置把先前的数据在这开始采集。
- 它其实一直用的是`memoryStore()`或`diskStore()`，要么放内存要么放磁盘。

#### 4.3————`checkpoint()`

会发现并没有发生任何执行的事。

```scala
/**
 * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
 * directory set with `SparkContext#setCheckpointDir` and all references to its parent
 * RDDs will be removed. This function must be called before any job has been
 * executed on this RDD. It is strongly recommended that this RDD is persisted in
 * memory, otherwise saving it on a file will require recomputation.
 * <br>
 * 不会发生具体的执行它只是 new 了一个对象, 被 checkpoint 引用
 */
def checkpoint(): Unit = RDDCheckpointData.synchronized {
  // NOTE: we use a global lock here due to complexities downstream with ensuring
  // children RDD partitions point to the correct parent partitions. In the future
  // we should revisit this consideration.
  if (context.checkpointDir.isEmpty) {
    throw new SparkException("Checkpoint directory has not been set in the SparkContext")
  } else if (checkpointData.isEmpty) {
    checkpointData = Some(new ReliableRDDCheckpointData(this))
  }
}
```

在`Action`算子中会调用`checkpoint()`。

```scala
if (!doCheckpointCalled) {
  doCheckpointCalled = true
  if (checkpointData.isDefined) {
    if (checkpointAllMarkedAncestors) {
      // TODO We can collect all the RDDs that needs to be checkpointed, and then checkpoint
      // them in parallel.
      // Checkpoint parents first because our lineage will be truncated after we
      // checkpoint ourselves
      dependencies.foreach(_.rdd.doCheckpoint())
    }
    // 最终要执行此方法
    checkpointData.get.checkpoint()
  } else {
    // 逆向遍历找到某个 RDD 的 checkpoint()
    dependencies.foreach(_.rdd.doCheckpoint())
  }
}
```

最终会根据`checkpoint()`调用`doCheckpoint()`调用`writeRDDToCheckpointDirectory()`，拿到`RDD`后，一定会把`runJob()`跑起来成一个作业。并且传一个函数。

------

### 五、为什么持久化会触发`runJob()`？

这就是为什么持久化会触发`runJob()`，而且那个`runJob()`是直接把那个有`checkpoint()`的`RDD`直接在它身上要参数，然后开始`runJob()`。其实就是为了要那个`RDD`中的迭代器把数据取回来后，写到你的输出环境中。

所以，只要是一回到`runJob()`，`runJob()`就会有`Task`，只要是有`Task`这个`RDD`曾经如果被`persist()`。在`Task`中的`compute()`时，就能够拿到`BlockManager`。缓存的数据就被阻断了，前面的东西就不会被执行了。

最终还要从`checkpoint()`勾回到`persist()`。这就是为什么优化的时候要在`checkpoint()`前调一次`persist()`。

------

## 章节`25`：`Spark-CORE`源码，`RDD`持久化，检查点，广播变量，累加器——Ⅱ

------

### 一、闭包

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 闭包
 */
object Lesson08_Other {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[String] = sc.parallelize(List(
      "hello Syndra",
      "hello Spark",
      "hi Syndra",
      "hello WangZX",
      "hello Syndra",
      "hello Hadoop"
    ))

    val data1: RDD[String] = data.flatMap(_.split(" "))

    val list = List("hello", "Syndra")
    val res: RDD[String] = data1.filter(x => list.contains(x)) // 闭包 ← 想闭包进函数, 必须实现了序列化
    res.foreach(println)
  }
}

```

`RDD[String] = data1.filter(x => list.contains(x))`中的`x => list.contains(x)`是一个函数，`list.contains(x)`是一个函数体，这个函数体是要整体作为参数传给`filter()`的。那么在这整个一个匿名函数中用到了一个对象，这个对象叫做`list`。

然后又知道其实`RDD`在`Driver`这一侧时并不会执行，只有遇到`Action`算子`foreach`之后，然后才会在`Executor`端执行。

所以此时这个函数发生了一个事情，这个专业的名词叫做**闭包**。闭包其实有一个依赖，如果你想把东西闭到你的函数中，就必须实现了序列化。

#### 1.1————进阶

稍微复杂点，现在是通过肉眼看到样板数据中`hello`和`Syndra`相对比较多。所以才做了`List("hello", "Syndra")`定义，但有时这个例子它是未知的。

如果想随着计算的不同的数据，由数据决定它其中哪些高频词，然后最终留下，也就是说完全靠代码逻辑决定`list`。

**那么统计的事情如何做？**

跑一个`wordcount`。

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 闭包
 */
object Lesson08_Other {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[String] = sc.parallelize(List(
      "hello Syndra",
      "hello Spark",
      "hi Syndra",
      "hello WangZX",
      "hello Syndra",
      "hello Hadoop"
    ))

    val data1: RDD[String] = data.flatMap(_.split(" "))

    // 推送出去到 executor 执行, 然后结果回收回 driver端
    val list: Array[String] = data1.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).keys.take(2)


    //    val list = List("hello", "Syndra") // 有的时候是未知的 ?

    val res: RDD[String] = data1.filter(x => list.contains(x)) // 闭包(发生在 driver, 执行发送到 executor) ← 想闭包进函数, 必须实现了序列化
    res.foreach(println)
  }
}

```

运行结果如下图：

![动态统计](D:\ideaProject\bigdata\bigdata-spark\image\动态统计.png)

第一个阶段是人为给出，第二个阶段是根据集群计算。

##### 1.1.1—————序列化带来的消耗如何调优？

`list.contains(x)`既然发生了闭包，那么`list`会伴随这个函数被序列化，然后被发送出去，那么这批任务就会使用到`list`数据，这个数据就会牵扯到一个从`Driver`端到`Executor`端发送的过程。

如果list代表的数据比较值钱，那么也就代表它会被很多的`Task`包含闭包闭合进去。用它用的越多，它还会被闭到不同的`Task`机器中的几率越大，然后它被发送的概率会越来越强。如果这个东西频繁的从`Java`端向不同的`Executor`端去发送的话，其实它会消耗一些时间以及消耗一些带宽和内存。

其实这个问题`Spark`已经帮我们做完了，它称为**广播变量**。

------

### 二、广播变量

`blist.value.contains(x)`中的`blist`被包装成**广播变量**一个对象。其实在序列化时，是把`blist`这个类型`Broadcast`序列化，这个`list`是否是在这个对象中？

也就是说`list`是否也会伴随着每个`Task`序列化？如果并没有伴随，`sc.broadcast(list)`的`list`并不是`Broadcast`的一个成员属性，而在闭包时其实在序列化的时候并没有把它序列化的话。那么只是序列化出了一个指针引用对象，如果把`blist`对象发出去的话在`Executor`端，其实想使用的对象是`blist`，如果使用`blist`的话，`blist`中曾经有没有`list`的话，那么这个数据是如何得到的？

前面在`Driver`端并没有把`list`传进去，并不会增加每次`list`都会发送、都会闭包、都会序列化。但是在`Executor`端其实有期望的是每次要执行到`blist.value.contains(x)`位置时，还在其中拿出`list`的内容。

站在`Driver`端，不想每次都把`list`发出去，最好是不同的`Task`使用它很多遍时，其实它只需要发第一次即可。只要那个`Executor`端的那个`Executor`的进程拉取过相应的数据，后续如果在它上面执行其他的任务，也用到这个`list`，无需重复发送，然后也看到`Spark`中有广播变量。那么如何实现需求？

关注`Spark`计算框架，其实它自己维护了一个存储层`BlockManager`，维护了一个`Block`块存储。

```scala
/**
 * Broadcast a read-only variable to the cluster, returning a
 * [[org.apache.spark.broadcast.Broadcast]] object for reading it in distributed functions.
 * The variable will be sent to each cluster only once.
 *
 * @param value value to broadcast to the Spark nodes
 * @return `Broadcast` object, a read-only variable cached on each machine
 */
def broadcast[T: ClassTag](value: T): Broadcast[T] = {
  assertNotStopped()
  require(!classOf[RDD[_]].isAssignableFrom(classTag[T].runtimeClass),
    "Can not directly broadcast RDDs; instead, call collect() and broadcast the result.")
  val bc = env.broadcastManager.newBroadcast[T](value, isLocal)
  val callSite = getCallSite
  logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
  cleaner.foreach(_.registerBroadcastForCleanup(bc))
  bc
}
```

`broadcast()`是`SparkContext`中的一个方法，最终会在`TorrentBroadcastFactory.scala`中找到`newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean, id: Long)`。

进入`TorrentBroadcast.scala`中会发现，

```scala
/**
 * Divide the object into multiple blocks and put those blocks in the block manager.
 *
 * @param value the object to divide
 * @return number of blocks this broadcast variable is divided into
 */
private def writeBlocks(value: T): Int = {
  import StorageLevel._
  // Store a copy of the broadcast variable in the driver so that tasks run on the driver
  // do not create a duplicate copy of the broadcast variable's value.
  val blockManager = SparkEnv.get.blockManager
  if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, tellMaster = false)) {
    throw new SparkException(s"Failed to store $broadcastId in BlockManager")
  }
  val blocks =
  TorrentBroadcast.blockifyObject(value, blockSize, SparkEnv.get.serializer, compressionCodec)
  if (checksumEnabled) {
    checksums = new Array[Int](blocks.length)
  }
  blocks.zipWithIndex.foreach { case (block, i) =>
    if (checksumEnabled) {
      checksums(i) = calcChecksum(block)
    }
    val pieceId = BroadcastBlockId(id, "piece" + i)
    val bytes = new ChunkedByteBuffer(block.duplicate())
    if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, tellMaster = true)) {
      throw new SparkException(s"Failed to store $pieceId of $broadcastId in local BlockManager")
    }
  }
  blocks.length
}
```

**广播变量**也是利用了它的存储能力。

其实此时如果退回到最原始的位置，它是在`sc.broadcast(list)`的`Driver`端时，`SparkContext`在`Driver`的`JVM`中，无论是`Driver`还是`Executor`都有`SparkEnv`。

#### 2.1————调优

也可以通过广播变量的垂直`Join`解决性能调优，在`data`节点不用产生`Shuffle`的情况下，可以完成数据的关联以及再加上`filter()`窄依赖就可以完成本地的数据整理。

------

### 三、累加器

累加器的应用场景，根据条件分区等等

```scala
package com.syndra.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 闭包
 */
object Lesson08_Other {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("contrl").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    var n = 0
    // 累加器
    val testAc: LongAccumulator = sc.longAccumulator("testAc")
    val data: RDD[Int] = sc.parallelize(1 to 10, 2)

    val count: Long = data.map(x => {
      if (x % 2 == 0) testAc.add(1) else testAc.add(100)
      n += 1
      println(s"Executor:n $n")
      x
    }).count()

    println(s"count : $count")
    println(s"Driver:n: $n")
    println(s"Driver:testAc: ${testAc}")
    println(s"Driver:testAc.avg: ${testAc.avg}")
    println(s"Driver:testAc.sum: ${testAc.sum}")
    println(s"Driver:testAc.count: ${testAc.count}")
  }
}
```

广播变量也好，还是直接闭包数据，其实取决于数据的使用频度。如果数据其实就是简单的使用的话那么闭包即可。如果数据频繁被不同的逻辑使用的话，最好是做广播变量。这两个无论怎么做都属于垂直`join`或`Map`端`Join`。

------

# 后续`Spark—SQL`基础使用详见`Spark—SQL.md`
