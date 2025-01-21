# `Spark—SQL`源码分析

------

## 章节`31`：`Spark—SQL`源码，`SQL`解析，`Dataset`到`RDD`的执行计划

------

### 序言

`SQL`解析，如果想实现`SQL`的话，要经历一个步骤。最终其实变成`RDD`，也就是我们未来要明白的一个软件框架，它能让你写`SQL`，它自主的还能帮你把`SQL`转成一些计算程序把它跑起来。其实这个意义是很大的，因为以前可能写`SQL`，那个`SQL`再怎么写它也是交给引擎，也就是数据库去执行。那么现在其实完全可以把`Spark—SQL`想成整个的一个数据库，就是`SQL`不只是交给了一个你看不到的东西而是你现在要分析他其中怎么去做的？

------

### 一、入口点

首先思考一下，在`Spark—CORE`时入口点是`SparkContext`，在`Spark—SQL`时入口点是`SparkSession`。那么为什么不是继续使用`SparkContext`？因为`SparkContext`只是创建`RDD`，但是它并没有能力去创建`SQL`解析这些能力。所以在基于兼容之前的`SparkContext`情况下，它用`SparkSession`其实包装了`SparkContext`的过程。也就是在原有的基础之上用包了一些进去。

一个是`SparkSession`另一个维度在`Spark—CORE`时，它的编程模型是`RDD`，基于`RDD`之上的一些转换和操作。那么其实我们在写的时候知道了它有一个东西叫`Dataset`或`DataFrame`。是以这个编程模型而不是使用`RDD`的编程模型。简单说`DataFrame`其实就是对`SQL`一方面的无论是`API`级还是这个字符串解析级的支持并扩充了`RDD`的一些能力，而且其实内部还有一些优化。

所以第一个入口点先去分析`SparkSession`，因为都是通过`SparkSession`得到的`Dataset`和`DataFrame`，所以他们两个有一个前后顺序和因果关系。

#### 1.1————`SparkSession`

通过`getOrCreate()`进入`sessionState`

```scala
/**
 * State isolated across sessions, including SQL configurations, temporary tables, registered
 * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
 * If `parentSessionState` is not null, the `SessionState` will be a copy of the parent.
 *
 * This is internal to Spark and there is no guarantee on interface stability.
 *
 * @since 2.2.0
 */
@InterfaceStability.Unstable
@transient
lazy val sessionState: SessionState = {
  parentSessionState
    .map(_.clone(this))
    .getOrElse {
      val state = SparkSession.instantiateSessionState(
        SparkSession.sessionStateClassName(sparkContext.conf),
        self)
      initialSessionOptions.foreach { case (k, v) => state.conf.setConfString(k, v) }
      state
  }
}
```

经由`sessionStateClassName()`到`hive`或`in-memory`模式匹配

```scala
private def sessionStateClassName(conf: SparkConf): String = {
  conf.get(CATALOG_IMPLEMENTATION) match {
    case "hive" => HIVE_SESSION_STATE_BUILDER_CLASS_NAME
    case "in-memory" => classOf[SessionStateBuilder].getCanonicalName
  }
}
```

进入`instantiateSessionState()`中的`builder()`

```scala
/**
 * Build the [[SessionState]].
 */
def build(): SessionState = {
  new SessionState(
    session.sharedState,
    conf,
    experimentalMethods,
    functionRegistry,
    udfRegistration,
    () => catalog,
    sqlParser,
    () => analyzer,
    () => optimizer,
    planner,
    streamingQueryManager,
    listenerManager,
    () => resourceLoader,
    createQueryExecution,
    createClone)
}
```

可以看到传递了很多参数。进入`SessionState`中的`QueryExecution.scala`中会发现，类中的成员属性均为`lazy`。

整个的流程大概为：一个逻辑计划进来后确认绑定元数据`→`得到元数据后再去优化`→`优化完后再转物理计划`→`转物理计划之后`→`得到`RDD`最终再执行。

整个的一颗语法树，最开始时，因为现在并没有涉及到`SQL`中文本字符串环节。相当于是`API`级。

`Dataset`是`ofRows`的私有属性？并不是。

因为`Dataset`是一个`object`，`Scala`中有`object`和`class`定义，所以它是一个伴生关系。然后在`Dataset`的`object`中，它会有一个`ofRows`，`ofRows`中是一个`Dataset`必须给它一个逻辑计划`logicalPlan`。且他必须要把逻辑计划传递给在`SparkSession`中的`sessionState.executePlan()`参数最终指向的是`createQueryExecution()`，`executePlan()`会接收一个计划`logicalPlan`，并且逻辑计划未来会和一些懒惰的小伙伴儿有一些依赖关系。只不过在`executePlan(logicalPlan)`时得到了`QueryExecution`但是它们并不会被执行，只得到一个`query`对象，这个对象只是持有了这个逻辑计划，也就是说在整个流程当中其实最终的目的是让`Dataset`中，因为构造时会发现其中传的并不是把一个逻辑计划传进去，它要传一个`QueryExecution`。但是`QueryExecution`要接收一个逻辑计划，作为参数才能完成它的构造这个对象。那么这个逻辑计划怎么来的？来自于`LogicalRelation`最终就制造出了一个`QueryExecution`，但是它怎么去用？

后续分析转换算子时更容易理解，最终得出结论`Dataset`必须要持有一个`qe`。在某一时刻这些懒惰的东西会被执行起来这是未来会发生的事情。

#### 1.2————总体思路

整个核心`Spark`没有变，就是`SparkContext`然后只包了一个`SparkSession`，最主要的目的是`QueryExecution`。以及逻辑计划树，其实它其中的`QueryExecution`贯穿了整个以`Dataset`编程模型对象包装的最核心的。如果用户通过`SparkSession`得到`Dataset`时，它的第一个贴源`Dataset`代表的是数据源。什么叫转换算子？如果拿着这个贴源的`Dataset`调转换算子之后，转换算子通用会调`withTypePlan`，然后会得到一个新的`Dataset`，这个`Dataset`的过程中所转换的加工逻辑会结合之前的`QueryExecution`封装出一个样例类，并继续向后传递变成一个`QueryExecution`。

并且一直都可以这样转换下去，但是到最后时候到某一个`Dataset`上，如果调了`Action`算子，它会基于最后这个`QueryExecution`得到物理执行计划，且在物理执行计划中得到`RDD`。

此时会发现跳过了中间很多优化的环节，直接从前面的`API`代码较为生硬得到逻辑计划，然后就是物理计划，然后得到`RDD`。

首先从逻辑计划变成物理计划，然后得到`RDD`的过程大体分析完毕。

##### 1.2.1—————`SparkPlan`

在`SparkPlan`中的`getByteArrayRdd()`里，一进来就调自己的`execute()`，这步就会得到`RDD`。只不过这个`RDD`要再做一次加工，加工的算子是`mapPartitionsInternal`，最终返回的也是一个`RDD`。

在`executor()`中调了`doExecute()`，进入后会返现没有实现了，因为子类具体的物理执行计划来实现此方法。

------

## 章节`32`：`Spark—SQL`源码，`ANTLRV4`的`SQL`解析，`AST`语法树的逻辑到物理转换

------

