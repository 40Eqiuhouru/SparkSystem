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