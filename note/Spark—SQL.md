# `Spark—SQL`

------

## 章节`26`：`Spark—SQL`，大数据中的`SQL`组成原理

------

### 一、为什么会有`Hive`出现？

是因为`MapReduce`写起来太麻烦了吗？不可否认，这的确是他其中一个问题。

一般的回答是， `MapReduce`写起来太复杂效率很低，所以`Hive`会接收`SQL`。那如果写`SQL`的话那就一定会比编程容易且受众也多，所以`Hive`出现了。基于`Hive`使用的过程中可以轻松地构建所谓的数仓。

但是，从另外一个维度。如果现在在一家公司工作了十年，这十年里公司会产生很多很多的数据，你会把你的数据存储到`HDFS`、`HBase`，`HDFS`存储了很多很多的数据。那么在将很多很多的数据存储到`HDFS`中会有各种类型的数据以文件文本的方式存进去。也就代表会有很多目录及文件。那么突然因为工作时间很久了，此时突然说了一句话，随便指向了一个文件请告诉我它第某某列代表的意思。在数据存储时都希望人为干预的压缩，也就是字典的概念。

#### 1.1————如果我们面向数据的时候，如果数据存成模型或者模式。

站在文件的维度一个维度是`data`，一个维度是元数据，但是此元数据是描述文件级别的，这两个维度的数据在`HDFS`文件系统中元数据交给了`NataNode`，数据切割成块儿后，打散到集群给`DataNode`。

##### 1.1.1—————`File Model`

其实站在文件的角度来存储管理文件时，其实丢了一笔数据这个数据也叫元数据，是数据的元数据`Data Metastore`。描述他们怎么分割，有哪些列是什么类型等等。

##### 1.1.2—————`Table Model`

还有另外一种模型是`Table`，一个`Table`具备`schema`，一部分对文件数据的元数据其实在`schema`中。另外一个就是表中的所有行`row`也就是数据。也就是说`schema + row`是表的模式。以结构化的方式存储数据。在有了`schema`和`row`后如果想查表中的数据那么就需要`SQL`，在文件模式下操作只有`Read/Write`。

###### 2.1——————`Metastore`

用数据库做的是持久化，`Metastore`只提供了元数据的对外结构，那么此时光有元数据是不够的。因为还要解决`SQL`问题，在`Hive`的架构中，`SQL`其实是文本，会调用解析器`Driver`，`Driver`会调用`yarn`推送一个`MR Program`提交的过程，即`Driver`起到提交的过程。通过`Metastore`完成与`Driver`的元数据交互。

#### 1.2————用户`SQL`到达`Drvier`的过程

##### 1.2.1—————第一种已经被淘汰的方式

`Hive Cli`，如果在某台`Linux`的集群中敲了个`Hive Cli`的话。`Cli`=`Command line interface`，它有一个约束，必须要登录到那台机器中启动它才能电影命令行执行`SQL`。

##### 1.2.2—————第二种方式`HiveServer2`

`Cli`只支持一个用户，`HiveServer2`支持多用户连接。用户不可以直接把`SQL`交给`HiveServer2`，因为其只能对外暴漏`JDBC`的`Thritf`协议的连接。此时要启动一个`beeline`。

最终用户把`SQL`交给`beeline`，由`beeline`提交`SQL`。

------

### 二、`Hive`中最值钱的就是`Metastore`

------

### 三、基于`Driver`的`API`级别的适配

非常重要的一个环节，它是`API`级别的适配。也就是说你只要有这个`API`的适配那么你可以适配这个世界上任何其他的计算框架，只要`SQL`解析完适配就可以了。

但是有一个小小的瑕疵，比如说如果是`Spark`只有`RDD`，那么其实无所谓，因为它暴露的API趋向于固定。

首先说计算框架第三方的`Hive`这个团队和`Spark`团队，`Hive`团队他是主要负责了对于`SQL`的解析过程，可以产生一些逻辑执行优化，其中也有一堆优化的东西。但优化的东西只能是依赖`API`级别。

### 四、`Hive Model`

#### 4.1————`Hive on Spark`

在`Hive`中将计算框架改成`Spark`，就是把适配实现改成了`Spark`，这个速度不是特别快。

#### 4.2————`shark`

`Spark`团队把`Hive`的适配做到极致。`Spark`曾经被研发出时，它暴露出来了一个比较严重的问题，出现的一个词叫做`RDD`。

最后的`D`是`DataSet`，模仿的是`Scala`中的`collection`这种集合的函数。是比较`Low Level`的，比如`flatMap, fliter`，所以此时`RDD`并不是太向上适配`SQL`。因为`SQL`语句中没有出现`flatMap`。`SQL`只有`groupByKey`，其实等等这个操作和`RDD`之间的贯通的`API`的距离比较远。

#### 4.3————`Spark on Hive`

这也是为什么在第三个版本中，`Spark`要做`Spark on Hive`，但是这其中出现了一个东西**真正的`DataSet`**，而且在这个过程中，`RDD`受限于`DAGScheduler`。

#### 4.4————`HQL`

在公司中`HQL`是趋向于稳定的。

------

### 五、`Spark—SQL`

面向元数据有三种形式

1. **临时的`Application`**
2. **`on Hive`自主**
3. **`on Hive`整合**

`Spark`在执行的时候可以没有`Metastore`，那么他是怎么去做这件事情的？

##### 5.1————`cataLog`

`Spark`没有重新开发一个`Metastore`，这个产品在很多个技术中都会出现，术语叫`cataLog`，编目、目录。

`Cloudera`整合搭建，`Kylin`整合技术连通使用。

如果非关系型然后数量很大，理想的`Redis`但是太小，那么此时如果复杂度又比较多的话，其实可以走`ElasticSearch`。

------

### 六、总结

1. **引出了什么是`Spark—SQL`以及它的一些特征还有解析过程，尤其是在分布式下**
2. **最终`Spark—SQL`也是趋向于和`Hive`整合，因此抛出了一系列的概念，会在后续章节对其进行推理并分析源码**
3. **期望的是纯写`SQL`**

------

## 章节`27`：`Spark—SQL`，`DataFram`到`Dataset`的开发

------

回忆之前的`CORE`编程时，只有`RDD`时，它有一个上下文的类`SparkContext`，可以维护创建`RDD`提交作业。`SQL`其实是对`CORE`的一种包装，一种外挂的东西，就是把它丰富起来了。

尤其是它其中的`Dataset`其实是对`RDD`上层的一种包裹。`Dataset`并不能触发作业，`Dataset`最终是要转换回`RDD`才能触发作业，这是一个潜台词。

`DataFrame`是一个类型，就和别名一样，本质是一个`Dataset`只不过其中数据元素的类型是`row`类型。

```scala
package org.apache.spark

import org.apache.spark.annotation.{DeveloperApi, InterfaceStability}
import org.apache.spark.sql.execution.SparkStrategy

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 *  @groupname dataType Data types
 *  @groupdesc Spark SQL data types.
 *  @groupprio dataType -3
 *  @groupname field Field
 *  @groupprio field -2
 *  @groupname row Row
 *  @groupprio row -1
 */
package object sql {

  /**
   * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
   * with the query planner and is not designed to be stable across spark releases.  Developers
   * writing libraries should instead consider using the stable APIs provided in
   * [[org.apache.spark.sql.sources]]
   */
  @DeveloperApi
  @InterfaceStability.Unstable
  type Strategy = SparkStrategy

  type DataFrame = Dataset[Row]
}
```

------

### 一、感受`Spark—SQL`

```scala
package com.syndra.bigdfata.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark SQL basic
 * <br>
 * <p>SQL 字符串 → Dataset 对 RDD 的一个包装(优化器) → 只有 RDD 才能触发 DAGScheduler.
 */
object Lesson01_SQL_Basic {
  def main(args: Array[String]): Unit = {
    // 变化 :
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sql")
    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      //      .appName("sql")
      //      .master("local")
      //.enableHiveSupport() // 开启这个选项是 Spark—SQL on Hive 才支持 DDL, 没开启 Spark 只有 cataLog
      .getOrCreate()

    // 通过 SparkSession 获取 SparkContext
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    // 以 session 为主的操作演示
    // DataFrame 是 Dataset 类型
    val df: DataFrame = session.read.json("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\json")
    df.show()
    df.printSchema()
  }
}

```

------

### 二、`cataLog`基本语法

```scala
package com.syndra.bigdfata.sql

import org.apache.spark.sql.catalog.{Database, Function, Table}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark SQL basic
 * <br>
 * <p>SQL 字符串 → Dataset 对 RDD 的一个包装(优化器) → 只有 RDD 才能触发 DAGScheduler.
 */
object Lesson01_SQL_Basic {
  def main(args: Array[String]): Unit = {
    // 变化 :
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sql")
    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      //      .appName("sql")
      //      .master("local")
      //.enableHiveSupport() // 开启这个选项是 Spark—SQL on Hive 才支持 DDL, 没开启 Spark 只有 cataLog
      .getOrCreate()

    // 通过 SparkSession 获取 SparkContext
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    // 以 session 为主的操作演示
    // DataFrame 是 Dataset[row]

    // SQL 为中心
    val databases: Dataset[Database] = session.catalog.listDatabases()
    databases.show()
    val tables: Dataset[Table] = session.catalog.listTables()
    tables.show()
    val functions: Dataset[Function] = session.catalog.listFunctions()
    functions.show(999, true)

    println("————————————————————————")

    val df: DataFrame = session.read.json("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\json")
    df.show()
    df.printSchema()

    // 这一过程是 df 通过 session 像 cataLog 中注册表名
    df.createTempView("sql_test")

    val frame: DataFrame = session.sql("select * from sql_test")
    frame.show()

    println("————————————————————————")

    session.catalog.listTables().show()
  }
}

```

其中`show()`是`Action`算子。

------

### 三、`Spark—SQL—API`

**数据 + 元数据 = `df`**

`DataFrame`倾向于有表头，列，元数据且从其中保存了一批对应列每个行的数据。换言之，`DataFrame`就是一张表。

基于`JSON`格式的`SQL`

```scala
package com.syndra.bigdfata.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark SQL API
 */
object Lesson02_SQL_API01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("API")
    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")

    // 数据 + 元数据 = df 就是一张表

    // 1.数据 : RDD[Row]
    val rdd: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\person.txt")
    val rddRow: RDD[Row] = rdd.map(_.split(" ")).map(arr => Row.apply(arr(0), arr(1).toInt))

    // 2.元数据 : StructType
    val fields = Array(
      StructField.apply("name", DataTypes.StringType, nullable = true),
      StructField.apply("age", DataTypes.IntegerType, nullable = true)
    )
    val schema: StructType = StructType.apply(fields)

    val dataFrame: DataFrame = session.createDataFrame(rddRow, schema)
    dataFrame.show()
    dataFrame.printSchema()
    dataFrame.createTempView("person")
    session.sql("SELECT * FROM person").show()
  }
}

```

- **第一种方式：`RDD[Row] + StructType`**

  ```scala
  package com.syndra.bigdfata.sql
  
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
  import org.apache.spark.sql.{DataFrame, Row, SparkSession}
  import org.apache.spark.{SparkConf, SparkContext}
  
  /**
   * Spark SQL API
   */
  object Lesson02_SQL_API01 {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setMaster("local").setAppName("API")
      val session: SparkSession = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
  
      val sc: SparkContext = session.sparkContext
      sc.setLogLevel("ERROR")
      val rdd: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\person.txt")
  
      // 第二种方式 : bean 类型的 RDD + javabean
  
      // V1.1 : 动态封装
      val userSchema = Array(
        "name string",
        "age int",
        "sex int"
      )
  
      // 1. Rdd[Row]
      /* UtilMethod */
      def toDataType(f: (String, Int)): Any = {
        userSchema(f._2).split(" ")(1) match {
          case "string" => f._1.toString
          case "int" => f._1.toInt
        }
      }
  
      val rowRDD: RDD[Row] = rdd.map(_.split(" "))
        .map(x => x.zipWithIndex)
        // [(WangZX, 0), (18, 1), (0, 2)]
        .map(x => x.map(toDataType(_)))
        .map(x => Row.fromSeq(x)) // Row 代表着很多的列, 每个列要标识出准确的类型
  
      // 2.StructType
      /* UtilMethod */
      def getDataType(t: String) = {
        t match {
          case "string" => DataTypes.StringType
          case "int" => DataTypes.IntegerType
        }
      }
  
      val fields: Array[StructField] = userSchema.map(_.split(" "))
        .map(x => StructField.apply(x(0), getDataType(x(1)), nullable = true))
  
      val schema: StructType = StructType.apply(fields)
  
      val schema01: StructType = StructType.fromDDL("name string, age int, sex int")
      val df: DataFrame = session.createDataFrame(rowRDD, schema01)
      df.show()
      df.printSchema()
    }
  }
  ```

- **第二种方式：`RDD[bean] + javabean`**

  ```scala
  package com.syndra.bigdfata.sql
  
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
  import org.apache.spark.{SparkConf, SparkContext}
  
  import scala.beans.BeanProperty
  
  /**
   * Spark SQL API
   */
  object Lesson02_SQL_API01 {
    def main(args: Array[String]): Unit = {
      val conf: SparkConf = new SparkConf().setMaster("local").setAppName("API")
      val session: SparkSession = SparkSession
        .builder()
        .config(conf)
        .getOrCreate()
  
      val sc: SparkContext = session.sparkContext
      sc.setLogLevel("ERROR")
      val rdd: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\person.txt")
      // 第二种方式 : bean 类型的 RDD + javabean
      val p = new Person()
      // 1.无论 MR | Spark 都属于 Pipeline 也就是 iter 一次内存飞过一条数据 → 这一条记录完成读取/计算/序列化
      // 2.分布式计算, 计算逻辑由 Driver 序列化发送给其他 JVM 的 Executor 中执行
      val rddBean: RDD[Person] = rdd.map(_.split(" "))
        .map(arr => {
          //        val p = new Person()
          p.setName(arr(0))
          p.setAge(arr(1).toInt)
          p
        })
  
      val df: DataFrame = session.createDataFrame(rddBean, classOf[Person])
      df.show()
      df.printSchema()
    }
  }
  class Person extends Serializable {
    @BeanProperty
    var name: String = ""
    @BeanProperty
    var age: Int = 0
  }
  ```

#### 3.1————为什么`Spark`中没有实现自己的`metastore`，而是它实现了一个`cataLog`？

因为它想和`Hive`结合用别人的`metastore`就没有必要自己关起门造轮子，从所有`DDL`的解析以及`DDL`这个`HQL`语法规则，都借用`metastore`，所以`cataLog`只是一个被动的目录。

它不做`DDL`语法解析，不做元数据存储，所以它不叫作`metastore`。

#### 3.2————有`metastore`后，`cataLog`的作用？

这是一个差集全集子集的概念，`Spark—SQL + Metastore`这是一个完整的，既有`SQL`的解析又有`meatstore`也有`HQL`语法。然后能查能存有元数据管理，能操作所有`SQL`，这是完整的。

但是现在讲的版本是砍掉了`Hive`元数据这一块，所以它就缺失了`DDL`的创建的过程。所以此时这样的场景中就需要一个东西`cataLog`。基于内存临时的缓冲的，因为缩表不需要通过`DDL`来`create`，你的表就是`DataFrame`，它就不需要`DDL`了，所以只需要有一个映射关系。可以把`cataLog`想象成`HashMap`。

------

### 四、`Dataset`

`Dataset`如果想把数据由非结构变成结构化的话，最好是转成`Tuple`。

`Dataset`强的地方在于，它其中过滤数据有识别，反序列化，优化序列化。它需要编码器能够明确的而且编码器最好是可以对未来的数据能做到寻址。优化虚拟化使用方式尽量数据在内存中不成为对象，因为给出一个合适的编码器就可以让数据一直在内存中变成字节数组。在分析源码时也知道数据在内存中是对象还是字节数组他们成本开销是不一样的，而且如果变成数组的话还可以充分利用它的钨斯计划。要么放在堆中变成字节数组，要么放在堆外变成字节数组。

```scala
package com.syndra.bigdfata.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.beans.BeanProperty

/**
 * Spark SQL API
 */
object Lesson02_SQL_API01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("API")
    val session: SparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("ERROR")
    val rdd: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\person.txt")

    // Spark 的 Dataset 即可以按 collection 类似于 RDD 的方法操作, 也可以按 SQL 领域语言定义的方式操作数据
    //    import session.implicits._

    /* 纯文本文件, 不带自描述, string 不被待见
    *  必须转结构化...再参与计算
    *  转换的过程可以由 Spark 完成
    *  Hive 数仓
    *  
    * */
    val ds01: Dataset[String] = session.read.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\person.txt")
    val person: Dataset[(String, Int)] = ds01.map(
      line => {
        val strings = line.split(" ")
        (strings(0), strings(1).toInt)
      })(Encoders.tuple(Encoders.STRING, Encoders.scalaInt))
    val cperson: DataFrame = person.toDF("name", "age")
    cperson.show()
    cperson.printSchema()
  }
}

class Person extends Serializable {
  @BeanProperty
  var name: String = ""
  @BeanProperty
  var age: Int = 0
}
```

接数接的是源数据，不删除不破坏，这个整体的过程叫做`ETL`，也叫做中间态。所有的计算发生在中间态。

中间态→一切以后续计算成本为考量

- **文件格式类型，哪种会用哪种，后续的计算场景更需要哪种类型选哪种类型。**
- **分区/分桶，在未来计算时，分区表可以让计算起步加载的数据量变少，分桶是会让计算过程当中的`Shuffle`移动数据量变少。**

这两方面可以极大地优化`IO`，一个是加载`IO`一个是`Shuffle IO`。

最终趋向于抛弃`RDD`使用`Dataset`。

------

## 章节`28`：`Spark—SQL`，整合`Hive`的`MetaStore`搭建企业级数仓——Ⅰ

------

通过`session`的隐式转换，可以通过这种方式`toDF()`，`toDF()`的好处是可以加一些列名。`toDS()`是一个不带列名的数据集。

`Dataset`可以支持`collection`这种数据集的操作，也可以支持`SQL`风格的操作。

------

### 一、`SQL API`

- **基于文件形式**
  - `session.read.parquet()`
  - `session.read.textFile()`
  - `session.read.json()`
  - `session.read.csv()`
- **读取任何格式的数据源都要转换成`DF`**
  - `res.write.parquet()`
  - `res.write.orc()`
  - `res.write.text()`

------

### 二、`JDBC`

```scala
val session: SparkSession = SparkSession
      .builder()
      .appName("SQL_JDBC")
      .master("local")
      .config("spark.sql.shuffle.partitions", "1") // 默认会有 200 并行度
      .getOrCreate()
val pro = new Properties()
    pro.put("url", "jdbc:mysql://hadoop01/spark")
    pro.put("user", "root")
    pro.put("password", "120912")
    pro.put("driver", "com.mysql.cj.jdbc.Driver")
```

**什么数据源拿到的都是`DS/DF`**。

------

### 三、`Standalone_Hive`

#### 3.1————开启`Hive`支持

```scala
val s: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Standalone_Hive")
      .config("spark.sql.shuffle.partitions", 1)
      .enableHiveSupport() // 开启 Hive 支持
      .getOrCreate()
```

以上在运行时会报如下错

```scala
Exception in thread "main" java.lang.IllegalArgumentException: Unable to instantiate SparkSession with Hive support because Hive classes are not found.
	at org.apache.spark.sql.SparkSession$Builder.enableHiveSupport(SparkSession.scala:869)
	at com.syndra.bigdata.sql.Lesson04_SQL_Standalone_Hive$.main(Lesson04_SQL_Standalone_Hive.scala:16)
	at com.syndra.bigdata.sql.Lesson04_SQL_Standalone_Hive.main(Lesson04_SQL_Standalone_Hive.scala)
```

需要引入`spark_hive`依赖

```xml
<!-- https://mvnrepository.com/artifact/org.apache.spark/spark-hive -->
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-hive_2.11</artifactId>
  <version>2.3.4</version>
</dependency>
```

所谓的大数据中建表就是创建一个目录。

##### 3.1.1—————`enableHiveSupport()`

自己会启动`Hive`的`Metastore`。

#### 3.2————数据库的概念

一定要有数据库的概念，其实它现在的作用域是默认库，它进来默认走`use default`，

```scala
config("spark.sql.warehouse.dir", "D:\\ideaProject\\bigdata\\bigdata-spark\\warehouse")
```

给出的目录并不是库的意思，代表可以向哪个目录下放东西。但是在整个程序执行的时候，他已经把默认库绑定到了这个目录下。程序启动之前和程序启动之后这是两个时间维度。

真正在企业中会做分库，因为有的数据可能只有咱们这几个部门或这几个项目组使用，然后别人用的时候可以用别的库。

`MySQL`是个软件，管理数据库的软件，一个`MySQL`可以创建很多的库`Database`这些库是隔离的，所以公司只装一个`MySQL`，不同的项目组自己用自己的库`Database`。`Spark/Hive`也是一样的。

##### 3.2.1—————`Hive`很重要

```scala
import s.sql
s.catalog.listTables().show() // 作用在 current 库

sql("create database syndra")
sql("create table table01(name string)") // 作用在 current 库
s.catalog.listTables().show() // 作用在 current 库

println("————————————————————")

sql("use syndra")
s.catalog.listTables().show() // 作用在 syndra 库

sql("create table table02(name string)") // 作用在 current 库
s.catalog.listTables().show() // 作用在 syndra 库
```

------

### 四、总结

外界不需要准备环境，`Spark`自己就可以玩，只要`enableHiveSupport()`它就可以自包含的启动一个`MetaStore`支持`DDL`语句等等操作。

------

## 章节`29`：`Spark—SQL`，整合`Hive`的`MetaStore`搭建企业级数仓——Ⅱ

主要是如何贴近企业中实际去用，生产环境中不可能这么去用。要刻意注意，很多人学架构时会特别在意项目这件事。但如果学大数据的话，不要刻意把项目这个事放在心上。项目被弱化了，整个公司有一个大数据集群所有的数据都放入其中。由此衍生出中台的概念。

需要配置`Hive`的`url`的地址`thrift`。所有技术的衔接点是靠这个衔接的。

**一家公司的元数据在变化上是趋向于稳定的**，每家公司不可能天天的对表做改动。元数据访问的都是`MySQL`的一端，`MySQL`等于做了一个负载均衡，仅此而已。

------

### 一、`Spark on Hive`

```scala
.config("hive.metastore.uris", "thrift://hadoop03:9083")
.enableHiveSupport()
```

以上为连接`Hive`的配套配置，需要整合`MetaStore`，因此需要通过`uri`。

`cataLog`是可以访问全局的库。

#### 1.1————`createTempView()`

```scala
val df01: DataFrame = List(
      "Syndra",
      "WangZX"
    ).toDF("name")
    df01.createTempView("tmp")
```

临时表，只在内存中有用。其实`Spark—SQL Driver`和`MetaStore`是有一个互动的过程。只不过那些临时的表示在它的内存中，然后剩下的`DDL`语句创建的还有原有的都会在`MetaStore`进行同步。

#### 2.2————`Linux`中配置`Spark on Hive`

##### 2.1.1—————修改`hive-site.xml`

在`spark/conf`目录下，`cp hive-2.3.4/conf/hive-site.xml ./`需要修改配置

```xml
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?><!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
<configuration>
     <property>
         <name>hive.metastore.uris</name>
         <value>thrift://hadoop03:9083</value>
     </property>
</configuration>
```

##### 2.2.2—————修改`spark-defaults.conf`

`spark-defaults.conf`需要修改的配置`hive.metastore.uris thrift://hadoop03:9083`。

##### 2.2.3—————启动`spark-shell`

随后输入`./spark-shell --master yarn`即可启动，如下图

![SparkonHive](D:\ideaProject\bigdata\bigdata-spark\image\spark-shellonhive.png)

会有一行`Warning: Ignoring non-spark config property: hive.metastore.uris=thrift://hadoop03:9083`的提示，至此就可以在`Linux`中写`Spark—SQL`并且不用`IDEA`了。

测试`SparkonHive`的`SQL`语句，如下图

![test_sparkonhive_sql](D:\ideaProject\bigdata\bigdata-spark\image\test_spark-shellonhive.png)

但是这个过程有些繁琐，这样一个企业级应该做的事，在任何位置可以向它去发送`SQL`语句。在`spark-shell`运行起来后，在整个集群中会**启动一个`Application`长运行**，如下图的

![spark-shell长运行](D:\ideaProject\bigdata\bigdata-spark\image\spark-shell长运行.png)

`Linux`中的`spark-shell`**集群不停，它就一直处于`RUNNING`状态**。且这个`Application`还持有了一些`Container`，如下图

![Application 持有的 Container](D:\ideaProject\bigdata\bigdata-spark\image\Application持有的container.png)

而且通过`WebUI`也可以知道，`Driver`启动了两个`Executor`。

`hadoop04`的后台进程会启动一个`CoarseGrainedExecutorBackend`，如下图

![CoarseGrainedExecutorBackend后台进程](D:\ideaProject\bigdata\bigdata-spark\image\CoarseGrainedExecutorBackend后台进程.png)

未来写`SQL`也会稍微快一些，因为它不像`Hive`，如果在`Hive`写`SQL`还复杂一点就会触发`MR`有**冷拉起的过程**。同样复杂的`SQL`，如果在以上方式写给`Spark`的话，它不需要冷拉起，因为`Executor`进程已经在运行。它只需要解然后发出去变成线程立刻就跑起来了，这就是两点差异。

------

### 二、为什么每家公司都要用大数据？

首先并不是因为大数据好大家才用大数据。是因为现在的互联网模型每天会爆发大量的数据。有些公司每天的数据会有`10T`左右，其实一家公司可以不在乎这些数据，它也可以在乎这些数据。如果之前没有大数据的话，而且慢网络的时候没有这么大并发量没有这么多需要做用户个性化。给用户必须提供`7 x 24h`必须快速的那种服务的要求不是那么高的时候，这个事可以不用做，但是现在随随便便一个互联网的网红就可以有很大流量的年代，如果连用户的服务都做不到的话，可能公司其实离挂也不远了。所以此时企业说了，因为也有大数据了，这些都成熟了那么之前我可能只需要招团队去开发我的电商产品。那么现在我还要再招一个团队，一直盯着我这个开发团队开发这个电商它每时每秒的运行是不是都很健康，我整个公司的所有系统是不是都能为上下游提供一个稳定的运转，这就是大数据冰山一角可以做的事情。那么现在再来看企业是否需要这个环节，除非你真的就是特立独行。

------

### 三、`spark—sql.sh`

`spark-shell`的`Hive`形式虽然很好，但是距离`SQL`还有点远，而且不能远程连接。所以`Spark`很贴心，除了有一个`spark—shell`还有一个**`spark—sql`**

```shell
# 切换到 spark 的 bin 目录下
cd spark-2.3.4-bin-hadoop2.6/bin
```

如下图

![spark—sql](D:\ideaProject\bigdata\bigdata-spark\image\spark—sql.png)

#### 3.1————启动`spark-sql`

```shell
# 启动 spark-sql.sh
./spark-sql --master yarn
```

`./spark-sql --master yarn`，如下图（**启动过程较长，共五个效果图**）

![spark-sql1运行](D:\ideaProject\bigdata\bigdata-spark\image\spark-sql1.png)

![spark-sql2](D:\ideaProject\bigdata\bigdata-spark\image\spark-sql2.png)

![spark-sql3](D:\ideaProject\bigdata\bigdata-spark\image\spark-sql3.png)

![spark-sql4](D:\ideaProject\bigdata\bigdata-spark\image\spark-sql4.png)

![spark-sql5](D:\ideaProject\bigdata\bigdata-spark\image\spark-sql5.png)

至此`spark-sql`启动成功。在`WebUI`同样有一个`SparkSQL`长连接，如下图

![SparkSQL长连接](D:\ideaProject\bigdata\bigdata-spark\image\SparkSQL长连接.png)

可以看出，**依然是`RUNNING`状态**。`SparkWebUI`的`SparkSQL`如下图

![SparkUI的SparkSQL](D:\ideaProject\bigdata\bigdata-spark\image\SparkWebUI的SparkSQL.png)

测试`spark-sql`，如下图

![测试 spark-sql](D:\ideaProject\bigdata\bigdata-spark\image\测试spark-sql.png)

在`spark-sql`中创建的表在`Hive`中也能查到，如下图

![spark-sql建表](D:\ideaProject\bigdata\bigdata-spark\image\spark中创建表.png)

![Hive 中查询 spark-sql 创建的表](D:\ideaProject\bigdata\bigdata-spark\image\在Hive中查看spark创建的表.png)

所以，**`Spark`和`Hive`共享了一个`MetaStore`，所以建表的`DDL`语句只需要在一边创建即可，另一边可以享受。**

那么能否启动一个类似`Hive`的`hiveserver2`并运行？可以在任何位置面向这个类似`hiveserver2`的服务发送`SQL`？并由这个服务跑`Spark`的程序。

也就是说，能否先跑一个`Spark`程序，不在命令行接收`SQL`，而是要对外暴漏`JDBC`服务。让所有人，例如通过`beeline`的形式提交`SQL`语句。

------

### 四、`start-thriftserver.sh`———长服务

#### 4.1————启动`Thrift JDBC`

在`sbin`目录下。

```shell
# 切换到 spark/sbin
cd spark-2.3.4-bin-hadoop2.6/sbin
```

因为**它一般倾向于伴随着集群启动**，会有一个`start-thriftserver.sh`如下图

![start-thriftserver](D:\ideaProject\bigdata\bigdata-spark\image\start-thriftserver.png)

启动`start-thriftserver.sh`，如下图

```shell
# 启动命令
./start-thriftserver.sh --master yarn
```

![启动 start-thriftserver](D:\ideaProject\bigdata\bigdata-spark\image\start-thriftserver-SparkSubmit.png)

可以看到启动过程很快，通过`jps`能发现启动了一个`SparkSubmit`的后台进程，在`HadoopUI`会看到一个`Thrift JDBC/ODBC Server`，如下图

![HadoopUI Thrift JDBC](D:\ideaProject\bigdata\bigdata-spark\image\HadoopUI-Thrift-JDBC.png)

它是一个运行分布式的对外提供`JDBC`连接服务。

#### 4.2————启动`beeline`

同理类似`Hive`的`beeline`，在`Spark`中也有`beeline`，如下图

![Spark 中的 beeline](D:\ideaProject\bigdata\bigdata-spark\image\Spark中的beeline.png)

运行`beeline`，如下图

```shell
cd spark-2.3.4-bin-hadoop2.6/bin
./beeline
```

![运行 beeline](D:\ideaProject\bigdata\bigdata-spark\image\运行Spark中的beeline.png)

可以直观感受到`beeline`的启动甚至比`Thrift JDBC`的启动过程还要快。

##### 4.2.1—————`beeline`连接`hiveserver2`

连接`hiveserver2`，如下图

```hive
!connect jdbc:hive2://hadoop03:10000
```

![连接 hiveserver2](D:\ideaProject\bigdata\bigdata-spark\image\spark中使用beeline连接hiveserver2.png)

------

### 五、思考

如果你是一家公司的技术决策者或技术总监、架构师，你觉得公司这么多人扑上来，要用到你所有的关于`SQL`对数据的加工，你应该暴露哪种服务形式？

是分发`Linux`系统的账户密码，让他们登录来执行`spark-shell、spark-sql`好，还是启动一个让他们从其他地方通过`JDBC`连过来执行好？

答案是后者，而且他们俩互抄了，无论`Hive`还是`Spark-SQL`都希望暴露的是`server`。

------

## 章节`30`：`Spark—SQL`，复杂`SQL`，函数，自定义函数，开窗`over`函数，`OLAP`

------

在写`SQL`时，基本都会用到函数，函数也是比较重要的一个概念。

[`Spark`官网`SQL Documents`](https://archive.apache.org/dist/spark/docs/2.3.4/api/scala/index.html#org.apache.spark.sql.functions$)

------

### 一、函数

#### 1.1————使用场景

有两类场景会使用函数

- `OLTP`：多数为时间函数，日期函数，金额函数等等，偏向于事务性。
- `OLAP`：趋向于分析性，复杂度会略高于`OLTP`，因为会涉及到对数据处理。

#### 1.2————维度划分

函数分为三个维度

##### 1.2.1—————字符串函数

包括、数值函数、金额函数、时间函数。

##### 1.2.2—————数值加工函数

聚合型函数、窗口函数（开窗函数）。

##### 1.2.3—————`System or Custom Definition`函数

系统会提供大量的函数。

------

### 二、聚合

如果要做聚合，那么首先要明白聚合它的前提要有组的概念。但是这个有组的概念必须要写`group by ?`吗？聚合函数必须要作用在`group by`上吗？

非必须，首先`group by`默认整张表拿这个聚合函数作用在这个列的所有值上，如果写了`group by`只不过将这张表根据后面那个列的值分成若干组，每一组数据单拿出来作用在一个函数上。不管怎么样最终都是一组数据作用在一个函数身上，那么函数要对这一组数据进行处理。

那么处理过程分为几个阶段，

- 首先组中的**数据一条一条**组成，**一条为单位**进入，要进入我们的聚合操作。
- 由于未来数据量不确定，所以会有`Buffer`的概念。一条记录后会有`schema`**识别数据**。而且`Buffer`前会有**初始化**的环节。
- 然后进行**`merge`**的过程。
- 最终**计算**。
- `Buffer`也需要一个**`schema`**。
- 输出的结果**返回类型`type`**。
- 输出后放到**结果表**的某一列。

#### 2.1————`UserDefinedAggregateFunction`

聚合函数需要实现的方法

```scala
class MyAggFunc extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = ???

  override def bufferSchema: StructType = ???

  override def dataType: DataType = ???

  override def deterministic: Boolean = ???

  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}
```

根据需求业务自定义实现。

------

### 三、`OLAP`

大数据主要做的就是`OLAP`。在分析时非常重要的一个就是开窗函数依赖`over`。`fun()`作用在`over()`，所谓`over()`其实就是分组。但这个分组它还和`group by`分组不一样，`group by`分组查询趋向于末端。开窗函数是趋向于查询完的那个结果集要再做一次分区的过程。

一般`over()`中会有`over(partition, order)`这两个维度。`fun()`可以是聚合或非聚合。

所谓开窗函数其实就是`over()`。

#### 3.1————案例

```scala
// over
s.sql("select *," +
        " count(score) over(partition by class) as num " +
        "from users")
      .show()
```

```shell
# SQL 执行结果
+----+-----+-----+---+
|name|class|score|num|
+----+-----+-----+---+
|   A|    1|   90|  3|
|   B|    1|   90|  3|
|   C|    1|   80|  3|
|   A|    2|   50|  2|
|   B|    2|   60|  2|
+----+-----+-----+---+
```

以上使用`over`，开窗函数其实虽然也会有`count`，但应该也是作用在`group by`上，但`count`如果作用在`over`身上，它其实是在为每一条记录补充汇总信息。

例如`A`报了课程`1`考了`90`分，那么`1`类中一共有`3`条记录。`B`也报了一类，考了`90`分。如果单查，既能知道分数也能知道报了哪个科目还知道此科目中共有多少条其他记录。这就是一个最终明细表且有汇总信息。

有时期望的是以上的表而不是以下的表，所以`SQL`语句没有对错只不过你把它用在什么地方。要灵活运用这才是`OLAP`。

```scala
// group by
s.sql("select class, count(score) as num from users group by class").show()
```

```shell
# SQL 执行结果
+-----+---+
|class|num|
+-----+---+
|    1|  3|
|    2|  2|
+-----+---+
```

如果这么写，以上的`group by`中一直在强调有个约束，结果表`group by`那组就只剩下一条记录了。`1`类学科中有三笔记录，`2`类学科中有两笔记录，前边所有信息都丢了，但是其实有时需要的是为每一个人补充他如果报了`1`类科目那么他所在的`1`类科目中一共有多少条记录？每一个人都要出现他所报的科目以及这个科目多少条记录，并为每个人补充这个数值。要把它分开贴上去而不是说最终做一个聚合这么一个结果。聚合这个结果可能没有意义你是要为每个人补充这么一个汇总信息。

所以如此看出`group by`最终做的是聚合汇总。

------

### 四、`OLTP`

无非就是`CRUD`等等那些`select`的过程。无非就是疯狂的在外面变条件，连分组`join`其实都应该去避免。如果真的要发生分组，这个分组或者`join`都要做一些预计算。比如视图，用视图方式来加速试图雾化。

比如在商品详情页，把数据都放入`Redis`，这样才能更快从而应对高并发。后续那些高并发的东西虽然它有很多机器去负载，但是某一台机器如果卡住了一个查询，后续那些就阻塞住了。

------

# 后续`Spark—SQL`源码分析详见`Spark—SQL_源码.md`
