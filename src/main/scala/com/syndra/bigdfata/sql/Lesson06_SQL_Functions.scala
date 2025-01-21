package com.syndra.bigdfata.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Spark SQL Functions
 */
object Lesson06_SQL_Functions {
  def main(args: Array[String]): Unit = {
    val s: SparkSession = SparkSession
      .builder()
      .appName("SQL_Functions")
      .master("local")
      .getOrCreate()
    s.sparkContext.setLogLevel("ERROR")

    import s.implicits._

    val dataDF: DataFrame = List(
      ("A", 1, 90),
      ("B", 1, 90),
      ("A", 2, 50),
      ("C", 1, 80),
      ("B", 2, 60)
    ).toDF("name", "class", "score")

    dataDF.createTempView("users")

    // OLAP
    /*
    s.sql("select *," +
        " count(score) over(partition by class) as num " +
        "from users")
      .show()
    s.sql("select class, count(score) as num from users group by class").show()*/

    /*
    * 开窗函数 依赖 fun() over(partition order)
    * group by 是纯聚合函数 : 一组最后就一条
    * */
    /*
    s.sql("select *," +
        "  rank() over(partition by class order by score desc) as rank, " +
        "  row_number() over(partition by class order by score desc) as number " +
        "from users")
      .show()*/

    // ODS(OpenDataSystem) DW(DataWarehouse) DM(DataMart) DI(DataIntegration, 仓库 真实/历史 拉链)

    // 行列转换
    /*
    s.sql("select name, " +
        "explode( " +
        "split( " +
        "  concat(case when class = 1 then 'AA' else 'BB' end, ' ', score), ' ' " +
        " ))" +
        "as ox  " +
        "from users")
      .show()*/

    // case when
    /*
    s.sql("select  " +
        "  case  " +
        "  when score <= 100 and score >= 90 then 'good'  " +
        "  when score < 90 and score >= 80 then 'you' " +
        "  else 'cha' " +
        "end as ox, " +
        "  count(*)  " +
        "from users  " +
        "group by  " +
        "  case  " +
        "  when score <= 100 and score >= 90 then 'good'  " +
        "  when score < 90 and score >= 80 then 'you' " +
        "  else 'cha' " +
        "end  "
      )
      .show()*/

    // 根据成绩评级
    /*
    s.sql("select * " +
        "case " +
        "  when score <= 100 and score >= 90 then 'good'  " +
        "  when score < 90 and score >= 80 then 'you' " +
        "  else 'cha' " +
        "end as ox " +
        "from users")
      .show()*/

    // UDF
    /*
    s.udf.register("ooxx", new MyAggFunc)
    s.sql("select name,    " +
        "  ooxx(score)   " +
        "from users  " +
        "group by name  ")
      .show()*/

    // 普通 UDF
    /*
    s.udf.register("ooxx", (x: Int) => {
      x * 10
    })
    s.sql("select * , ooxx(score) as ox from users").show()*/

    // 分组, 排序统计
    // 全表的二次排序
    /*
    s.sql("select * from users order by name desc, score asc").show()*/

    // V1
    /*
    val res: DataFrame = s.sql("select name,   " +
      " sum(score) " +
      "from users " +
      "group by name " +
      "order by name desc")

    res.show()
    println("——————————")
    res.explain(true)*/

    /*
    val res: DataFrame = s.sql("select ta.name, ta.class, tb.score " +
      "from " +
      "(select name, class from users) as ta " +
      "join" +
      "(select name, score from users) as tb " +
      "on ta.name = tb.name " +
      "where tb.score > 60")
    res.show()
    println("——————————")
    res.explain(true)
  }*/
}

/**
 * 自定义聚合函数
 */
class MyAggFunc extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    // ooxx(score)
    // 顺序为传参的顺序
    StructType.apply(Array(StructField.apply("score", IntegerType, nullable = false)))
  }

  override def bufferSchema: StructType = {
    // avg sum / count = avg
    StructType.apply(Array(
      StructField.apply("sum", IntegerType, nullable = false),
      StructField.apply("count", IntegerType, nullable = false)
    ))
  }

  override def dataType: DataType = {
    DoubleType
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 1
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 组内, 一条记录调用一次
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  override def evaluate(buffer: Row): Double = {
    buffer.getInt(0) / buffer.getInt(1)
  }
}
