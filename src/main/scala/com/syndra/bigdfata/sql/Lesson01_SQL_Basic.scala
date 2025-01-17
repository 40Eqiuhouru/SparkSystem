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
    functions.show(999, truncate = true)

    println("————————————————————————")

    val df: DataFrame = session.read.json("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\json")
    df.show()
    df.printSchema()

    // 这一过程是 df 通过 session 像 cataLog 中注册表名
    df.createTempView("sql_test")

    //    val frame: DataFrame = session.sql("select * from sql_test")
    //    frame.show()
    //
    //    println("————————————————————————")
    //
    //    session.catalog.listTables().show()
    import scala.io.StdIn._

    while (true) {
      val sql: String = readLine("input your sql: ")
      session.sql(sql).show()
    }
  }
}
