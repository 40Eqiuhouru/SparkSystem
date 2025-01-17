package com.syndra.bigdfata.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Spark SQL Standalone Hive
 */
object Lesson04_SQL_Standalone_Hive {
  def main(args: Array[String]): Unit = {
    /*
    * 学 Spark—SQL 不会对 Hive 进行更多的复习
    * */
    val s: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Standalone_Hive")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.warehouse.dir", "D:\\ideaProject\\bigdata\\bigdata-spark\\warehouse")
      .enableHiveSupport() // 开启 Hive 支持
      .getOrCreate()
    val sc: SparkContext = s.sparkContext
//    sc.setLogLevel("ERROR")

    import s.sql

//    s.sql("create table sh_test_V1(name string, age int)")

//    s.sql("insert into sh_test_V1 values('Syndra', 29), ('WangZX', 24)")

//    sql("create database syndra")
//    sql("create table syndra.v4(name string, age int)")
//    sql("insert into syndra.v4('Syndra', 29), ('WangZX', 24)")

    s.catalog.listTables().show() // 作用在 current 库

    sql("create database syndra")
    sql("create table table01(name string)") // 作用在 current 库
    s.catalog.listTables().show() // 作用在 current 库

    println("————————————————————")

    sql("use syndra")
    s.catalog.listTables().show() // 作用在 syndra 库

    sql("create table table02(name string)") // 作用在 current 库
    s.catalog.listTables().show() // 作用在 syndra 库

//    sql("select * from sh_test_V1").show()

//    sql("select * from syndra.v4").show()
  }
}
