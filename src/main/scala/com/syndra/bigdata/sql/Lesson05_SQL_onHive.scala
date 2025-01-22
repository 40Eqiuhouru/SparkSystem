package com.syndra.bigdata.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark on Hive
 */
object Lesson05_SQL_onHive {
  def main(args: Array[String]): Unit = {
    val s: SparkSession = SparkSession
      .builder()
      .appName("test_on_Hive")
      .master("local")
      .config("hive.metastore.uris", "thrift://hadoop03:9083")
      .enableHiveSupport()
      .getOrCreate()
    val sc = s.sparkContext
    sc.setLogLevel("ERROR")

    import s.implicits._

    val df01: DataFrame = List(
      "Syndra",
      "WangZX"
    ).toDF("name")
    df01.createTempView("tmp_1")

    //    s.sql("create table tmp(id int)") // DDL

    s.sql("use default")
    s.sql("insert into tmp values (3), (6), (7)") // DML
    df01.write.saveAsTable("tmp_sat")

    // 能否看到表 ?
    s.catalog.listTables().show()

    // 如果没有 Hive 时, 表最开始一定是 Dataset/DataFrame
    s.sql("use spark")
    val df: DataFrame = s.sql("show tables")
    df.show()
  }
}
