package com.syndra.bigdata.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * Spark SQL with JDBC
 */
object Lesson03_SQL_JDBC {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("SQL_JDBC")
      .master("local")
      .config("spark.sql.shuffle.partitions", "1") // 默认会有 200 并行度
      .getOrCreate()
    val sc: SparkContext = session.sparkContext
    sc.setLogLevel("INFO")

    val pro = new Properties()
    pro.put("url", "jdbc:mysql://hadoop01/spark")
    pro.put("user", "root")
    pro.put("password", "120912")
    pro.put("driver", "com.mysql.cj.jdbc.Driver")

    val usersDF: DataFrame = session.read.jdbc(pro.get("url").toString, "users", pro)
    val scoreDF: DataFrame = session.read.jdbc(pro.get("url").toString, "score", pro)

    usersDF.createTempView("users_tab")
    scoreDF.createTempView("score_tab")

    val resDF: DataFrame = session.sql("select users_tab.id, users_tab.name, users_tab.age, score_tab.score from users_tab join score_tab on users_tab.id = score_tab.id")
    resDF.show()
    resDF.printSchema()

    //    println(resDF.rdd.partitions.length)
    //    val resDF01: Dataset[Row] = resDF.coalesce(1)
    //    println(resDF01.rdd.partitions.length)

    resDF.write.jdbc(pro.get("url").toString, "partitions_v2", pro)

    // 什么数据源拿到的都是 DS/DF
    //    val jdbcDF: DataFrame = session.read.jdbc(pro.get("url").toString, "test_jdbc", pro)
    //    jdbcDF.show()
    //
    //    jdbcDF.createTempView("spark")
    //
    //    session.sql("select * from spark").show()
    //
    //    jdbcDF.write.jdbc(pro.get("url").toString, "test_write", pro)
  }
}
