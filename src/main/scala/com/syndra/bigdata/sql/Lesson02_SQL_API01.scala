package com.syndra.bigdata.sql

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

    // 第二种方式 : bean 类型的 RDD + javabean
    //    val p = new Person()
    //    // 1.无论 MR | Spark 都属于 Pipeline 也就是 iter 一次内存飞过一条数据 → 这一条记录完成读取/计算/序列化
    //    // 2.分布式计算, 计算逻辑由 Driver 序列化发送给其他 JVM 的 Executor 中执行
    //    val rddBean: RDD[Person] = rdd.map(_.split(" "))
    //      .map(arr => {
    //        //        val p = new Person()
    //        p.setName(arr(0))
    //        p.setAge(arr(1).toInt)
    //        p
    //      })
    //
    //    val df: DataFrame = session.createDataFrame(rddBean, classOf[Person])
    //    df.show()
    //    df.printSchema()

    // V1.1 : 动态封装
    //    val userSchema = Array(
    //      "name string",
    //      "age int",
    //      "sex int"
    //    )
    //
    //    // 1. Rdd[Row]
    //    /* UtilMethod */
    //    def toDataType(f: (String, Int)): Any = {
    //      userSchema(f._2).split(" ")(1) match {
    //        case "string" => f._1.toString
    //        case "int" => f._1.toInt
    //      }
    //    }
    //
    //    val rowRDD: RDD[Row] = rdd.map(_.split(" "))
    //      .map(x => x.zipWithIndex)
    //      // [(WangZX, 0), (18, 1), (0, 2)]
    //      .map(x => x.map(toDataType(_)))
    //      .map(x => Row.fromSeq(x)) // Row 代表着很多的列, 每个列要标识出准确的类型
    //
    //    // 2.StructType
    //    /* UtilMethod */
    //    def getDataType(t: String) = {
    //      t match {
    //        case "string" => DataTypes.StringType
    //        case "int" => DataTypes.IntegerType
    //      }
    //    }
    //
    //    val fields: Array[StructField] = userSchema.map(_.split(" "))
    //      .map(x => StructField.apply(x(0), getDataType(x(1)), nullable = true))
    //
    //    val schema: StructType = StructType.apply(fields)
    //
    //    val schema01: StructType = StructType.fromDDL("name string, age int, sex int")
    //    val df: DataFrame = session.createDataFrame(rowRDD, schema01)
    //    df.show()
    //    df.printSchema()

    // 第一种方式 : row 类型的 RDD + StructType
    //    // 数据 + 元数据 = df 就是一张表
    //
    //    // 1.数据 : RDD[Row]
    //    val rdd: RDD[String] = sc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\person.txt")
    //    val rddRow: RDD[Row] = rdd.map(_.split(" ")).map(arr => Row.apply(arr(0), arr(1).toInt))
    //
    //    // 2.元数据 : StructType
    //    val fields = Array(
    //      StructField.apply("name", DataTypes.StringType, nullable = true),
    //      StructField.apply("age", DataTypes.IntegerType, nullable = true)
    //    )
    //    val schema: StructType = StructType.apply(fields)
    //
    //    val dataFrame: DataFrame = session.createDataFrame(rddRow, schema)
    //    dataFrame.show()
    //    dataFrame.printSchema()
    //    dataFrame.createTempView("person")
    //    session.sql("SELECT * FROM person").show()
  }
}

class Person extends Serializable {
  @BeanProperty
  var name: String = ""
  @BeanProperty
  var age: Int = 0
}
