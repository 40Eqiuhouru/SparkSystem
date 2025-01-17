package com.syndra.bigdfata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * <h1>Java WordCount Program</h1>
 */
public class WordCountJava {

    public static void main(String[] args) {
        // 模板写法
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("java-wordcount");
        sparkConf.setMaster("local");

        // 创建 JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        // 读取数据集
        JavaRDD<String> fileRDD = jsc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\testdata.txt");
        // 扁平化切分单词
        // flatMap(FlatMapFunction<String, U> f) 接收的是一个类型
        fileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer oldValue, Integer v) throws Exception {
                return oldValue + v;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> value) throws Exception {
                System.out.println(value._1 + "\t" + value._2);
            }
        });


//        // 读取数据集
//        JavaRDD<String> fileRDD = jsc.textFile("D:\\ideaProject\\bigdata\\bigdata-spark\\data\\testdata.txt");
//        // 扁平化切分单词
//        // flatMap(FlatMapFunction<String, U> f) 接收的是一个类型
//        JavaRDD<String> wordsRDD = fileRDD.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String line) throws Exception {
//                return Arrays.asList(line.split(" ")).iterator();
//            }
//        });
//        // 转换成键值对, 需要走 map 映射
//        JavaPairRDD<String, Integer> pairWord = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String word) throws Exception {
//                return new Tuple2<String, Integer>(word, 1);
//            }
//        });
//
//        // 键值对有了之后, 进行统计 reduceByKey
//        JavaPairRDD<String, Integer> res = pairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer oldValue, Integer v) throws Exception {
//                return oldValue + v;
//            }
//        });
//
//        // 不转换, 只输出数据集, 不回收
//        res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> value) throws Exception {
//                System.out.println(value._1 + "\t" + value._2);
//            }
//        });
    }

}
