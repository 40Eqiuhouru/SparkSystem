package com.syndra.bigdata.Lesson08_Other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark RDD High-Level Operations
 */
object Lesson05_RDD_HighLevel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10, 5)

    // 数据抽样
    // 种子 : 一般抽样都会涉及到种子的概念, 如果给出种子, 相同的种子多少次抽到的都是相同的那批数据
    // sample(是否允许重新抽, 抽取占比, 种子)
    // 一种场景
    // A, B 在同一个项目组, 数据其中一批在集群中, 然后 A, B 共同开发
    // 二人约定种子都是 22, A 在本地拿 22 抽出一批数据, B 在另外一台机器中也用 22 抽出一批数据
    // 然后 A, B 抽到的数据相同, 如果有时项目中刻意的有相同代码
    // 要基于相同的样本去开发业务逻辑时, 那就用 22 约定一个种子就可以了
    //    data.sample(false, 0.1, 222).foreach(println)
    //    println("--------------------")
    //    data.sample(false, 0.1, 222).foreach(println)
    //    println("--------------------")
    //    data.sample(false, 0.1, 221).foreach(println)
    //    println("--------------------")

    // 大数据中, 并行度是计算提速的一个方式
    // 但是并行度绝对不是说越高越好, 要保证一个并行度粒度中的数据量
    // 即符合 IO 又符合内存的消耗, 因为创建进程的时间也有损耗
    // 如果只有 3台 机器, 每个两个核心的话, 理论上最多的并行度是 6 个 JVM
    // 一个 JVM 消耗一个 CPU, 6 个就够了, 但是有可能几十个 RDD 关联后, 它的分区变的好多, 并行度就上去了
    // 那么此时其实同意时间只能有 6 个进程的话, 如果有 60 个分区, 那就要分十次跑完
    // 那还不如就把它变成 6 个分区, 每个分区数量大一点, 所以调整分区是一定会在未来的工作
    // 在业务层面之外调优不可避免, 而同时是面试环节必须要聊的一件事, 含金量很高
    // 这其中细活很多!!!
    println(s"data:${data.getNumPartitions}")

    println("--------------------")

    // 为每条记录打印标识
    val data1: RDD[(Int, Int)] = data.mapPartitionsWithIndex(
      (pI, pT) => {
        pT.map(e => (pI, e))
      }
    )
    data1.foreach(println)

    println("--------------------")

    // 1.调整分区量
    //    val repartition = data1.repartition(8)
    // 使用 repartition 最底层的算子
    // 设置为 false 不产生 shuffle, 并没有重新规划分区
    // 如果分区想从 5 -> 6, 仔细思考就会明白
    // 数据分裂了五个分区去六个分区, 它是必然会产生 shuffle 的, 如果没有 shuffle
    // 它怎么知道应该拿哪一条记录 ?
    // 怎么去计算它的哈希值模下游的分区数, 重新算它的分区值呢 ?
    // 这就是结论
    // 如果有小的分区数变多的分区数时, 在那种特殊情况下, 也就代表曾经其中的一个分区的数据, 必然会散到更多的分区中去
    // 因为分区数变多了, 肯定会把一个分区数据向几个分区去散, 才能保证同批数据集由少分区数变成多分区数
    // 必须有这个散的过程, 但是你又不让它有 shuffle, 没有 shuffle 就约等于没有分区器
    // 没有分区器的话每条记录就不能算它的分区号, 那么这个散的过程就不成立, 所以最终运行结果
    // 它并没有重新规划分区
    // 如果设置为 true, 那么触发 shuffle, 其实就代表它就有分区器
    // 有分区器, 此时前面的每个元素就可以算出它的分区号, 因为要模下游的 numPartitions
    // 这样每个元素就知道要去哪个分区了
    // 分布式情况下, 最基本的一个本质就是, 如果数据想从节点到不同节点的一个散列必须要分区器
    // 要分区的潜台词就是要触发 shuffle, 这个是不可逾越的事
    // 那么分区数变少, 是否也必须要有 shuffle 才能减少分区数 ?
    // 分布式情况下, 数据的移动方式分为 : IO 移动, Shuffle 移动
    // 什么时候必须用 Shuffle 移动数据呢 ?
    // 每条元素要去的地方不一样, 它们必须用一个分区计算出自己该去哪
    // 什么是 IO 移动 ?
    // 数据移动过来就可以了, 我不区分对待
    // 结论 : coalesce() 由多变少的过程是可以不用 shuffle 的, 但是由少变多必须要 shuffle 才能成功
    val repartition: RDD[(Int, Int)] = data1.coalesce(3, true)

    // 转换后的数据分区
    val res: RDD[(Int, (Int, Int))] = repartition.mapPartitionsWithIndex(
      (pI, pT) => {
        // 曾经是哪个分区的哪条数据
        pT.map(e => (pI, e))
      }
    )

    println(s"data:${res.getNumPartitions}")
    res.foreach(println)

    while (true) {
    }
  }
}
