package com.syndra.bigdata.scala

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Scala Collections
 */
object Lesson02_Collections {

  def main(args: Array[String]): Unit = {
    // 你是一个 JavaCoder
    val listJava = new util.LinkedList[String]()
    listJava.add("Java")

    // Scala 还有自己的 collections
    // 1.数组
    // Java 中泛型是 <>, Scala 中是 [], 所以数组用 (n)
    // val 约等于 final, 不可变描述的是 val 指定的引用的值(值 : 字面值, 地址)
    val arr01 = Array[Int](xs = 1, 2, 3, 4)
    arr01(1) = 99
    println(arr01(1))

    for (elem <- arr01) {
      println(elem)
    }
    // 遍历元素, 需要函数接收元素
    arr01.foreach(println)

    println("-------- List --------")
    // 2.链表
    // Scala 中 collections 中有 2 个包 : immutable, mutable, 默认的是不可变的 immutable
    val list01 = List(1, 2, 3, 4, 5)
    for (elem <- list01) {
      println(elem)
    }
    list01.foreach(println)

    val list02 = new ListBuffer[Int]()
    list02.+=:(1)
    list02.+=(2)
    list02.+=(3)
    // TODO Scala 数据集中的 ++, +=, ++:, :++
    list02.foreach(println)

    println("-------- Set --------")
    val set01 = Set(1, 2, 3, 4, 3, 2, 1)
    for (elem <- set01) {
      println(elem)
    }
    set01.foreach(println)

    // 可变的
    import scala.collection.mutable.Set
    val set02: mutable.Set[Int] = Set(11, 22, 33, 44)
    set02.add(55)
    set02.foreach(println)

    val set03: Predef.Set[Int] = scala.collection.immutable.Set(33, 44, 22, 11)
    set03.foreach(println)
    //    set03.add

    println("-------- Tuple --------")

    val t2 = new Tuple2(11, "Syndra")
    val t3 = Tuple3(22, "WangZX", 'S')
    val t4: (Int, Int, Int, Int) = (1, 2, 3, 4)
    val t22: ((Int, Int) => Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)
    = ((a: Int, b: Int) => a + b + 8, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4)
    println(t2._1)
    println(t4._3)
    //    val i: Int = t22._1(8)
    //    println(i)
    println(t22._1)

    // 迭代器
    val tIter: Iterator[Any] = t22.productIterator
    while (tIter.hasNext) {
      println(tIter.next())
    }

    println("-------- Map --------")
    val map01: Map[String, Int] = Map(("S", 29), "y" -> 29, ("n", 29), ("d", 23), ("S", 23))
    val keys: Iterable[String] = map01.keys
    // option : none, some
    println(map01.get("S").get)
    //    println(map01.get("a").get)
    println(map01.get("S").getOrElse("wants to be fucked"))
    println(map01.get("a").getOrElse("wants to be fucked"))

    for (elem <- keys) {
      println(s"key: $elem value: ${map01.get(elem).get}")
    }

    //    keys.foreach()

    val map02: mutable.Map[String, Int] = scala.collection.mutable.Map(("w", 23), ("a", 23))
    map02.put("n", 23)

    println("-------- Art --------")

    // 这个 List 是不可变集合
    val list = List(1, 2, 3, 4, 5, 6)
    list.foreach(println)
    // 其实这种函数是取其中元素加工产生一个新集合, 中间数据不会受到影响
    val listMap: List[Int] = list.map((x: Int) => x + 10)
    listMap.foreach(println)
    val listMap02: List[Int] = list.map(_ * 10)
    list.foreach(println)
    listMap02.foreach(println)

    println("-------- Art Sublimation --------")

    val listStr = List(
      "hello world",
      "hello syndra",
      "good idea"
    )

    //    val listStr = Array(
    //      "hello world",
    //      "hello syndra",
    //      "good idea"
    //    )

    // 只要是数据集, 基本操作都 hold 住, 只不过其中的顺序可能不同
    // 根据不同的类型, 它可能过程中会有些差异, 只要数据集中的元素, 它们的逻辑方法是一样的, 逻辑可能有差异
    //    val listStr = Set(
    //      "hello world",
    //      "hello syndra",
    //      "good idea"
    //    )

    // 扁平化
    // flatMap 拿了一个 hello world 给你的函数
    // 你的函数把 hello world 切割之后变成 hello, world 两个东西
    // flatMap 会把 hello, world 扔到这个结果 list 中
    // 变成各自独立的元素
    // 那么曾经 listStr 中有三个元素, flatMap 执行完后变成了六个元素
    val flatMap = listStr.flatMap((x: String) => x.split(" "))
    flatMap.foreach(println)
    // 映射成 KV(wc 单词统计)
    val mapList = flatMap.map((_, 1))
    mapList.foreach(println)

    // 以上代码有什么问题吗 ?
    // 内存扩大了 N 倍, 每次计算内存都留有对象数据.
    // 有没有什么现成的技术解决数据计算中间状态占用内存这一问题 ?
    // Iterator

    println("-------- Art Sublimation Iterator --------")

    // 什么是迭代器, 为什么会有迭代器模式 ? 迭代器中不存数据 !
    val iter: Iterator[String] = listStr.iterator

    val iterFlatMap = iter.flatMap((x: String) => x.split(" "))
    // 迭代器还有一个不可逃避的问题, 迭代器中只放了个指针
    // 在此行代码执行时, 已经将这个迭代器的指针经过遍历移动到了末尾,
    // 然后再去使用迭代器时, 因为已经指到末尾了, 就迭代不出来了
    //    iterFlatMap.foreach(println)

    // 符合的语义 : 数据集单向只发生到结果, 中间是没有留有状态的, 这个数据直接流转过来了
    val iterMapList = iterFlatMap.map((_, 1))
    //    iterMapList.foreach(println)

    while (iterMapList.hasNext) {
      val tuple: (String, Int) = iterMapList.next()
      println(tuple)
    }

    // 1.listStr 是真正的数据集, 有数据的
    // 2.iter.flatMap 没有发生计算, 返回了一个新的迭代器
  }

}
