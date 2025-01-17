package com.syndra.bigdfata.scala

/**
 * Scala 流程控制
 * <p>自己的特征 : class object
 */
object Lesson00_Object_IF {

  def main(args: Array[String]): Unit = {
    var a = 3
    if (3 < 0) {
      println(s"$a < 3")
    } else {
      println(s"$a >= 0")
    }

    println("---------- while ----------")

    var s = 0
    //    val s1 = 0 val 定义的类型是 final 的不能被修改
    while (s < 10) {
      println(s)
      s += 1
      //      s1 = s1 + 1
    }

    println("---------- for ----------")
    // for
    //    for (i = 0; i < 10; i++)
    //    for (P x : xs)
    val seqs: Range.Inclusive = 1 to(10, 2)
    println(seqs)

    println("---------- foreach ----------")
    // 循环逻辑, 业务逻辑
    // i % 2 == 0 相当于 Java 中的 break
    for (i <- seqs if (i % 2 == 0)) {
      println(i)
    }

    println("---------- 嵌套循环 ----------")
    var num = 0
    // 嵌套循环的简写方式
    // 防止 CPU 空转加入 i <= j 判断
    for (i <- 1 to 9; j <- 1 to 9 if (j <= i)) {
      num += 1
      if (j <= i) print(s"$i * $j = ${i * j}\t")
      if (j == i) println()
    }
    println(num)

    val seqss: IndexedSeq[Int] = for (i <- 1 to 10) yield {
      // 既有循环的变量
      var x = 8
      // 又有内部的函数的变量
      // 相加的和会被回收
      i + x
    }
//    println(seqss)
    for (i <- seqss) {
      println(i)
    }
  }

}
