package com.syndra.bigdfata.scala

import java.util

/**
 * Scala implicit 隐式转换
 */
object Lesson07_implicit {
  def main(args: Array[String]): Unit = {
    val listLinked = new util.LinkedList[Int]()
    listLinked.add(1)
    listLinked.add(2)
    listLinked.add(3)

    val listArray = new util.ArrayList[Int]()
    listArray.add(1)
    listArray.add(2)
    listArray.add(3)
    //    list.foreach(println) // 3 个东西 : list 数据集, foreach 遍历行为, println 处理函数

    //    // 把 list, 函数 传入, 这个 foreach 完成迭代遍历的行为
    //    def foreach[T](list: util.LinkedList[T], f: (T) => Unit): Unit = {
    //      val iter: util.Iterator[T] = list.iterator()
    //      while (iter.hasNext) {
    //        f(iter.next())
    //      }
    //    }
    //
    //    foreach(list, println)

    //    val xx = new XXX(list)
    //    xx.foreach(println)

    // 隐式转换 : 隐式转换方法
    implicit def what[T](list: util.LinkedList[T]): XXX[T] = {
      val iter: util.Iterator[T] = list.iterator()
      new XXX(iter)
    }

    implicit def whatWhat[T](list: java.util.ArrayList[T]): XXX[T] = {
      val iter: util.Iterator[T] = list.iterator()
      new XXX(iter)
    }

    listLinked.foreach(println)

    listArray.foreach(println)

    // 隐式转换类
    // Spark 中有个东西叫做 RDD, 一个弹性的分布式数据集, 这个 类/对象 有 N 个方法
    // 未来想扩展方法, 就需要定义一些隐式转换方法可以辨别 RDD 的数据, 然后就多了些方法
    // 可以无限扩展某一个类的能力而不修改源码的情况下, 实现功能的增强.
    //    implicit class XXX[T](list: util.LinkedList[T]) {
    //      def foreach[S](f: (T) => Unit): Unit = {
    //        val iter: util.Iterator[T] = list.iterator()
    //        while (iter.hasNext) {
    //          f(iter.next())
    //        }
    //      }
    //    }

    //    list.foreach(println) // 必须先承认一件事 : list 有 foreach 方法吗 ? 肯定是没有的 ! 在 Java 里这么写肯定报错.
    // 这些代码最终交给的是 Scala 的编译器
    // 1.Scala 编译器发现 list.foreach(println) 有 BUG
    // 2. 寻找有没有 implicit 定义的方法, 且方法的参数正好是 list 的类型
    // 3.编译期 : 完成曾经人类
    // new XXX(list)
    // xx.foreach(println)
    // 4.编译器帮你把代码改写了.

    // 自定义变量, 变量名字无所谓, 尤其它的值类型是 String
    implicit val shit: String = "AiSiDS"
    //    implicit val daemon: String = "HeJiang"
    implicit val Wife: Int = 27

    // 参数未出现定义 name
    // implicit 这个关键字, 只要出现在参数列表里, 其中有多少个参数, 在调用时
    // 要么明确给值, 要么想使用隐式写法的话, 就必须在外界定义它的那个值的位置
    // 这个关键字会造成, 参数列表中的所有参数都是隐式查找.
    // 如果就是需要有些参数动态传递, 有些参数隐式查找, 怎么解决 ?
    // 柯里化
    // 而且带隐式的一定要放在后面, 而且 implicit 可写可不写
    def so(age: Int)(implicit name: String): Unit = {
      println(name + " " + age)
    }

    //    so("HanY")
    so(27)("阿玛")
    // 如果编译器看到这行, 它发现语法不通过, 因为 so 的函数中是必须有参数的, 你为什么不写 ?
    // 在 Java 中一定会报错
    // 但 Scala 中会认为, 你这报错了, 不急, 我先找一找别人有没有注册过它定义的这个类型有没有找到,
    // 找到了就把它的值取出来代替你传参
    so(27)
  }
}

// 包装
class XXX[T](list: util.Iterator[T]) {
  def foreach(f: (T) => Unit): Unit = {
    while (list.hasNext) {
      f(list.next())
    }
  }
}

//class XXX[T](list: util.LinkedList[T]) {
//  def foreach[S](f: (T) => Unit): Unit = {
//    val iter: util.Iterator[T] = list.iterator()
//    while (iter.hasNext) {
//      f(iter.next())
//    }
//  }
//}