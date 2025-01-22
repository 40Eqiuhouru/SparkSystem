package com.syndra.bigdata.scala

import com.sun.org.apache.xalan.internal.lib.ExsltDatetime.date

import java.util
import java.util.Date

/**
 * 流程控制, 高级函数
 */
object Lesson01_Functions {

  // 成员方法
  def other(): Unit = {
    println("Hello Scala")
  }

  def main(args: Array[String]): Unit = {
    // 方法 函数
    println("-------- 1.Basic --------")

    // 返回值, 参数, 函数体
    def fun01() {
      println("Hello World")
    }

    fun01()
    var x = 3
    val y = fun01()
    println(y)

    // 有返回值
    // 如果写了准确的关键字, 函数方法后必须人工给出类型, 有 return 必须要给出返回类型
    def fun02(): util.LinkedList[String] = {
      // 特征 : Scala 和 Java 可以混编
      new util.LinkedList[String]()
    }

    // params : must give type, in function must is val, but only can be val
    // class name after add structure, is var or val
    // 推断系统, 有一个地方它绝对推断不出, 就是参数列表, 因为写的东西别人会调, 如果不规定它必须传什么类型
    // 就有可能传任何类型, 里边可能做数学计算, 但是它给你传了一个字符串就懵了, 所以一定会有一个规约, 只能传约束的类型
    def fun03(a: Int): Unit = {
      println(a)
    }

    fun03(12)

    println("-------- 2.Recursive Functions --------")

    // 递归函数先写触底 : 触发什么报错 ? 栈溢出
    def fun04(num: Int): Int = {
      if (num == 1) {
        num
      } else {
        num * fun04(num - 1)
      }
    }

    val i: Int = fun04(7)
    println(i)

    println("-------- 3.Default Value Functions --------")

    def fun05(a: Int = 8, b: String = "syndra"): Unit = {
      println(s"$a\t$b")
    }

    fun05()
    fun05(1, "WangZX")
    fun05(23)
    fun05(b = "HanYing")

    println("-------- 4.Anonymous Functions --------")

    // 函数是第一类值
    // function
    // 1.签名 : (Int, Int) => Int (参数类型列表) => (返回值类型)
    // 2.匿名函数 : (a: Int, b: Int) => { a + b } (参数实现列表) => 函数体
    var e: Int = 3

    var f: (Int, Int) => Int = (a: Int, b: Int) => {
      a + b
    }
    val w: Int = f(3, 4)
    println(w)

    println("-------- 5.Nested Functions --------")

    def fun06(a: String): Unit = {
      def fun05(): Unit = {
        println(a)
      }

      fun05()
    }

    fun06("Syndra")

    println("-------- 6.Partial Application Functions --------")

    def fun07(data: Date, tp: String, msg: String): Unit = {
      println(s"$date\t$tp\t$msg")
    }

    fun07(new Date(), "info", "SUCCESS")

    var info = fun07(_: Date, "info", _: String)
    var error = fun07(_: Date, "error", _: String)
    info(new Date(), "RESUCCESS")
    error(new Date(), "FAILURE")

    println("-------- 7.Variable Functions --------")

    def fun08(a: Int*): Unit = {
      for (e <- a) {
        println(e)
      }
      // 扩展知识点
      //      def foreach[U](f: A => U): Unit
      a.foreach((x: Int) => {
        println(x)
      })
      a.foreach(println(_))
    }

    fun08(2)
    fun08(2, 3, 4)

    println("-------- 8.Higher-Level Functions --------")

    // 函数作为参数, 作为返回值
    // 函数作为参数
    def computer(a: Int, b: Int, f: (Int, Int) => Int): Unit = {
      val res: Int = f(a, b)
      println(res)
    }

    computer(40, 35, (x: Int, y: Int) => {
      x + y
    })
    computer(40, 35, (x: Int, y: Int) => {
      x * y
    })
    computer(40, 35, _ * _)

    // 函数作为返回值
    def factory(i: String): (Int, Int) => Int = {
      def plus(x: Int, y: Int): Int = {
        x + y
      }

      if (i.equals("+")) {
        plus
      } else {
        (x: Int, y: Int) => {
          x * y
        }
      }
    }

    computer(29, 23, factory("+"))

    println("-------- 9.柯里化 --------")

    def fun09(a: Int)(b: Int)(c: String): Unit = {
      println(s"$a\t$b\t$c")
    }

    fun09(3)(8)("Syndra")

    def fun10(a: Any*)(b: String*): Unit = {
      a.foreach(println)
      b.foreach(println)
    }

    fun10(1, 2, 3)("Syndra", "WangZX")

    println("-------- 10.Method --------")

    // 方法不想执行, 赋值给一个引用 : 方法名 + 空格 + 下划线
    val funcc = other
    println(funcc)
    val func = other _
    func()

    // 语法 -> 编译器 -> 字节码 <- JVM 规则
    // 编译器 : 衔接人和机器的过程
    // Java 中 + : 关键字
    // Scala 中 + : 方法函数
    // Scala 语法中, 没有基本类型, 所以你写一个数字 3 编辑器/语法, 其实就是把 3 看待成 Int 这个对象
//    3 + 2
//    3.+ (2)
    3: Int

    // 编译型 C : 贼快.
    // 解释型 Python : 贼慢.(只有在数据量特别大的时候, 这种解释型语言的弊端会更大)
    // Java 其实不值钱, 最值钱的是 JVM.
    // Java 本身其实也算解释型, 但它需要一个编译的过程, 在这个过程中会把比如说, 同样是一个 99, 在 Python 中是字符串, 在 Java 中是一个四字节的 int, 二进制的东西,
    // 编译最重要也是厉害的是类型, 其中所有数据是有类型的, 二进制放在内存中不需要一个中转翻译的过程, 拿过来直接用.
    // Java 虽然是解释型, 但它会在解释型这个队列中会快一点, 不需要解释也不需要翻译, 这块比 Python 快.
    // JVM 为什么值钱 ?
    // 因为 JVM 是 C 写的, 字节码(二进制) > JVM(堆里堆外) < kernel(mmap, sendfile)
  }
}
