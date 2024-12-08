package com.syndra.bigdata.scala

/**
 * Scala PartialFunction(偏函数)
 * <br>
 * 类型固定 {@code PartialFunction[A, B]}, 注意后边要写泛型, 这个泛型中有两个泛型,
 * 且这个 xxx 函数未来只能接收一个元素, 这个元素会在它的函数体中去判定其中的一种情况,
 * 然后输出.
 * <br>
 * 这就是所谓的样例类那个偏函数的一种使用方式.
 */
object Lesson06_PartialFunction {
  def main(args: Array[String]): Unit = {
    def xxx: PartialFunction[Any, String] = {
      case "hello" => "val is hello"
      case x: Int => s"$x...is int"
      case _ => "none"
    }

    println(xxx(44))
    println(xxx("hello"))
    println(xxx("fucked"))
  }
}
