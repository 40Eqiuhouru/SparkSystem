package com.syndra.bigdfata.scala

/**
 * Scala Test Class
 * <p>约等于 object 就是一个静态的单例对象;
 * <p>单例 new Scala 的编译器很人性化, 让你人少写了很多 code (很重要);
 */
object TestScala {
  //  val name = "Syndra"

  //  private val t1: Test01 = new Test01()

  println("before")

  private val t1 = new TestScala("80")

  private val name = "object:WangZX"

  def main(args: Array[String]): Unit = {
    println("御姐女朋友")
    t1.printMsg()
  }

  println("after")
}

/**
 * class 中裸露的 code 默认是构造中的, 有默认构造;
 * <p>只需要关注个性化构造;
 * <p>类名构造器中的参数是类的成员属性, 且默认值是 val 类型, 且默认是 private;
 * <p>只有在类名构造器中的参数有可以设置成 var, 其他方法函数中的参数都是 val 类型, 且不允许设置成 var 类型;
 */
class TestScala(sex: String) {
  //  var a: Int = 40;

  var name = "class:WangZX"

  //  def this(rename: String) {
  //    // 如果是个性化构造, 就必须调用默认构造
  //    this()
  //    name = rename
  //  }

  def this(rename: Int) {
    this("abc")
  }

  // 可以在一个字符串双引号前边写一个 s, 里面写上明文的字符串以及 $符号 可以取一个变量的值
  //  println(s"test...before$a")

  def printMsg(): Unit = {
    // TestScala 可以被当作工具类使用, 且不能被 new 实例化
    println(s"test...${TestScala.name}")
  }
  // 也可以在一个字符串中使用 ${} 包裹的表达式, 然后在外部调用时传入变量的值
  //  println(s"test...after${a + 4}")
}