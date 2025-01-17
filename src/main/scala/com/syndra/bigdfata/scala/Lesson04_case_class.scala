package com.syndra.bigdfata.scala

/**
 * Scala Case Class
 * <br>
 * 什么是样例类 ? 适用场景 ?
 * <br>
 * 我可能需要在一台机器准备一批数据封装成对象, 序列化后,
 * 交由另外一台机器中的一个进程, 它收到这批数据后, 它要判定这批数据代表什么意思,
 * 也就是说它是哪个对象的哪个类型, 这个判定的时候, 如果是对象很难比较是不是类型,
 * 更重要的是可能大家发的都是一个类型, 但是其中的内容可能有不一样的时候, 要么挨个字符串比较,
 * 要么重写 {@code toString()} 等等一系列的 {@code equals()}, 要么使用最简单的方式,
 * 直接定义一套 {@code case.class} 把所有 {@link message} 这种数据对象都封装一下.
 * <br>
 * 多数情况下, 作用在定义信封中的信人那种消息时, 会使用这个 样例类.
 */
case class Dog(name: String, age: Int) {
}

object Lesson04_case_class {
  def main(args: Array[String]): Unit = {
    // 样例类可以省略 new 关键字, 直接创建对象
    val dog1 = Dog("Syndra", 29)
    val dog2 = Dog("Syndra", 29)
    println(dog1.equals(dog2))
    println(dog1 == dog2)
  }
}
