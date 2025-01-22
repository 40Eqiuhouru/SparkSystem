package com.syndra.bigdata.scala

/**
 * Scala Trait
 */
trait Girl {
  def eat(): Unit = {
    println("eat JJ")
  }
}

trait YuJie {
  def mouth(): Unit = {
    println("mouth J8")
  }

  def QiuOu(): Unit
}

class Person(name: String) extends Girl with YuJie {
  def fucked(): Unit = {
    println(s"$name wants to be fucked")
  }

  override def QiuOu(): Unit = {
    println("YuJie QiuOu")
  }
}

object Lesson03_Trait {
  def main(args: Array[String]): Unit = {
    // 父类引用指向子类实现, 父类只能调用父类中的方法, 属于 Java 的基本语法
//    val p:Girl = new Person("Syndra")
    val p = new Person("Syndra")
    p.eat()
    p.mouth()
    p.fucked()
    p.QiuOu()
  }
}
