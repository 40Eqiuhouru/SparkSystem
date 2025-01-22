package com.syndra.bigdata.scala

/**
 * Scala Match(匹配)
 * <br>
 * 官方说, match 联想 Java 中的 switch.
 */
object Lesson05_match {
  def main(args: Array[String]): Unit = {
    val tup: (Double, Int, String, Boolean, Char) = (1.0, 29, "Syndra", false, 'W')
    // tup 自身没有 foreach 这些通用的方法
    // Any 是顶级父级类型, 它里面叠加的每个元素时, 等于父类应用指向子类的那个类型
    // 然后其中的每一个元素, 未来迭代时, 还是具备它那个具体类型的特征.
    // 不要认为出现了 Any 就等于其他的类型就抹掉了, 其实并没有抹掉
    // 先拿到了 tuple 的迭代器, 未来可以通过 map 迭代出里边的每一个元素
    // 然后依次扔给 x
    val iter: Iterator[Any] = tup.productIterator

    val res: Iterator[Unit] = iter.map((x) => {
      // 1.match 的语法规则
      // 每次要匹配且只能匹配其中一条
      x match {
        // 首先判断 x 的类型是不是 Int, 如果是, x 的值赋给 o
        case 1 => println(s"$x...is 1")
        case 29 => println(s"$x...is 29")
        case false => println(s"$x...is false")
        case w: Int if w > 50 => println(s"$w...is > 50")
        case _ => println("YuJie wants to be fucked")
      }
    })
    while (res.hasNext) {
      // 从 tuple 中拿到了一个迭代器
      // 迭代器在 res 时并没有发生运算, 得到了一个 res
      // res.hasNext 判定兜到数据集上, 然后调 next(), 在第一次循环调 next() 时,
      // 向前走迭代器传到开始拿了 1.0, 往回走嵌套到函数这, 匹配上了 1, 这句话被打印, 所以 1.0...is 1 并不是 res 中的东西
      // 而是 println 打印出来的, 且 println 函数有返回值 Unit, 注意在 Scala 中没有返回值也是有值的意思, 只不过是 Unit,
      // Unit 只是一个 (), 所以当 1.0 进到 map 中被执行打印完后, map 是把这个数据处理完后, 它的返回值也就是这个函数的返回值会继续传递给 res,
      // 所以此时打印完后, 那个 Unit 类型就被返回了, 返回给 res 后, 在 println(s"$x...is 1") 中打印的 (), 到此是第一圈循环.
      println(res.next())
    }
  }
}
