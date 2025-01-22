package com.syndra.bigdata.sql.parser


import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream}

/**
 * test AntlrV4
 */
object TestAntlr {
  def main(args: Array[String]): Unit = {
    // 分词
    val lexer = new TestLexer(new ANTLRInputStream("{1,{2,3},4}"))
    // 获得 token
    val tokens = new CommonTokenStream(lexer)
    // 解析
    val parser = new TestParser(tokens)

    // 解析树
    val tree: ParseTree = parser.tsinit()

    println(tree.toStringTree(parser))
  }
}
