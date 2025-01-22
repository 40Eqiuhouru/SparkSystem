// Generated from D:/ideaProject/bigdata/bigdata-spark/src/main/scala/com/syndra/bigdata/antlrv4/Test.g4 by ANTLR 4.13.2
package com.syndra.bigdata.sql.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link TestParser}.
 */
public interface TestListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link TestParser#tsinit}.
	 * @param ctx the parse tree
	 */
	void enterTsinit(TestParser.TsinitContext ctx);
	/**
	 * Exit a parse tree produced by {@link TestParser#tsinit}.
	 * @param ctx the parse tree
	 */
	void exitTsinit(TestParser.TsinitContext ctx);
	/**
	 * Enter a parse tree produced by {@link TestParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(TestParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link TestParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(TestParser.ValueContext ctx);
}