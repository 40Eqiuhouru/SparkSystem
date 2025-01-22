// Generated from D:/ideaProject/bigdata/bigdata-spark/src/main/scala/com/syndra/bigdata/antlrv4/Test.g4 by ANTLR 4.13.2
package com.syndra.bigdata.sql.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link TestParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface TestVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link TestParser#tsinit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitTsinit(TestParser.TsinitContext ctx);
	/**
	 * Visit a parse tree produced by {@link TestParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(TestParser.ValueContext ctx);
}