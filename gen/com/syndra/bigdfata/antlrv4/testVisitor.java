// Generated from D:/ideaProject/bigdata/bigdata-spark/src/main/scala/com/syndra/bigdfata/antlrv4/Test.g4 by ANTLR 4.13.2
package com.syndra.bigdata.antlrv4;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link testParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface testVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link testParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(testParser.ValueContext ctx);
}