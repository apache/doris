// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.ExpressionContext;
import org.apache.doris.nereids.DorisParser.ExpressionWithEofContext;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.parser.CaseInsensitiveStream;
import org.apache.doris.nereids.parser.ParseErrorListener;
import org.apache.doris.nereids.parser.PostProcessor;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;

/**
 * Standalone facade for Doris SQL syntax parsing. Produces an ANTLR parse tree
 * (CST) without any semantic analysis. Thread-safe (stateless).
 */
public final class DorisSqlParser {
    private static final ParseErrorListener PARSE_ERROR_LISTENER = new ParseErrorListener();
    private static final PostProcessor POST_PROCESSOR = new PostProcessor();

    private final boolean noBackslashEscapes;
    private final boolean ansiSqlSyntax;

    public DorisSqlParser() {
        this(false, false);
    }

    /**
     * @param noBackslashEscapes maps to MySQL's NO_BACKSLASH_ESCAPES sql mode; controls how
     *                           the lexer treats backslashes inside string literals.
     * @param ansiSqlSyntax      enables ANSI behavior in a few corners of the grammar
     *                           (matches GlobalVariable.enable_ansi_query_organization_behavior).
     */
    public DorisSqlParser(boolean noBackslashEscapes, boolean ansiSqlSyntax) {
        this.noBackslashEscapes = noBackslashEscapes;
        this.ansiSqlSyntax = ansiSqlSyntax;
    }

    /** Parse a single SQL statement. */
    public SingleStatementContext parseStatement(String sql) {
        return (SingleStatementContext) toAst(sql, DorisParser::singleStatement);
    }

    /** Parse one or more SQL statements separated by semicolons. */
    public MultiStatementsContext parseStatements(String sql) {
        return (MultiStatementsContext) toAst(sql, DorisParser::multiStatements);
    }

    /** Parse a single SQL expression (no trailing tokens allowed). */
    public ExpressionContext parseExpression(String sql) {
        ExpressionWithEofContext ctx = (ExpressionWithEofContext) toAst(sql, DorisParser::expressionWithEof);
        return ctx.expression();
    }

    /** Build a freshly configured lexer for advanced callers that want to walk tokens directly. */
    public DorisLexer newLexer(String sql) {
        DorisLexer lexer = new DorisLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.isNoBackslashEscapes = noBackslashEscapes;
        return lexer;
    }

    /** Build a freshly configured parser bound to {@code lexer}. Caller owns the returned parser. */
    public DorisParser newParser(DorisLexer lexer) {
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();
        return configure(new DorisParser(tokenStream));
    }

    private ParserRuleContext toAst(String sql, Function<DorisParser, ParserRuleContext> parseFunction) {
        CommonTokenStream tokenStream = tokenize(sql);
        DorisParser parser = configure(new DorisParser(tokenStream));
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            return parseFunction.apply(parser);
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0);
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            return parseFunction.apply(parser);
        }
    }

    private CommonTokenStream tokenize(String sql) {
        DorisLexer lexer = newLexer(sql);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();
        return tokenStream;
    }

    private DorisParser configure(DorisParser parser) {
        parser.ansiSQLSyntax = ansiSqlSyntax;
        parser.addParseListener(POST_PROCESSOR);
        parser.removeErrorListeners();
        parser.addErrorListener(PARSE_ERROR_LISTENER);
        return parser;
    }
}
