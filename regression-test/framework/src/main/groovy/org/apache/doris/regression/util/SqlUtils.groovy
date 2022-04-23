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

package org.apache.doris.regression.util

import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.apache.doris.regression.parser.SqlSeparatorSyntaxLexer
import org.apache.doris.regression.parser.SqlSeparatorSyntaxParser

import java.util.function.Function

class SqlUtils {
    private static int invocationCount

    static List<String> splitAndGetNonEmptySql(String sql) {
        SqlSeparatorSyntaxParser.StatementsContext statementsContext = parse(sql, { it.statements() } )
        List<String> sqls = statementsContext.statement().collect { statementContext ->
            boolean isEmptySql = statementContext.children.every {
                // comment || whitespace
                it instanceof SqlSeparatorSyntaxParser.CommentContext || it instanceof SqlSeparatorSyntaxParser.WsContext
            }
            isEmptySql ? null : statementContext.text
        }.findAll { it != null }
        return sqls
    }

    static <T> T parse(String sql, Function<SqlSeparatorSyntaxParser, T> parserFunction) {
        def lexer = new SqlSeparatorSyntaxLexer(CharStreams.fromString(sql))
        def tokenStream = new CommonTokenStream(lexer)
        def parser = new SqlSeparatorSyntaxParser(tokenStream)
        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL)
            tree = parserFunction.apply(parser);
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.reset() // rewind input stream
            parser.reset()

            parser.getInterpreter().setPredictionMode(PredictionMode.LL)
            tree = parserFunction.apply(parser)
        } finally {
            clearDFAIfNecessary(lexer, parser)
        }
        return tree
    }

    private static synchronized void clearDFAIfNecessary(SqlSeparatorSyntaxLexer lexer, SqlSeparatorSyntaxParser parser) {
        if ((++invocationCount) >= 1000) {
            lexer.getInterpreter().clearDFA()
            parser.getInterpreter().clearDFA()
            invocationCount = 0;
        }
    }
}
