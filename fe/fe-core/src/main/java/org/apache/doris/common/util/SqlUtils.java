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

package org.apache.doris.common.util;

import org.apache.doris.parser.DorisSqlSeparatorLexer;
import org.apache.doris.parser.DorisSqlSeparatorParser;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.StringWriter;
import java.util.Collections;
import java.util.List;

public class SqlUtils {
    public static String escapeUnquote(String ident) {
        return ident.replaceAll("``", "`");
    }

    public static String getIdentSql(String ident) {
        StringBuilder sb = new StringBuilder();
        sb.append('`');
        for (char ch : ident.toCharArray()) {
            if (ch == '`') {
                sb.append("``");
            } else {
                sb.append(ch);
            }
        }
        sb.append('`');
        return sb.toString();
    }

    public static String escapeQuota(String str) {
        if (Strings.isNullOrEmpty(str)) {
            return str;
        }
        return str.replaceAll("\"", "\\\\\"");
    }

    /**
     * add escape characters for string
     * @param str
     * @return
     */
    public static String addEscapeCharacters(String str) {
        StringWriter writer = new StringWriter();
        int strLen = str.length();
        for (int i = 0; i < strLen; ++i) {
            char c = str.charAt(i);
            switch (c) {
                case '\n':
                    writer.append("\\n");
                    break;
                case '\t':
                    writer.append("\\t");
                    break;
                case '\r':
                    writer.append("\\r");
                    break;
                case '\b':
                    writer.append("\\b");
                    break;
                case '\0':
                    writer.append("\\0");
                    break;
                case '\032':
                    writer.append("\\Z");
                    break;
                case '\'':
                    writer.append("\\'");
                    break;
                case '\"':
                    writer.append("\\\"");
                    break;

                default:
                    writer.append(c);
                    break;
            }
        }

        return writer.toString();
    }

    public static List<String> splitMultiStmts(String sql) {
        DorisSqlSeparatorLexer lexer = new DorisSqlSeparatorLexer(CharStreams.fromString(sql));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        DorisSqlSeparatorParser parser = new DorisSqlSeparatorParser(tokenStream);
        ParserRuleContext tree;

        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.statements();
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0);
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.statement();
        }

        DorisSqlSeparatorParser.StatementsContext stmt = (DorisSqlSeparatorParser.StatementsContext) tree;
        List<String> singleStmtList = Lists.newArrayList();
        for (DorisSqlSeparatorParser.StatementContext statementContext : stmt.statement()) {
            if (!isEmptySql(statementContext)) {
                singleStmtList.add(statementContext.getText());
            }
        }

        return Collections.unmodifiableList(singleStmtList);
    }

    private static boolean isEmptySql(DorisSqlSeparatorParser.StatementContext statementContext) {
        if (statementContext.children == null) {
            return true;
        }
        for (ParseTree child : statementContext.children) {
            if (!(child instanceof DorisSqlSeparatorParser.CommentContext)
                    && !(child instanceof DorisSqlSeparatorParser.WsContext)) {
                return false;
            }
        }
        return true;
    }
}
