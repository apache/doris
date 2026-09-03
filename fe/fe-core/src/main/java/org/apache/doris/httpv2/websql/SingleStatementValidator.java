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

package org.apache.doris.httpv2.websql;

import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.MultiStatementsContext;
import org.apache.doris.nereids.parser.CaseInsensitiveStream;
import org.apache.doris.nereids.parser.NereidsParser;

import org.antlr.v4.runtime.CommonTokenStream;

/**
 * Uses Doris's own lexer and parser to require exactly one SQL statement per HTTP request.
 * The caller supplies the persistent JDBC session's active string-escape mode so valid SQL is
 * judged by the same lexer rules that Doris will use to execute it.
 */
public final class SingleStatementValidator {
    private SingleStatementValidator() {
    }

    public static String requireSingleStatement(String sql) {
        return requireSingleStatement(sql, false);
    }

    public static String requireSingleStatement(String sql, boolean noBackslashEscapes) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
        }

        try {
            DorisLexer lexer = new DorisLexer(CaseInsensitiveStream.fromString(sql));
            lexer.isNoBackslashEscapes = noBackslashEscapes;
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MultiStatementsContext parsed = (MultiStatementsContext) NereidsParser.toAst(
                    tokens, DorisParser::multiStatements);
            if (parsed.statement().size() != 1) {
                throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
            }
            return sql.trim();
        } catch (WebSqlException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT, exception);
        }
    }
}
