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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * Uses Doris's own lexer and parser to require exactly one SQL statement per HTTP request.
 * Parsing under both string-escape modes keeps the boundary safe even when the persistent
 * JDBC session changed sql_mode in an earlier request.
 */
public final class SingleStatementValidator {
    private SingleStatementValidator() {
    }

    public static String requireSingleStatement(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
        }

        requireSingleStatement(sql, false);
        requireSingleStatement(sql, true);
        return sql.trim();
    }

    private static void requireSingleStatement(String sql, boolean noBackslashEscapes) {
        try {
            DorisLexer lexer = new DorisLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
            lexer.isNoBackslashEscapes = noBackslashEscapes;
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            MultiStatementsContext parsed = (MultiStatementsContext) NereidsParser.toAst(
                    tokens, DorisParser::multiStatements);
            if (parsed.statement().size() != 1) {
                throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
            }
        } catch (WebSqlException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT, exception);
        }
    }
}
