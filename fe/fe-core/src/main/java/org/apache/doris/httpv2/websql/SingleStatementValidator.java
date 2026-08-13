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

public final class SingleStatementValidator {
    private SingleStatementValidator() {
    }

    public static String requireSingleStatement(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
        }

        int terminatingSemicolon = -1;
        State state = State.NORMAL;
        for (int i = 0; i < sql.length(); i++) {
            char current = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            switch (state) {
                case NORMAL:
                    if (current == '\'') {
                        state = State.SINGLE_QUOTE;
                    } else if (current == '"') {
                        state = State.DOUBLE_QUOTE;
                    } else if (current == '`') {
                        state = State.BACKTICK;
                    } else if (current == '-' && next == '-') {
                        state = State.LINE_COMMENT;
                        i++;
                    } else if (current == '#') {
                        state = State.LINE_COMMENT;
                    } else if (current == '/' && next == '*') {
                        state = State.BLOCK_COMMENT;
                        i++;
                    } else if (current == ';') {
                        if (terminatingSemicolon >= 0) {
                            throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
                        }
                        terminatingSemicolon = i;
                    } else if (terminatingSemicolon >= 0 && !Character.isWhitespace(current)) {
                        throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
                    }
                    break;
                case SINGLE_QUOTE:
                    if (current == '\\') {
                        i++;
                    } else if (current == '\'' && next == '\'') {
                        i++;
                    } else if (current == '\'') {
                        state = State.NORMAL;
                    }
                    break;
                case DOUBLE_QUOTE:
                    if (current == '\\') {
                        i++;
                    } else if (current == '"' && next == '"') {
                        i++;
                    } else if (current == '"') {
                        state = State.NORMAL;
                    }
                    break;
                case BACKTICK:
                    if (current == '`' && next == '`') {
                        i++;
                    } else if (current == '`') {
                        state = State.NORMAL;
                    }
                    break;
                case LINE_COMMENT:
                    if (current == '\n' || current == '\r') {
                        state = State.NORMAL;
                    }
                    break;
                case BLOCK_COMMENT:
                    if (current == '*' && next == '/') {
                        state = State.NORMAL;
                        i++;
                    }
                    break;
                default:
                    throw new IllegalStateException("Unknown SQL scanner state");
            }
        }
        if (state == State.SINGLE_QUOTE || state == State.DOUBLE_QUOTE
                || state == State.BACKTICK || state == State.BLOCK_COMMENT) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
        }

        String statement = terminatingSemicolon >= 0 ? sql.substring(0, terminatingSemicolon) : sql;
        if (statement.trim().isEmpty()) {
            throw new WebSqlException(WebSqlError.INVALID_STATEMENT);
        }
        return statement.trim();
    }

    private enum State {
        NORMAL,
        SINGLE_QUOTE,
        DOUBLE_QUOTE,
        BACKTICK,
        LINE_COMMENT,
        BLOCK_COMMENT
    }
}
