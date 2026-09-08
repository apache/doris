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

import org.antlr.v4.runtime.Token;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class DorisLexerLeanTokenModeTest {
    private static final String SQL = "SELECT /* ordinary */ /*+ ORDERED */ a -- tail\r\nFROM t";

    @Test
    void keepsFullTokenStreamByDefault() {
        Assertions.assertEquals(Arrays.asList(
                        "SELECT|SELECT|0|0|5|1|0",
                        "WS| |1|6|6|1|6",
                        "BRACKETED_COMMENT|/* ordinary */|2|7|20|1|7",
                        "WS| |1|21|21|1|21",
                        "BRACKETED_COMMENT|/*+ ORDERED */|2|22|35|1|22",
                        "WS| |1|36|36|1|36",
                        "IDENTIFIER|a|0|37|37|1|37",
                        "WS| |1|38|38|1|38",
                        "SIMPLE_COMMENT|-- tail\\r\\n|1|39|47|1|39",
                        "FROM|FROM|0|48|51|2|0",
                        "WS| |1|52|52|2|4",
                        "IDENTIFIER|t|0|53|53|2|5",
                        "EOF|<EOF>|0|54|53|2|6"),
                snapshot(SQL, false, false));
    }

    @Test
    void skipsOnlyWhitespaceAndSimpleComments() {
        List<String> inputs = Arrays.asList(
                SQL,
                "SELECT 'a\\'b' -- comment\n, \"x\\\"y\"",
                "SELECT 中文😀\tFROM 表 -- 尾注释",
                "-- first\n/* regular */ /*+ SET_VAR(query_timeout=1) */ SELECT 1");

        for (boolean noBackslashEscapes : new boolean[] {false, true}) {
            for (String sql : inputs) {
                List<Token> fullTokens = lex(sql, noBackslashEscapes, false);
                List<Token> expectedLeanTokens = fullTokens.stream()
                        .filter(token -> token.getType() != DorisLexer.WS
                                && token.getType() != DorisLexer.SIMPLE_COMMENT)
                        .collect(Collectors.toList());
                List<Token> leanTokens = lex(sql, noBackslashEscapes, true);

                Assertions.assertEquals(snapshot(expectedLeanTokens), snapshot(leanTokens));
                Assertions.assertTrue(leanTokens.stream()
                        .filter(token -> token.getType() == DorisLexer.BRACKETED_COMMENT)
                        .allMatch(token -> token.getChannel() == 2));
            }
        }
    }

    @Test
    void lexesDeterministicallyWithSharedStaticDfa() throws Exception {
        List<List<String>> expected = new ArrayList<>();
        for (boolean noBackslashEscapes : new boolean[] {false, true}) {
            for (boolean leanTokenMode : new boolean[] {false, true}) {
                expected.add(snapshot(SQL, noBackslashEscapes, leanTokenMode));
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int thread = 0; thread < 8; thread++) {
                tasks.add(() -> {
                    for (int repetition = 0; repetition < 100; repetition++) {
                        int caseIndex = 0;
                        for (boolean noBackslashEscapes : new boolean[] {false, true}) {
                            for (boolean leanTokenMode : new boolean[] {false, true}) {
                                Assertions.assertEquals(expected.get(caseIndex++),
                                        snapshot(SQL, noBackslashEscapes, leanTokenMode));
                            }
                        }
                    }
                    return null;
                });
            }
            for (Future<Void> result : executor.invokeAll(tasks)) {
                result.get();
            }
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static List<String> snapshot(String sql, boolean noBackslashEscapes, boolean leanTokenMode) {
        return snapshot(lex(sql, noBackslashEscapes, leanTokenMode));
    }

    private static List<String> snapshot(List<Token> tokens) {
        return tokens.stream().map(token -> {
            String type = DorisLexer.VOCABULARY.getSymbolicName(token.getType());
            String text = token.getText().replace("\r", "\\r").replace("\n", "\\n");
            return type + "|" + text + "|" + token.getChannel() + "|"
                    + token.getStartIndex() + "|" + token.getStopIndex() + "|"
                    + token.getLine() + "|" + token.getCharPositionInLine();
        }).collect(Collectors.toList());
    }

    private static List<Token> lex(String sql, boolean noBackslashEscapes, boolean leanTokenMode) {
        DorisLexer lexer = new DorisSqlParser(noBackslashEscapes, false).newLexer(sql);
        lexer.isLeanTokenMode = leanTokenMode;
        List<Token> tokens = new ArrayList<>();
        Token token;
        do {
            token = lexer.nextToken();
            tokens.add(token);
        } while (token.getType() != Token.EOF);
        return tokens;
    }
}
