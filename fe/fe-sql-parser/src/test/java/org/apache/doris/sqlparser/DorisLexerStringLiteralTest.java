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
import org.apache.doris.nereids.parser.CaseInsensitiveStream;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RuleContext;
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

class DorisLexerStringLiteralTest {
    @Test
    void lexesValidStringsAsOneTokenInBothSqlModes() {
        List<String> literals = Arrays.asList(
                singleQuoted(""),
                doubleQuoted(""),
                singleQuoted("plain ASCII"),
                doubleQuoted("plain ASCII"),
                singleQuoted("中文😀"),
                doubleQuoted("中文😀"),
                singleQuoted("it''s"),
                doubleQuoted("a\"\"b"),
                singleQuoted("line\nbreak"),
                singleQuoted("a\\b"),
                singleQuoted("a\\\\"));

        for (boolean noBackslashEscapes : new boolean[] {false, true}) {
            for (String literal : literals) {
                assertSingleStringToken(literal, noBackslashEscapes);
            }
        }

        assertSingleStringToken(singleQuoted("a\\'b"), false);
        assertSingleStringToken(doubleQuoted("a\\\"b"), false);
        assertSingleStringToken(singleQuoted("a\\"), true);
        assertSingleStringToken(doubleQuoted("a\\"), true);
    }

    @Test
    void preservesModeSensitiveTokenBoundariesAndPositions() {
        String escapedQuote = singleQuoted("a\\'b");
        Assertions.assertEquals(Arrays.asList(
                        "STRING_LITERAL|'a\\'b'|0|0|5|1|0",
                        "EOF|<EOF>|0|6|5|1|6"),
                snapshot(escapedQuote, false));
        Assertions.assertEquals(Arrays.asList(
                        "STRING_LITERAL|'a\\'|0|0|3|1|0",
                        "IDENTIFIER|b|0|4|4|1|4",
                        "UNRECOGNIZED|'|0|5|5|1|5",
                        "EOF|<EOF>|0|6|5|1|6"),
                snapshot(escapedQuote, true));

        String trailingBackslash = singleQuoted("a\\");
        Assertions.assertEquals(Arrays.asList(
                        "UNRECOGNIZED|'|0|0|0|1|0",
                        "IDENTIFIER|a|0|1|1|1|1",
                        "UNRECOGNIZED|\\|0|2|2|1|2",
                        "UNRECOGNIZED|'|0|3|3|1|3",
                        "EOF|<EOF>|0|4|3|1|4"),
                snapshot(trailingBackslash, false));
        Assertions.assertEquals(Arrays.asList(
                        "STRING_LITERAL|'a\\'|0|0|3|1|0",
                        "EOF|<EOF>|0|4|3|1|4"),
                snapshot(trailingBackslash, true));
    }

    @Test
    void lexesLongStringsWithoutChangingTheirSourceInterval() {
        for (int length : new int[] {0, 1, 16, 256, 4096, 65536}) {
            for (boolean noBackslashEscapes : new boolean[] {false, true}) {
                assertSingleStringToken(singleQuoted("a".repeat(length)), noBackslashEscapes);
                assertSingleStringToken(singleQuoted("''".repeat(length)), noBackslashEscapes);
                assertSingleStringToken(singleQuoted("\\n".repeat(length)), noBackslashEscapes);
            }
        }
    }

    @Test
    void semanticPredicateCallsDoNotScaleWithStringLength() {
        int shortCount = predicateCalls(singleQuoted("a".repeat(16)), false);
        int longCount = predicateCalls(singleQuoted("a".repeat(4096)), false);
        int noBackslashShortCount = predicateCalls(singleQuoted("a".repeat(16)), true);
        int noBackslashLongCount = predicateCalls(singleQuoted("a".repeat(4096)), true);

        Assertions.assertTrue(longCount <= shortCount + 2,
                () -> "default mode predicate calls scaled from " + shortCount + " to " + longCount);
        Assertions.assertTrue(noBackslashLongCount <= noBackslashShortCount + 2,
                () -> "NO_BACKSLASH_ESCAPES predicate calls scaled from "
                        + noBackslashShortCount + " to " + noBackslashLongCount);
    }

    @Test
    void lexesDeterministicallyWithSharedStaticDfa() throws Exception {
        List<String> inputs = Arrays.asList(
                singleQuoted("plain"),
                doubleQuoted("中文😀"),
                singleQuoted("a\\'b"),
                doubleQuoted("a\\\"b"),
                singleQuoted("''\\n"),
                doubleQuoted("\"\"\\n"),
                "'unterminated",
                "\"unterminated");
        List<List<String>> expected = new ArrayList<>();
        for (boolean noBackslashEscapes : new boolean[] {false, true}) {
            for (String input : inputs) {
                expected.add(snapshot(input, noBackslashEscapes));
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
                            for (String input : inputs) {
                                Assertions.assertEquals(expected.get(caseIndex++),
                                        snapshot(input, noBackslashEscapes));
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

    private static int predicateCalls(String sql, boolean noBackslashEscapes) {
        CountingLexer lexer = new CountingLexer(sql);
        lexer.isNoBackslashEscapes = noBackslashEscapes;
        while (lexer.nextToken().getType() != Token.EOF) {
            // Consume all tokens.
        }
        return lexer.predicateCalls;
    }

    private static void assertSingleStringToken(String literal, boolean noBackslashEscapes) {
        List<Token> tokens = lex(literal, noBackslashEscapes);
        Assertions.assertEquals(2, tokens.size(), () -> snapshot(literal, noBackslashEscapes).toString());
        int codePointLength = literal.codePointCount(0, literal.length());
        Token string = tokens.get(0);
        Assertions.assertEquals(DorisLexer.STRING_LITERAL, string.getType());
        Assertions.assertEquals(literal, string.getText());
        Assertions.assertEquals(Token.DEFAULT_CHANNEL, string.getChannel());
        Assertions.assertEquals(0, string.getStartIndex());
        Assertions.assertEquals(codePointLength - 1, string.getStopIndex());
        Assertions.assertEquals(1, string.getLine());
        Assertions.assertEquals(0, string.getCharPositionInLine());

        Token eof = tokens.get(1);
        Assertions.assertEquals(Token.EOF, eof.getType());
        Assertions.assertEquals(codePointLength, eof.getStartIndex());
        Assertions.assertEquals(codePointLength - 1, eof.getStopIndex());
    }

    private static List<String> snapshot(String sql, boolean noBackslashEscapes) {
        List<String> snapshot = new ArrayList<>();
        for (Token token : lex(sql, noBackslashEscapes)) {
            String type = DorisLexer.VOCABULARY.getSymbolicName(token.getType());
            snapshot.add(type + "|" + token.getText().replace("\n", "\\n") + "|"
                    + token.getChannel() + "|" + token.getStartIndex() + "|" + token.getStopIndex()
                    + "|" + token.getLine() + "|" + token.getCharPositionInLine());
        }
        return snapshot;
    }

    private static List<Token> lex(String sql, boolean noBackslashEscapes) {
        DorisLexer lexer = new DorisSqlParser(noBackslashEscapes, false).newLexer(sql);
        List<Token> tokens = new ArrayList<>();
        Token token;
        do {
            token = lexer.nextToken();
            tokens.add(token);
        } while (token.getType() != Token.EOF);
        return tokens;
    }

    private static String singleQuoted(String payload) {
        return "'" + payload + "'";
    }

    private static String doubleQuoted(String payload) {
        return "\"" + payload + "\"";
    }

    private static class CountingLexer extends DorisLexer {
        private int predicateCalls;

        CountingLexer(String sql) {
            super(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        }

        @Override
        public boolean sempred(RuleContext localctx, int ruleIndex, int predIndex) {
            predicateCalls++;
            return super.sempred(localctx, ruleIndex, predIndex);
        }
    }
}
