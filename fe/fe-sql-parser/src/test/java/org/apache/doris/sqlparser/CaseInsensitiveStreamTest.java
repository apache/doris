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

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.IntStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

class CaseInsensitiveStreamTest {
    private static final String SOURCE_NAME = "case-insensitive-stream-test";
    private static final String DESERET_SMALL_LONG_I = new String(Character.toChars(0x10428));
    private static final String UNPAIRED_HIGH_SURROGATE = Character.toString((char) 0xD801);

    @Test
    void matchesReferenceLookaheadTextAndNavigation() {
        List<String> inputs = Arrays.asList(
                "",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$",
                "éÉıſß中",
                "a" + DESERET_SMALL_LONG_I + "Z",
                "unpaired" + UNPAIRED_HIGH_SURROGATE,
                "select 'MiXeD 中文😀' -- comment\nfrom `Table`");

        for (String input : inputs) {
            CharStream actual = new CaseInsensitiveStream(CharStreams.fromString(input, SOURCE_NAME));
            CharStream reference = new ReferenceCaseInsensitiveStream(CharStreams.fromString(input, SOURCE_NAME));
            assertEquivalentStream(input, actual, reference);

            CharStream stringBacked = CaseInsensitiveStream.fromString(input);
            CharStream defaultReference = new ReferenceCaseInsensitiveStream(CharStreams.fromString(input));
            assertEquivalentStream(input, stringBacked, defaultReference);
        }
    }

    private static void assertEquivalentStream(String input, CharStream actual, CharStream reference) {
        Assertions.assertEquals(reference.size(), actual.size());
        Assertions.assertEquals(reference.getSourceName(), actual.getSourceName());
        for (int start = 0; start <= reference.size(); start++) {
            for (int end : new int[] {start - 1, start, reference.size() - 1, reference.size() + 2}) {
                Assertions.assertEquals(reference.getText(Interval.of(start, end)),
                        actual.getText(Interval.of(start, end)),
                        "input=" + input + ", interval=" + start + ".." + end);
            }
        }

        for (int position = 0; position <= reference.size(); position++) {
            reference.seek(position);
            actual.seek(position);
            Assertions.assertEquals(reference.index(), actual.index());
            for (int offset = -reference.size() - 2; offset <= reference.size() + 2; offset++) {
                Assertions.assertEquals(reference.LA(offset), actual.LA(offset),
                        "input=" + input + ", position=" + actual.index() + ", offset=" + offset);
            }
            Assertions.assertEquals(reference.mark(), actual.mark());
            reference.release(-1);
            actual.release(-1);
        }

        reference.seek(0);
        actual.seek(0);
        while (reference.LA(1) != IntStream.EOF) {
            Assertions.assertEquals(reference.LA(1), actual.LA(1));
            reference.consume();
            actual.consume();
        }
        Assertions.assertEquals(reference.index(), actual.index());
        Assertions.assertEquals(IntStream.EOF, actual.LA(1));
        Assertions.assertThrows(IllegalStateException.class, reference::consume);
        Assertions.assertThrows(IllegalStateException.class, actual::consume);
    }

    @Test
    void matchesReferenceTokensAcrossSqlAndStringModes() {
        List<String> inputs = new ArrayList<>(Arrays.asList(
                "",
                "select lower_name, Mixed_Name, UPPER_NAME from db.tbl where id = 1",
                "ſelect 1, ıd from café",
                "select " + DESERET_SMALL_LONG_I + "_name, 中文列 from 数据表",
                "select 1.2e3x, 1.2z, .5d, 2.3W",
                "select 'lowerCase 中文😀', \"MiXeD\", `QuotedName` -- lower comment\n"
                        + "from tbl /* Mixed 中文😀 */ where c = 'a\\'b'",
                "/*+ SET_VAR(query_timeout=1) */ select value from table_name",
                "'unterminated\\",
                "-- comment without newline"));
        inputs.addAll(randomInputs());

        for (boolean noBackslashEscapes : new boolean[] {false, true}) {
            for (String input : inputs) {
                Assertions.assertEquals(referenceSnapshot(input, noBackslashEscapes),
                        actualSnapshot(input, noBackslashEscapes),
                        () -> "input=" + input + ", noBackslashEscapes=" + noBackslashEscapes);
            }
        }
    }

    private static List<String> randomInputs() {
        String[] alphabet = {
                "a", "z", "A", "Z", "0", "9", "_", "$", " ", "\t", "\n",
                "'", "\"", "`", "\\", "-", "/", "*", ".", ",", "(", ")", "+", "=",
                "é", "ı", "ſ", "中", DESERET_SMALL_LONG_I
        };
        Random random = new Random(20260902L);
        List<String> inputs = new ArrayList<>();
        for (int caseIndex = 0; caseIndex < 200; caseIndex++) {
            int length = random.nextInt(65);
            StringBuilder input = new StringBuilder();
            for (int index = 0; index < length; index++) {
                input.append(alphabet[random.nextInt(alphabet.length)]);
            }
            inputs.add(input.toString());
        }
        return inputs;
    }

    private static List<String> actualSnapshot(String sql, boolean noBackslashEscapes) {
        DorisLexer lexer = new DorisSqlParser(noBackslashEscapes, false).newLexer(sql);
        return snapshot(lexer);
    }

    private static List<String> referenceSnapshot(String sql, boolean noBackslashEscapes) {
        DorisLexer lexer = new DorisLexer(
                new ReferenceCaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.isNoBackslashEscapes = noBackslashEscapes;
        return snapshot(lexer);
    }

    private static List<String> snapshot(DorisLexer lexer) {
        ErrorCollector errors = new ErrorCollector();
        lexer.removeErrorListeners();
        lexer.addErrorListener(errors);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();

        List<String> snapshot = new ArrayList<>();
        for (Token token : tokenStream.getTokens()) {
            snapshot.add(token.getType() + "|" + token.getChannel() + "|" + token.getText()
                    + "|" + token.getStartIndex() + "|" + token.getStopIndex()
                    + "|" + token.getLine() + "|" + token.getCharPositionInLine()
                    + "|" + token.getTokenIndex());
        }
        snapshot.addAll(errors.errors);
        return snapshot;
    }

    private static class ErrorCollector extends BaseErrorListener {
        private final List<String> errors = new ArrayList<>();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                int charPositionInLine, String message, RecognitionException exception) {
            errors.add("ERROR|" + line + "|" + charPositionInLine + "|" + message);
        }
    }

    private static class ReferenceCaseInsensitiveStream implements CharStream {
        private final CharStream stream;

        ReferenceCaseInsensitiveStream(CharStream stream) {
            this.stream = stream;
        }

        @Override
        public String getText(Interval interval) {
            return stream.getText(interval);
        }

        @Override
        public void consume() {
            stream.consume();
        }

        @Override
        public int LA(int offset) {
            int result = stream.LA(offset);
            switch (result) {
                case 0:
                case IntStream.EOF:
                    return result;
                default:
                    return Character.toUpperCase(result);
            }
        }

        @Override
        public int mark() {
            return stream.mark();
        }

        @Override
        public void release(int marker) {
            stream.release(marker);
        }

        @Override
        public int index() {
            return stream.index();
        }

        @Override
        public void seek(int index) {
            stream.seek(index);
        }

        @Override
        public int size() {
            return stream.size();
        }

        @Override
        public String getSourceName() {
            return stream.getSourceName();
        }
    }
}
