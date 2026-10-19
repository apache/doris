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

package org.apache.doris.sqlparser.benchmark;

import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.sqlparser.DorisSqlParser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Emits a deterministic token snapshot for baseline/candidate differential testing. */
public final class StringLiteralTokenCorpus {
    private static final String[] ALPHABET = {"a", "'", "\"", "\\", "\n"};

    private StringLiteralTokenCorpus() {
    }

    public static void main(String[] args) throws IOException {
        List<String> payloads = new ArrayList<>();
        for (int length = 0; length <= 5; length++) {
            enumeratePayloads(new StringBuilder(), length, payloads);
        }
        payloads.add("中文😀");
        payloads.add("\\\r\n");
        payloads.add("'\"\\\n中😀");

        try (PrintWriter output = new PrintWriter(Files.newBufferedWriter(
                Path.of(args[0]), StandardCharsets.UTF_8))) {
            int caseId = 0;
            for (boolean noBackslashEscapes : new boolean[] {false, true}) {
                for (String quote : new String[] {"'", "\""}) {
                    for (String payload : payloads) {
                        emit(output, caseId++, noBackslashEscapes, quote + payload + quote);
                        emit(output, caseId++, noBackslashEscapes, quote + payload);
                        emit(output, caseId++, noBackslashEscapes,
                                quote + payload + quote + " next /* comment */");
                    }
                }
            }
        }
    }

    private static void enumeratePayloads(StringBuilder payload, int remaining, List<String> output) {
        if (remaining == 0) {
            output.add(payload.toString());
            return;
        }
        for (String character : ALPHABET) {
            int start = payload.length();
            payload.append(character);
            enumeratePayloads(payload, remaining - 1, output);
            payload.setLength(start);
        }
    }

    private static void emit(PrintWriter output, int caseId, boolean noBackslashEscapes, String sql) {
        DorisLexer lexer = new DorisSqlParser(noBackslashEscapes, false).newLexer(sql);
        ErrorCollector errors = new ErrorCollector();
        lexer.removeErrorListeners();
        lexer.addErrorListener(errors);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        tokens.fill();

        StringBuilder snapshot = new StringBuilder();
        snapshot.append(caseId).append('|').append(noBackslashEscapes).append('|').append(encode(sql));
        for (Token token : tokens.getTokens()) {
            snapshot.append('|').append(DorisLexer.VOCABULARY.getSymbolicName(token.getType()))
                    .append(',').append(token.getChannel())
                    .append(',').append(token.getStartIndex())
                    .append(',').append(token.getStopIndex())
                    .append(',').append(token.getLine())
                    .append(',').append(token.getCharPositionInLine())
                    .append(',').append(token.getTokenIndex())
                    .append(',').append(encode(token.getText()));
        }
        for (String error : errors.errors) {
            snapshot.append("|ERROR,").append(error);
        }
        output.println(snapshot);
    }

    private static String encode(String value) {
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static class ErrorCollector extends BaseErrorListener {
        private final List<String> errors = new ArrayList<>();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                int charPositionInLine, String message, RecognitionException exception) {
            errors.add(line + "," + charPositionInLine + "," + encode(message));
        }
    }
}
