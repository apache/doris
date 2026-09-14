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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Measures case-folding stream construction, lookahead, lexing, and end-to-end parsing. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 4, time = 300, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class CaseInsensitiveStreamBenchmark {
    @Param({
            "shortQuery",
            "lowercaseIdentifiers",
            "mixedCaseIdentifiers",
            "uppercaseIdentifiers",
            "stringAndComment",
            "unicode"
    })
    public String workload;

    private DorisSqlParser parser;
    private String sql;
    private CharStream prebuiltStream;

    @Setup(Level.Trial)
    public void setUp() {
        parser = new DorisSqlParser();
        switch (workload) {
            case "shortQuery":
                sql = "select 1";
                break;
            case "lowercaseIdentifiers":
                sql = projection("lowercase_column_", false);
                break;
            case "mixedCaseIdentifiers":
                sql = projection("mixedCaseColumn_", false);
                break;
            case "uppercaseIdentifiers":
                sql = projection("UPPERCASE_COLUMN_", true);
                break;
            case "stringAndComment":
                sql = "select 'lowercase MixedCase 中文😀' as text_value, lower_name, mixedName "
                        + "from lower_table /* lowercase MixedCase 中文😀 */ "
                        + "where lower_name = 'another string' -- trailing comment\nlimit 10";
                break;
            case "unicode":
                String supplementary = new String(Character.toChars(0x10428));
                sql = "select café_列, élève_列, " + supplementary + "_column, 'ı ſ 中文 😀' "
                        + "from 数据表 where café_列 > 10";
                break;
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }
        prebuiltStream = parser.newLexer(sql).getInputStream();
    }

    @Benchmark
    public Object createLexer() {
        return parser.newLexer(sql);
    }

    @Benchmark
    public int foldPrebuiltCharacters() {
        prebuiltStream.seek(0);
        int checksum = 1;
        int character;
        while ((character = prebuiltStream.LA(1)) != Token.EOF) {
            checksum = 31 * checksum + character;
            prebuiltStream.consume();
        }
        return checksum;
    }

    @Benchmark
    public int tokenize() {
        DorisLexer lexer = parser.newLexer(sql);
        int checksum = 1;
        Token token;
        do {
            token = lexer.nextToken();
            checksum = 31 * checksum + token.getType();
            checksum = 31 * checksum + token.getStartIndex();
            checksum = 31 * checksum + token.getStopIndex();
        } while (token.getType() != Token.EOF);
        return checksum;
    }

    @Benchmark
    public Object parseStatement() {
        return parser.parseStatement(sql);
    }

    private static String projection(String prefix, boolean uppercase) {
        String projection = IntStream.range(0, 64)
                .mapToObj(index -> prefix + index)
                .collect(Collectors.joining(", "));
        String sql = "select " + projection + " from identifier_heavy_table where " + prefix + "0 > 10";
        return uppercase ? sql.toUpperCase(Locale.ROOT) : sql;
    }
}
