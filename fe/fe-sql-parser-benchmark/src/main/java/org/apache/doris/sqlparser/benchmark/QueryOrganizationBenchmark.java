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

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.parser.ParseErrorListener;
import org.apache.doris.nereids.parser.PostProcessor;
import org.apache.doris.sqlparser.DorisSqlParser;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ListTokenSource;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Measures query-organization ownership in ANSI and legacy modes. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 4, time = 300, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class QueryOrganizationBenchmark {
    @Param({"false", "true"})
    public boolean ansi;

    @Param({"plainSelect", "orderedSelect", "unionTail", "parenthesizedUnion", "inlineValues"})
    public String workload;

    private final PostProcessor postProcessor = new PostProcessor();
    private final ParseErrorListener errorListener = new ParseErrorListener();

    private DorisSqlParser facade;
    private String sql;
    private List<Token> tokens;

    @Setup(Level.Trial)
    public void setUp() {
        facade = new DorisSqlParser(false, ansi);
        String union = buildUnion(12);
        switch (workload) {
            case "plainSelect":
                sql = "SELECT a, b, c FROM t WHERE a > 1";
                break;
            case "orderedSelect":
                sql = "SELECT a, b, c FROM t WHERE a > 1 ORDER BY a, b DESC LIMIT 20 OFFSET 10";
                break;
            case "unionTail":
                sql = union + " ORDER BY 1 LIMIT 10";
                break;
            case "parenthesizedUnion":
                sql = "(" + union + ") ORDER BY 1 LIMIT 10";
                break;
            case "inlineValues":
                sql = "VALUES (1, 2), (3, 4), (5, 6), (7, 8) ORDER BY 1 LIMIT 3";
                break;
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }

        CommonTokenStream stream = new CommonTokenStream(facade.newLexer(sql));
        stream.fill();
        tokens = List.copyOf(stream.getTokens());
    }

    @Benchmark
    public Object parseEndToEnd() {
        return facade.parseStatement(sql);
    }

    @Benchmark
    public Object parsePreTokenized() {
        CommonTokenStream stream = new CommonTokenStream(new ListTokenSource(tokens));
        DorisParser parser = new DorisParser(stream);
        parser.ansiSQLSyntax = ansi;
        parser.addParseListener(postProcessor);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        return parser.singleStatement();
    }

    private static String buildUnion(int count) {
        return IntStream.range(0, count)
                .mapToObj(index -> "SELECT " + index + " AS k")
                .collect(Collectors.joining(" UNION ALL "));
    }
}
