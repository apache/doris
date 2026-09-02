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

/** Measures the grammar change both in isolation and through the public parser facade. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 4, time = 300, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class PrimaryExpressionBenchmark {
    @Param({"control", "typical", "specialForms", "postfixChain", "wideProjection"})
    public String workload;

    private final DorisSqlParser facade = new DorisSqlParser();
    private final ParseErrorListener errorListener = new ParseErrorListener();

    private String sql;
    private List<Token> tokens;

    @Setup(Level.Trial)
    public void setUp() {
        switch (workload) {
            case "control":
                sql = "SELECT 1";
                break;
            case "typical":
                sql = "SELECT a + b * c FROM t "
                        + "WHERE (d = 1 OR e[2].f > 3) AND g IS NOT NULL";
                break;
            case "specialForms":
                sql = "SELECT CASE a WHEN 1 THEN CONVERT(b USING utf8) "
                        + "WHEN 2 THEN CAST(c AS BIGINT) ELSE d + 1 END FROM t "
                        + "WHERE CASE WHEN e > 0 THEN TRUE ELSE FALSE END";
                break;
            case "postfixChain":
                sql = "SELECT fn(a)[1:2][3].field[4].nested[5:6].leaf "
                        + "COLLATE utf8_general_ci FROM t";
                break;
            case "wideProjection":
                sql = "SELECT " + IntStream.range(0, 64)
                        .mapToObj(i -> "c" + i + " + " + i)
                        .collect(Collectors.joining(", "))
                        + " FROM t WHERE key_col[1].field > 0";
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
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        return parser.singleStatement();
    }
}
