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

/** Measures common EXPLAIN/CTE prefix dispatch in isolation and through the public parser facade. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 4, time = 300, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class QueryOrDmlCommonPrefixBenchmark {
    @Param({"control", "explain", "cteSelect", "cteInsert", "explainCteInsert"})
    public String workload;

    private final DorisSqlParser facade = new DorisSqlParser();
    private final ParseErrorListener errorListener = new ParseErrorListener();

    private String sql;
    private List<Token> tokens;

    @Setup(Level.Trial)
    public void setUp() {
        String cte = buildCte(12);
        switch (workload) {
            case "control":
                sql = "SELECT 1";
                break;
            case "explain":
                sql = "EXPLAIN SELECT a, b FROM t WHERE a > 1";
                break;
            case "cteSelect":
                sql = cte + " SELECT * FROM c11";
                break;
            case "cteInsert":
                sql = cte + " INSERT INTO target_table SELECT * FROM c11";
                break;
            case "explainCteInsert":
                sql = "EXPLAIN " + cte + " INSERT INTO target_table SELECT * FROM c11";
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

    private static String buildCte(int count) {
        return "WITH " + IntStream.range(0, count)
                .mapToObj(index -> index == 0
                        ? "c0 AS (SELECT 0 AS k)"
                        : "c" + index + " AS (SELECT k + 1 AS k FROM c" + (index - 1) + ")")
                .collect(Collectors.joining(", "));
    }
}
