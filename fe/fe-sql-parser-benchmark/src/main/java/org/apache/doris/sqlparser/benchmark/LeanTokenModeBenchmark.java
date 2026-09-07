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
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.sqlparser.DorisSqlParser;

import org.antlr.v4.runtime.CommonTokenStream;
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

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Measures whitespace and ordinary-comment token allocation in full and lean modes. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 4, time = 300, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class LeanTokenModeBenchmark {
    @Param({"false", "true"})
    public boolean leanTokenMode;

    @Param({"typical", "commentHeavy", "wideSelect", "hinted"})
    public String workload;

    private final DorisSqlParser facade = new DorisSqlParser();
    private String sql;

    @Setup(Level.Trial)
    public void setUp() {
        switch (workload) {
            case "typical":
                sql = "SELECT a, b, c FROM t WHERE a > 1 AND b < 10 ORDER BY c LIMIT 20";
                break;
            case "commentHeavy":
                sql = IntStream.range(0, 32)
                        .mapToObj(index -> "c" + index + " -- column " + index + "\n")
                        .collect(Collectors.joining(", ", "SELECT ", "FROM t WHERE c0 > 0"));
                break;
            case "wideSelect":
                sql = IntStream.range(0, 128)
                        .mapToObj(index -> "c" + index + " AS alias_" + index)
                        .collect(Collectors.joining(", ", "SELECT ", " FROM wide_table"));
                break;
            case "hinted":
                sql = "SELECT /*+ ORDERED */ a, b FROM t1 JOIN t2 ON t1.id = t2.id WHERE a > 1";
                break;
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }
    }

    @Benchmark
    public Object tokenize() {
        DorisLexer lexer = newLexer();
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();
        return tokenStream.getTokens();
    }

    @Benchmark
    public Object parseStatement() {
        DorisParser parser = facade.newParser(newLexer());
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        return parser.singleStatement();
    }

    private DorisLexer newLexer() {
        DorisLexer lexer = facade.newLexer(sql);
        lexer.isLeanTokenMode = leanTokenMode;
        return lexer;
    }
}
