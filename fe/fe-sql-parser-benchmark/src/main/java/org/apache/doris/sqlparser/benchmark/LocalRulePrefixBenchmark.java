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

/** Measures local grammar-prefix changes in isolation and through the public parser facade. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(value = 3, jvmArgsAppend = {"-Xms1g", "-Xmx1g"})
@Warmup(iterations = 4, time = 300, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 7, time = 400, timeUnit = TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class LocalRulePrefixBenchmark {
    private static final int STATEMENT_COUNT = 32;

    @Param({"control", "limit", "predicate", "relation", "tvf", "primary", "show", "ddl"})
    public String workload;

    private final DorisSqlParser facade = new DorisSqlParser();
    private final PostProcessor postProcessor = new PostProcessor();
    private final ParseErrorListener errorListener = new ParseErrorListener();

    private String sql;
    private List<Token> tokens;
    private List<Token> targetTokens;

    @Setup(Level.Trial)
    public void setUp() {
        sql = repeat(statement(workload));
        CommonTokenStream stream = new CommonTokenStream(facade.newLexer(sql));
        stream.fill();
        tokens = List.copyOf(stream.getTokens());
        CommonTokenStream targetStream = new CommonTokenStream(facade.newLexer(target(workload)));
        targetStream.fill();
        targetTokens = List.copyOf(targetStream.getTokens());
    }

    @Benchmark
    public Object parseEndToEnd() {
        return facade.parseStatements(sql);
    }

    @Benchmark
    public Object parsePreTokenized() {
        return newParser(tokens).multiStatements();
    }

    @Benchmark
    public Object parseTargetRule() {
        DorisParser parser = newParser(targetTokens);
        switch (workload) {
            case "control":
                return parser.singleStatement();
            case "limit":
                return parser.limitClause();
            case "predicate":
                return parser.predicate();
            case "relation":
            case "tvf":
                return parser.relationPrimary();
            case "primary":
                return parser.primaryExpression();
            case "show":
                return parser.showStatement();
            case "ddl":
                return parser.createStatement();
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }
    }

    private DorisParser newParser(List<Token> input) {
        CommonTokenStream stream = new CommonTokenStream(new ListTokenSource(input));
        DorisParser parser = new DorisParser(stream);
        parser.addParseListener(postProcessor);
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
        return parser;
    }

    private static String statement(String workload) {
        switch (workload) {
            case "control":
                return "SELECT 1";
            case "limit":
                return "SELECT c FROM t LIMIT 100; SELECT c FROM t LIMIT 100 OFFSET 20;"
                        + " SELECT c FROM t LIMIT 20, 100";
            case "predicate":
                return "SELECT c FROM t WHERE c NOT BETWEEN 1 AND 10"
                        + " AND s NOT LIKE 'x%' ESCAPE '\\\\'"
                        + " AND r NOT REGEXP 'a.*'"
                        + " AND i NOT IN (1, 2, 3)"
                        + " AND j IN (SELECT j FROM u)"
                        + " AND n IS NOT NULL AND b IS NOT FALSE";
            case "relation":
                return "SELECT t0.c FROM catalog.db.t0 AS t0"
                        + IntStream.range(1, 12)
                                .mapToObj(i -> " JOIN catalog.db.t" + i + " AS t" + i
                                        + " ON t" + (i - 1) + ".k = t" + i + ".k")
                                .collect(Collectors.joining());
            case "tvf":
                return "SELECT number FROM numbers(\"number\" = \"100\") AS n";
            case "primary":
                return "SELECT " + IntStream.range(0, 48)
                        .mapToObj(i -> i % 4 == 0 ? "db.fn" + i + "(c" + i + ")"
                                : i % 4 == 1 ? "c" + i + ".field"
                                : i % 4 == 2 ? "c" + i + "[1].field"
                                : "c" + i)
                        .collect(Collectors.joining(", ")) + " FROM catalog.db.t";
            case "show":
                return "SHOW TABLES FROM db LIKE 'fact%'";
            case "ddl":
                return "CREATE TABLE IF NOT EXISTS db.t (k BIGINT, v VARCHAR(32))"
                        + " DISTRIBUTED BY HASH(k) BUCKETS 8"
                        + " PROPERTIES (\"replication_num\" = \"1\")";
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }
    }

    private static String repeat(String statement) {
        return IntStream.range(0, STATEMENT_COUNT)
                .mapToObj(ignored -> statement)
                .collect(Collectors.joining("; "));
    }

    private static String target(String workload) {
        switch (workload) {
            case "control":
                return "SELECT 1";
            case "limit":
                return "LIMIT 100 OFFSET 20";
            case "predicate":
                return "NOT IN (1, 2, 3)";
            case "relation":
                return "catalog.db.t AS t";
            case "tvf":
                return "numbers(\"number\" = \"100\") AS n";
            case "primary":
                return "db.fn(c)[1].field";
            case "show":
                return "SHOW TABLES FROM db LIKE 'fact%'";
            case "ddl":
                return "CREATE TABLE IF NOT EXISTS db.t (k BIGINT, v VARCHAR(32))"
                        + " DISTRIBUTED BY HASH(k) BUCKETS 8"
                        + " PROPERTIES (\"replication_num\" = \"1\")";
            default:
                throw new IllegalArgumentException("Unknown workload: " + workload);
        }
    }
}
