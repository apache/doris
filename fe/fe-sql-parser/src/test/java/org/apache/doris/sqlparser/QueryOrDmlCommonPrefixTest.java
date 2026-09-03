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

import org.apache.doris.nereids.DorisParser.SingleStatementContext;
import org.apache.doris.nereids.exceptions.ParseException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class QueryOrDmlCommonPrefixTest {
    private final DorisSqlParser parser = new DorisSqlParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("explainableStatements")
    void parsesExplainAndCtePrefixes(String description, String sql) {
        SingleStatementContext context = parser.parseStatement(sql);
        Assertions.assertNotNull(context.statement());
    }

    private static Stream<Arguments> explainableStatements() {
        String longCte = buildCte(20);
        return Stream.of(
                Arguments.of("query", "SELECT 1"),
                Arguments.of("explain query", "EXPLAIN SELECT 1"),
                Arguments.of("CTE query", "WITH c AS (SELECT 1) SELECT * FROM c"),
                Arguments.of("explain CTE query", "EXPLAIN WITH c AS (SELECT 1) SELECT * FROM c"),
                Arguments.of("long CTE query", longCte + " SELECT * FROM c19"),
                Arguments.of("CTE insert", "WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"),
                Arguments.of("long CTE insert", longCte + " INSERT INTO t SELECT * FROM c19"),
                Arguments.of("explain CTE insert",
                        "EXPLAIN WITH c AS (SELECT 1) INSERT OVERWRITE TABLE t SELECT * FROM c"),
                Arguments.of("outer and source CTE",
                        "WITH c AS (SELECT 1) INSERT INTO t WITH d AS (SELECT * FROM c) SELECT * FROM d"),
                Arguments.of("CTE update",
                        "WITH c AS (SELECT 1 AS id) UPDATE t SET v = 1 FROM c WHERE t.id = c.id"),
                Arguments.of("CTE delete",
                        "WITH c AS (SELECT 1 AS id) DELETE FROM t USING c WHERE t.id = c.id"),
                Arguments.of("CTE merge",
                        "WITH c AS (SELECT 1 AS id) MERGE INTO t USING c ON t.id = c.id "
                                + "WHEN MATCHED THEN UPDATE SET v = 1"),
                Arguments.of("job DML prefix",
                        "CREATE JOB db.job ON SCHEDULE AT '2026-01-01 00:00:00' DO "
                                + "EXPLAIN WITH c AS (SELECT 1) INSERT INTO t SELECT * FROM c"),
                Arguments.of("non-explainable DML", "TRUNCATE TABLE t"),
                Arguments.of("warm-up explain", "EXPLAIN WARM UP SELECT * FROM t"),
                Arguments.of("describe", "DESC t"));
    }

    @ParameterizedTest(name = "rejects: {0}")
    @MethodSource("invalidPrefixCombinations")
    void rejectsPrefixesThatWereNotPreviouslyAccepted(String sql) {
        Assertions.assertThrows(ParseException.class, () -> parser.parseStatement(sql));
    }

    private static Stream<String> invalidPrefixCombinations() {
        return Stream.of(
                "EXPLAIN TRUNCATE TABLE t",
                "WITH c AS (SELECT 1) TRUNCATE TABLE t",
                "CREATE JOB db.job ON SCHEDULE AT '2026-01-01 00:00:00' DO "
                        + "WITH c AS (SELECT 1) SELECT * FROM c");
    }

    @ParameterizedTest(name = "truncated: {0}")
    @MethodSource("truncatedPrefixes")
    void reportsTruncatedPrefixesAtEndOfInput(String sql) {
        ParseException exception = Assertions.assertThrows(ParseException.class, () -> parser.parseStatement(sql));
        Assertions.assertTrue(exception.getMessage().contains("line 1, pos " + sql.length()), exception::getMessage);
    }

    private static Stream<String> truncatedPrefixes() {
        return Stream.of(
                "EXPLAIN",
                "WITH c AS (SELECT 1)",
                "EXPLAIN WITH c AS (SELECT 1) INSERT INTO t");
    }

    private static String buildCte(int count) {
        return "WITH " + IntStream.range(0, count)
                .mapToObj(index -> index == 0
                        ? "c0 AS (SELECT 0 AS k)"
                        : "c" + index + " AS (SELECT k + 1 AS k FROM c" + (index - 1) + ")")
                .collect(Collectors.joining(", "));
    }
}
