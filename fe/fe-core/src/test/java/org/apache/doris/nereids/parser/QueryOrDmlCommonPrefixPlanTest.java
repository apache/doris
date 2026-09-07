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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.trees.plans.commands.DeleteFromUsingCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.commands.UpdateCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.merge.MergeIntoCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class QueryOrDmlCommonPrefixPlanTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    @ParameterizedTest(name = "{0}")
    @MethodSource("statementPlans")
    public void buildsExpectedPlanAfterCommonPrefix(
            String description, String sql, Class<? extends LogicalPlan> expectedClass) {
        Assertions.assertInstanceOf(expectedClass, parser.parseSingle(sql));
    }

    private static Stream<Arguments> statementPlans() {
        return Stream.of(
                Arguments.of("CTE query", "WITH c AS (SELECT 1) SELECT * FROM c", UnboundResultSink.class),
                Arguments.of("explain CTE query", "EXPLAIN WITH c AS (SELECT 1) SELECT * FROM c",
                        ExplainCommand.class),
                Arguments.of("CTE insert", "WITH c AS (SELECT 1) INSERT INTO db.t SELECT * FROM c",
                        InsertIntoTableCommand.class),
                Arguments.of("CTE update",
                        "WITH c AS (SELECT 1 AS id) UPDATE db.t SET v = 1 FROM c WHERE t.id = c.id",
                        UpdateCommand.class),
                Arguments.of("CTE delete",
                        "WITH c AS (SELECT 1 AS id) DELETE FROM db.t USING c WHERE t.id = c.id",
                        DeleteFromUsingCommand.class),
                Arguments.of("CTE merge",
                        "WITH c AS (SELECT 1 AS id) MERGE INTO db.t USING c ON t.id = c.id "
                                + "WHEN MATCHED THEN UPDATE SET v = 1",
                        MergeIntoCommand.class));
    }

    @Test
    public void preservesExplainAndCtePlanShape() {
        ExplainCommand explain = (ExplainCommand) parser.parseSingle(
                "EXPLAIN WITH c AS (SELECT 1) SELECT * FROM c");
        UnboundResultSink<?> sink = (UnboundResultSink<?>) explain.getLogicalPlan();
        Assertions.assertInstanceOf(LogicalCTE.class, sink.child());
    }

    @Test
    public void preservesExplainAndCteForInsert() {
        ExplainCommand explain = (ExplainCommand) parser.parseSingle(
                "EXPLAIN WITH c AS (SELECT 1) INSERT INTO db.t SELECT * FROM c");
        Assertions.assertInstanceOf(InsertIntoTableCommand.class, explain.getLogicalPlan());
        Assertions.assertTrue(explain.getLogicalPlan().toDigest().contains("WITH\n"));
    }

    @ParameterizedTest
    @MethodSource("invalidPrefixCombinations")
    public void rejectsInvalidPrefixCombinations(String sql) {
        Assertions.assertThrows(ParseException.class, () -> parser.parseSingle(sql));
    }

    private static Stream<String> invalidPrefixCombinations() {
        return Stream.of(
                "EXPLAIN TRUNCATE TABLE t",
                "WITH c AS (SELECT 1) TRUNCATE TABLE t",
                "WITH c AS (SELECT 1)");
    }
}
