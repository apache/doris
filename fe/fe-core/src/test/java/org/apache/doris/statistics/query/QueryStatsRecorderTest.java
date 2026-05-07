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

package org.apache.doris.statistics.query;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.Command;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class QueryStatsRecorderTest {

    private boolean originalConfigValue;

    @BeforeEach
    public void setUp() {
        originalConfigValue = Config.enable_query_hit_stats;
    }

    @AfterEach
    public void tearDown() {
        // Restore config to avoid affecting other tests.
        Config.enable_query_hit_stats = originalConfigValue;
    }

    // ── shouldRecord guard tests ─────────────────────────────────────────────

    @Test
    public void testShouldNotRecordWhenConfigOff() {
        Config.enable_query_hit_stats = false;
        StatementContext ctx = new StatementContext();
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class), ctx);
        ctx.setParsedStatement(stmt);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordWhenStatementIsNull() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        // parsedStatement not set — getParsedStatement() returns null
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordExplain() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class), ctx);
        // isExplain() returns true when explainOptions is non-null.
        stmt.setIsExplain(new ExplainOptions(false, false, false));
        ctx.setParsedStatement(stmt);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldNotRecordDml() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        Command dmlCommand = Mockito.mock(Command.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(dmlCommand, ctx);
        ctx.setParsedStatement(stmt);
        Assertions.assertFalse(QueryStatsRecorder.shouldRecord(ctx));
    }

    @Test
    public void testShouldRecordNormalSelect() {
        Config.enable_query_hit_stats = true;
        StatementContext ctx = new StatementContext();
        // A LogicalPlanAdapter wrapping a non-Command plan represents a SELECT.
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan selectPlan =
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class);
        LogicalPlanAdapter stmt = new LogicalPlanAdapter(selectPlan, ctx);
        ctx.setParsedStatement(stmt);
        Assertions.assertTrue(QueryStatsRecorder.shouldRecord(ctx));
    }
}
