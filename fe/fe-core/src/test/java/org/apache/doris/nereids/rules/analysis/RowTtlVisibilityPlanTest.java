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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RowTtlIsVisible;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RowTtlVisibilityPlanTest extends TestWithFeService {
    private static final String DB = "test_row_ttl_visibility_plan";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB);

        createTable("CREATE TABLE " + DB + ".ttl_source_dup (\n"
                + "  k INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1',\n"
                + "  'binlog.enable' = 'true',\n"
                + "  'binlog.format' = 'ROW',\n"
                + "  'function_column.enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day',\n"
                + "  'function_column.ttl_time_zone' = '+08:00');");
        createTable("CREATE TABLE " + DB + ".ttl_source_mow (\n"
                + "  k INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1',\n"
                + "  'enable_unique_key_merge_on_write' = 'true',\n"
                + "  'function_column.enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day',\n"
                + "  'function_column.ttl_time_zone' = '-05:30');");

        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testSourceTimeQueryInjectsRowTtlVisibilityFilter() {
        String plan = rewrittenPlan("select k, v from ttl_source_dup");
        assertContains(plan, "row_ttl_is_visible");
        assertContains(plan, Column.TTL_COL);
        assertContains(plan, "86400000000");
        assertContains(plan, "28800");
    }

    @Test
    void testUniqueTableComposesDeleteSignAndRowTtlVisibilityFilters() {
        String plan = rewrittenPlan("select k, v from ttl_source_mow");
        assertContains(plan, "row_ttl_is_visible");
        assertContains(plan, Column.DELETE_SIGN);
    }

    @Test
    void testRejectPreAggOpenHint() {
        assertAnalysisError("select k, v from ttl_source_dup /*+PREAGGOPEN*/",
                "PREAGGOPEN hint is not supported on tables with row TTL");
    }

    @Test
    void testMissingLegacyTimeZoneFailsClosed() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(DB);
        OlapTable table = (OlapTable) db.getTableOrDdlException("ttl_source_dup");
        String timeZone = table.getTableProperty().getProperties().remove("function_column.ttl_time_zone");
        try {
            assertAnalysisError("select k, v from ttl_source_dup", "row ttl time zone is missing from table");
        } finally {
            table.getTableProperty().getProperties().put("function_column.ttl_time_zone", timeZone);
        }
    }

    @Test
    void testVisibilityFunctionIsNonDeterministic() {
        Assertions.assertFalse(new RowTtlIsVisible(new BigIntLiteral(1), new BigIntLiteral(1)).isDeterministic());
    }

    @Test
    void testRejectHistoryAndChangeReads() {
        assertAnalysisError("select k, v from ttl_source_dup for version as of 1",
                "FOR VERSION/TIME AS OF is not supported on tables with row TTL.");
        assertAnalysisError("select k, v from ttl_source_dup for time as of '2026-07-01 00:00:00'",
                "FOR VERSION/TIME AS OF is not supported on tables with row TTL.");
        assertAnalysisError("select k, v from ttl_source_dup@incr()",
                "INCR query is not supported on tables with row TTL.");
    }

    private String rewrittenPlan(String sql) {
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        return plan.treeString();
    }

    private void assertContains(String actual, String expected) {
        Assertions.assertTrue(actual.contains(expected), () -> actual);
    }

    private void assertAnalysisError(String sql, String expected) {
        assertContains(Assertions.assertThrows(
                AnalysisException.class, () -> rewrittenPlan(sql)).getMessage(), expected);
    }
}
