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
                + "  'enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day');");
        createTable("CREATE TABLE " + DB + ".ttl_direct_dup (\n"
                + "  k INT NOT NULL,\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1',\n"
                + "  'enable_row_ttl' = 'true');");
        createTable("CREATE TABLE " + DB + ".ttl_source_mow (\n"
                + "  k INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") ENGINE=OLAP\n"
                + "UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1',\n"
                + "  'enable_unique_key_merge_on_write' = 'true',\n"
                + "  'enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day');");

        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testSourceTimeQueryInjectsRowTtlVisibilityFilter() {
        String plan = rewrittenPlan("select k, v from ttl_source_dup");
        assertContains(plan, "row_ttl_is_visible");
        assertContains(plan, Column.TTL_COL);
        assertContains(plan, "86400000000");
    }

    @Test
    void testUniqueTableComposesDeleteSignAndRowTtlVisibilityFilters() {
        String plan = rewrittenPlan("select k, v from ttl_source_mow");
        assertContains(plan, "row_ttl_is_visible");
        assertContains(plan, Column.DELETE_SIGN);
    }

    @Test
    void testDirectExpirationQueryUsesHiddenBigintColumn() {
        String plan = rewrittenPlan("select k, v from ttl_direct_dup");
        assertContains(plan, "row_ttl_is_visible");
        assertContains(plan, Column.TTL_COL);
        assertContains(plan, "-1");
    }

    @Test
    void testTimeTravelSourceTimeUsesVisibleSourceColumn() {
        String plan = rewrittenPlan("select k, v from ttl_source_dup for version as of 1");
        assertContains(plan, "row_ttl_is_visible");
        assertContains(plan, "event_time");
        assertContains(plan, Column.COMMIT_TSO_COL);
    }

    private String rewrittenPlan(String sql) {
        Plan plan = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        return plan.treeString();
    }

    private void assertContains(String plan, String expected) {
        Assertions.assertTrue(plan.contains(expected), () -> plan);
    }
}
