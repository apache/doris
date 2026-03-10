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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr.ColumnMinMax;
import org.apache.doris.nereids.stats.SimpleAggCacheMgr.ColumnMinMaxKey;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

/**
 * Tests for {@link RewriteSimpleAggToConstantRule}.
 *
 * <p>This rule rewrites simple aggregation queries (count/min/max on DUP_KEYS tables
 * without GROUP BY) into constant values from FE metadata, producing
 * LogicalProject -> LogicalOneRowRelation plans.
 */
class RewriteSimpleAggToConstantRuleTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        // DUP_KEYS table with NOT NULL columns
        createTable("CREATE TABLE test.dup_tbl (\n"
                + "  k1 INT NOT NULL,\n"
                + "  v1 INT NOT NULL,\n"
                + "  v2 BIGINT NOT NULL,\n"
                + "  v3 DATE NOT NULL,\n"
                + "  v4 VARCHAR(128)\n"
                + ") DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num' = '1');");

        // UNIQUE_KEYS table (should NOT be rewritten)
        createTable("CREATE TABLE test.uniq_tbl (\n"
                + "  k1 INT NOT NULL,\n"
                + "  v1 INT NOT NULL\n"
                + ") UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num' = '1');");

        // AGG_KEYS table (should NOT be rewritten)
        createTable("CREATE TABLE test.agg_tbl (\n"
                + "  k1 INT NOT NULL,\n"
                + "  v1 INT SUM NOT NULL\n"
                + ") AGGREGATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num' = '1');");

        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        // Install a mock SimpleAggCacheMgr that returns known min/max and row count for dup_tbl
        SimpleAggCacheMgr.setTestInstance(new MockMinMaxStatsMgr());
    }

    @AfterAll
    public static void tearDown() {
        SimpleAggCacheMgr.clearTestInstance();
    }

    private OlapTable getOlapTable(String tableName) throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        return (OlapTable) db.getTableOrMetaException(tableName);
    }

    // ======================== Positive tests: should rewrite ========================

    @Test
    void testCountStarRewrite() {
        // count(*) on DUP_KEYS with reported row counts → rewrite to constant
        PlanChecker.from(connectContext)
                .analyze("SELECT count(*) FROM dup_tbl")
                .rewrite()
                .matches(logicalResultSink(logicalOneRowRelation()))
                .printlnTree();
    }

    @Test
    void testCountNotNullColumnRewrite() {
        // count(not-null column) on DUP_KEYS → rewrite to constant (equals rowCount)
        PlanChecker.from(connectContext)
                .analyze("SELECT count(k1) FROM dup_tbl")
                .rewrite()
                .matches(logicalResultSink(logicalOneRowRelation()))
                .printlnTree();
    }

    @Test
    void testMinRewrite() {
        // min(int col) on DUP_KEYS with cache hit → rewrite to constant
        PlanChecker.from(connectContext)
                .analyze("SELECT min(v1) FROM dup_tbl")
                .rewrite()
                .matches(logicalResultSink(logicalOneRowRelation()))
                .printlnTree();
    }

    @Test
    void testMaxRewrite() {
        // max(bigint col) on DUP_KEYS with cache hit → rewrite to constant
        PlanChecker.from(connectContext)
                .analyze("SELECT max(v2) FROM dup_tbl")
                .rewrite()
                .matches(logicalResultSink(logicalOneRowRelation()))
                .printlnTree();
    }

    @Test
    void testMinMaxDateRewrite() {
        // min/max(date col) on DUP_KEYS with cache hit → rewrite to constant
        PlanChecker.from(connectContext)
                .analyze("SELECT min(v3), max(v3) FROM dup_tbl")
                .rewrite()
                .matches(logicalResultSink(logicalOneRowRelation()))
                .printlnTree();
    }

    @Test
    void testMixedCountMinMax() {
        // count(*), min(v1), max(v2) on DUP_KEYS → rewrite to constant
        PlanChecker.from(connectContext)
                .analyze("SELECT count(*), min(v1), max(v2) FROM dup_tbl")
                .rewrite()
                .matches(logicalResultSink(logicalOneRowRelation()))
                .printlnTree();
    }

    // ======================== Negative tests: should NOT rewrite ========================

    @Test
    void testUniqueKeysNotRewrite() {
        // UNIQUE_KEYS table → rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT count(*) FROM uniq_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testAggKeysNotRewrite() {
        // AGG_KEYS table → rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT count(*) FROM agg_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testGroupByNotRewrite() {
        // GROUP BY present → rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT count(*) FROM dup_tbl GROUP BY k1")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testUnsupportedAggFuncNotRewrite() {
        // SUM is not supported → rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT sum(v1) FROM dup_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testAvgNotRewrite() {
        // AVG is not supported → rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT avg(v1) FROM dup_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testDistinctCountNotRewrite() {
        // count(distinct col) → rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT count(distinct k1) FROM dup_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testCountNullableColumnNotRewrite() {
        // count(nullable column v4) → cannot guarantee count(v4) == rowCount, rule should NOT trigger
        PlanChecker.from(connectContext)
                .analyze("SELECT count(v4) FROM dup_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testMinMaxStringColumnNotRewrite() {
        // min(varchar col) → not supported for string types
        PlanChecker.from(connectContext)
                .analyze("SELECT min(v4) FROM dup_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    @Test
    void testMixedSupportedAndUnsupportedNotRewrite() {
        // If ANY agg function cannot be replaced, the entire rewrite is skipped.
        // count(*) can be replaced, but sum(v1) cannot → entire query NOT rewritten
        PlanChecker.from(connectContext)
                .analyze("SELECT count(*), sum(v1) FROM dup_tbl")
                .rewrite()
                .nonMatch(logicalResultSink(logicalOneRowRelation()));
    }

    // ======================== Mock SimpleAggCacheMgr ========================

    /**
     * A simple mock that returns known min/max values and row count for dup_tbl.
     * It accepts any version (returns the values regardless of version).
     */
    private class MockMinMaxStatsMgr extends SimpleAggCacheMgr {

        @Override
        public Optional<ColumnMinMax> getStats(ColumnMinMaxKey key, long version) {
            try {
                OlapTable table = getOlapTable("dup_tbl");
                if (key.getTableId() != table.getId()) {
                    return Optional.empty();
                }
            } catch (Exception e) {
                return Optional.empty();
            }

            String colName = key.getColumnName().toLowerCase();
            switch (colName) {
                case "k1":
                    return Optional.of(new ColumnMinMax("1", "100"));
                case "v1":
                    return Optional.of(new ColumnMinMax("10", "999"));
                case "v2":
                    return Optional.of(new ColumnMinMax("100", "99999"));
                case "v3":
                    return Optional.of(new ColumnMinMax("2024-01-01", "2025-12-31"));
                default:
                    // v4 (varchar) and unknown columns → no cache
                    return Optional.empty();
            }
        }

        @Override
        public OptionalLong getRowCount(long tableId, long version) {
            try {
                OlapTable table = getOlapTable("dup_tbl");
                if (tableId == table.getId()) {
                    return OptionalLong.of(100L);
                }
            } catch (Exception e) {
                // fall through
            }
            return OptionalLong.empty();
        }

        @Override
        public void removeStats(ColumnMinMaxKey key) {
            // no-op for mock
        }
    }
}
