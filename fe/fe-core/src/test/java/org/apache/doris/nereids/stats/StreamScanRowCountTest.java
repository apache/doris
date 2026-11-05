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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.statistics.analysis.AnalysisInfo;
import org.apache.doris.statistics.analysis.AnalysisInfoBuilder;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * A stream read is planned over a lightweight wrapper of its base table, and the row count it is
 * estimated with has to come from the table, not from the wrapper's own (empty) partition map --
 * otherwise the read looks like a one-row scan and the optimizer lets it become the build side of
 * a broadcast join, which is what exhausts memory in an incremental view maintenance refresh.
 *
 * <p>The whole planner runs here -- analyze, the stream rewrite, the statistics and the join rules
 * -- over a table whose row count was injected the way {@code ALTER TABLE ... SET STATS
 * ('row_count' = ...)} injects one, and the plan is checked for the estimate of the stream scan.
 */
public class StreamScanRowCountTest extends TestWithFeService {

    private static final long BASE_TABLE_ROW_COUNT = 400_000_000L;
    private static final long JOINED_TABLE_ROW_COUNT = 1_000L;

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_table_stream = true;
        Config.enable_feature_changelog = true;
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createDatabase("test");
        useDatabase("test");
        // One partition: a partition without a reported tablet row count is estimated with the
        // row count of the whole table, which a single-partition table makes exact.
        createTable("CREATE TABLE stream_base (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES (\"replication_num\" = \"1\", \"changelog.enable\" = \"true\")");
        createTable("CREATE TABLE joined (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES (\"replication_num\" = \"1\")");
        // The partitions have committed something, which is what a stream records as the offset it
        // consumes from; without it a snapshot read answers with nothing and keeps no scan.
        OlapTable base = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException("stream_base");
        for (Partition partition : base.getPartitions()) {
            partition.setVisibleVersionAndTime(2, System.currentTimeMillis(), 100L);
        }
        injectRowCount("stream_base", BASE_TABLE_ROW_COUNT);
        injectRowCount("joined", JOINED_TABLE_ROW_COUNT);
        createStream("CREATE STREAM stream_on_base ON TABLE stream_base "
                + "PROPERTIES ('show_initial_rows' = 'false')");
    }

    /**
     * Injects a row count the way {@code ALTER TABLE ... MODIFY COLUMN ... SET STATS ('row_count'
     * = ...)} does: a statistics record the planner reads instead of the reported tablets, which
     * this environment has none of.
     */
    private static void injectRowCount(String tableName, long rowCount) throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException(tableName);
        AnalysisInfo injected = new AnalysisInfoBuilder()
                .setTblUpdateTime(table.getUpdateTime())
                .setColName("")
                .setRowCount(rowCount)
                .setJobColumns(Sets.newHashSet())
                .setUserInject(true)
                .setJobType(AnalysisInfo.JobType.MANUAL)
                .build();
        injected.addIndexRowCount(table.getBaseIndexId(), rowCount);
        Env.getCurrentEnv().getAnalysisManager().updateTableStatsForAlterStats(injected, table);
    }

    private void createStream(String sql) throws Exception {
        // CREATE STREAM is a DDL of its own; the framework's createTable only covers tables.
        executeSql(sql);
    }

    /**
     * The estimate of the scan of the stream's base table in the plan of {@code sql}.
     */
    private long streamScanRowCount(String sql) {
        long[] rowCount = new long[] {-1};
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            List<PhysicalOlapScan> scans = new ArrayList<>();
            collectScans(planner.getOptimizedPlan(), scans);
            for (PhysicalOlapScan scan : scans) {
                if (scan.getTable().getName().equals("stream_base")) {
                    rowCount[0] = (long) scan.getStats().getRowCount();
                }
            }
        });
        Assertions.assertTrue(rowCount[0] > 0, "no scan of stream_base in the plan of " + sql);
        return rowCount[0];
    }

    private static void collectScans(Plan plan, List<PhysicalOlapScan> scans) {
        if (plan instanceof PhysicalOlapScan) {
            scans.add((PhysicalOlapScan) plan);
        }
        for (Plan child : plan.children()) {
            collectScans(child, scans);
        }
    }

    @Test
    public void testASnapshotReadIsEstimatedFromTheTable() {
        // The image at the stream's offset, joined with a small table: the stream side keeps the
        // row count of its base table instead of collapsing to one row.
        long rowCount = streamScanRowCount(
                "select * from stream_on_base@snapshot() join joined on stream_on_base.k = joined.k");
        Assertions.assertEquals(BASE_TABLE_ROW_COUNT, rowCount,
                "a snapshot read of a large table must not be estimated at one row");
    }

    @Test
    public void testAResetReadIsEstimatedFromTheTable() {
        Assertions.assertEquals(BASE_TABLE_ROW_COUNT, streamScanRowCount("select * from stream_on_base@reset()"));
    }
}
