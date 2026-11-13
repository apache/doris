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
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapChangelogTableWrapper;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.analysis.AnalysisInfo;
import org.apache.doris.statistics.analysis.AnalysisInfoBuilder;
import org.apache.doris.tso.TSOTimestamp;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
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
    private static final long START_TSO = TSOTimestamp.composePhysicalTimestamp(System.currentTimeMillis());
    private static final long END_TSO = START_TSO + 100;

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
            partition.setVisibleVersionAndTime(2, System.currentTimeMillis(), START_TSO);
            partition.getBaseIndex().setRowCount(BASE_TABLE_ROW_COUNT);
            partition.getBaseIndex().setRowCountReported(true);
            partition.getIndex(base.getChangelogIndexMeta().getIndexId()).setRowCount(1000L);
        }
        injectRowCount("stream_base", BASE_TABLE_ROW_COUNT);
        injectRowCount("joined", JOINED_TABLE_ROW_COUNT);
        createStream("CREATE STREAM stream_on_base ON TABLE stream_base "
                + "PROPERTIES ('show_initial_rows' = 'false')");
        for (Partition partition : base.getPartitions()) {
            partition.setVisibleVersionAndTime(3, System.currentTimeMillis(), END_TSO);
            advanceReplicaVersions(partition);
            partition.getBaseIndex().setRowCount(BASE_TABLE_ROW_COUNT + 700);
            partition.getIndex(base.getChangelogIndexMeta().getIndexId()).setRowCount(1200L);
        }
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
        return streamScanRowCount(sql, "stream_base");
    }

    private long streamScanRowCount(String sql, String tableName) {
        long[] rowCount = new long[] {-1};
        PlanChecker.from(connectContext).checkExplain(sql, planner -> {
            List<PhysicalOlapScan> scans = new ArrayList<>();
            collectScans(planner.getOptimizedPlan(), scans);
            for (PhysicalOlapScan scan : scans) {
                if (scan.getTable().getName().equals(tableName)) {
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

    @Test
    public void testIncrementalDeltaTakesPrecedenceOverInjectedBaseRows() {
        Assertions.assertEquals(700L, streamScanRowCount("select * from stream_on_base"));
    }

    @Test
    public void testCloudReadStateMatchesOriginalOffsetBeforeTtl() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable base = (OlapTable) db.getTableOrMetaException("stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("stream_on_base");
        long id = base.getPartitionIds().get(0);
        for (long offset : new long[] {START_TSO, START_TSO + 50}) {
            OlapTableStreamWrapper wrapper = new OlapTableStreamWrapper(stream, base, base.getPartitionIds());
            wrapper.installCloudReadStates(ImmutableMap.of(id, Cloud.TableStreamPartitionReadStatePB.newBuilder()
                    .setPartitionId(id)
                    .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                    .setOffsetTso(offset).setEndTso(END_TSO).setVisibleVersion(3L).build()));
            // TTL has moved the scan start, but validity is checked against the MS consumption offset.
            Assertions.assertEquals(offset == START_TSO ? 700L : 1200L,
                    wrapper.estimateChangelogRead(base.getPartitionIds(), ImmutableMap.of(id, Pair.of(START_TSO + 80, END_TSO + 1)),
                            BaseTableStream.StreamScanType.APPEND_ONLY).get(id));
        }
    }

    @Test
    public void testRestoreDiscardsBaselineAndLateCallbacksCannotOverwrite() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable base = (OlapTable) db.getTableOrMetaException("stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("stream_on_base");
        OlapTableStream restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(stream), OlapTableStream.class);
        long id = base.getPartitionIds().get(0);
        Assertions.assertNull(restored.getStreamRowCountBaseline(id));
        restored.writeLock();
        try {
            restored.unprotectedRecordStreamRowCountBaseline(id, START_TSO + 200);
            restored.unprotectedRecordStreamRowCountBaseline(id, START_TSO + 150);
            Assertions.assertEquals(START_TSO + 200, restored.getStreamRowCountBaseline(id).getBaselineOffset());
            restored.unprotectedUpdateStreamUpdate(new OlapTableStreamUpdate(ImmutableMap.of(id, START_TSO + 200),
                    ImmutableMap.of(id, START_TSO + 300)), 1000L, true);
            Assertions.assertNull(restored.getStreamRowCountBaseline(id));
            restored.unprotectedUpdateStreamUpdate(new OlapTableStreamUpdate(ImmutableMap.of(id, START_TSO + 300),
                    ImmutableMap.of(id, START_TSO + 400)), 2000L);
            Assertions.assertEquals(START_TSO + 400, restored.getStreamRowCountBaseline(id).getBaselineOffset());
        } finally {
            restored.writeUnlock();
        }
        Assertions.assertEquals(START_TSO, stream.getStreamRowCountBaseline(id).getBaselineOffset(),
                "independent streams must retain independent baselines");
    }

    @Test
    public void testSelectedPartitionsKeepTheirScaleWithInjectedStatistics() throws Exception {
        createTable("CREATE TABLE stream_partitioned (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                + "PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (10), "
                + "PARTITION p2 VALUES LESS THAN (MAXVALUE)) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1', 'changelog.enable'='true')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException("stream_partitioned");
        for (Partition partition : table.getPartitions()) {
            long rows = partition.getName().equals("p1") ? 1000L : 2000L;
            partition.getBaseIndex().setRowCount(rows);
            partition.getBaseIndex().setRowCountReported(true);
            partition.getIndex(table.getChangelogIndexMeta().getIndexId()).setRowCount(rows);
            partition.setVisibleVersionAndTime(2, System.currentTimeMillis(), START_TSO);
        }
        injectRowCount("stream_partitioned", 100_000_000L);
        createStream("CREATE STREAM stream_partitioned_s ON TABLE stream_partitioned "
                + "PROPERTIES ('show_initial_rows'='false')");
        for (Partition partition : table.getPartitions()) {
            long delta = partition.getName().equals("p1") ? 10L : 20L;
            partition.getBaseIndex().setRowCount(partition.getBaseIndex().getRowCount() + delta);
            partition.getIndex(table.getChangelogIndexMeta().getIndexId())
                    .setRowCount(partition.getBaseIndex().getRowCount());
            partition.setVisibleVersionAndTime(3, System.currentTimeMillis(), END_TSO);
            advanceReplicaVersions(partition);
        }
        Assertions.assertEquals(30, streamScanRowCount("select * from stream_partitioned_s", "stream_partitioned"));
        Assertions.assertEquals(10, streamScanRowCount("select * from stream_partitioned_s where k < 10",
                "stream_partitioned"));
        Assertions.assertEquals(1000, streamScanRowCount("select * from stream_partitioned_s@snapshot() where k < 10",
                "stream_partitioned"));
    }

    @Test
    public void testUniqueExpansionAndSnapshotReconstructionBranchScales() throws Exception {
        createTable("CREATE TABLE stream_unique (k INT NOT NULL, v INT) UNIQUE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'changelog.enable'='true', "
                + "'changelog.need_historical_value'='true')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException("stream_unique");
        Partition partition = table.getPartitions().iterator().next();
        partition.setVisibleVersionAndTime(2, System.currentTimeMillis(), START_TSO);
        partition.getBaseIndex().setRowCount(1000L);
        partition.getBaseIndex().setRowCountReported(true);
        partition.getIndex(table.getChangelogIndexMeta().getIndexId()).setRowCount(1000L);
        for (String type : new String[] {"APPEND_ONLY", "DETAIL", "MIN_DELTA"}) {
            createStream("CREATE STREAM unique_" + type + " ON TABLE stream_unique "
                    + "PROPERTIES ('show_initial_rows'='false', 'type'='" + type + "')");
        }
        partition.setVisibleVersionAndTime(3, System.currentTimeMillis(), END_TSO);
        advanceReplicaVersions(partition);
        // TTL removed 500 records while 700 were added: base growth supplies the larger delta.
        partition.getBaseIndex().setRowCount(1700L);
        partition.getIndex(table.getChangelogIndexMeta().getIndexId()).setRowCount(1200L);
        for (String type : new String[] {"APPEND_ONLY", "DETAIL", "MIN_DELTA"}) {
            Assertions.assertEquals(type.equals("APPEND_ONLY") ? 700L : 1400L,
                    streamScanRowCount("select * from unique_" + type, "stream_unique"));
        }
        PlanChecker.from(connectContext).checkExplain("select * from unique_MIN_DELTA@snapshot()", planner -> {
            List<PhysicalOlapScan> scans = new ArrayList<>();
            collectScans(planner.getOptimizedPlan(), scans);
            Assertions.assertEquals(2, scans.size());
            for (PhysicalOlapScan scan : scans) {
                Assertions.assertEquals(scan.getTable() instanceof OlapChangelogTableWrapper ? 1400L : 1700L,
                        (long) scan.getStats().getRowCount());
            }
        });
    }

    private static void advanceReplicaVersions(Partition partition) {
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true)) {
            index.getTablets().forEach(tablet -> tablet.getReplicas()
                    .forEach(replica -> replica.updateVersionForRestore(3L)));
        }
    }

}
