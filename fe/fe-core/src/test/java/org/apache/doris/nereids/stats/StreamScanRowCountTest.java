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
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.OlapTableWrapper.PartitionScanState;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStream.PartitionStreamState;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamUpdate.PartitionUpdate;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.TableStreamUpdateInfo;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.StreamConsumptionInfoExtractor;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
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
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    public void testRepeatedExplicitPartitionKeepsOneStreamScanEstimate() {
        Assertions.assertEquals(700L,
                streamScanRowCount("select * from stream_on_base partition(stream_base, stream_base)"));
    }

    @Test
    public void testBoundScanDoesNotWaitForMetadataWriterOrFollowLaterConsumption() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable base = (OlapTable) db.getTableOrMetaException("stream_base");
        OlapTableStream original = (OlapTableStream) db.getTableOrMetaException("stream_on_base");
        OlapTableStream stream = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(original), OlapTableStream.class);
        long id = base.getPartitionIds().get(0);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        stream.writeLock();
        try {
            OlapTableStreamWrapper wrapper = worker.submit(() -> {
                base.readLock();
                try {
                    return new OlapTableStreamWrapper(stream, base, List.of(id));
                } finally {
                    base.readUnlock();
                }
            }).get(5, TimeUnit.SECONDS);
            stream.unprotectedUpdateStreamUpdate(new OlapTableStreamUpdate(Map.of(
                    id, new PartitionUpdate(START_TSO, PartitionStreamState.initial().atOffset(END_TSO)))),
                    System.currentTimeMillis());
            Assertions.assertEquals(List.of(id), wrapper.filterIncrementalPartitionIds(List.of(id)));
            Assertions.assertEquals(Pair.of(START_TSO, END_TSO), wrapper.getOutputUpdate(id));
            Assertions.assertEquals(700L, wrapper.estimateChangelogRead(List.of(id),
                    Map.of(id, Pair.of(START_TSO + 1, END_TSO + 1)),
                    BaseTableStream.StreamScanType.APPEND_ONLY).get(id));
        } finally {
            stream.writeUnlock();
            worker.shutdownNow();
        }
    }

    @Test
    public void testNonemptyRangeWithStaleReportsUsesStockFallback() throws Exception {
        OlapTable base = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException("stream_base");
        Partition partition = base.getPartitions().iterator().next();
        MaterializedIndex changelog = partition.getIndex(base.getChangelogIndexMeta().getIndexId());
        long savedBase = partition.getBaseIndex().getRowCount();
        long savedChangelog = changelog.getRowCount();
        try {
            // The visible offset advanced, but reports still equal the creation/consumption baseline.
            partition.getBaseIndex().setRowCount(BASE_TABLE_ROW_COUNT);
            changelog.setRowCount(1000L);
            Assertions.assertEquals(1000L, streamScanRowCount("select * from stream_on_base"));
        } finally {
            partition.getBaseIndex().setRowCount(savedBase);
            changelog.setRowCount(savedChangelog);
        }
    }

    @Test
    public void testCloudReadStateMatchesOriginalOffsetBeforeTtl() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable base = (OlapTable) db.getTableOrMetaException("stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("stream_on_base");
        long id = base.getPartitionIds().get(0);
        OlapTableStream restored = new OlapTableStream(stream.getId(), stream.getName(), base);
        Cloud.TableStreamSourceEpochPB epoch = stream.getPartitionState(id).cloudSourceEpoch();
        for (boolean validEpoch : new boolean[] {true, false}) {
            OlapTableStreamWrapper wrapper = new OlapTableStreamWrapper(restored, base, base.getPartitionIds());
            wrapper.installCloudReadStates(ImmutableMap.of(id, Cloud.TableStreamPartitionReadStatePB.newBuilder()
                    .setPartitionId(id)
                    .setOffsetState(Cloud.TableStreamOffsetStatePB.TABLE_STREAM_OFFSET_CONSUMED)
                    .setOffsetTso(START_TSO + 50).setEndTso(END_TSO).setVisibleVersion(3L)
                    .setBaseRowCount(BASE_TABLE_ROW_COUNT).setChangelogRowCount(1000L)
                    .setSourceEpoch(validEpoch ? epoch : epoch.toBuilder()
                            .setPartitionEpoch(epoch.getPartitionEpoch() + 1).build()).build()));
            // The plan uses MS's baseline even without local stream state; TTL clipping only changes the scan range.
            Map<Long, Pair<Long, Long>> ranges = ImmutableMap.of(id, Pair.of(START_TSO + 80, END_TSO + 1));
            long rows = wrapper.estimateChangelogRead(base.getPartitionIds(), ranges,
                    BaseTableStream.StreamScanType.APPEND_ONLY).get(id);
            LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                    new OlapChangelogTableWrapper(base, Map.of(id,
                            new PartitionScanState(ranges.get(id), 3L, rows))));
            Assertions.assertEquals(validEpoch ? 700L : 1200L, new StatsCalculator(null).computeOlapScan(scan)
                    .getRowCount());
        }
    }

    @Test
    public void testRecoveredCatalogPreservesIncrementalScanEstimate() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable base = (OlapTable) db.getTableOrMetaException("stream_base");
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("stream_on_base");
        OlapTableStream restored = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(stream), OlapTableStream.class);
        OlapTable restoredBase = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(base), OlapTable.class);
        long id = base.getPartitionIds().get(0);
        Assertions.assertTrue(restored.getPartitionState(id).matches(restoredBase, id, START_TSO));
        OlapTableStreamWrapper wrapper = new OlapTableStreamWrapper(restored, restoredBase, List.of(id));
        Map<Long, Pair<Long, Long>> ranges = Map.of(id, Pair.of(START_TSO + 1, END_TSO + 1));
        long rows = wrapper.estimateChangelogRead(List.of(id), ranges, BaseTableStream.StreamScanType.APPEND_ONLY)
                .get(id);
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                new OlapChangelogTableWrapper(restoredBase, Map.of(id,
                        new PartitionScanState(ranges.get(id), null, rows))));
        Assertions.assertEquals(700L, new StatsCalculator(null).computeOlapScan(scan).getRowCount());
    }

    @Test
    public void testKnownPartitionEstimatesSkipWholeTableCounts() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable base = Mockito.spy((OlapTable) db.getTableOrMetaException("stream_base"));
        OlapTableStream stream = (OlapTableStream) db.getTableOrMetaException("stream_on_base");
        long id = base.getPartitionIds().get(0);
        OlapTableStreamWrapper wrapper = new OlapTableStreamWrapper(stream, base, List.of(id));
        Map<Long, Pair<Long, Long>> ranges = Map.of(id, Pair.of(START_TSO + 1, END_TSO + 1));
        Assertions.assertEquals(700L, wrapper.estimateChangelogRead(List.of(id), ranges,
                BaseTableStream.StreamScanType.APPEND_ONLY).get(id));
        Assertions.assertEquals(BASE_TABLE_ROW_COUNT, wrapper.estimateSnapshotRead(List.of(id)).get(id));
        Mockito.verify(base, Mockito.never()).getPartitions();

        OlapTable ordinary = Mockito.spy(createUnknownPartitionedTable("stream_no_whole_index_stats"));
        long partitionId = ordinary.getPartitionIds().get(0);
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(),
                new OlapTableWrapper(ordinary, Map.of(partitionId, new PartitionScanState(null, null, 25L))))
                .withSelectedPartitionIds(List.of(partitionId));
        Assertions.assertEquals(25L, new StatsCalculator(null).computeOlapScan(scan).getRowCount());
        Mockito.verify(ordinary, Mockito.never()).getRowCountForIndex(Mockito.anyLong(), Mockito.anyBoolean());
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

        String indexPartitionScan = "select * from stream_partitioned_s index stream_partitioned "
                + "partition(p1) where k < 10";
        long p1 = table.getPartition("p1").getId();
        Plan analyzed = PlanChecker.from(connectContext).analyze(indexPartitionScan).getPlan();
        List<TableStreamUpdateInfo> updates = StreamConsumptionInfoExtractor.extract(analyzed);
        Assertions.assertEquals(1, updates.size());
        OlapTableStreamUpdate update = (OlapTableStreamUpdate) updates.get(0).getUpdate();
        Assertions.assertEquals(Set.of(p1), update.getPartitionUpdates().keySet());
        PlanChecker.from(connectContext).checkExplain(indexPartitionScan, planner -> {
            List<PhysicalOlapScan> scans = new ArrayList<>();
            collectScans(planner.getOptimizedPlan(), scans);
            Assertions.assertEquals(1, scans.size());
            Assertions.assertEquals(List.of(p1), scans.get(0).getSelectedPartitionIds());
            Assertions.assertEquals(10, scans.get(0).getStats().getRowCount());
        });
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
        // The undo scan uses MIN_DELTA even when the stream's incremental mode is APPEND_ONLY.
        PlanChecker.from(connectContext).checkExplain("select * from unique_APPEND_ONLY@snapshot()", planner -> {
            List<PhysicalOlapScan> scans = new ArrayList<>();
            collectScans(planner.getOptimizedPlan(), scans);
            Assertions.assertEquals(2, scans.size());
            for (PhysicalOlapScan scan : scans) {
                Assertions.assertEquals(scan.getTable() instanceof OlapChangelogTableWrapper ? 1400L : 1700L,
                        (long) scan.getStats().getRowCount());
            }
        });
        // With a negative changelog delta, only scans that output deletes use the base decline.
        partition.getBaseIndex().setRowCount(900L);
        partition.getIndex(table.getChangelogIndexMeta().getIndexId()).setRowCount(400L);
        for (String type : new String[] {"APPEND_ONLY", "DETAIL", "MIN_DELTA"}) {
            Assertions.assertEquals(type.equals("APPEND_ONLY") ? 400L : 200L,
                    streamScanRowCount("select * from unique_" + type, "stream_unique"));
        }
    }

    @Test
    public void testLargeInjectedFallbackKeepsMowScanEstimatePositive() throws Exception {
        createTable("CREATE TABLE stream_large_stats (k INT NOT NULL, v INT) UNIQUE KEY(k) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1', "
                + "'enable_unique_key_merge_on_write'='true', 'changelog.enable'='true', "
                + "'changelog.need_historical_value'='true')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException("stream_large_stats");
        Partition partition = table.getPartitions().iterator().next();
        partition.setVisibleVersionAndTime(2, System.currentTimeMillis(), START_TSO);
        injectRowCount("stream_large_stats", 5_000_000_000_000_000_000L);
        createStream("CREATE STREAM large_stats_detail ON TABLE stream_large_stats "
                + "PROPERTIES ('show_initial_rows'='false', 'type'='DETAIL')");
        partition.setVisibleVersionAndTime(3, System.currentTimeMillis(), END_TSO);
        advanceReplicaVersions(partition);

        Assertions.assertEquals(Long.MAX_VALUE,
                streamScanRowCount("select * from large_stats_detail", "stream_large_stats"));
    }

    @Test
    public void testAllUnknownExplicitEstimatesPreserveJoinReorderInput() throws Exception {
        OlapTable table = createUnknownPartitionedTable("stream_unknown_stats");
        List<Long> ids = table.getPartitionIds();
        OlapTableWrapper wrapper = new OlapTableWrapper(table, Map.of(
                ids.get(0), new PartitionScanState(null, null, -1L),
                ids.get(1), new PartitionScanState(null, null, -1L),
                ids.get(2), new PartitionScanState(null, null, -1L)));
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), wrapper);
        Assertions.assertEquals(-1, StatsCalculator.getMaxTableRowCount(List.of(scan), null));
        Assertions.assertEquals(1, new StatsCalculator(null).computeOlapScan(scan).getRowCount());
        LogicalOlapScan pruned = scan.withSelectedPartitionIds(List.of(ids.get(1)));
        Assertions.assertEquals(-1, StatsCalculator.getMaxTableRowCount(List.of(pruned), null));
        Assertions.assertEquals(1, new StatsCalculator(null).computeOlapScan(pruned).getRowCount());
    }

    @Test
    public void testMixedExplicitEstimatesKeepKnownSelectedPartitions() throws Exception {
        OlapTable table = createUnknownPartitionedTable("stream_mixed_stats");
        List<Long> ids = table.getPartitionIds();
        OlapTableWrapper wrapper = new OlapTableWrapper(table, Map.of(
                ids.get(0), new PartitionScanState(null, null, 10L),
                ids.get(1), new PartitionScanState(null, null, -1L),
                ids.get(2), new PartitionScanState(null, null, -1L)));
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), wrapper)
                .withSelectedPartitionIds(List.of(ids.get(0), ids.get(1)));
        Assertions.assertEquals(11, StatsCalculator.getMaxTableRowCount(List.of(scan), null));
        Assertions.assertEquals(11, new StatsCalculator(null).computeOlapScan(scan).getRowCount());
        injectRowCount(table.getName(), 9000L);
        // Only the unknown selected partition uses the ordinary table's 9000 / 3 fallback.
        Assertions.assertEquals(3010, StatsCalculator.getMaxTableRowCount(List.of(scan), null));
        Assertions.assertEquals(3010, new StatsCalculator(null).computeOlapScan(scan).getRowCount());
    }

    @Test
    public void testTemporaryOnlyStreamPlanWithUnknownRows() throws Exception {
        createTable("CREATE TABLE stream_temp_base (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                + "PARTITION BY RANGE(k) () DISTRIBUTED BY HASH(k) BUCKETS 1 "
                + "PROPERTIES ('replication_num'='1', 'changelog.enable'='true')");
        createStream("CREATE STREAM stream_temp_source ON TABLE stream_temp_base "
                + "PROPERTIES ('show_initial_rows'='false')");
        executeSql("ALTER TABLE stream_temp_base ADD TEMPORARY PARTITION tp VALUES LESS THAN (10)");
        OlapTable base = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException("stream_temp_base");
        Partition temporary = base.getPartition("tp", true);
        temporary.setVisibleVersionAndTime(3L, System.currentTimeMillis(), END_TSO);
        advanceReplicaVersions(temporary);
        // Simulate a visible load before the first statistics report on the temporary partition.
        long changelogIndexId = base.getChangelogIndexMeta().getIndexId();
        temporary.getIndex(changelogIndexId).setRowCount(-1L);
        temporary.getBaseIndex().setRowCount(-1L);
        Assertions.assertEquals(0, base.getPartitionNum());
        Assertions.assertEquals(0L, base.getRowCountForIndex(changelogIndexId, true));

        long id = temporary.getId();
        OlapChangelogTableWrapper wrapper = new OlapChangelogTableWrapper(base, Map.of(id,
                new PartitionScanState(Pair.of(null, END_TSO + 1), null, -1L)));
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), wrapper)
                .withSelectedPartitionIds(List.of(id));
        Assertions.assertEquals(-1L, wrapper.getRowCountEstimate());
        Assertions.assertEquals(-1, StatsCalculator.getMaxTableRowCount(List.of(scan), null));
        Assertions.assertEquals(1L, streamScanRowCount("select * from stream_temp_source temporary partition(tp)",
                "stream_temp_base"));
    }

    private OlapTable createUnknownPartitionedTable(String name) throws Exception {
        createTable("CREATE TABLE " + name + " (k INT NOT NULL, v INT) DUPLICATE KEY(k) "
                + "PARTITION BY RANGE(k) (PARTITION p1 VALUES LESS THAN (10), "
                + "PARTITION p2 VALUES LESS THAN (20), PARTITION p3 VALUES LESS THAN (MAXVALUE)) "
                + "DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ('replication_num'='1')");
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException("test")
                .getTableOrMetaException(name);
        Assertions.assertEquals(-1, table.getRowCountForIndex(table.getBaseIndexId(), true));
        return table;
    }

    private static void advanceReplicaVersions(Partition partition) {
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL, true)) {
            index.getTablets().forEach(tablet -> tablet.getReplicas()
                    .forEach(replica -> replica.updateVersionForRestore(3L)));
        }
    }

}
