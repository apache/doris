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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Split planning driven entirely off recorded admin answers, which is the point: the states worth
 * asserting — a bucket nobody ever wrote to, a partition the engine pruned away, a lake table whose
 * tiering has not committed once — are a nuisance to produce on a live cluster and trivial here. The
 * cluster test alongside this one covers that the recorded answers match what a real cluster gives.
 */
public class FlussSplitPlanTest {

    private static final TablePath LOG_TABLE = TablePath.of("db", "log_tbl");
    private static final TablePath PK_TABLE = TablePath.of("db", "pk_tbl");

    /** Fixture shorthand: this bucket has never been snapshotted. */
    private static final long NO_SNAPSHOT = -1L;

    private RecordingFlussAdminOps adminOps;
    private ConnectorSession session;

    @BeforeEach
    public void setUp() {
        adminOps = new RecordingFlussAdminOps();
        session = new FlussTestSession(1L, "q1");
    }

    // ---------------------------------------------------------------- unpartitioned log table

    @Test
    public void everyNonEmptyBucketGetsARangeFromEarliestToTheOffsetPlanningSaw() {
        registerLogTable(LOG_TABLE, 3);
        latestOffsets(null, 10L, 25L, 7L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        Assertions.assertEquals(3, ranges.size());
        assertLogRange(ranges.get(0), 0, -2L, 10L);
        assertLogRange(ranges.get(1), 1, -2L, 25L);
        assertLogRange(ranges.get(2), 2, -2L, 7L);
    }

    /**
     * All of a partition's buckets stop at one view of the table. Taking the offsets per bucket instead
     * would let a bucket planned later include rows written after the query started.
     */
    @Test
    public void offsetsAreTakenOncePerPartition() {
        registerLogTable(LOG_TABLE, 4);
        latestOffsets(null, 1L, 1L, 1L, 1L);

        plan(LOG_TABLE, catalog());

        long offsetCalls = adminOps.calls.stream().filter(c -> c.startsWith("listOffsets(")).count();
        Assertions.assertEquals(1, offsetCalls, adminOps.calls.toString());
        Assertions.assertTrue(adminOps.calls.get(0).contains("[0, 1, 2, 3]"), adminOps.calls.toString());
        Assertions.assertTrue(adminOps.calls.get(0).contains("LatestSpec"), adminOps.calls.toString());
    }

    /** A bucket nobody has written to yields no range: an empty scanner would only cost a round trip. */
    @Test
    public void neverWrittenBucketsAreSkipped() {
        registerLogTable(LOG_TABLE, 3);
        latestOffsets(null, 0L, 12L, 0L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        Assertions.assertEquals(1, ranges.size());
        assertLogRange(ranges.get(0), 1, -2L, 12L);
    }

    @Test
    public void anEmptyTablePlansNothing() {
        registerLogTable(LOG_TABLE, 2);
        latestOffsets(null, 0L, 0L);

        Assertions.assertTrue(plan(LOG_TABLE, catalog()).isEmpty());
    }

    // ---------------------------------------------------------------- partitioned log table

    @Test
    public void eachPartitionContributesItsOwnBuckets() {
        registerPartitionedLogTable(2, "20260101", "20260102");
        latestOffsets("20260101", 5L, 6L);
        latestOffsets("20260102", 7L, 0L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        Assertions.assertEquals(3, ranges.size());
        assertPartition(ranges.get(0), "dt=20260101", 100L, 0, 5L);
        assertPartition(ranges.get(1), "dt=20260101", 100L, 1, 6L);
        assertPartition(ranges.get(2), "dt=20260102", 101L, 0, 7L);
    }

    @Test
    public void onlyThePartitionsTheEnginePrunedToAreScanned() {
        registerPartitionedLogTable(1, "20260101", "20260102", "20260103");
        latestOffsets("20260102", 9L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog(),
                Collections.singletonList("dt=20260102"));

        Assertions.assertEquals(1, ranges.size());
        assertPartition(ranges.get(0), "dt=20260102", 101L, 0, 9L);
        // The other two partitions must not even be asked for their offsets.
        Assertions.assertEquals(1,
                adminOps.calls.stream().filter(c -> c.startsWith("listOffsets(")).count(),
                adminOps.calls.toString());
    }

    /**
     * The pruned names the engine hands back are the ones the metadata listing produced, so planning has
     * to render partition names exactly the same way. Rendering them separately is how "prune to one
     * partition, then match nothing and scan zero rows" happens, and it looks like an empty table.
     */
    @Test
    public void plannedPartitionNamesMatchTheOnesPruningSaw() {
        registerPartitionedLogTable(1, "20260101", "20260102");
        latestOffsets("20260101", 3L);
        latestOffsets("20260102", 3L);
        ConnectorTableHandle handle = handle(LOG_TABLE);

        List<String> listedNames = new ArrayList<>();
        for (ConnectorPartitionInfo partition
                : metadata().listPartitions(session, handle, Optional.empty())) {
            listedNames.add(partition.getPartitionName());
        }
        // Feeding the listing's own names back as the pruned set must select every partition.
        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog(), listedNames);

        Assertions.assertEquals(Arrays.asList("dt=20260101", "dt=20260102"), listedNames);
        Assertions.assertEquals(2, ranges.size());
    }

    /** Dropped between pruning and planning: nothing left to read, and nothing to fail about. */
    @Test
    public void prunedPartitionThatNoLongerExistsIsSkipped() {
        registerPartitionedLogTable(1, "20260101");
        latestOffsets("20260101", 3L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog(),
                Arrays.asList("dt=20260101", "dt=19990101"));

        Assertions.assertEquals(1, ranges.size());
        assertPartition(ranges.get(0), "dt=20260101", 100L, 0, 3L);
    }

    @Test
    public void partitionColumnsAreDeclaredToTheEngineNotReadBackFromTheScanner() {
        registerPartitionedLogTable(1, "20260101");

        Map<String, String> props = nodeProperties(LOG_TABLE, catalog());

        Assertions.assertEquals("dt", props.get(ScanNodePropertyKeys.PATH_PARTITION_KEYS));
    }

    @Test
    public void anUnpartitionedTableDeclaresNoPartitionKeys() {
        registerLogTable(LOG_TABLE, 1);

        Map<String, String> props = nodeProperties(LOG_TABLE, catalog());

        Assertions.assertFalse(props.containsKey(ScanNodePropertyKeys.PATH_PARTITION_KEYS));
    }

    // ---------------------------------------------------------------- primary-key table

    /**
     * A primary-key bucket is read as "the kv snapshot, then the change log that followed it". The
     * starting offset therefore has to be the one the snapshot ended at: starting anywhere earlier
     * replays changes the snapshot already contains, and anywhere later drops changes it does not.
     */
    @Test
    public void primaryKeyBucketsAreReadFromTheirSnapshotForward() {
        registerPkTable(PK_TABLE, 3);
        kvSnapshots(null, new long[] {4L, 9L, 2L}, new long[] {40L, 90L, 20L});
        latestOffsets(null, 55L, 90L, 31L);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(3, ranges.size());
        assertPkRange(ranges.get(0), 0, 4L, 40L, 55L);
        // Nothing new since the snapshot: still planned, and the scanner reads the snapshot alone.
        assertPkRange(ranges.get(1), 1, 9L, 90L, 90L);
        assertPkRange(ranges.get(2), 2, 2L, 20L, 31L);
    }

    /**
     * With no snapshot the whole state is rebuilt by replaying the change log from the beginning,
     * which is correct because a primary-key table's log carries every change. The two facts that say
     * so — {@code -1} and the earliest sentinel — travel separately, and a bucket that got one without
     * the other would read a snapshot that does not exist or start from the wrong place.
     */
    @Test
    public void bucketsWithoutSnapshotsReplayTheirWholeChangeLog() {
        registerPkTable(PK_TABLE, 2);
        kvSnapshots(null, new long[] {NO_SNAPSHOT, 3L}, new long[] {0L, 30L});
        latestOffsets(null, 12L, 44L);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        assertPkRange(ranges.get(0), 0, -1L, -2L, 12L);
        assertPkRange(ranges.get(1), 1, 3L, 30L, 44L);
    }

    /** Neither snapshotted nor written to: nothing to read, so nothing for BE to open a scanner for. */
    @Test
    public void neverWrittenPrimaryKeyBucketsAreSkipped() {
        registerPkTable(PK_TABLE, 3);
        kvSnapshots(null, new long[] {NO_SNAPSHOT, 7L, NO_SNAPSHOT}, new long[] {0L, 70L, 0L});
        latestOffsets(null, 0L, 88L, 5L);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        assertPkRange(ranges.get(0), 1, 7L, 70L, 88L);
        assertPkRange(ranges.get(1), 2, -1L, -2L, 5L);
    }

    /**
     * A bucket the offsets answer says nothing about still has to be read when it has a snapshot: the
     * snapshot holds rows regardless of what the log is doing. Skipping on "no stopping offset" alone
     * would drop them, and the query would succeed with a bucket's worth of rows missing.
     */
    @Test
    public void snapshottedBucketMissingFromTheOffsetsAnswerIsStillRead() {
        registerPkTable(PK_TABLE, 2);
        kvSnapshots(null, new long[] {6L, NO_SNAPSHOT}, new long[] {60L, 0L});
        Map<Integer, Long> partialOffsets = new LinkedHashMap<>();
        partialOffsets.put(1, 9L);
        adminOps.latestOffsetsByPartition.put(null, partialOffsets);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        // No log to read, so the range covers the snapshot alone.
        assertPkRange(ranges.get(0), 0, 6L, 60L, 0L);
        assertPkRange(ranges.get(1), 1, -1L, -2L, 9L);
    }

    /**
     * Snapshots are asked for BEFORE offsets, and the order is the whole point. A snapshot committed
     * between the two calls ends past the offset planning stopped at; that bucket would then be read
     * from its snapshot — which already contains rows written after the query started — while every
     * other bucket stopped where planning saw them. Log offsets only move forward, so asking in this
     * order keeps every snapshot at or behind the stopping offset.
     */
    @Test
    public void snapshotsAreAskedForBeforeTheOffsetsThatBoundThem() {
        registerPkTable(PK_TABLE, 1);
        kvSnapshots(null, new long[] {1L}, new long[] {10L});
        latestOffsets(null, 20L);

        plan(PK_TABLE, catalog());

        int snapshotCall = indexOfCall("getLatestKvSnapshots(");
        int offsetCall = indexOfCall("listOffsets(");
        Assertions.assertTrue(snapshotCall >= 0 && snapshotCall < offsetCall, adminOps.calls.toString());
    }

    @Test
    public void partitionedPrimaryKeyTablesTakeSnapshotsAndOffsetsPerPartition() {
        registerPartitionedPkTable(1, "20260101", "20260102");
        kvSnapshots("20260101", new long[] {5L}, new long[] {50L});
        latestOffsets("20260101", 60L);
        kvSnapshots("20260102", new long[] {NO_SNAPSHOT}, new long[] {0L});
        latestOffsets("20260102", 8L);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        Assertions.assertEquals("dt=20260101", ranges.get(0).getProperties().get("fluss.partition_name"));
        assertPkRange(ranges.get(0), 0, 5L, 50L, 60L);
        Assertions.assertEquals("dt=20260102", ranges.get(1).getProperties().get("fluss.partition_name"));
        assertPkRange(ranges.get(1), 0, -1L, -2L, 8L);
    }

    /** Pruning applies to a primary-key table exactly as it does to a log table. */
    @Test
    public void prunedPartitionsOfPrimaryKeyTablesAreNotEvenAskedAbout() {
        registerPartitionedPkTable(1, "20260101", "20260102");
        kvSnapshots("20260102", new long[] {2L}, new long[] {20L});
        latestOffsets("20260102", 25L);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog(),
                Collections.singletonList("dt=20260102"));

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(1,
                adminOps.calls.stream().filter(c -> c.startsWith("getLatestKvSnapshots(")).count(),
                adminOps.calls.toString());
    }

    // ---------------------------------------------------------------- what is refused, and why

    /**
     * A tiered primary-key table is refused for the same reason a tiered log table is: the fluss-only
     * read returns whatever has not been tiered away yet, which is a successful query with missing rows.
     * The primary-key read being implemented does not change that.
     */
    @Test
    public void tieredPrimaryKeyTableIsRefusedUntilTheUnionReadExists() {
        adminOps.tableInfos.put(PK_TABLE, FlussTestTables.builder(PK_TABLE)
                .column("id", DataTypes.INT().copy(false))
                .column("v", DataTypes.STRING())
                .primaryKey("id")
                .buckets(2, "id")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .build());
        adminOps.readableLakeSnapshot = new LakeSnapshot(7L, Collections.emptyMap());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog()));
        Assertions.assertTrue(e.getMessage().contains("not supported yet"), e.getMessage());
    }

    /**
     * A lake table read as fluss-only returns only what the log still holds, dropping everything tiering
     * has moved into the lake — a query that succeeds with missing rows. Until the union read exists,
     * the refusal is the correct answer.
     */
    @Test
    public void tieredLakeTableIsRefusedUntilTheUnionReadExists() {
        registerLakeTable(2);
        adminOps.readableLakeSnapshot = new LakeSnapshot(7L, Collections.emptyMap());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(LOG_TABLE, catalog()));
        Assertions.assertTrue(e.getMessage().contains("not supported yet"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains(FlussConnectorProperties.UNION_READ_MODE),
                e.getMessage());
    }

    /** Nothing in the lake means the log holds the whole table, so the fluss-only read IS complete. */
    @Test
    public void lakeTableThatHasNeverTieredFallsBackToTheLogAlone() {
        registerLakeTable(2);
        latestOffsets(null, 4L, 4L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
    }

    @Test
    public void requiredModeRefusesTheFallbackSoATestCannotPassByAccident() {
        registerLakeTable(2);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(LOG_TABLE, catalog(FlussConnectorProperties.UNION_READ_MODE, "required")));
        Assertions.assertTrue(e.getMessage().contains("no readable lake snapshot"), e.getMessage());
    }

    /** Disabled is the user asking for the fluss-only read on purpose; the lake is not even consulted. */
    @Test
    public void disabledModeReadsTheLogWithoutAskingAboutTheLake() {
        registerLakeTable(2);
        latestOffsets(null, 4L, 4L);

        List<ConnectorScanRange> ranges =
                plan(LOG_TABLE, catalog(FlussConnectorProperties.UNION_READ_MODE, "disabled"));

        Assertions.assertEquals(2, ranges.size());
        Assertions.assertTrue(adminOps.calls.stream().noneMatch(c -> c.startsWith("getReadableLakeSnapshot")),
                adminOps.calls.toString());
    }

    // ---------------------------------------------------------------- what BE and EXPLAIN receive

    @Test
    public void scanLevelParamsCarryTheClientConfigAndTableIdentity() {
        registerLogTable(LOG_TABLE, 1);
        Map<String, String> catalog = catalog();
        catalog.put("fluss.client.writer.batch-size", "2mb");

        TFileScanRangeParams params = new TFileScanRangeParams();
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, catalog);
        provider.populateScanLevelParams(params, nodeProperties(LOG_TABLE, catalog));

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("fluss.db_name", "db");
        expected.put("fluss.table_name", "log_tbl");
        expected.put("fluss.client.bootstrap.servers", "localhost:9123");
        expected.put("fluss.client.client.writer.batch-size", "2mb");
        Assertions.assertEquals(expected, params.getFlussProperties());
    }

    /** The engine's own keys are not the scanner's; forwarding them wholesale would be noise at best. */
    @Test
    public void scanLevelParamsDropTheEngineOnlyKeys() {
        registerPartitionedLogTable(1, "20260101");
        Map<String, String> nodeProps = new LinkedHashMap<>(nodeProperties(LOG_TABLE, catalog()));
        nodeProps.put(ScanNodePropertyKeys.SYNTHETIC_TOTAL_READ_SPLITS, "3");

        TFileScanRangeParams params = new TFileScanRangeParams();
        new FlussScanPlanProvider(adminOps, catalog()).populateScanLevelParams(params, nodeProps);

        Assertions.assertFalse(params.getFlussProperties()
                .containsKey(ScanNodePropertyKeys.PATH_PARTITION_KEYS));
        Assertions.assertFalse(params.getFlussProperties()
                .containsKey(ScanNodePropertyKeys.SYNTHETIC_TOTAL_READ_SPLITS));
    }

    /**
     * {@code auto} falls back to a fluss-only read without saying so anywhere else in the plan, so a
     * union-read regression test has nothing to assert on but this line.
     */
    @Test
    public void explainReportsHowTheScanWasActuallyPlanned() {
        registerLogTable(LOG_TABLE, 3);
        latestOffsets(null, 1L, 0L, 5L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, catalog());
        provider.planScan(session, request(handle(LOG_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "  ", Collections.emptyMap());

        Assertions.assertEquals(
                "  flussScan: unionRead=no, lakeSplits=0, logRanges=2, pkRanges=0, mode=auto\n",
                output.toString());
    }

    /**
     * Counted apart from log ranges rather than lumped together: the two are read by different code
     * paths, and a test that only sees a total cannot tell a primary-key table that was planned the
     * wrong way from one that was planned the right way.
     */
    @Test
    public void explainCountsPrimaryKeyRangesApartFromLogRanges() {
        registerPkTable(PK_TABLE, 3);
        kvSnapshots(null, new long[] {1L, NO_SNAPSHOT, NO_SNAPSHOT}, new long[] {10L, 0L, 0L});
        latestOffsets(null, 12L, 4L, 0L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, catalog());
        provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertEquals(
                "flussScan: unionRead=no, lakeSplits=0, logRanges=0, pkRanges=2, mode=auto\n",
                output.toString());
    }

    @Test
    public void explainReportsTheConfiguredMode() {
        registerLogTable(LOG_TABLE, 1);
        latestOffsets(null, 1L);
        Map<String, String> catalog = catalog(FlussConnectorProperties.UNION_READ_MODE, "disabled");
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, catalog);
        provider.planScan(session, request(handle(LOG_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertTrue(output.toString().contains("mode=disabled"), output.toString());
    }

    // ---------------------------------------------------------------- helpers

    private Map<String, String> catalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put(FlussConnectorProperties.BOOTSTRAP_SERVERS, "localhost:9123");
        return properties;
    }

    private Map<String, String> catalog(String key, String value) {
        Map<String, String> properties = catalog();
        properties.put(key, value);
        return properties;
    }

    private FlussConnectorMetadata metadata() {
        return new FlussConnectorMetadata(adminOps, new FlussTypeMapping.Options(false, false),
                properties -> {
                    throw new AssertionError("no lake sibling is expected in this test");
                },
                handle -> null);
    }

    private ConnectorTableHandle handle(TablePath tablePath) {
        return FlussTableHandle.of(adminOps.tableInfos.get(tablePath));
    }

    private List<ConnectorScanRange> plan(TablePath tablePath, Map<String, String> catalogProperties) {
        return plan(tablePath, catalogProperties, Collections.emptyList());
    }

    private List<ConnectorScanRange> plan(TablePath tablePath, Map<String, String> catalogProperties,
            List<String> requiredPartitions) {
        return new FlussScanPlanProvider(adminOps, catalogProperties)
                .planScan(session, request(handle(tablePath), requiredPartitions));
    }

    private Map<String, String> nodeProperties(TablePath tablePath, Map<String, String> catalogProperties) {
        return new FlussScanPlanProvider(adminOps, catalogProperties).getScanNodeProperties(
                session, handle(tablePath), Collections.emptyList(), Optional.empty());
    }

    private static ConnectorScanRequest request(ConnectorTableHandle handle, List<String> requiredPartitions) {
        return ConnectorScanRequest.builder(handle, Collections.emptyList())
                .requiredPartitions(requiredPartitions)
                .build();
    }

    private void registerLogTable(TablePath tablePath, int buckets) {
        adminOps.tableInfos.put(tablePath, FlussTestTables.builder(tablePath)
                .column("id", DataTypes.INT())
                .column("v", DataTypes.STRING())
                .buckets(buckets)
                .build());
    }

    /** A log table partitioned by {@code dt}, with partition ids 100, 101, ... in the order given. */
    private void registerPartitionedLogTable(int buckets, String... partitionValues) {
        adminOps.tableInfos.put(LOG_TABLE, FlussTestTables.builder(LOG_TABLE)
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .partitionedBy("dt")
                .buckets(buckets)
                .build());
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionValues.length; i++) {
            partitions.add(new PartitionInfo(100L + i,
                    ResolvedPartitionSpec.fromPartitionValue("dt", partitionValues[i]), null));
        }
        adminOps.partitionsByTable.put(LOG_TABLE, partitions);
    }

    private void registerLakeTable(int buckets) {
        adminOps.tableInfos.put(LOG_TABLE, FlussTestTables.builder(LOG_TABLE)
                .column("id", DataTypes.INT())
                .buckets(buckets)
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .build());
    }

    private void registerPkTable(TablePath tablePath, int buckets) {
        adminOps.tableInfos.put(tablePath, FlussTestTables.builder(tablePath)
                .column("id", DataTypes.INT().copy(false))
                .column("v", DataTypes.STRING())
                .primaryKey("id")
                .buckets(buckets, "id")
                .build());
    }

    /** A primary-key table partitioned by {@code dt}, with partition ids 100, 101, ... in order. */
    private void registerPartitionedPkTable(int buckets, String... partitionValues) {
        adminOps.tableInfos.put(PK_TABLE, FlussTestTables.builder(PK_TABLE)
                .column("id", DataTypes.INT().copy(false))
                .column("dt", DataTypes.STRING().copy(false))
                .primaryKey("id", "dt")
                .partitionedBy("dt")
                .buckets(buckets, "id")
                .build());
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionValues.length; i++) {
            partitions.add(new PartitionInfo(100L + i,
                    ResolvedPartitionSpec.fromPartitionValue("dt", partitionValues[i]), null));
        }
        adminOps.partitionsByTable.put(PK_TABLE, partitions);
    }

    /**
     * Latest kv snapshot per bucket 0..n-1 of {@code partitionName} ({@code null} = unpartitioned);
     * {@link #NO_SNAPSHOT} for a bucket that has never been snapshotted. Such a bucket gets a null
     * snapshot id AND a null log offset, which is the only shape a cluster produces — the client
     * asserts the two are both set or both absent when it decodes the response.
     */
    private void kvSnapshots(String partitionName, long[] snapshotIds, long[] logOffsets) {
        Map<Integer, Long> ids = new LinkedHashMap<>();
        Map<Integer, Long> offsets = new LinkedHashMap<>();
        for (int bucket = 0; bucket < snapshotIds.length; bucket++) {
            boolean snapshotted = snapshotIds[bucket] != NO_SNAPSHOT;
            ids.put(bucket, snapshotted ? snapshotIds[bucket] : null);
            offsets.put(bucket, snapshotted ? logOffsets[bucket] : null);
        }
        adminOps.kvSnapshotsByPartition.put(partitionName, new KvSnapshots(1L, null, ids, offsets));
    }

    /** Latest offsets for buckets 0..n-1 of {@code partitionName} ({@code null} = unpartitioned). */
    private void latestOffsets(String partitionName, long... offsets) {
        Map<Integer, Long> byBucket = new LinkedHashMap<>();
        for (int bucket = 0; bucket < offsets.length; bucket++) {
            byBucket.put(bucket, offsets[bucket]);
        }
        adminOps.latestOffsetsByPartition.put(partitionName, byBucket);
    }

    private static void assertLogRange(ConnectorScanRange range, int bucket, long start, long stop) {
        Map<String, String> props = range.getProperties();
        Assertions.assertEquals("LOG", props.get("fluss.range_type"));
        Assertions.assertEquals(String.valueOf(bucket), props.get("fluss.bucket_id"));
        Assertions.assertEquals(String.valueOf(start), props.get("fluss.log_start_offset"));
        Assertions.assertEquals(String.valueOf(stop), props.get("fluss.log_stop_offset"));
    }

    private static void assertPkRange(ConnectorScanRange range, int bucket, long snapshotId,
            long start, long stop) {
        Map<String, String> props = range.getProperties();
        Assertions.assertEquals("PK_FULL", props.get("fluss.range_type"));
        Assertions.assertEquals(String.valueOf(bucket), props.get("fluss.bucket_id"));
        Assertions.assertEquals(String.valueOf(snapshotId), props.get("fluss.kv_snapshot_id"));
        Assertions.assertEquals(String.valueOf(start), props.get("fluss.log_start_offset"));
        Assertions.assertEquals(String.valueOf(stop), props.get("fluss.log_stop_offset"));
    }

    /** Position of the first recorded call starting with {@code prefix}, or -1. */
    private int indexOfCall(String prefix) {
        for (int i = 0; i < adminOps.calls.size(); i++) {
            if (adminOps.calls.get(i).startsWith(prefix)) {
                return i;
            }
        }
        return -1;
    }

    private static void assertPartition(ConnectorScanRange range, String partitionName,
            long partitionId, int bucket, long stop) {
        Map<String, String> props = range.getProperties();
        Assertions.assertEquals(partitionName, props.get("fluss.partition_name"));
        Assertions.assertEquals(String.valueOf(partitionId), props.get("fluss.partition_id"));
        assertLogRange(range, bucket, -2L, stop);
    }

}
