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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;
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
import java.util.Set;

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

    /**
     * The lake sibling, built only when a test asks for it. A test that never registers a lake table must
     * never reach it, and {@link #lakeSibling} fails loud if it does — a fluss-only plan that quietly
     * consulted the lake would still look correct in every other assertion.
     */
    private RecordingLakeSibling sibling;
    private boolean siblingExpected;

    @BeforeEach
    public void setUp() {
        adminOps = new RecordingFlussAdminOps();
        session = new FlussTestSession(1L, "q1");
        sibling = null;
        siblingExpected = false;
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

    // ------------------------------------------- tiered primary-key tables: read from fluss alone

    /**
     * A primary-key table whose tiering has never committed is read from fluss alone, and what replaces the
     * union is NOT the "whatever has not been tiered away" fallback a log table would get: fluss keeps such
     * a table's state in full, so its kv snapshot plus the log after it is every row.
     */
    @Test
    public void tieredPrimaryKeyTableWithoutALakeSnapshotIsReadFromFlussAlone() {
        registerTieredPkTable(2);
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 12L, 25L);

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        assertPkRange(ranges.get(0), 0, 4L, 10L, 12L);
        assertPkRange(ranges.get(1), 1, 5L, 20L, 25L);
    }

    /**
     * The fallback is the very read {@code disabled} asks for outright, down to the ranges — not a third
     * code path that happens to look similar.
     */
    @Test
    public void theFallbackIsTheReadDisabledModeAsksForOutright() {
        registerTieredPkTable(2);
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 12L, 25L);

        List<Map<String, String>> auto = rangeProperties(plan(PK_TABLE, catalog()));
        List<Map<String, String>> disabled = rangeProperties(
                plan(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "disabled")));

        Assertions.assertEquals(auto, disabled);
    }

    /**
     * {@code required} is what a regression test sets so that it cannot pass without the lake having been
     * read, so a primary-key table whose tiering has not committed fails loud rather than falling back —
     * the same answer a log table gets, for the same reason.
     */
    @Test
    public void requiredModeRefusesAPrimaryKeyTableWithNothingInItsLake() {
        registerTieredPkTable(2);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "required")));
        Assertions.assertTrue(e.getMessage().contains("no readable lake snapshot yet"), e.getMessage());
    }

    /**
     * The plan's own account of not having read the lake. Nothing else in the plan of a tiered primary-key
     * table distinguishes it from one that was read as a union.
     */
    @Test
    public void explainShowsATieredPrimaryKeyTableWasReadWithoutItsLake() {
        registerTieredPkTable(1);
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 12L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);
        provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertEquals("flussScan: unionRead=no, lakeSplits=0, suppressedLakeSplits=0,"
                + " logRanges=0, pkRanges=1, pkTailRanges=0, mode=auto\n", output.toString());
    }


    // ---------------------------------------------------------------- union read: lake + log

    /**
     * The two halves have to MEET: the lake holds everything up to the offset its snapshot recorded, and
     * the log range starts at exactly that offset. One off in either direction is a wrong answer that no
     * row count catches — reading the log from the earliest offset instead duplicates every tiered row,
     * and starting one later drops one.
     */
    @Test
    public void theLogIsReadFromWhereTheLakeSnapshotLeftOff() {
        registerLakeTable(2);
        lakeSnapshotAt(7L, offsets(5L, 3L));
        latestOffsets(null, 9L, 3L);
        lakeRanges(2);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        // Two lake ranges, then the one bucket whose log went past the snapshot. Bucket 1's log has not
        // moved since it was tiered, so everything it holds is already in the lake and it yields nothing.
        Assertions.assertEquals(3, ranges.size());
        assertLogRange(ranges.get(2), 0, 5L, 9L);
    }

    /**
     * A bucket the lake snapshot never mentions has never been tiered, so nothing of it is in the lake
     * and its whole log is still the truth. Starting it at the snapshot's (absent) offset would be the
     * bug: there is no offset to start at, and treating that as zero rows would drop the bucket.
     */
    @Test
    public void bucketTheLakeNeverSawIsReadFromTheEarliestOffset() {
        registerLakeTable(2);
        lakeSnapshotAt(7L, offsets(4L, null));
        latestOffsets(null, 6L, 8L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        assertLogRange(ranges.get(0), 0, 4L, 6L);
        assertLogRange(ranges.get(1), 1, -2L, 8L);
    }

    /** A bucket whose log has not moved past the lake needs no log range at all. */
    @Test
    public void bucketTheLakeHasCaughtUpWithYieldsNoLogRange() {
        registerLakeTable(2);
        lakeSnapshotAt(7L, offsets(6L, 6L));
        latestOffsets(null, 6L, 6L);

        Assertions.assertTrue(plan(LOG_TABLE, catalog()).isEmpty());
    }

    /**
     * The lake is pinned to the snapshot FLUSS says is readable, not to whatever the lake calls latest.
     * Unpinned, the lake half would drift ahead of the offsets the log half stops at and the rows in
     * between would be read twice.
     */
    @Test
    public void theLakeIsPinnedToTheSnapshotFlussReportsAsReadable() {
        registerLakeTable(1);
        lakeSnapshotAt(7L, offsets(2L));
        latestOffsets(null, 5L);

        plan(LOG_TABLE, catalog());

        Assertions.assertEquals(7L, sibling.plannedHandle.pinnedSnapshotId);
        // The pin is expressed in the SPI's terms only: a snapshot id and no connector-specific options.
        Assertions.assertTrue(sibling.calls.contains("applySnapshot:7:{}"), sibling.calls.toString());
    }

    /**
     * The lake snapshot must be read BEFORE the log's stopping offsets. Log offsets only move forward, so
     * asking in this order keeps the snapshot at or behind where the log half stops; asked the other way
     * round, a snapshot committed in between would cover rows past that point and the bucket's log range
     * would start after it ended.
     */
    @Test
    public void theLakeSnapshotIsReadBeforeTheStoppingOffsets() {
        registerLakeTable(1);
        lakeSnapshotAt(7L, offsets(2L));
        latestOffsets(null, 5L);

        plan(LOG_TABLE, catalog());

        int snapshotCall = indexOfCall("getReadableLakeSnapshot");
        int offsetsCall = indexOfCall("listOffsets");
        Assertions.assertTrue(snapshotCall >= 0 && snapshotCall < offsetsCall, adminOps.calls.toString());
    }

    /**
     * The lake half is planned on the LAKE's own column handles. The sibling projects by its own handle
     * type and ignores anything else, so handing it fluss's handles would leave it reading every column —
     * including the three system columns tiering appends — and no assertion about rows would notice.
     */
    @Test
    public void theLakeHalfIsPlannedOnTheLakeTablesOwnColumnHandles() {
        registerLakeTable(1);
        lakeTableColumns("id", "__bucket", "__offset", "__timestamp");
        lakeSnapshotAt(7L, offsets(0L));
        latestOffsets(null, 5L);

        new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()), this::lakeSibling).planScan(session,
                ConnectorScanRequest.builder(handle(LOG_TABLE),
                        Collections.singletonList(new FlussColumnHandle("id", 0))).build());

        Assertions.assertEquals(
                Collections.singletonList(new RecordingLakeSibling.LakeColumn("id")),
                sibling.plannedColumns);
    }

    /** A column the lake table does not have means the two schemas are not the same table's. */
    @Test
    public void columnMissingFromTheLakeTableIsRefused() {
        registerLakeTable(1);
        lakeTableColumns("id");
        lakeSnapshotAt(7L, offsets(0L));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                        this::lakeSibling).planScan(session,
                        ConnectorScanRequest.builder(handle(LOG_TABLE),
                                Collections.singletonList(new FlussColumnHandle("gone", 1))).build()));
        Assertions.assertTrue(e.getMessage().contains("does not exist in its lake table"), e.getMessage());
    }

    /**
     * One scan node, one property map: the engine calls {@code populateScanLevelParams} once, so the lake
     * half's entries have to travel in the same map or its ranges arrive at BE unreadable.
     */
    @Test
    public void nodePropertiesCarryBothHalves() {
        registerLakeTable(1);
        lakeSnapshotAt(7L, offsets(0L));
        sibling(); // built up-front so the node properties can be set before they are asked for
        sibling.lakeNodeProperties = Collections.singletonMap("paimon.serialized_table", "encoded");

        Map<String, String> props = nodeProperties(LOG_TABLE, catalog());

        Assertions.assertEquals("db", props.get("fluss.db_name"));
        Assertions.assertEquals("encoded", props.get("paimon.serialized_table"));
    }

    /** The lake half gets its turn at the same thrift params, and only it knows which entries are its. */
    @Test
    public void scanLevelParamsAreOfferedToTheLakeHalfToo() {
        registerLakeTable(1);
        lakeSnapshotAt(7L, offsets(0L));
        sibling();
        sibling.lakeNodeProperties = Collections.singletonMap("paimon.serialized_table", "encoded");
        Map<String, String> catalog = catalog();
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog),
                this::lakeSibling);
        Map<String, String> props = provider.getScanNodeProperties(
                session, handle(LOG_TABLE), Collections.emptyList(), Optional.empty());

        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, props);

        // fluss took only its own keys; the sibling saw the whole map, its own entries included.
        Assertions.assertFalse(params.getFlussProperties().containsKey("paimon.serialized_table"));
        Assertions.assertEquals("encoded",
                sibling.populatedNodeProperties.get("paimon.serialized_table"));
    }

    /**
     * The split between file columns and partition columns is decided ONCE for the scan node, so two
     * halves that disagree about it would read different columns out of the same tuple. They cannot
     * legitimately disagree, which is why a disagreement is raised rather than resolved by picking one.
     */
    @Test
    public void halvesThatDisagreeAboutTheSharedPropertiesAreRefused() {
        registerPartitionedLakeTable(1, "20260101");
        lakeSnapshotAt(7L, offsets(0L));
        sibling();
        sibling.lakeNodeProperties =
                Collections.singletonMap(ScanNodePropertyKeys.PATH_PARTITION_KEYS, "not_dt");

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> nodeProperties(LOG_TABLE, catalog()));
        Assertions.assertTrue(e.getMessage().contains(ScanNodePropertyKeys.PATH_PARTITION_KEYS),
                e.getMessage());
    }

    /** A partitioned table's lake offsets are per (partition, bucket), not per bucket. */
    @Test
    public void partitionedTablesLogIsResumedPerPartitionAndBucket() {
        registerPartitionedLakeTable(1, "20260101", "20260102");
        Map<TableBucket, Long> lakeOffsets = new HashMap<>();
        lakeOffsets.put(new TableBucket(FlussTestTables.TABLE_ID, 100L, 0), 4L);
        lakeOffsets.put(new TableBucket(FlussTestTables.TABLE_ID, 101L, 0), 1L);
        lakeSnapshotAt(7L, lakeOffsets);
        latestOffsets("20260101", 9L);
        latestOffsets("20260102", 6L);

        List<ConnectorScanRange> ranges = plan(LOG_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        assertLogRange(ranges.get(0), 0, 4L, 9L);
        assertLogRange(ranges.get(1), 0, 1L, 6L);
    }

    /** The line a union-read regression test reads to know the lake was actually part of the answer. */
    @Test
    public void explainReportsTheUnionAndTheSizeOfEachHalf() {
        registerLakeTable(2);
        lakeSnapshotAt(7L, offsets(1L, 9L));
        latestOffsets(null, 5L, 9L);
        lakeRanges(3);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);
        provider.planScan(session, request(handle(LOG_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertEquals(
                "flussScan: unionRead=yes, lakeSplits=3, suppressedLakeSplits=0, logRanges=1,"
                + " pkRanges=0, pkTailRanges=0, mode=auto\n", output.toString());
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
                () -> plan(LOG_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "required")));
        Assertions.assertTrue(e.getMessage().contains("no readable lake snapshot"), e.getMessage());
    }

    /** Disabled is the user asking for the fluss-only read on purpose; the lake is not even consulted. */
    @Test
    public void disabledModeReadsTheLogWithoutAskingAboutTheLake() {
        registerLakeTable(2);
        latestOffsets(null, 4L, 4L);

        List<ConnectorScanRange> ranges =
                plan(LOG_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "disabled"));

        Assertions.assertEquals(2, ranges.size());
        Assertions.assertTrue(adminOps.calls.stream().noneMatch(c -> c.startsWith("getReadableLakeSnapshot")),
                adminOps.calls.toString());
    }

    // ------------------------------------------- union read of a primary-key table: lake + log tail

    /**
     * The shape of the whole arrangement, in one plan. Bucket 0 has been tiered and has written more
     * since, so its lake split is bound to the tail that supersedes part of it AND the tail is planned as
     * its own range — the lake split only hides rows, it never produces the new ones. Bucket 1 has been
     * tiered and written nothing since, so its lake split is passed through untouched and fluss
     * contributes nothing at all.
     */
    @Test
    public void bucketWithATailBindsItToItsLakeSplitsAndPlansTheTailOnce() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, 200L));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 200L);
        earliestOffsets(null, 0L, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(0),
                RecordingLakeSibling.LakeRange.inBucket(1));

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(3, ranges.size());
        assertSuppressed(ranges.get(0), 0, 100L, 105L);
        Assertions.assertTrue(ranges.get(1) instanceof RecordingLakeSibling.LakeRange,
                "bucket 1 has no tail and must be passed through: " + ranges.get(1));
        assertTailRange(ranges.get(2), 0, 100L, 105L);
    }

    /**
     * The suppressing tail and the range that produces the tail's rows must be the SAME window. Any gap
     * between them is a wrong answer in one direction or the other: a suppression window wider than the
     * produced one hides rows nothing brings back, a narrower one lets a superseded row through beside
     * its replacement.
     */
    @Test
    public void theSuppressedWindowIsExactlyTheWindowTheTailRangeReads() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 130L);
        earliestOffsets(null, 7L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(0));

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        FlussSuppressedLakeRange.Tail tail = ((FlussSuppressedLakeRange) ranges.get(0)).getTail();
        Map<String, String> tailRange = ranges.get(1).getProperties();
        Assertions.assertEquals(String.valueOf(tail.getStartOffset()),
                tailRange.get("fluss.log_start_offset"));
        Assertions.assertEquals(String.valueOf(tail.getStopOffset()),
                tailRange.get("fluss.log_stop_offset"));
    }

    /**
     * A bucket the lake has never seen is read whole from fluss, exactly as it would be with no lake at
     * all — and nothing of it can be suppressed, because the lake holds none of it.
     */
    @Test
    public void bucketTheLakeHasNeverSeenIsReadWholeFromFluss() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, null));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 100L, 25L);
        earliestOffsets(null, 0L, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(0));

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(2, ranges.size());
        Assertions.assertTrue(ranges.get(0) instanceof RecordingLakeSibling.LakeRange, "bucket 0 is tiered"
                + " up to where its log ends and must be passed through: " + ranges.get(0));
        assertPkRange(ranges.get(1), 1, 5L, 20L, 25L);
    }

    /**
     * The tail of one bucket must not be bound to another bucket's split. Bucket-blind binding still
     * produces a plausible plan — every split suppressed by SOME tail — and returns wrong rows: the keys
     * of bucket 0's tail say nothing about the rows of bucket 1.
     */
    @Test
    public void eachBucketsSplitsAreBoundToThatBucketsTail() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, 200L));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 250L);
        earliestOffsets(null, 0L, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(1),
                RecordingLakeSibling.LakeRange.inBucket(0));

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        assertSuppressed(ranges.get(0), 1, 200L, 250L);
        assertSuppressed(ranges.get(1), 0, 100L, 105L);
    }

    /** The same, one level up: a partition's splits are bound to the tails of THAT partition's buckets. */
    @Test
    public void eachPartitionsSplitsAreBoundToThatPartitionsTails() {
        registerPartitionedPkLakeTable(1, "20260101", "20260102");
        adminOps.readableLakeSnapshot = new LakeSnapshot(9L, partitionedOffsets(
                new long[] {100L}, new long[] {700L}));
        kvSnapshots("20260101", new long[] {1L}, new long[] {10L});
        kvSnapshots("20260102", new long[] {2L}, new long[] {20L});
        latestOffsets("20260101", 105L);
        latestOffsets("20260102", 750L);
        earliestOffsets("20260101", 0L);
        earliestOffsets("20260102", 0L);
        lakeSplits(
                RecordingLakeSibling.LakeRange.inBucket(0, Collections.singletonMap("dt", "20260102")),
                RecordingLakeSibling.LakeRange.inBucket(0, Collections.singletonMap("dt", "20260101")));

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        assertSuppressed(ranges.get(0), 0, 700L, 750L);
        assertSuppressed(ranges.get(1), 0, 100L, 105L);
    }

    /**
     * A lake split of a partition this scan does not read from fluss — one fluss has dropped, or one the
     * engine pruned away — has no tail to be bound to and is passed through. It cannot be dropped either:
     * partition pruning removes the fluss half of a partition, not the predicate that pruned it.
     */
    @Test
    public void lakeSplitOfAPartitionThisScanDoesNotReadIsPassedThrough() {
        registerPartitionedPkLakeTable(1, "20260101");
        adminOps.readableLakeSnapshot = new LakeSnapshot(9L, partitionedOffsets(new long[] {100L}));
        kvSnapshots("20260101", new long[] {1L}, new long[] {10L});
        latestOffsets("20260101", 105L);
        earliestOffsets("20260101", 0L);
        lakeSplits(
                RecordingLakeSibling.LakeRange.inBucket(0, Collections.singletonMap("dt", "20251231")));

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertTrue(ranges.get(0) instanceof RecordingLakeSibling.LakeRange, ranges.toString());
        assertTailRange(ranges.get(1), 0, 100L, 105L);
    }

    /**
     * The bucket a split holds is the one fact this connector cannot work out for itself, and an older
     * paimon plugin does not report it. Reading that absence as "this bucket has no tail" would return
     * every superseded row a second time, so it is an error instead.
     */
    @Test
    public void lakeSplitThatDoesNotSayWhichBucketItHoldsIsRefused() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 105L);
        earliestOffsets(null, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.withoutABucket());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog()));
        Assertions.assertTrue(e.getMessage().contains("paimon.bucket"), e.getMessage());
    }

    /** A lake table bucketed differently from the fluss table cannot be matched to it bucket by bucket. */
    @Test
    public void lakeSplitInABucketThisTableDoesNotHaveIsRefused() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, 200L));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 205L);
        earliestOffsets(null, 0L, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(7));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog()));
        Assertions.assertTrue(e.getMessage().contains("bucketed alike"), e.getMessage());
    }

    /**
     * The lake holding data for a bucket fluss has no tiering offset for means the two disagree about what
     * has been tiered. Passing the split through would return that bucket's rows twice — once from the
     * lake, once from the {@code PK_FULL} range the missing offset produces.
     */
    @Test
    public void lakeSplitOfABucketFlussRecordsNoTieringOffsetForIsRefused() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, null));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 25L);
        earliestOffsets(null, 0L, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(1));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog()));
        Assertions.assertTrue(e.getMessage().contains("metadata disagrees"), e.getMessage());
    }

    /**
     * Fluss deletes old log segments on a timer that does not wait for tiering, so the beginning of a tail
     * can be gone. There is nowhere else to read it from — the lake's copy stops exactly where the missing
     * tail begins — so {@code auto} gives up the lake half rather than return fewer rows than the table
     * holds, and says so in EXPLAIN.
     */
    @Test
    public void tailTheLogNoLongerHoldsGivesUpTheLakeHalf() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, 200L));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 205L);
        earliestOffsets(null, 0L, 201L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);

        List<ConnectorScanRange> ranges =
                provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        Assertions.assertEquals(2, ranges.size());
        assertPkRange(ranges.get(0), 0, 4L, 10L, 105L);
        assertPkRange(ranges.get(1), 1, 5L, 20L, 205L);
        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());
        Assertions.assertTrue(output.toString().contains("unionRead=no"), output.toString());
        Assertions.assertTrue(output.toString().contains("degraded=tail-truncated"), output.toString());
    }

    /** A bucket fluss will not report an earliest offset for is not "fine", it is unverifiable. */
    @Test
    public void tailThatCannotBeVerifiedGivesUpTheLakeHalf() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 105L);
        adminOps.earliestOffsetsByPartition.put(null, Collections.emptyMap());

        List<ConnectorScanRange> ranges = plan(PK_TABLE, catalog());

        Assertions.assertEquals(1, ranges.size());
        assertPkRange(ranges.get(0), 0, 4L, 10L, 105L);
    }

    /**
     * What replaces the lake half is the very read {@code disabled} asks for outright, down to the ranges.
     * That is what makes it safe to give up so late: a primary-key table read from fluss alone is the whole
     * table.
     */
    @Test
    public void whatReplacesTheLakeHalfIsTheReadDisabledModeAsksForOutright() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, 200L));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 205L);
        earliestOffsets(null, 104L, 0L);

        List<Map<String, String>> degraded = rangeProperties(plan(PK_TABLE, catalog()));
        List<Map<String, String>> disabled = rangeProperties(
                plan(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "disabled")));

        Assertions.assertEquals(disabled, degraded);
    }

    /** Under {@code required} the same truncated tail is an error: there is nothing to fall back to. */
    @Test
    public void requiredModeRefusesATailTheLogNoLongerHolds() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 105L);
        earliestOffsets(null, 101L);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "required")));
        Assertions.assertTrue(e.getMessage().contains("the log now starts at 101"), e.getMessage());
    }

    /**
     * A key column Doris cannot compare exactly is a permanent property of the table, so it is settled
     * before anything is asked of the lake — that is what lets the answer be the same at plan-translation
     * time, when the key columns are kept in the scan's tuple, as it is here.
     */
    @Test
    public void keyColumnThatCannotBeComparedExactlyGivesUpTheLakeHalf() {
        registerPkLakeTableKeyedBy(DataTypes.DOUBLE());
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 105L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);

        List<ConnectorScanRange> ranges =
                provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        Assertions.assertEquals(1, ranges.size());
        assertPkRange(ranges.get(0), 0, 4L, 10L, 105L);
        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());
        Assertions.assertTrue(output.toString().contains("degraded=key-type"), output.toString());
        // Settled without asking the lake anything at all.
        Assertions.assertTrue(adminOps.calls.stream().noneMatch(c -> c.startsWith("getReadableLakeSnapshot")),
                adminOps.calls.toString());
    }

    @Test
    public void requiredModeRefusesAKeyColumnThatCannotBeComparedExactly() {
        registerPkLakeTableKeyedBy(DataTypes.DOUBLE());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "required")));
        Assertions.assertTrue(e.getMessage().contains("primary-key column 'id'"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("floating-point"), e.getMessage());
    }

    /**
     * A lake split is matched to a fluss partition by comparing rendered partition values, so a partition
     * column of a type the two sides may spell differently is refused the same way. Fluss itself allows
     * such a column — INT, DATE, BOOLEAN are all legal partition keys — so this is not a case the schema
     * makes impossible.
     */
    @Test
    public void partitionColumnThatMayNotRenderAlikeGivesUpTheLakeHalf() {
        registerPartitionedPkLakeTable(1, DataTypes.INT(), "20260101");
        kvSnapshots("20260101", new long[] {1L}, new long[] {10L});
        latestOffsets("20260101", 105L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);

        List<ConnectorScanRange> ranges =
                provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        Assertions.assertEquals(1, ranges.size());
        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());
        Assertions.assertTrue(output.toString().contains("degraded=partition-type"), output.toString());
    }

    @Test
    public void requiredModeRefusesAPartitionColumnThatMayNotRenderAlike() {
        registerPartitionedPkLakeTable(1, DataTypes.INT(), "20260101");

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "required")));
        Assertions.assertTrue(e.getMessage().contains("partition column 'dt'"), e.getMessage());
    }

    /**
     * The kv snapshots are read for every partition even when the lake half turns out to cover it, because
     * the fallback needs them and reading them after the offsets would leave a bucket bounded by an offset
     * older than the snapshot it is read from. The order is the assertion.
     */
    @Test
    public void snapshotsAreReadBeforeTheOffsetsEvenWhenTheLakeCoversEverything() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 105L);
        earliestOffsets(null, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(0));

        plan(PK_TABLE, catalog());

        int snapshotCall = indexOfCall("getLatestKvSnapshots");
        int latestCall = indexOfCall("listOffsets(db.pk_tbl, [0], LatestSpec)");
        int earliestCall = indexOfCall("listOffsets(db.pk_tbl, [0], EarliestSpec)");
        Assertions.assertTrue(snapshotCall >= 0 && snapshotCall < latestCall, adminOps.calls.toString());
        Assertions.assertTrue(latestCall < earliestCall, adminOps.calls.toString());
    }

    /** EXPLAIN accounts for all three parts, so a regression test can tell which of them did the work. */
    @Test
    public void explainCountsTheSuppressedSplitsAndTheTailsApart() {
        registerPkLakeTable(2);
        lakeSnapshotAt(9L, offsets(100L, 200L));
        kvSnapshots(null, new long[] {4L, 5L}, new long[] {10L, 20L});
        latestOffsets(null, 105L, 200L);
        earliestOffsets(null, 0L, 0L);
        lakeSplits(RecordingLakeSibling.LakeRange.inBucket(0),
                RecordingLakeSibling.LakeRange.inBucket(0),
                RecordingLakeSibling.LakeRange.inBucket(1));
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);
        provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertEquals("flussScan: unionRead=yes, lakeSplits=3, suppressedLakeSplits=2,"
                + " logRanges=0, pkRanges=0, pkTailRanges=1, mode=auto\n", output.toString());
    }

    // ------------------------------------------- the key columns BE has to read either way

    /**
     * BE identifies the rows a tail supersedes by their keys, so those columns have to be read whether or
     * not the query asked for them. The engine keeps them in the scan's tuple on the strength of this
     * answer; the projection above the scan drops them again before the user sees a row.
     */
    @Test
    public void unionReadKeepsTheKeyColumnsInTheScan() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));

        Assertions.assertEquals(Collections.singleton("id"), mustReadColumns(PK_TABLE, catalog()));
    }

    /** Only the physical key: a partition column is the same for every row of a bucket. */
    @Test
    public void thePartitionColumnsAreNotPartOfTheKeyBeMustRead() {
        registerPartitionedPkLakeTable(1, "20260101");
        adminOps.readableLakeSnapshot = new LakeSnapshot(9L, partitionedOffsets(new long[] {100L}));

        Assertions.assertEquals(Collections.singleton("id"), mustReadColumns(PK_TABLE, catalog()));
    }

    /**
     * Nothing else keeps a column it was not asked for. Each of these reads is served by one scanner that
     * needs no key at all, so keeping one would be a column read for nobody — and, for a log table, would
     * pull the lake snapshot's round trip forward into plan translation for nothing.
     */
    @Test
    public void everyOtherReadKeepsNothing() {
        registerLakeTable(2);
        lakeSnapshotAt(7L, offsets(1L, 1L));
        Assertions.assertEquals(Collections.emptySet(), mustReadColumns(LOG_TABLE, catalog()));
        Assertions.assertTrue(adminOps.calls.isEmpty(), adminOps.calls.toString());

        registerPkTable(PK_TABLE, 1);
        Assertions.assertEquals(Collections.emptySet(), mustReadColumns(PK_TABLE, catalog()));

        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        Assertions.assertEquals(Collections.emptySet(),
                mustReadColumns(PK_TABLE, catalog(FlussCatalogProperties.UNION_READ_MODE, "disabled")));

        registerPkLakeTableKeyedBy(DataTypes.DOUBLE());
        Assertions.assertEquals(Collections.emptySet(), mustReadColumns(PK_TABLE, catalog()));
    }

    /**
     * The engine asks for the key columns while translating the plan and plans the ranges later, and the
     * two answers must come from ONE resolution. Were they resolved twice, a snapshot committed in between
     * could make the first say "no lake" (so the key columns are pruned away) and the second say "lake",
     * leaving BE to look for a key column that is not in its projection.
     */
    @Test
    public void bothQuestionsAreAnsweredByTheSameResolution() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));
        kvSnapshots(null, new long[] {4L}, new long[] {10L});
        latestOffsets(null, 105L);
        earliestOffsets(null, 0L);
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);

        provider.getMustReadColumns(session, handle(PK_TABLE));
        provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        Assertions.assertEquals(1,
                adminOps.calls.stream().filter(c -> c.startsWith("getReadableLakeSnapshot")).count(),
                adminOps.calls.toString());
    }

    // ---------------------------------------------------------------- what BE and EXPLAIN receive

    /**
     * What BE needs to suppress by key, and only on the read that suppresses: the key columns by name (the
     * types travel as ordinary slot descriptors, because the columns are ordinary projected columns) and
     * the ceiling on how much tail it may hold while doing it.
     */
    @Test
    public void primaryKeyUnionTellsBeWhichColumnsTheKeyIsMadeOf() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));

        Map<String, String> props = nodeProperties(PK_TABLE, catalog());

        Assertions.assertEquals("id", props.get("fluss.union.pk_names"));
        Assertions.assertEquals("2000000", props.get("fluss.union.max_tail_rows"));
    }

    @Test
    public void theTailCeilingIsTheOneTheCatalogDeclares() {
        registerPkLakeTable(1);
        lakeSnapshotAt(9L, offsets(100L));

        Map<String, String> props = nodeProperties(PK_TABLE,
                catalog(FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS, "500"));

        Assertions.assertEquals("500", props.get("fluss.union.max_tail_rows"));
    }

    /** A read that suppresses nothing must not describe a key: there is no reader on the far side. */
    @Test
    public void readThatSuppressesNothingSendsNoKey() {
        registerLakeTable(1);
        lakeSnapshotAt(7L, offsets(1L));

        Map<String, String> props = nodeProperties(LOG_TABLE, catalog());

        Assertions.assertFalse(props.containsKey("fluss.union.pk_names"), props.toString());
        Assertions.assertFalse(props.containsKey("fluss.union.max_tail_rows"), props.toString());
    }

    @Test
    public void scanLevelParamsCarryTheClientConfigAndTableIdentity() {
        registerLogTable(LOG_TABLE, 1);
        Map<String, String> catalog = catalog();
        catalog.put("fluss.client.writer.batch-size", "2mb");

        TFileScanRangeParams params = new TFileScanRangeParams();
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog),
                this::lakeSibling);
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
        new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling).populateScanLevelParams(params, nodeProps);

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
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);
        provider.planScan(session, request(handle(LOG_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "  ", Collections.emptyMap());

        Assertions.assertEquals(
                "  flussScan: unionRead=no, lakeSplits=0, suppressedLakeSplits=0, logRanges=2,"
                + " pkRanges=0, pkTailRanges=0, mode=auto\n", output.toString());
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
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog()),
                this::lakeSibling);
        provider.planScan(session, request(handle(PK_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertEquals(
                "flussScan: unionRead=no, lakeSplits=0, suppressedLakeSplits=0, logRanges=0,"
                + " pkRanges=2, pkTailRanges=0, mode=auto\n", output.toString());
    }

    @Test
    public void explainReportsTheConfiguredMode() {
        registerLogTable(LOG_TABLE, 1);
        latestOffsets(null, 1L);
        Map<String, String> catalog = catalog(FlussCatalogProperties.UNION_READ_MODE, "disabled");
        FlussScanPlanProvider provider = new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalog),
                this::lakeSibling);
        provider.planScan(session, request(handle(LOG_TABLE), Collections.emptyList()));

        StringBuilder output = new StringBuilder();
        provider.appendExplainInfo(output, "", Collections.emptyMap());

        Assertions.assertTrue(output.toString().contains("mode=disabled"), output.toString());
    }

    // ---------------------------------------------------------------- helpers

    private Map<String, String> catalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put(FlussCatalogProperties.BOOTSTRAP_SERVERS, "localhost:9123");
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
        return new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalogProperties), this::lakeSibling)
                .planScan(session, request(handle(tablePath), requiredPartitions));
    }

    private Map<String, String> nodeProperties(TablePath tablePath, Map<String, String> catalogProperties) {
        return new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalogProperties),
                this::lakeSibling).getScanNodeProperties(
                session, handle(tablePath), Collections.emptyList(), Optional.empty());
    }

    private Set<String> mustReadColumns(TablePath tablePath, Map<String, String> catalogProperties) {
        return new FlussScanPlanProvider(adminOps, FlussCatalogProperties.of(catalogProperties), this::lakeSibling)
                .getMustReadColumns(session, handle(tablePath));
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
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
                .build());
        siblingExpected = true;
    }

    private void registerPkTable(TablePath tablePath, int buckets) {
        adminOps.tableInfos.put(tablePath, FlussTestTables.builder(tablePath)
                .column("id", DataTypes.INT().copy(false))
                .column("v", DataTypes.STRING())
                .primaryKey("id")
                .buckets(buckets, "id")
                .build());
    }

    /**
     * A primary-key table tiered into a lake. Deliberately does NOT mark a sibling as expected: its lake is
     * never read, so reaching the sibling factory at all is a failure.
     */
    private void registerTieredPkTable(int buckets) {
        adminOps.tableInfos.put(PK_TABLE, FlussTestTables.builder(PK_TABLE)
                .column("id", DataTypes.INT().copy(false))
                .column("v", DataTypes.STRING())
                .primaryKey("id")
                .buckets(buckets, "id")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
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
        adminOps.latestOffsetsByPartition.put(partitionName, byBucket(offsets));
    }

    /**
     * Earliest offsets for buckets 0..n-1 — how far back fluss can still serve. Only a union read of a
     * primary-key table asks for these, which is why the log-table fixtures do not set them: a test that
     * needed them and did not say so gets an error from the recorder, not a default.
     */
    private void earliestOffsets(String partitionName, long... offsets) {
        adminOps.earliestOffsetsByPartition.put(partitionName, byBucket(offsets));
    }

    private static Map<Integer, Long> byBucket(long... offsets) {
        Map<Integer, Long> byBucket = new LinkedHashMap<>();
        for (int bucket = 0; bucket < offsets.length; bucket++) {
            byBucket.put(bucket, offsets[bucket]);
        }
        return byBucket;
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

    /** The ranges' payloads, in order — what two plans have to agree on to be the same read. */
    private static List<Map<String, String>> rangeProperties(List<ConnectorScanRange> ranges) {
        List<Map<String, String>> properties = new ArrayList<>(ranges.size());
        for (ConnectorScanRange range : ranges) {
            properties.add(range.getProperties());
        }
        return properties;
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

    // ---------------------------------------------------------------- union-read fixtures

    /**
     * The lake sibling this catalog would build. Fails loud when a test that registered no lake table
     * reaches it: a fluss-only plan that quietly consulted the lake still passes every other assertion.
     */
    private Connector lakeSibling(Map<String, String> siblingProperties) {
        if (!siblingExpected) {
            throw new AssertionError("no lake sibling is expected in this test");
        }
        if (sibling == null) {
            sibling = new RecordingLakeSibling(siblingProperties);
        }
        return sibling;
    }

    /** The sibling, built now so a test can set what it answers before planning asks. */
    private RecordingLakeSibling sibling() {
        lakeSibling(Collections.emptyMap());
        return sibling;
    }

    /** The readable lake snapshot fluss reports, with the log offset it recorded for each bucket. */
    private void lakeSnapshotAt(long snapshotId, Map<TableBucket, Long> bucketOffsets) {
        adminOps.readableLakeSnapshot = new LakeSnapshot(snapshotId, bucketOffsets);
    }

    /**
     * Lake offsets for buckets 0..n-1 of an unpartitioned table; a {@code null} entry is a bucket the
     * snapshot does not mention at all (never tiered), which is not the same as offset 0.
     */
    private static Map<TableBucket, Long> offsets(Long... byBucket) {
        Map<TableBucket, Long> offsets = new HashMap<>();
        for (int bucket = 0; bucket < byBucket.length; bucket++) {
            if (byBucket[bucket] != null) {
                offsets.put(new TableBucket(FlussTestTables.TABLE_ID, bucket), byBucket[bucket]);
            }
        }
        return offsets;
    }

    /** The exact splits the sibling's scan planner returns, in order. */
    private void lakeSplits(RecordingLakeSibling.LakeRange... splits) {
        sibling().lakeRanges = new ArrayList<>(Arrays.asList(splits));
    }

    /** Lake offsets for buckets 0..n-1 of each partition, partition ids 100, 101, ... in order. */
    private static Map<TableBucket, Long> partitionedOffsets(long[]... byPartition) {
        Map<TableBucket, Long> offsets = new HashMap<>();
        for (int partition = 0; partition < byPartition.length; partition++) {
            for (int bucket = 0; bucket < byPartition[partition].length; bucket++) {
                offsets.put(new TableBucket(FlussTestTables.TABLE_ID, 100L + partition, bucket),
                        byPartition[partition][bucket]);
            }
        }
        return offsets;
    }

    /**
     * A primary-key table tiered into a lake whose lake IS read. The same table as
     * {@link #registerTieredPkTable}, but declaring that reaching the sibling is expected — the other
     * fixture keeps that guard on, for the tests that assert the lake was never consulted.
     */
    private void registerPkLakeTable(int buckets) {
        registerTieredPkTable(buckets);
        siblingExpected = true;
    }

    /** The same, keyed by a column of {@code keyType} — for the types a union read cannot compare. */
    private void registerPkLakeTableKeyedBy(DataType keyType) {
        adminOps.tableInfos.put(PK_TABLE, FlussTestTables.builder(PK_TABLE)
                .column("id", keyType.copy(false))
                .column("v", DataTypes.STRING())
                .primaryKey("id")
                .buckets(1, "id")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
                .build());
        siblingExpected = true;
    }

    /** A partitioned primary-key lake table, partitioned by a STRING {@code dt}. */
    private void registerPartitionedPkLakeTable(int buckets, String... partitionValues) {
        registerPartitionedPkLakeTable(buckets, DataTypes.STRING(), partitionValues);
    }

    private void registerPartitionedPkLakeTable(int buckets, DataType partitionType,
            String... partitionValues) {
        adminOps.tableInfos.put(PK_TABLE, FlussTestTables.builder(PK_TABLE)
                .column("id", DataTypes.INT().copy(false))
                .column("dt", partitionType.copy(false))
                .primaryKey("id", "dt")
                .partitionedBy("dt")
                .buckets(buckets, "id")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
                .build());
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionValues.length; i++) {
            partitions.add(new PartitionInfo(100L + i,
                    ResolvedPartitionSpec.fromPartitionValue("dt", partitionValues[i]), null));
        }
        adminOps.partitionsByTable.put(PK_TABLE, partitions);
        siblingExpected = true;
    }

    /** A lake split bound to the tail of {@code bucket} over {@code [start, stop)}. */
    private static void assertSuppressed(ConnectorScanRange range, int bucket, long start, long stop) {
        Assertions.assertTrue(range instanceof FlussSuppressedLakeRange,
                "expected a suppressed lake split but got " + range);
        FlussSuppressedLakeRange.Tail tail = ((FlussSuppressedLakeRange) range).getTail();
        Assertions.assertEquals(bucket, tail.getBucketId());
        Assertions.assertEquals(start, tail.getStartOffset());
        Assertions.assertEquals(stop, tail.getStopOffset());
    }

    private static void assertTailRange(ConnectorScanRange range, int bucket, long start, long stop) {
        Map<String, String> props = range.getProperties();
        Assertions.assertEquals("PK_TAIL", props.get("fluss.range_type"));
        Assertions.assertEquals(String.valueOf(bucket), props.get("fluss.bucket_id"));
        Assertions.assertEquals(String.valueOf(start), props.get("fluss.log_start_offset"));
        Assertions.assertEquals(String.valueOf(stop), props.get("fluss.log_stop_offset"));
    }

    /** {@code count} ranges for the sibling's scan planner to return as the lake half. */
    private void lakeRanges(int count) {
        List<ConnectorScanRange> ranges = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ranges.add(new RecordingLakeSibling.LakeRange());
        }
        sibling().lakeRanges = ranges;
    }

    /** The columns the lake table reports, in order. */
    private void lakeTableColumns(String... names) {
        Map<String, ConnectorColumnHandle> columns = new LinkedHashMap<>();
        for (String name : names) {
            columns.put(name, new RecordingLakeSibling.LakeColumn(name));
        }
        sibling().lakeColumns = columns;
    }

    /** A lake-enabled log table partitioned by {@code dt}, with partition ids 100, 101, ... in order. */
    private void registerPartitionedLakeTable(int buckets, String... partitionValues) {
        adminOps.tableInfos.put(LOG_TABLE, FlussTestTables.builder(LOG_TABLE)
                .column("id", DataTypes.INT())
                .column("dt", DataTypes.STRING())
                .partitionedBy("dt")
                .buckets(buckets)
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
                .build());
        List<PartitionInfo> partitions = new ArrayList<>();
        for (int i = 0; i < partitionValues.length; i++) {
            partitions.add(new PartitionInfo(100L + i,
                    ResolvedPartitionSpec.fromPartitionValue("dt", partitionValues[i]), null));
        }
        adminOps.partitionsByTable.put(LOG_TABLE, partitions);
        siblingExpected = true;
    }
}
