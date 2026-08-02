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

    // ---------------------------------------------------------------- what is refused, and why

    @Test
    public void primaryKeyTableIsRefusedRatherThanReadAsALog() {
        TablePath pkTable = TablePath.of("db", "pk_tbl");
        adminOps.tableInfos.put(pkTable, FlussTestTables.builder(pkTable)
                .column("id", DataTypes.INT().copy(false))
                .column("v", DataTypes.STRING())
                .primaryKey("id")
                .buckets(2, "id")
                .build());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> plan(pkTable, catalog()));
        Assertions.assertTrue(e.getMessage().contains("primary-key"), e.getMessage());
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
                "  flussScan: unionRead=no, lakeSplits=0, logRanges=2, mode=auto\n", output.toString());
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
        return new FlussConnectorMetadata(adminOps, new FlussTypeMapping.Options(false, false));
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

    private static void assertPartition(ConnectorScanRange range, String partitionName,
            long partitionId, int bucket, long stop) {
        Map<String, String> props = range.getProperties();
        Assertions.assertEquals(partitionName, props.get("fluss.partition_name"));
        Assertions.assertEquals(String.valueOf(partitionId), props.get("fluss.partition_id"));
        assertLogRange(range, bucket, -2L, stop);
    }

}
