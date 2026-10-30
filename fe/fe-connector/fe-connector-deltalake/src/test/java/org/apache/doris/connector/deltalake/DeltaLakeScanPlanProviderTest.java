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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;
import org.apache.doris.thrift.TDeltaLakeFileDesc;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class DeltaLakeScanPlanProviderTest {
    @TempDir
    Path warehouse;

    @Test
    void splitsByteRangesAndKeepsPartitionsAndFilesOnThePinnedVersion() throws Exception {
        createTable();
        appendFile(0, "s3a://bucket/first.parquet", "a/b", 257);
        appendFile(0, "s3a://bucket/null.parquet", null, 25);
        appendFile(0, "s3a://bucket/sentinel.parquet", "__HIVE_DEFAULT_PARTITION__", 25);
        TestingConnectorSession session = new TestingConnectorSession("first", Map.of("file_split_size", "128"));
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            appendFile(1, "s3a://bucket/later.parquet", "later", 25);
            List<ConnectorPartitionInfo> partitions = metadata.listPartitions(session, handle, Optional.empty());
            Assertions.assertEquals(3, partitions.size());
            Assertions.assertEquals(3, partitions.stream().map(ConnectorPartitionInfo::getPartitionName)
                    .distinct().count());
            Assertions.assertEquals(1, partitions.stream()
                    .filter(partition -> partition.getPartitionValueNullFlags().get(0)).count());
            ConnectorScanPlanProvider planner = connector.getScanPlanProvider();
            List<ConnectorScanRange> ranges = planner.planScan(session, request(handle).build());
            Assertions.assertEquals(5, ranges.size());
            Assertions.assertEquals(307, ranges.stream().mapToLong(ConnectorScanRange::getLength).sum());
            List<ConnectorScanRange> first = ranges.stream()
                    .filter(range -> range.getPath().orElseThrow().equals("s3://bucket/first.parquet"))
                    .collect(Collectors.toList());
            Assertions.assertEquals(Arrays.asList(0L, 128L, 256L),
                    first.stream().map(ConnectorScanRange::getStart).collect(Collectors.toList()));
            Assertions.assertEquals(3, planner.scannedPartitionCount(ranges).orElseThrow());
            Assertions.assertEquals(3, planner.planScan(session, request(handle)
                    .requiredPartitions(Collections.singletonList("p=a%2Fb")).build()).size());
            Assertions.assertTrue(planner.planScan(session, request(handle).partitionsPrunedToEmpty(true).build())
                    .isEmpty());
            Map<String, String> properties = planner.getScanNodeProperties(session, handle,
                    Collections.emptyList(), Optional.empty());
            Assertions.assertEquals("parquet", properties.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
            Assertions.assertEquals("p", properties.get(ScanNodePropertyKeys.PATH_PARTITION_KEYS));
            Assertions.assertEquals("test-key", properties.get(ScanNodePropertyKeys.LOCATION_PREFIX + "AWS_ACCESS_KEY"));
            connector.invalidateTable("db", "table");
            TestingConnectorSession next = new TestingConnectorSession("next");
            ConnectorTableHandle latest = metadata.getTableHandle(next, "db", "table").orElseThrow();
            Assertions.assertEquals(4, metadata.listPartitions(next, latest, Optional.empty()).size());
        }
    }

    @Test
    void emptyTableReturnsNoSplitsAndInvalidFilesFailPlanning() throws Exception {
        createTable();
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(null)) {
            ConnectorTableHandle empty = metadata.getTableHandle(null, "db", "table").orElseThrow();
            Assertions.assertTrue(connector.getScanPlanProvider().planScan(null, request(empty).build()).isEmpty());
            appendFile(1, "broken.parquet", "a", 0);
            ConnectorTableHandle invalid = metadata.getTableHandle(null, "db", "table").orElseThrow();
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> connector.getScanPlanProvider().planScan(null, request(invalid).build()));
        }
    }

    @Test
    void rangeThriftKeepsNullEmptyAndSentinelValuesDistinctAndOwnsItsDescriptor() {
        TDeltaLakeFileDesc descriptor = new TDeltaLakeFileDesc();
        descriptor.setDeletionVector(new TDeltaLakeDeletionVectorDesc().setSizeInBytes(12).setCardinality(1)
                .setInlineData(new byte[] {1, 2, 3}));
        for (String value : Arrays.asList(null, "", "__HIVE_DEFAULT_PARTITION__")) {
            DeltaLakeScanRange range = new DeltaLakeScanRange("s3://bucket/data.parquet", 0, 10, 10, 1, 128,
                    Collections.singletonMap("p", value), descriptor);
            TFileRangeDesc file = new TFileRangeDesc();
            file.setColumnsFromPath(Collections.singletonList("wrong generic path value"));
            TTableFormatFileDesc format = new TTableFormatFileDesc();
            range.populateRangeParams(format, file);
            Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, file.getFormatType());
            Assertions.assertEquals(Collections.singletonList("p"), file.getColumnsFromPathKeys());
            Assertions.assertEquals(Collections.singletonList(value == null ? "" : value), file.getColumnsFromPath());
            Assertions.assertEquals(Collections.singletonList(value == null), file.getColumnsFromPathIsNull());
            Assertions.assertFalse(format.getDeltalakeParams().isSetSchemaId());
            format.getDeltalakeParams().getDeletionVector().setCardinality(999);
            range.populateRangeParams(format, file);
            Assertions.assertEquals(1, format.getDeltalakeParams().getDeletionVector().getCardinality());
        }
    }

    @Test
    void skipsFilesByStatisticsAndCountsOnlyCompleteUnfilteredMetadata() throws Exception {
        createTable();
        appendStatsFile("a.parquet", "a", 1, 10);
        appendStatsFile("b.parquet", "b", 11, 20);
        TestingConnectorSession session = new TestingConnectorSession("stats");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            ConnectorScanPlanProvider provider = connector.getScanPlanProvider();
            ConnectorComparison filter = new ConnectorComparison(ConnectorComparison.Operator.LT,
                    new ConnectorColumnRef("id", ConnectorType.of("BIGINT")), ConnectorLiteral.ofLong(5));
            List<ConnectorScanRange> filtered = provider.planScan(session, request(handle)
                    .filter(Optional.of(filter)).build());
            Assertions.assertEquals(1, filtered.size());
            Assertions.assertTrue(filtered.get(0).getPath().orElseThrow().endsWith("a.parquet"));
            Assertions.assertFalse(provider.canServeMetadataOnlyCount(session, handle, Optional.of(filter)));
            Assertions.assertTrue(provider.canServeMetadataOnlyCount(session, handle, Optional.empty()));
            List<ConnectorScanRange> count = provider.planScan(session, request(handle).countPushdown(true).build());
            Assertions.assertEquals(1, count.size());
            Assertions.assertEquals(20, count.get(0).getPushDownRowCount());
            TTableFormatFileDesc format = new TTableFormatFileDesc();
            count.get(0).populateRangeParams(format, new TFileRangeDesc());
            Assertions.assertEquals(20, format.getTableLevelRowCount());
            Assertions.assertTrue(count.get(0).isNativeReadRange());
            Assertions.assertEquals(-1, provider.planScan(session, request(handle).countPushdown(true)
                    .filter(Optional.of(filter)).build()).get(0).getPushDownRowCount());
        }
    }

    @Test
    void missingStatisticsFallBackToAllFilesWithoutGuessingCounts() throws Exception {
        createTable();
        appendStatsFile("a.parquet", "a", 1, 10);
        appendFile(0, "b.parquet", "b", 25);
        TestingConnectorSession session = new TestingConnectorSession("missing-stats");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            ConnectorScanPlanProvider provider = connector.getScanPlanProvider();
            Assertions.assertFalse(provider.canServeMetadataOnlyCount(session, handle, Optional.empty()));
            List<ConnectorScanRange> ranges = provider.planScan(session, request(handle).countPushdown(true).build());
            Assertions.assertEquals(2, ranges.size());
            Assertions.assertTrue(ranges.stream().allMatch(range -> range.getPushDownRowCount() == -1));
        }
    }

    @Test
    void streamingMatchesSynchronousByteRangesAndKeepsThePinnedVersion() throws Exception {
        createTable();
        appendFile(0, "a.parquet", "a", 257);
        appendFile(0, "b.parquet", null, 25);
        TestingConnectorSession session = new TestingConnectorSession("stream", Map.of("file_split_size", "128"));
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            ConnectorScanPlanProvider provider = connector.getScanPlanProvider();
            Assertions.assertTrue(provider.streamingSplitEstimate(session, handle, Optional.empty(), false) >= 0);
            try (ConnectorSplitSource source = provider.streamSplits(session, handle,
                    Collections.emptyList(), Optional.empty(), -1)) {
                appendFile(1, "later.parquet", "later", 25);
                List<ConnectorScanRange> streamed = new ArrayList<>();
                while (source.hasNext()) {
                    streamed.add(source.next());
                }
                List<ConnectorScanRange> synchronous = provider.planScan(session, request(handle).build());
                Assertions.assertEquals(synchronous.size(), streamed.size());
                for (int i = 0; i < streamed.size(); i++) {
                    Assertions.assertEquals(synchronous.get(i).getPath(), streamed.get(i).getPath());
                    Assertions.assertEquals(synchronous.get(i).getStart(), streamed.get(i).getStart());
                    Assertions.assertEquals(synchronous.get(i).getLength(), streamed.get(i).getLength());
                    Assertions.assertEquals(synchronous.get(i).getPartitionValues(), streamed.get(i).getPartitionValues());
                }
                Assertions.assertEquals(4, streamed.size());
                Assertions.assertFalse(source.hasNext());
            }
        }
    }

    @Test
    void streamingCountCollapsesOnlyCompleteStatistics() throws Exception {
        createTable();
        appendStatsFile("a.parquet", "a", 1, 10);
        TestingConnectorSession session = new TestingConnectorSession("stream-count");
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            ConnectorScanPlanProvider provider = connector.getScanPlanProvider();
            Assertions.assertTrue(provider.streamingSplitEstimate(session, handle, Optional.empty(), true) >= 0);
            try (ConnectorSplitSource source = provider.streamSplits(session, handle,
                    Collections.emptyList(), Optional.empty(), -1)) {
                Assertions.assertTrue(source.hasNext());
                Assertions.assertEquals(10, source.next().getPushDownRowCount());
                Assertions.assertFalse(source.hasNext());
            }
            appendFile(1, "b.parquet", "b", 25);
            connector.invalidateTable("db", "table");
            TestingConnectorSession next = new TestingConnectorSession("stream-no-stats");
            ConnectorTableHandle newer = metadata.getTableHandle(next, "db", "table").orElseThrow();
            Assertions.assertTrue(provider.streamingSplitEstimate(next, newer, Optional.empty(), true) >= 0);
            try (ConnectorSplitSource source = provider.streamSplits(next, newer,
                    Collections.emptyList(), Optional.empty(), -1)) {
                int files = 0;
                while (source.hasNext()) {
                    Assertions.assertEquals(-1, source.next().getPushDownRowCount());
                    files++;
                }
                Assertions.assertEquals(2, files);
            }
        }
    }

    @Test
    void streamingDefersReadingUntilConsumptionAndHonorsTheBatchToggle() throws Exception {
        createTable();
        TestingConnectorSession session = new TestingConnectorSession("lazy",
                Map.of("enable_external_table_batch_mode", "false"));
        try (Connector connector = connector(); ConnectorMetadata metadata = connector.getMetadata(session)) {
            ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "table").orElseThrow();
            ConnectorScanPlanProvider provider = connector.getScanPlanProvider();
            Assertions.assertEquals(-1, provider.streamingSplitEstimate(session, handle, Optional.empty(), false));
            ConnectorSplitSource source = provider.streamSplits(session, handle,
                    Collections.emptyList(), Optional.empty(), -1);
            Files.delete(warehouse.resolve("db/table/_delta_log/00000000000000000000.json"));
            source.close();
            Assertions.assertFalse(source.hasNext());
        }
    }

    private void appendStatsFile(String path, String partition, long minimum, long maximum) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String stats = mapper.writeValueAsString(Map.of("numRecords", maximum - minimum + 1,
                "minValues", Map.of("id", minimum), "maxValues", Map.of("id", maximum),
                "nullCount", Map.of("id", 0)));
        String action = mapper.writeValueAsString(Map.of("add", Map.of("path", path, "size", 25,
                "partitionValues", Map.of("p", partition), "stats", stats,
                "modificationTime", 1L, "dataChange", true)));
        Files.writeString(warehouse.resolve("db/table/_delta_log/00000000000000000000.json"),
                action + "\n", StandardOpenOption.APPEND);
    }

    private void createTable() throws Exception {
        DeltaLakeTestTables.commit(warehouse.resolve("db/table"), 0, "table-id",
                new StructType().add("id", LongType.LONG).add("p", StringType.STRING),
                Collections.singletonList("p"));
    }

    private void appendFile(long version, String path, String partition, long size) throws Exception {
        String action = new ObjectMapper().writeValueAsString(Map.of("add", Map.of("path", path, "size", size,
                "partitionValues", Collections.singletonMap("p", partition),
                "modificationTime", 1L, "dataChange", true)));
        Files.writeString(warehouse.resolve("db/table/_delta_log").resolve(String.format("%020d.json", version)),
                action + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static ConnectorScanRequest.Builder request(ConnectorTableHandle handle) {
        return ConnectorScanRequest.builder(handle, Collections.emptyList());
    }

    private Connector connector() {
        return new DeltaLakeConnectorProvider().create(Collections.singletonMap("warehouse", warehouse.toString()),
                new RecordingConnectorContext() {
                    @Override
                    public ConnectorStorageContext getStorageContext() {
                        return new ConnectorStorageContext() {
                            @Override
                            public String normalizeStorageUri(String uri) {
                                return uri.replace("s3a://", "s3://");
                            }

                            @Override
                            public Map<String, String> getBackendStorageProperties() {
                                return Map.of("AWS_ACCESS_KEY", "test-key");
                            }
                        };
                    }
                });
    }
}
