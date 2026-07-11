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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.thrift.TFileCompressType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests the two connector-local batch-mode overrides {@link HiveScanPlanProvider} adds so a large partitioned
 * hive scan streams its splits per partition batch instead of materializing every file synchronously into the FE
 * heap (post-HMS-cutover, hive scans through the generic {@code PluginDrivenScanNode}, which enters batch mode
 * only when the connector opts in).
 *
 * <p>WHY (Rule 9): the two overrides encode two correctness-critical decisions.</p>
 * <ul>
 *   <li>{@code supportsBatchScan}: batch iff PARTITIONED and NOT transactional. ACID tables are excluded so the
 *       per-batch resolution does not open (and leak) a metastore read transaction per batch — they keep the
 *       synchronous path.</li>
 *   <li>{@code planScanForPartitionBatch}: hive's {@code planScan} resolves partitions from the handle's pruned
 *       set and IGNORES the passed partition set, so hive MUST override the batch hook to scope resolution to the
 *       batch. If it inherited the SPI default (which re-runs the whole-pruned-set {@code planScan} per batch),
 *       every partition's files would be emitted once per batch — DUPLICATE ROWS. The {@code ranges.size()==1}
 *       (and single-location listing) assertion below fails precisely under that bug.</li>
 * </ul>
 *
 * <p>No Mockito: reuses the proven {@code FakeHmsClient} echo (getPartitions(names) -&gt; one partition per name,
 * location = name) and a counting {@link HiveFileListingCache.DirectoryLister} so the listed locations can be
 * asserted, exactly as {@code HiveConnectorMetadataPartitionPruningTest} / {@code HiveFileListingCacheTest}.</p>
 */
public class HiveScanBatchModeTest {

    private static final List<String> PART_KEYS = Arrays.asList("year", "month");
    private static final String PARQUET_INPUT_FORMAT =
            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    private static final String PARQUET_SERDE =
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

    // ==================== supportsBatchScan: partitioned AND non-transactional ====================

    @Test
    public void supportsBatchScanIsTrueForPartitionedNonTransactionalTable() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .build();
        // WHY: a partitioned, non-transactional table is exactly the case that must stream its splits per batch.
        Assertions.assertTrue(provider.supportsBatchScan(new FakeSession(), handle));
    }

    @Test
    public void supportsBatchScanIsFalseForUnpartitionedTable() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.emptyList())
                .build();
        // WHY: an unpartitioned table has a single partition (the table location); there is nothing to batch, so
        // it stays on the synchronous planScan path.
        Assertions.assertFalse(provider.supportsBatchScan(new FakeSession(), handle));
    }

    @Test
    public void supportsBatchScanIsFalseForTransactionalPartitionedTable() {
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        Map<String, String> txnParams = new HashMap<>();
        txnParams.put("transactional", "true");
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(PART_KEYS)
                .tableParameters(txnParams)
                .build();
        // WHY (Rule 9): ACID tables are excluded DELIBERATELY — running planScanForPartitionBatch per batch on
        // background threads would open (and leak) a metastore read transaction per batch. They keep the
        // synchronous path even though they are partitioned.
        Assertions.assertFalse(provider.supportsBatchScan(new FakeSession(), handle));
    }

    // ==================== planScanForPartitionBatch: scoped to the batch, no duplication ====================

    @Test
    public void planScanForPartitionBatchResolvesOnlyTheBatch() {
        CountingLister lister = new CountingLister();
        // getPartitions echoes each requested name back as a partition whose location IS the name, so the counting
        // lister's keys are the partition names actually resolved for this batch.
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), lister);

        // The handle carries the FULL pruned set (all three partitions). If the batch hook were NOT scoped to the
        // batch (SPI default -> whole-pruned-set planScan), all three would be listed -> 3 ranges.
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Arrays.asList(
                        part("year=2023/month=12"),
                        part("year=2024/month=01"),
                        part("year=2024/month=02")))
                .build();

        List<ConnectorScanRange> ranges = provider.planScanForPartitionBatch(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(),
                Optional.empty(), -1L, Collections.singletonList("year=2024/month=01"));

        // WHY (Rule 9): exactly one range for the single-partition batch. This fails (== 3) precisely if the batch
        // is not partition-scoped — the duplicate-splits bug the override exists to prevent.
        Assertions.assertEquals(1, ranges.size());
        // ...and the file lister was asked ONLY for the batch's partition, never the other two.
        Assertions.assertEquals(1, lister.totalCalls);
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("year=2024/month=01"));
        Assertions.assertNull(lister.callsPerLocation.get("year=2023/month=12"));
        Assertions.assertNull(lister.callsPerLocation.get("year=2024/month=02"));
    }

    // ===== helpers =====

    @Test
    public void testAdjustFileCompressTypeRemapsOnlyLz4Frame() {
        // hadoop/hive write .lz4 as the LZ4 BLOCK codec, but the generic node infers LZ4FRAME from the .lz4
        // extension; BE's text reader then fails on block bytes decoded as frame. Restore legacy
        // HiveScanNode.getFileCompressType parity: remap ONLY LZ4FRAME -> LZ4BLOCK, pass every other codec
        // through unchanged. MUTATION: broadening or dropping the remap would either mis-decode other codecs
        // or reintroduce the LZ4F_getFrameInfo failure on .lz4 text tables.
        HiveScanPlanProvider provider = provider(null, new CountingLister());
        Assertions.assertEquals(TFileCompressType.LZ4BLOCK,
                provider.adjustFileCompressType(TFileCompressType.LZ4FRAME));
        Assertions.assertEquals(TFileCompressType.LZ4BLOCK,
                provider.adjustFileCompressType(TFileCompressType.LZ4BLOCK));
        Assertions.assertEquals(TFileCompressType.GZ,
                provider.adjustFileCompressType(TFileCompressType.GZ));
        Assertions.assertEquals(TFileCompressType.ZSTD,
                provider.adjustFileCompressType(TFileCompressType.ZSTD));
        Assertions.assertEquals(TFileCompressType.SNAPPYBLOCK,
                provider.adjustFileCompressType(TFileCompressType.SNAPPYBLOCK));
        Assertions.assertEquals(TFileCompressType.PLAIN,
                provider.adjustFileCompressType(TFileCompressType.PLAIN));
    }

    private static HiveScanPlanProvider provider(HmsClient hmsClient, CountingLister lister) {
        return new HiveScanPlanProvider(hmsClient, Collections.emptyMap(), new FakeConnectorContext(),
                new HiveReadTransactionManager(), new HiveFileListingCache(Collections.emptyMap(), lister));
    }

    private static HmsPartitionInfo part(String name) {
        return new HmsPartitionInfo(Collections.emptyList(), name, null, null, null, Collections.emptyMap());
    }

    /**
     * A {@link HiveFileListingCache.DirectoryLister} double: counts calls (total + per location) and returns a
     * fresh single-file listing per call, so each partition location contributes exactly one range.
     */
    private static final class CountingLister implements HiveFileListingCache.DirectoryLister {
        final Map<String, Integer> callsPerLocation = new HashMap<>();
        int totalCalls;

        @Override
        public List<HiveFileStatus> list(String location, FileSystem fs) {
            totalCalls++;
            callsPerLocation.merge(location, 1, Integer::sum);
            return new ArrayList<>(Collections.singletonList(new HiveFileStatus(location + "/000000_0", 10L, 1L)));
        }
    }

    /**
     * Minimal {@link HmsClient} double whose {@code getPartitions} echoes each requested name back as an
     * {@link HmsPartitionInfo} whose location IS the name, so the batch-scoped resolution can be asserted through
     * the listed locations. The rest fail loud.
     */
    private static final class FakeHmsClient implements HmsClient {
        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            List<HmsPartitionInfo> result = new ArrayList<>();
            for (String name : partNames) {
                result.add(new HmsPartitionInfo(Collections.emptyList(), name,
                        null, null, null, Collections.emptyMap()));
            }
            return result;
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listDatabases() {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> listTables(String dbName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }

    /** Minimal {@link ConnectorSession} (no split-size override, empty session properties). */
    private static final class FakeSession implements ConnectorSession {
        @Override
        public String getQueryId() {
            return "q";
        }

        @Override
        public String getUser() {
            return "u";
        }

        @Override
        public String getTimeZone() {
            return "UTC";
        }

        @Override
        public String getLocale() {
            return "en_US";
        }

        @Override
        public long getCatalogId() {
            return 0L;
        }

        @Override
        public String getCatalogName() {
            return "hive_catalog";
        }

        @Override
        public <T> T getProperty(String name, Class<T> type) {
            return null;
        }

        @Override
        public Map<String, String> getCatalogProperties() {
            return Collections.emptyMap();
        }
    }
}
