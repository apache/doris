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
import org.apache.doris.thrift.TFileScanRangeParams;

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

    // ===== object-store native read (FIX-hive-s3a: scheme normalization + canonical creds) =====

    @Test
    public void nativeScanRangePathNormalizedS3aToS3() {
        // BE's native S3 reader rejects the s3a scheme (S3URI accepts only s3/http/https). The connector lists
        // files with the raw scheme (Hadoop wants s3a) but must normalize the BE-facing range path to s3://.
        // MUTATION: dropping the splitFile normalization leaves the range path s3a:// -> BE "Invalid S3 URI".
        HiveFileListingCache.DirectoryLister s3aLister = (location, fs) ->
                new ArrayList<>(Collections.singletonList(
                        new HiveFileStatus("s3a://bucket/db/t/p/000000_0", 10L, 1L)));
        FakeConnectorContext normCtx = new FakeConnectorContext() {
            @Override
            public String normalizeStorageUri(String rawUri) {
                return rawUri == null ? null : rawUri.replace("s3a://", "s3://");
            }
        };
        HiveScanPlanProvider provider = new HiveScanPlanProvider(new FakeHmsClient(),
                Collections.emptyMap(), normCtx, new HiveReadTransactionManager(),
                new HiveFileListingCache(Collections.emptyMap(), s3aLister));
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .partitionKeyNames(PART_KEYS)
                .prunedPartitions(Collections.singletonList(part("year=2024/month=01")))
                .build();

        List<ConnectorScanRange> ranges = provider.planScanForPartitionBatch(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(),
                Optional.empty(), -1L, Collections.singletonList("year=2024/month=01"));

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("s3://bucket/db/t/p/000000_0",
                ((HiveScanRange) ranges.get(0)).getPath().orElse(null),
                "native hive range path must be scheme-normalized s3a->s3 for BE's native reader");
    }

    @Test
    public void scanNodePropertiesEmitsCanonicalCredsForNativeReader() {
        // BE's native FILE_S3 reader reads ONLY AWS_ACCESS_KEY/AWS_SECRET_KEY/AWS_ENDPOINT (s3_util.cpp), never the
        // raw s3.access_key alias. getScanNodeProperties must emit the BE-canonical creds
        // (getBackendStorageProperties) like legacy HiveScanNode.getLocationProperties did; without them a private
        // bucket 403s. MUTATION: dropping the canonical emission (pre-fix: only raw s3. aliases were emitted).
        FakeConnectorContext credCtx = new FakeConnectorContext() {
            @Override
            public Map<String, String> getBackendStorageProperties() {
                return Collections.singletonMap("AWS_ACCESS_KEY", "canonAK");
            }
        };
        HiveScanPlanProvider provider = new HiveScanPlanProvider(new FakeHmsClient(),
                Collections.singletonMap("s3.access_key", "aliasAK"), credCtx,
                new HiveReadTransactionManager(),
                new HiveFileListingCache(Collections.emptyMap(), new CountingLister()));
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .build();

        Map<String, String> props = provider.getScanNodeProperties(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());

        Assertions.assertEquals("canonAK", props.get("location.AWS_ACCESS_KEY"),
                "BE-canonical AWS_* creds must be emitted for the native reader (legacy parity)");
        // the raw s3. alias is still forwarded (harmless, ignored by BE), so no configured key is dropped
        Assertions.assertEquals("aliasAK", props.get("location.s3.access_key"));
    }

    // ============ #65437: scan-level transactional_hive marker (FileScannerV2 exclusion) ============
    // Sibling to supportsBatchScan's ACID exclusion above. BE picks FileScannerV2 from SCAN-LEVEL params before the
    // per-range splits arrive, and V2 does not apply ACID delete deltas. A full-ACID hive scan must therefore carry
    // the "transactional_hive" marker at scan level (not only per-range) so BE keeps it on FileScanner V1 —
    // otherwise deleted rows reappear.

    @Test
    public void getScanNodePropertiesEmitsTransactionalHiveForFullAcid() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        Map<String, String> txnParams = new HashMap<>();
        txnParams.put("transactional", "true"); // full-ACID: transactional AND not insert_only
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .tableParameters(txnParams)
                .build();
        Map<String, String> props = provider.getScanNodeProperties(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());
        // WHY (Rule 9): drives the scan-level stamp so BE excludes the full-ACID read from FileScannerV2.
        Assertions.assertEquals("true", props.get(HiveScanPlanProvider.PROP_TRANSACTIONAL_HIVE));
    }

    @Test
    public void getScanNodePropertiesOmitsTransactionalHiveForInsertOnly() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        Map<String, String> params = new HashMap<>();
        params.put("transactional", "true");
        params.put("transactional_properties", "insert_only");
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .tableParameters(params)
                .build();
        Map<String, String> props = provider.getScanNodeProperties(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());
        // Insert-only ACID reads have no delete deltas, so FileScannerV2 is correct for them: the marker gates on
        // isFullAcid() (NOT isTransactional()), so it must be ABSENT here to keep the V2 fast path.
        Assertions.assertNull(props.get(HiveScanPlanProvider.PROP_TRANSACTIONAL_HIVE));
    }

    @Test
    public void getScanNodePropertiesOmitsTransactionalHiveForNonTransactional() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat(PARQUET_INPUT_FORMAT)
                .serializationLib(PARQUET_SERDE)
                .build();
        Map<String, String> props = provider.getScanNodeProperties(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());
        Assertions.assertNull(props.get(HiveScanPlanProvider.PROP_TRANSACTIONAL_HIVE));
    }

    @Test
    public void populateScanLevelParamsStampsTransactionalHiveOnlyWhenSignalled() {
        HiveScanPlanProvider provider = provider(new FakeHmsClient(), new CountingLister());
        // With the signal -> scan-level table_format_type = "transactional_hive" (the exact string BE matches on,
        // identical to the per-range HiveScanRange marker).
        TFileScanRangeParams marked = new TFileScanRangeParams();
        provider.populateScanLevelParams(marked,
                Collections.singletonMap(HiveScanPlanProvider.PROP_TRANSACTIONAL_HIVE, "true"));
        Assertions.assertTrue(marked.isSetTableFormatParams());
        Assertions.assertEquals("transactional_hive", marked.getTableFormatParams().getTableFormatType());
        // Without the signal -> untouched, so non-ACID hive keeps the FileScannerV2 fast path.
        TFileScanRangeParams unmarked = new TFileScanRangeParams();
        provider.populateScanLevelParams(unmarked, Collections.<String, String>emptyMap());
        Assertions.assertFalse(unmarked.isSetTableFormatParams());
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
