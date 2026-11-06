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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorTableNotFoundException;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileIterator;
import org.apache.doris.filesystem.FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongBiFunction;

/**
 * Tests {@link HiveConnectorMetadata#estimateDataSizeByListingFiles} (§4.2 read-side SPI, layer 3 — the
 * file-list data-size estimate that fe-core divides by the row width when no exact count or metastore size
 * exists). The real {@code FileSystem} listing is injected as a {@code ToLongFunction<String>} so the
 * sampling / scale-up / summing math (the tricky part, ported from legacy
 * {@code HMSExternalTable.getRowCountFromFileList}) is unit-tested; the raw filesystem I/O is covered by the
 * docker e2e gate.
 */
public class HiveConnectorMetadataFileListStatsTest {

    private static HiveConnectorMetadata metadata(HmsClient client) {
        return new HiveConnectorMetadata(client, HiveTestProperties.minimal(), new FakeConnectorContext());
    }

    private static HiveTableHandle unpartitioned(String location) {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .location(location)
                .build();
    }

    private static HiveTableHandle partitioned() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .partitionKeyNames(Collections.singletonList("dt"))
                .build();
    }

    @Test
    public void unpartitionedTableSumsTableLocation() {
        long size = metadata(new PartitionFakeHmsClient(Collections.emptyList()))
                .estimateDataSize(unpartitioned("s3://wh/t"), 30,
                        (loc, vals) -> "s3://wh/t".equals(loc) ? 1000 : 0);
        Assertions.assertEquals(1000L, size);
    }

    @Test
    public void unpartitionedTableWithNoLocationReturnsMinusOne() {
        long size = metadata(new PartitionFakeHmsClient(Collections.emptyList()))
                .estimateDataSize(unpartitioned(null), 30, (loc, vals) -> 999);
        Assertions.assertEquals(-1L, size);
    }

    @Test
    public void allPartitionsSummedWhenBelowSampleCap() {
        // 3 partitions (< sample cap): the whole table is listed, no scale-up. 100+200+300 = 600.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1", "p2"));
        ToLongBiFunction<String, List<String>> sizes =
                (loc, vals) -> loc.endsWith("p0") ? 100 : loc.endsWith("p1") ? 200 : 300;
        Assertions.assertEquals(600L, metadata(client).estimateDataSize(partitioned(), 30, sizes));
    }

    @Test
    public void sampledPartitionsAreScaledUpToTheWholeTable() {
        // 4 partitions, sampleSize 2 -> list 2, scale up by total/sampled = 4/2. With a UNIFORM per-partition
        // size the result is deterministic regardless of which 2 are shuffled in: 2*100 * (4/2) = 400. This
        // pins the legacy scale-up (totalSize * totalPartitions / samplePartitions). MUTATION: dropping the
        // scale-up returns 200 -> red.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1", "p2", "p3"));
        Assertions.assertEquals(400L, metadata(client).estimateDataSize(partitioned(), 2, (loc, vals) -> 100));
    }

    @Test
    public void zeroTotalSizeReturnsMinusOne() {
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1"));
        Assertions.assertEquals(-1L, metadata(client).estimateDataSize(partitioned(), 30, (loc, vals) -> 0));
    }

    @Test
    public void listingErrorDegradesToMinusOneNotThrow() {
        // A per-location listing failure aborts the estimate to -1 (legacy all-or-nothing best-effort), and
        // must NOT propagate as a query-killing exception. MUTATION: not catching -> the estimate throws -> red.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0"));
        long size = Assertions.assertDoesNotThrow(() -> metadata(client).estimateDataSize(
                partitioned(), 30, (loc, vals) -> {
                    throw new RuntimeException("boom");
                }));
        Assertions.assertEquals(-1L, size);
    }

    @Test
    public void tableAbsenceEscapesFileEstimateAndRestoresClassLoader() {
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Collections.emptyList());
        ConnectorTableNotFoundException missing = new ConnectorTableNotFoundException("db", "t", null);
        client.listingFailure = new HmsClientException("native partition listing failed", missing);
        HiveConnectorMetadata metadata = metadata(client);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader marker = new URLClassLoader(new URL[0], original);
        Thread.currentThread().setContextClassLoader(marker);
        try {
            Assertions.assertSame(missing, Assertions.assertThrows(ConnectorTableNotFoundException.class,
                    () -> metadata.estimateDataSizeByListingFiles(null, partitioned())));
            Assertions.assertSame(marker, Thread.currentThread().getContextClassLoader());
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void ordinaryPartitionListingFailureStillDegradesEstimate() {
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Collections.emptyList());
        client.listingFailure = new HmsClientException("permission denied");
        Assertions.assertEquals(-1L, metadata(client).estimateDataSizeByListingFiles(null, partitioned()));
    }

    @Test
    public void partitionWithoutLocationIsSkipped() {
        // A partition carrying no location contributes nothing but must not break the estimate.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1"));
        client.dropLocationFor("p1");
        Assertions.assertEquals(100L, metadata(client).estimateDataSize(
                partitioned(), 30, (loc, vals) -> loc.endsWith("p0") ? 100 : 0));
    }

    @Test
    public void missingCachedPartitionMakesTheWholeEstimateUnknown() {
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1"));
        Assertions.assertEquals(-1L, metadata(client).estimateDataSize(partitioned(), 30,
                (loc, vals) -> loc.endsWith("p0") ? 100 : -1));
    }

    @Test
    public void scaleSampledSizeMultipliesBeforeDividing() {
        // Non-divisible case pins multiply-first (250*4/3 = 333), distinguishing it from a divide-first
        // reordering (250/3*4 = 83*4 = 332) that a mutation might introduce. MUTATION: divide-first -> 332 -> red.
        Assertions.assertEquals(333L, HiveConnectorMetadata.scaleSampledSize(250, 4, 3));
    }

    @Test
    public void publicColdEntryReturnsUnknownAndRestoresClassLoaderWithoutListing() {
        // A cold statistics lookup must neither enumerate the directory nor change the caller's TCCL.
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .location("file:///__doris_stats_nonexistent_xyz_998877").build();
        HiveConnectorMetadata metadata = metadata(new PartitionFakeHmsClient(Collections.emptyList()));

        ClassLoader marker = new URLClassLoader(new URL[0],
                HiveConnectorMetadataFileListStatsTest.class.getClassLoader());
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(marker);
        try {
            long size = Assertions.assertDoesNotThrow(
                    () -> metadata.estimateDataSizeByListingFiles(null, handle));
            Assertions.assertEquals(-1L, size, "a cold location has no complete size hint");
            Assertions.assertSame(marker, Thread.currentThread().getContextClassLoader(),
                    "the TCCL pin must be restored on a cache miss (try/finally)");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void completedSizeSurvivesAbandonedOrDisabledListCandidatesWithoutRelisting() {
        for (String[] limits : new String[][] {{"4096", "67108864"}, {"0", "67108864"}, {"8388608", "0"}}) {
            HiveCatalogProperties properties = HiveCatalogProperties.of(HiveTestProperties.mapWith(
                    HiveCatalogProperties.FILE_LISTING_CACHE_ENTRY_MAX_BYTES, limits[0],
                    HiveCatalogProperties.FILE_LISTING_CACHE_PENDING_MAX_BYTES, limits[1]));
            HiveFileListingCache cache = new HiveFileListingCache(properties);
            AtomicInteger listings = new AtomicInteger();
            AtomicInteger reads = new AtomicInteger();
            FakeFileSystem fs = new FakeFileSystem().withIteratorFactory(location -> {
                listings.incrementAndGet();
                return new FileIterator() {
                    @Override
                    public boolean hasNext() {
                        return reads.get() < 100;
                    }

                    @Override
                    public FileEntry next() {
                        return FakeFileSystem.file("s3://wh/t/" + "x".repeat(300)
                                + reads.getAndIncrement() + ".parquet", 80, 1);
                    }

                    @Override
                    public void close() {
                    }
                };
            });
            HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                    new PartitionFakeHmsClient(Collections.emptyList()), properties, new FakeConnectorContext(),
                    () -> null, () -> null, handle -> null, cache);
            HiveTableHandle table = unpartitioned("s3://wh/t");
            Assertions.assertEquals(-1, metadata.estimateDataSizeByListingFiles(null, table));
            Assertions.assertEquals(0, listings.get(), "cold planning must not list files");
            try (HiveFileIterator files = cache.openDataFiles("db", "t", table.getLocation(), List.of(), fs,
                    new ConnectorScanContext())) {
                files.next();
                Assertions.assertEquals(-1, metadata.estimateDataSizeByListingFiles(null, table),
                        "a consumed prefix is not a complete size summary");
                files.forEachRemaining(file -> { });
            }
            Assertions.assertEquals(100, reads.get());
            Assertions.assertEquals(0, cache.size(), "the full file list must not be retained");
            Assertions.assertEquals(8000, metadata.estimateDataSizeByListingFiles(null, table));
            Assertions.assertEquals(8000, metadata.estimateDataSizeByListingFiles(null, table));
            Assertions.assertEquals(1, listings.get(), "future row-count reloads use only the complete summary");
            cache.invalidateTable("db", "t");
            Assertions.assertEquals(-1, metadata.estimateDataSizeByListingFiles(null, table));
            Assertions.assertEquals(1, listings.get(), "refresh must not synchronously relist either");
        }
    }

    @Test
    public void listFileSizesPropagatesListingErrorAndRestoresClassLoader() {
        // ANALYZE ... WITH SAMPLE reads raw per-file sizes here. UNLIKE estimateDataSizeByListingFiles (best-effort
        // -1 for query planning), a listing failure must PROPAGATE: legacy HMSExternalTable.getChunkSizes failed the
        // sampled ANALYZE loud rather than let the sampler collapse the scale factor to 1.0 while TABLESAMPLE still
        // fires (a silent stat undercount). The TCCL pin must still be restored on the throw path (try/finally).
        // MUTATION: re-adding a catch -> Collections.emptyList swallows the error -> assertThrows red.
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(
                new PartitionFakeHmsClient(Collections.emptyList()), HiveTestProperties.minimal(), new FakeConnectorContext(),
                () -> null, () -> null, handle -> null, new ThrowingFileListingCache());
        HiveTableHandle handle = unpartitioned("s3://wh/t");

        ClassLoader marker = new URLClassLoader(new URL[0],
                HiveConnectorMetadataFileListStatsTest.class.getClassLoader());
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(marker);
        try {
            Assertions.assertThrows(RuntimeException.class, () -> metadata.listFileSizes(null, handle),
                    "a listing failure during sample analyze must fail loud, not degrade to empty");
            Assertions.assertSame(marker, Thread.currentThread().getContextClassLoader(),
                    "the TCCL pin must be restored on the throw path (try/finally)");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    public void nonHiveTableTypeIsNotEstimated() {
        // A hudi/iceberg-on-HMS table is served by its own connector; the hive gateway must return -1 BEFORE
        // any filesystem I/O (the public entry point's tableType guard). MUTATION: dropping the guard would
        // list files for a foreign format.
        HiveTableHandle hudiHandle = new HiveTableHandle.Builder("db", "t", HiveTableType.HUDI)
                .location("s3://wh/t").build();
        Assertions.assertEquals(-1L,
                metadata(new PartitionFakeHmsClient(Collections.emptyList()))
                        .estimateDataSizeByListingFiles(null, hudiHandle));
    }

    /** A {@link HiveFileListingCache} whose listing always fails, to prove listFileSizes propagates (not swallows). */
    private static final class ThrowingFileListingCache extends HiveFileListingCache {
        ThrowingFileListingCache() {
            super(HiveTestProperties.minimal());
        }

        @Override
        HiveFileIterator openDataFiles(String dbName, String tableName, String location,
                List<String> partitionValues, FileSystem fs, ConnectorScanContext context) {
            throw new RuntimeException("simulated listing failure");
        }
    }

    /**
     * {@link HmsClient} double serving a fixed set of partition names, each with a synthetic location
     * {@code s3://wh/t/<name>}. {@code listPartitionNames} echoes the names; {@code getPartitions} builds an
     * {@link HmsPartitionInfo} per requested name. The rest fail loud.
     */
    private static final class PartitionFakeHmsClient implements HmsClient {
        private final List<String> partitionNames;
        private RuntimeException listingFailure;
        private final java.util.Set<String> withoutLocation = new java.util.HashSet<>();

        PartitionFakeHmsClient(List<String> partitionNames) {
            this.partitionNames = partitionNames;
        }

        void dropLocationFor(String name) {
            withoutLocation.add(name);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            if (listingFailure != null) {
                throw listingFailure;
            }
            return partitionNames;
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            List<HmsPartitionInfo> result = new ArrayList<>(partNames.size());
            for (String name : partNames) {
                String location = withoutLocation.contains(name) ? null : "s3://wh/t/" + name;
                result.add(new HmsPartitionInfo(
                        Collections.singletonList(name), location, null, null, null, Collections.emptyMap()));
            }
            return result;
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
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
