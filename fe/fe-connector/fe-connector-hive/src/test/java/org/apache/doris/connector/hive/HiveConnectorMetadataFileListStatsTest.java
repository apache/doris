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
import org.apache.doris.connector.hms.HmsDatabaseInfo;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

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
        return new HiveConnectorMetadata(client, Collections.emptyMap(), new FakeConnectorContext());
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
                .estimateDataSize(unpartitioned("s3://wh/t"), 30, loc -> "s3://wh/t".equals(loc) ? 1000 : 0);
        Assertions.assertEquals(1000L, size);
    }

    @Test
    public void unpartitionedTableWithNoLocationReturnsMinusOne() {
        long size = metadata(new PartitionFakeHmsClient(Collections.emptyList()))
                .estimateDataSize(unpartitioned(null), 30, loc -> 999);
        Assertions.assertEquals(-1L, size);
    }

    @Test
    public void allPartitionsSummedWhenBelowSampleCap() {
        // 3 partitions (< sample cap): the whole table is listed, no scale-up. 100+200+300 = 600.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1", "p2"));
        ToLongFunction<String> sizes = loc -> loc.endsWith("p0") ? 100 : loc.endsWith("p1") ? 200 : 300;
        Assertions.assertEquals(600L, metadata(client).estimateDataSize(partitioned(), 30, sizes));
    }

    @Test
    public void sampledPartitionsAreScaledUpToTheWholeTable() {
        // 4 partitions, sampleSize 2 -> list 2, scale up by total/sampled = 4/2. With a UNIFORM per-partition
        // size the result is deterministic regardless of which 2 are shuffled in: 2*100 * (4/2) = 400. This
        // pins the legacy scale-up (totalSize * totalPartitions / samplePartitions). MUTATION: dropping the
        // scale-up returns 200 -> red.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1", "p2", "p3"));
        Assertions.assertEquals(400L, metadata(client).estimateDataSize(partitioned(), 2, loc -> 100));
    }

    @Test
    public void zeroTotalSizeReturnsMinusOne() {
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1"));
        Assertions.assertEquals(-1L, metadata(client).estimateDataSize(partitioned(), 30, loc -> 0));
    }

    @Test
    public void listingErrorDegradesToMinusOneNotThrow() {
        // A per-location listing failure aborts the estimate to -1 (legacy all-or-nothing best-effort), and
        // must NOT propagate as a query-killing exception. MUTATION: not catching -> the estimate throws -> red.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0"));
        long size = Assertions.assertDoesNotThrow(() -> metadata(client).estimateDataSize(
                partitioned(), 30, loc -> {
                    throw new RuntimeException("boom");
                }));
        Assertions.assertEquals(-1L, size);
    }

    @Test
    public void partitionWithoutLocationIsSkipped() {
        // A partition carrying no location contributes nothing but must not break the estimate.
        PartitionFakeHmsClient client = new PartitionFakeHmsClient(Arrays.asList("p0", "p1"));
        client.dropLocationFor("p1");
        Assertions.assertEquals(100L, metadata(client).estimateDataSize(
                partitioned(), 30, loc -> loc.endsWith("p0") ? 100 : 0));
    }

    @Test
    public void scaleSampledSizeMultipliesBeforeDividing() {
        // Non-divisible case pins multiply-first (250*4/3 = 333), distinguishing it from a divide-first
        // reordering (250/3*4 = 83*4 = 332) that a mutation might introduce. MUTATION: divide-first -> 332 -> red.
        Assertions.assertEquals(333L, HiveConnectorMetadata.scaleSampledSize(250, 4, 3));
    }

    @Test
    public void publicEntryDegradesToMinusOneAndRestoresClassLoaderOnError() {
        // Drives the PUBLIC entry point: its tableType guard passes for HIVE, then it pins the thread context
        // classloader and does real FileSystem I/O. A bogus location makes the listing fail, so the estimate
        // must degrade to -1 WITHOUT throwing, and the TCCL must be restored to whatever it was (the pin is in
        // a try/finally). MUTATION: dropping the finally leaves the plugin loader set -> the marker assertion
        // fails; letting the listing error propagate -> assertDoesNotThrow fails.
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
            Assertions.assertEquals(-1L, size, "an unlistable location must degrade to -1");
            Assertions.assertSame(marker, Thread.currentThread().getContextClassLoader(),
                    "the TCCL pin must be restored on the error path (try/finally)");
        } finally {
            Thread.currentThread().setContextClassLoader(original);
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
                new PartitionFakeHmsClient(Collections.emptyList()), Collections.emptyMap(), new FakeConnectorContext(),
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
            super(Collections.emptyMap());
        }

        @Override
        public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location,
                Configuration conf) {
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
        private final java.util.Set<String> withoutLocation = new java.util.HashSet<>();

        PartitionFakeHmsClient(List<String> partitionNames) {
            this.partitionNames = partitionNames;
        }

        void dropLocationFor(String name) {
            withoutLocation.add(name);
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
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
