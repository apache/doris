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
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.hms.HmsPartitionInfo;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests {@link HiveFileListingCache}: the connector-owned directory-listing cache (D2's second layer, separate
 * from the CachingHmsClient metastore layer — Trino keeps CachingDirectoryLister separate too).
 *
 * <p>WHY (Rule 9): after the hms cutover the fe-core engine-side {@code HiveExternalMetaCache} stops routing to a
 * hive catalog, so without this connector-owned cache every scan (and every periodic row-count refresh) would
 * re-list every partition directory from the filesystem. These tests pin the behaviours that make the re-homed
 * cache correct: (1) a directory listing is cached keyed by {@code (db, table, location)} so it loads once; the db
 * / table / location dimensions never collide; (2) {@code invalidateTable} drops exactly one table's entries and
 * {@code invalidateAll} clears all (arming REFRESH); (3) an I/O failure propagates and is NOT cached; (4) the
 * {@code meta.cache.hive.file.*} knobs turn it off; (5) the real filesystem lister filters directories and
 * {@code _}/{@code .}-hidden files. Two integration tests prove BOTH consumers — the scan provider and the
 * row-count estimate — are served from the SAME cache, so a repeated scan / refresh does not re-list.</p>
 *
 * <p>Dormant: {@code "hms"} is not in {@code SPI_READY_TYPES}, so no live catalog builds a connector; this
 * exercises the cache directly.</p>
 */
public class HiveFileListingCacheTest {

    private static final Configuration CONF = new Configuration();

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // ==================== caching: hit / miss keyed by (db, table, location) ====================

    @Test
    public void listingIsCachedPerLocation() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        List<HiveFileStatus> a = cache.listDataFiles("db", "t", "loc1", CONF);
        List<HiveFileStatus> b = cache.listDataFiles("db", "t", "loc1", CONF);
        // WHY: a hit must serve the cached listing without re-listing the filesystem.
        Assertions.assertSame(a, b);
        Assertions.assertEquals(1, lister.totalCalls);

        // WHY: a different directory is a different key — must re-list.
        cache.listDataFiles("db", "t", "loc2", CONF);
        Assertions.assertEquals(2, lister.totalCalls);
    }

    @Test
    public void keyIsScopedByDbTableAndLocation() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        // Same location string, different db / table. WHY: (db, table) must be part of the key so invalidateTable
        // can scope one table — and so two tables that happen to share a path never serve each other's listing.
        cache.listDataFiles("db1", "t", "loc", CONF);
        cache.listDataFiles("db2", "t", "loc", CONF);
        cache.listDataFiles("db1", "t2", "loc", CONF);
        Assertions.assertEquals(3, lister.totalCalls);
    }

    // ==================== invalidation (arms REFRESH TABLE / REFRESH CATALOG) ====================

    @Test
    public void invalidateTableDropsOnlyThatTablesEntries() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        cache.listDataFiles("db", "t1", "locA", CONF);
        cache.listDataFiles("db", "t2", "locB", CONF);
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateTable("db", "t1");

        // WHY: t1 must re-list after its invalidation...
        cache.listDataFiles("db", "t1", "locA", CONF);
        Assertions.assertEquals(2, (int) lister.callsPerLocation.get("locA"));
        // ...while t2's entry (a different table) must survive — invalidateTable is scoped by (db, table).
        cache.listDataFiles("db", "t2", "locB", CONF);
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("locB"));
    }

    @Test
    public void invalidateAllDropsEverything() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        cache.listDataFiles("db", "t1", "locA", CONF);
        cache.listDataFiles("db", "t2", "locB", CONF);
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateAll();

        // WHY: every entry must reload after invalidateAll (REFRESH CATALOG).
        cache.listDataFiles("db", "t1", "locA", CONF);
        cache.listDataFiles("db", "t2", "locB", CONF);
        Assertions.assertEquals(4, lister.totalCalls);
    }

    // ==================== failures are propagated, never cached ====================

    @Test
    public void listingFailureIsPropagatedAndNotCached() {
        CountingLister lister = new CountingLister();
        lister.error = new DorisConnectorException("boom");
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> cache.listDataFiles("db", "t", "loc", CONF));
        Assertions.assertEquals("boom", e.getMessage());
        Assertions.assertEquals(1, lister.totalCalls);

        // WHY: a transient listing failure must NOT be cached — after recovery the next call re-lists and succeeds
        // (otherwise a momentary storage blip would poison the listing for the whole TTL).
        lister.error = null;
        List<HiveFileStatus> ok = cache.listDataFiles("db", "t", "loc", CONF);
        Assertions.assertEquals(1, ok.size());
        Assertions.assertEquals(2, lister.totalCalls);
    }

    // ==================== per-entry property knob ====================

    @Test
    public void disablingViaPropsBypassesTheCache() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(
                props("meta.cache.hive.file.enable", "false"), lister);

        cache.listDataFiles("db", "t", "loc", CONF);
        cache.listDataFiles("db", "t", "loc", CONF);
        // WHY: meta.cache.hive.file.enable=false must bypass caching entirely — every call re-lists.
        Assertions.assertEquals(2, lister.totalCalls);
    }

    // ==================== the real filesystem lister: filters dirs + hidden files ====================

    @Test
    public void listFromFileSystemFiltersDirectoriesAndHiddenFiles(@TempDir java.nio.file.Path dir) throws Exception {
        Files.write(dir.resolve("data1"), new byte[100]);
        Files.write(dir.resolve("data2"), new byte[200]);
        Files.write(dir.resolve("_SUCCESS"), new byte[3]);      // hive success marker — must be skipped
        Files.write(dir.resolve(".hidden"), new byte[3]);       // dot-file — must be skipped
        Files.createDirectory(dir.resolve("subdir"));           // directory — must be skipped

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(dir.toUri().toString(), CONF);

        // WHY: only the two real data files survive; the total size is exactly their sum. This pins the same
        // dir/_-/.-filter the pre-cache scan and estimate paths applied inline.
        Assertions.assertEquals(2, files.size(), "only the two data files must survive the dir/hidden filter");
        long totalSize = files.stream().mapToLong(HiveFileStatus::getLength).sum();
        Assertions.assertEquals(300L, totalSize);
        for (HiveFileStatus f : files) {
            String name = f.getPath().substring(f.getPath().lastIndexOf('/') + 1);
            Assertions.assertTrue(name.equals("data1") || name.equals("data2"), "unexpected file: " + name);
        }
    }

    // ==================== failure split: systemic (FileSystem.get) is loud, local (listStatus) is skippable ======

    @Test
    public void listFromFileSystemFailsLoudWhenFilesystemCannotBeResolved() {
        // An unknown scheme makes FileSystem.get fail (no FileSystem for scheme) — a SYSTEMIC storage-config error
        // that affects every partition of the table. It must throw a plain DorisConnectorException (which the scan
        // path does NOT skip), and NOT the skippable HiveDirectoryListingException.
        // WHY (Rule 9): if this were the skippable subtype, a misconfigured storage would silently return an empty
        // scan instead of failing the query loud — the exact regression this fix prevents.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveFileListingCache.listFromFileSystem("nosuchscheme://host/path", CONF));
        Assertions.assertFalse(e instanceof HiveDirectoryListingException,
                "a filesystem-resolution failure must be loud (plain DorisConnectorException), not skippable");
    }

    @Test
    public void listFromFileSystemIsSkippableWhenDirectoryMissing(@TempDir java.nio.file.Path dir) {
        // A resolvable filesystem (file://) but a non-existent directory makes listStatus fail — a LOCAL failure of
        // one partition directory. It must throw the skippable HiveDirectoryListingException so the scan skips just
        // that partition (pre-cache tolerance of a missing/unreadable partition dir).
        String missing = dir.resolve("does-not-exist").toUri().toString();
        Assertions.assertThrows(HiveDirectoryListingException.class,
                () -> HiveFileListingCache.listFromFileSystem(missing, CONF));
    }

    @Test
    public void getFailureThroughCacheFailsLoudAndIsNotCached() {
        // Drives the REAL production lister THROUGH the cache lookup with an unresolvable scheme, so
        // FileSystem.get genuinely fails. WHY (Rule 9): pins that (1) a systemic storage-config failure propagates
        // from listDataFiles as the loud plain DorisConnectorException — MetaCacheEntry's manual-miss load rethrows
        // RuntimeException unwrapped, so the type survives the cache boundary — and NOT the skippable subtype; and
        // (2) the failure leaves NO cache entry. (2) kills the mutation "catch -> return emptyList" in the loader,
        // which would cache a poisoned empty listing and silently turn every later scan into 0 rows for the TTL.
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap());

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> cache.listDataFiles("db", "t", "nosuchfs://host/path", CONF));
        Assertions.assertFalse(e instanceof HiveDirectoryListingException,
                "a filesystem-resolution failure through the cache must be the loud type, not the skippable one");
        Assertions.assertEquals(0L, cache.size(), "a failed load must never leave a cache entry");
    }

    @Test
    public void listStatusFailureThroughCacheIsSkippableAndNotCached(@TempDir java.nio.file.Path dir) {
        // Drives the REAL production lister THROUGH the cache with a resolvable filesystem (file://) but a missing
        // directory, so listStatus genuinely fails. WHY (Rule 9): pins that the LOCAL per-directory failure keeps
        // its distinct skippable type (HiveDirectoryListingException, exactly what the scan's per-partition skip
        // catches) across the cache boundary, and leaves no cache entry either.
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap());
        String missing = dir.resolve("does-not-exist").toUri().toString();

        Assertions.assertThrows(HiveDirectoryListingException.class,
                () -> cache.listDataFiles("db", "t", missing, CONF));
        Assertions.assertEquals(0L, cache.size(), "a failed load must never leave a cache entry");
    }

    @Test
    public void scanSkipsOnlyTheFailedPartitionAndPlansTheRest() {
        // A LOCAL per-directory listing failure (HiveDirectoryListingException) on ONE partition among several
        // must be tolerated PER PARTITION: the failed one is skipped with a warning, every other partition still
        // plans its splits — the pre-cache resilience. WHY (Rule 9): if the scan aborted on the subtype, or the
        // skip were scan-wide instead of per-partition, one bad directory would lose the whole table.
        CountingLister lister = new CountingLister();
        lister.error = new HiveDirectoryListingException("dir gone", new IOException("boom"));
        lister.errorLocation = "loc/dt=1";
        HiveScanPlanProvider provider = new HiveScanPlanProvider(null, Collections.emptyMap(),
                new HiveReadTransactionManager(), new HiveFileListingCache(Collections.emptyMap(), lister));

        List<ConnectorScanRange> ranges = provider.planScan(
                new FakeSession(), twoPartitionHandle(), Collections.<ConnectorColumnHandle>emptyList(),
                Optional.empty());

        // Both partitions were attempted; only the healthy one produced its (one-file, one-range) split.
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("loc/dt=1"));
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("loc/dt=2"));
        Assertions.assertEquals(1, ranges.size(),
                "the failed partition is skipped, the healthy partition must still plan its split");
    }

    @Test
    public void scanFailsLoudOnSystemicFilesystemFailure() {
        // A SYSTEMIC filesystem-resolution failure (plain DorisConnectorException, affects every partition) must
        // fail the query loud, NOT be swallowed into a silent empty scan.
        // MUTATION: if listAndSplitFiles caught the base DorisConnectorException (the pre-fix behavior) this would
        // return an empty range list instead of throwing -> red.
        CountingLister lister = new CountingLister();
        lister.error = new DorisConnectorException("bad storage config");
        HiveScanPlanProvider provider = new HiveScanPlanProvider(null, Collections.emptyMap(),
                new HiveReadTransactionManager(), new HiveFileListingCache(Collections.emptyMap(), lister));

        Assertions.assertThrows(DorisConnectorException.class, () -> provider.planScan(
                new FakeSession(), singlePartitionHandle(), Collections.<ConnectorColumnHandle>emptyList(),
                Optional.empty()));
    }

    private static HiveTableHandle singlePartitionHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .partitionKeyNames(Collections.singletonList("dt"))
                .prunedPartitions(Collections.singletonList(
                        new HmsPartitionInfo(Collections.singletonList("1"), "loc/dt=1", null, null, null, null)))
                .build();
    }

    private static HiveTableHandle twoPartitionHandle() {
        return new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .partitionKeyNames(Collections.singletonList("dt"))
                .prunedPartitions(Arrays.asList(
                        new HmsPartitionInfo(Collections.singletonList("1"), "loc/dt=1", null, null, null, null),
                        new HmsPartitionInfo(Collections.singletonList("2"), "loc/dt=2", null, null, null, null)))
                .build();
    }

    // ==================== integration: the scan provider is cache-backed ====================

    @Test
    public void scanProviderServesRepeatedScansFromTheCache() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);
        // hmsClient is null: with pruned partitions on the handle the scan never calls the metastore.
        HiveScanPlanProvider provider = new HiveScanPlanProvider(
                null, Collections.emptyMap(), new HiveReadTransactionManager(), cache);

        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .partitionKeyNames(Collections.singletonList("dt"))
                .prunedPartitions(Arrays.asList(
                        new HmsPartitionInfo(Collections.singletonList("1"), "loc/dt=1", null, null, null, null),
                        new HmsPartitionInfo(Collections.singletonList("2"), "loc/dt=2", null, null, null, null)))
                .build();

        List<ConnectorScanRange> first = provider.planScan(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());
        List<ConnectorScanRange> second = provider.planScan(
                new FakeSession(), handle, Collections.<ConnectorColumnHandle>emptyList(), Optional.empty());

        // WHY: each partition directory is listed exactly once across the two scans — the second scan is served
        // from the cache. Without the cache the file listing would be an uncached RPC/FS call on every scan.
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("loc/dt=1"));
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("loc/dt=2"));
        // One 10-byte file per partition -> one range each; both scans plan the same ranges.
        Assertions.assertEquals(2, first.size());
        Assertions.assertEquals(2, second.size());
    }

    // ==================== integration: the row-count estimate is cache-backed (7-arg wiring) ====================

    @Test
    public void estimateDataSizeIsServedFromTheCache() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);
        // The production 7-arg constructor injects the connector's shared cache; the sibling seams are unused for
        // a plain-hive handle, so dummy suppliers suffice.
        HiveConnectorMetadata md = new HiveConnectorMetadata(
                null, Collections.emptyMap(), new FakeConnectorContext(),
                () -> null, () -> null, h -> null, cache);

        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .location("file:///wh/t")
                .build();

        long first = md.estimateDataSizeByListingFiles(null, handle);
        long second = md.estimateDataSizeByListingFiles(null, handle);

        // WHY: the estimate returns the one 10-byte file's size, and the second (periodic ExternalRowCountCache
        // refresh) call is served from the SAME cache — one listing, not two. This proves the row-count refresh
        // reuses a listing a scan warmed (and vice versa), the §4.4 5th-cache coupling.
        Assertions.assertEquals(10L, first);
        Assertions.assertEquals(10L, second);
        Assertions.assertEquals(1, lister.totalCalls);
    }

    /**
     * A {@link HiveFileListingCache.DirectoryLister} double: counts calls (total + per location) and returns a
     * fresh single-file listing per call, so reference identity distinguishes a cache hit from a reload. Throws
     * {@link #error} when set, to exercise the failure-not-cached path.
     */
    private static final class CountingLister implements HiveFileListingCache.DirectoryLister {
        final Map<String, Integer> callsPerLocation = new HashMap<>();
        int totalCalls;
        RuntimeException error;
        // When set, error is thrown only for this location (a single bad partition among healthy ones).
        String errorLocation;

        @Override
        public List<HiveFileStatus> list(String location, Configuration conf) {
            totalCalls++;
            callsPerLocation.merge(location, 1, Integer::sum);
            if (error != null && (errorLocation == null || errorLocation.equals(location))) {
                throw error;
            }
            return new ArrayList<>(Collections.singletonList(new HiveFileStatus(location + "/000000_0", 10L, 1L)));
        }
    }

    /** Minimal {@link ConnectorSession} for planScan (no split-size override, empty session properties). */
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
