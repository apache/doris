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
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
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
 * {@code meta.cache.hive.file.*} knobs turn it off; (5) the production lister — now driving the engine-injected
 * Doris {@link FileSystem} instead of bare Hadoop — filters directories and {@code _}/{@code .}-hidden files, lists
 * literally (never glob-expands), and keeps the two failure semantics (systemic loud vs local skippable). Two
 * integration tests prove BOTH consumers — the scan provider and the row-count estimate — are served from the SAME
 * cache, so a repeated scan / refresh does not re-list.</p>
 *
 * <p>Dormant: {@code "hms"} is not in {@code SPI_READY_TYPES}, so no live catalog builds a connector; this
 * exercises the cache directly.</p>
 */
public class HiveFileListingCacheTest {

    // A filesystem placeholder for the CountingLister-based tests: the fake lister ignores it (it lists above the
    // FS seam), so any non-null FileSystem suffices — mirrors the role the old Configuration CONF constant played.
    private static final FileSystem FS = new FakeFileSystem();

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

        List<HiveFileStatus> a = cache.listDataFiles("db", "t", "loc1", FS);
        List<HiveFileStatus> b = cache.listDataFiles("db", "t", "loc1", FS);
        // WHY: a hit must serve the cached listing without re-listing the filesystem.
        Assertions.assertSame(a, b);
        Assertions.assertEquals(1, lister.totalCalls);

        // WHY: a different directory is a different key — must re-list.
        cache.listDataFiles("db", "t", "loc2", FS);
        Assertions.assertEquals(2, lister.totalCalls);
    }

    @Test
    public void keyIsScopedByDbTableAndLocation() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        // Same location string, different db / table. WHY: (db, table) must be part of the key so invalidateTable
        // can scope one table — and so two tables that happen to share a path never serve each other's listing.
        cache.listDataFiles("db1", "t", "loc", FS);
        cache.listDataFiles("db2", "t", "loc", FS);
        cache.listDataFiles("db1", "t2", "loc", FS);
        Assertions.assertEquals(3, lister.totalCalls);
    }

    // ==================== invalidation (arms REFRESH TABLE / REFRESH CATALOG) ====================

    @Test
    public void invalidateTableDropsOnlyThatTablesEntries() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        cache.listDataFiles("db", "t1", "locA", FS);
        cache.listDataFiles("db", "t2", "locB", FS);
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateTable("db", "t1");

        // WHY: t1 must re-list after its invalidation...
        cache.listDataFiles("db", "t1", "locA", FS);
        Assertions.assertEquals(2, (int) lister.callsPerLocation.get("locA"));
        // ...while t2's entry (a different table) must survive — invalidateTable is scoped by (db, table).
        cache.listDataFiles("db", "t2", "locB", FS);
        Assertions.assertEquals(1, (int) lister.callsPerLocation.get("locB"));
    }

    @Test
    public void invalidateAllDropsEverything() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        cache.listDataFiles("db", "t1", "locA", FS);
        cache.listDataFiles("db", "t2", "locB", FS);
        Assertions.assertEquals(2, lister.totalCalls);

        cache.invalidateAll();

        // WHY: every entry must reload after invalidateAll (REFRESH CATALOG).
        cache.listDataFiles("db", "t1", "locA", FS);
        cache.listDataFiles("db", "t2", "locB", FS);
        Assertions.assertEquals(4, lister.totalCalls);
    }

    // ==================== failures are propagated, never cached ====================

    @Test
    public void listingFailureIsPropagatedAndNotCached() {
        CountingLister lister = new CountingLister();
        lister.error = new DorisConnectorException("boom");
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap(), lister);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> cache.listDataFiles("db", "t", "loc", FS));
        Assertions.assertEquals("boom", e.getMessage());
        Assertions.assertEquals(1, lister.totalCalls);

        // WHY: a transient listing failure must NOT be cached — after recovery the next call re-lists and succeeds
        // (otherwise a momentary storage blip would poison the listing for the whole TTL).
        lister.error = null;
        List<HiveFileStatus> ok = cache.listDataFiles("db", "t", "loc", FS);
        Assertions.assertEquals(1, ok.size());
        Assertions.assertEquals(2, lister.totalCalls);
    }

    // ==================== per-entry property knob ====================

    @Test
    public void disablingViaPropsBypassesTheCache() {
        CountingLister lister = new CountingLister();
        HiveFileListingCache cache = new HiveFileListingCache(
                props("meta.cache.hive.file.enable", "false"), lister);

        cache.listDataFiles("db", "t", "loc", FS);
        cache.listDataFiles("db", "t", "loc", FS);
        // WHY: meta.cache.hive.file.enable=false must bypass caching entirely — every call re-lists.
        Assertions.assertEquals(2, lister.totalCalls);
    }

    // ==================== the production lister: filters dirs + hidden files, lists literally ====================

    @Test
    public void listFromFileSystemFiltersDirectoriesAndHiddenFiles() {
        String dir = "file:///wh/db/t/dt=1";
        FakeFileSystem fs = new FakeFileSystem().withEntries(
                FakeFileSystem.file(dir + "/data1", 100L, 1L),
                FakeFileSystem.file(dir + "/data2", 200L, 1L),
                FakeFileSystem.file(dir + "/_SUCCESS", 3L, 1L),   // hive success marker — must be skipped
                FakeFileSystem.file(dir + "/.hidden", 3L, 1L),    // dot-file — must be skipped
                FakeFileSystem.dir(dir + "/subdir"));             // directory — must be skipped

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(dir, fs);

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

    @Test
    public void listFromFileSystemPreservesPathLengthAndModTime() {
        // WHY (Rule 9): the FileEntry -> HiveFileStatus mapping must be byte-parity — the path string verbatim
        // (Location.uri(), no re-encoding of hive's %3A timestamp partitions), the byte length and the mtime the
        // BE reads to detect a changed split. A field transposition would flip these.
        String dir = "file:///wh/db/t/pt=2024-04-09 12%3A34%3A56";
        FakeFileSystem fs = new FakeFileSystem().withEntries(
                FakeFileSystem.file(dir + "/000000_0", 4096L, 1712660096000L));

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(dir, fs);

        Assertions.assertEquals(1, files.size());
        Assertions.assertEquals(dir + "/000000_0", files.get(0).getPath());
        Assertions.assertEquals(4096L, files.get(0).getLength());
        Assertions.assertEquals(1712660096000L, files.get(0).getModificationTime());
    }

    @Test
    public void listFromFileSystemListsLiterallyForGlobCharLocation() {
        // WHY (Rule 9): a location containing a glob metachar ('[' '*' '?') — e.g. a custom external SET LOCATION —
        // must be listed LITERALLY (old fs.listStatus never glob-expanded). The production lister must use the
        // literal list(), not the per-scheme glob-aware listFiles() override. FakeFileSystem.listFiles() throws
        // AssertionError, so if the lister ever regressed to listFiles() this test would fail loudly.
        String dir = "file:///wh/db/my[table]/dt=1";
        FakeFileSystem fs = new FakeFileSystem().withEntries(
                FakeFileSystem.file(dir + "/000000_0", 10L, 1L));

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(dir, fs);

        Assertions.assertEquals(1, files.size(), "the literal directory must be listed, not glob-matched");
        Assertions.assertEquals(dir + "/000000_0", files.get(0).getPath());
    }

    // ==================== recursive listing (hive.recursive_directories, default true) ====================

    /** A three-level tree: top-level exp_a plus sub-directories 1/ (exp_b) and 2/ (exp_c). */
    private static Map<String, List<FileEntry>> recursiveTree(String top) {
        Map<String, List<FileEntry>> tree = new HashMap<>();
        tree.put(top, Arrays.asList(
                FakeFileSystem.file(top + "/exp_a", 1L, 1L),
                FakeFileSystem.dir(top + "/1"),
                FakeFileSystem.dir(top + "/2")));
        tree.put(top + "/1", Collections.singletonList(FakeFileSystem.file(top + "/1/exp_b", 1L, 1L)));
        tree.put(top + "/2", Collections.singletonList(FakeFileSystem.file(top + "/2/exp_c", 1L, 1L)));
        return tree;
    }

    @Test
    public void recursiveDescendsIntoSubdirectories() {
        // WHY (Rule 9): a table whose data lives in sub-directories (top + 1/ + 2/) must contribute ALL its files
        // when recursion is on — else those rows are silently lost (the regression this restores). Mirrors
        // hive_config_test's hive_recursive_directories_table (tags 2/21 = 6 rows).
        String top = "file:///wh/db/t/dt=1";
        FakeFileSystem fs = new FakeFileSystem().withTree(recursiveTree(top));

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(top, fs, true);

        Assertions.assertEquals(3, files.size(), "recursive listing must include files from every sub-directory");
    }

    @Test
    public void nonRecursiveListsTopLevelOnly() {
        // WHY (Rule 9): with recursion off, only top-level files are returned — sub-directories are NOT descended
        // (byte-identical to today; pins hive_config_test tag 1 = 2 rows).
        String top = "file:///wh/db/t/dt=1";
        FakeFileSystem fs = new FakeFileSystem().withTree(recursiveTree(top));

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(top, fs, false);

        Assertions.assertEquals(1, files.size(), "non-recursive listing must not descend into sub-directories");
        Assertions.assertTrue(files.get(0).getPath().endsWith("/exp_a"), "only the top-level file survives");
    }

    @Test
    public void recursiveSkipsHiddenSubdirectoriesAndFiles() {
        // WHY (Rule 9): recursion must skip hidden sub-directories (_temporary / .hive-staging write-staging) and
        // hidden files at every level — exact net parity with legacy's full-path containsHiddenPath filter. A
        // descend-all-then-leaf-filter regression would surface _temporary/part-0 staging files.
        String top = "file:///wh/db/t/dt=1";
        Map<String, List<FileEntry>> tree = new HashMap<>();
        tree.put(top, Arrays.asList(
                FakeFileSystem.file(top + "/exp_a", 1L, 1L),
                FakeFileSystem.file(top + "/.hidden", 1L, 1L),   // hidden file — skipped
                FakeFileSystem.dir(top + "/_temporary"),         // hidden dir — NOT descended
                FakeFileSystem.dir(top + "/1")));                // real sub-dir — descended
        tree.put(top + "/_temporary",
                Collections.singletonList(FakeFileSystem.file(top + "/_temporary/part-0", 1L, 1L)));
        tree.put(top + "/1", Collections.singletonList(FakeFileSystem.file(top + "/1/exp_b", 1L, 1L)));
        FakeFileSystem fs = new FakeFileSystem().withTree(tree);

        List<HiveFileStatus> files = HiveFileListingCache.listFromFileSystem(top, fs, true);

        Assertions.assertEquals(2, files.size(), "only real data files survive; hidden dir/file are excluded");
        for (HiveFileStatus f : files) {
            Assertions.assertFalse(f.getPath().contains("_temporary"),
                    "must not read a hidden staging dir: " + f.getPath());
            Assertions.assertFalse(f.getPath().endsWith("/.hidden"), "must not read a hidden file");
        }
    }

    @Test
    public void defaultIsRecursive() {
        // WHY (Rule 9): with NO hive.recursive_directories property the catalog defaults to recursive (legacy
        // default "true"); pins tag 21. Drives the REAL production lister through listDataFiles — the
        // RED-against-literal-HEAD guarantee (today's non-recursive lister returns 1, not 3).
        String top = "file:///wh/db/t/dt=1";
        FakeFileSystem fs = new FakeFileSystem().withTree(recursiveTree(top));
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap());

        List<HiveFileStatus> files = cache.listDataFiles("db", "t", top, fs);

        Assertions.assertEquals(3, files.size(), "default (no property) must be recursive");
    }

    @Test
    public void recursiveFlagFalseFromProperty() {
        // WHY (Rule 9): hive.recursive_directories=false is honoured through the public ctor / listDataFiles path —
        // pins that the tag-1 vs tag-2 divergence is driven by the property, not hardcoded.
        String top = "file:///wh/db/t/dt=1";
        FakeFileSystem fs = new FakeFileSystem().withTree(recursiveTree(top));
        HiveFileListingCache cache = new HiveFileListingCache(props("hive.recursive_directories", "false"));

        List<HiveFileStatus> files = cache.listDataFiles("db", "t", top, fs);

        Assertions.assertEquals(1, files.size(), "hive.recursive_directories=false must list top-level only");
    }

    @Test
    public void recursiveSubdirListingFailureIsSkippable() {
        // WHY (Rule 9): a listing failure in a DESCENDED sub-directory must reach the SAME classifier as a
        // top-level failure — a local FileNotFound stays the skippable HiveDirectoryListingException (so the scan
        // skips just this partition). Guards a future swallowing/reclassifying catch around the recursion.
        String top = "file:///wh/db/t/dt=1";
        Map<String, List<FileEntry>> tree = new HashMap<>();
        tree.put(top, Arrays.asList(
                FakeFileSystem.file(top + "/exp_a", 1L, 1L),
                FakeFileSystem.dir(top + "/1")));
        FakeFileSystem fs = new FakeFileSystem().withTree(tree)
                .failListAt(top + "/1", new FileNotFoundException("Path does not exist"));

        Assertions.assertThrows(HiveDirectoryListingException.class,
                () -> HiveFileListingCache.listFromFileSystem(top, fs, true));
    }

    // ==================== failure split: systemic is loud, local is skippable ====================

    @Test
    public void listFromFileSystemFailsLoudWhenFsIsNull() {
        // A null engine filesystem (catalog with no storage) is a SYSTEMIC config error affecting every partition.
        // WHY (Rule 9): it must fail the query loud (plain DorisConnectorException), not skip / return empty.
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveFileListingCache.listFromFileSystem("hdfs://host/path", null));
        Assertions.assertFalse(e instanceof HiveDirectoryListingException,
                "a missing filesystem must be loud, not skippable");
    }

    @Test
    public void listFromFileSystemFailsLoudWhenFilesystemCannotBeResolved() {
        // forLocation failing (unresolvable scheme / no StorageProperties / factory error) is a SYSTEMIC
        // storage-config error affecting every partition. It must throw a plain DorisConnectorException (which the
        // scan path does NOT skip), and NOT the skippable HiveDirectoryListingException.
        // WHY (Rule 9): if this were the skippable subtype, a misconfigured storage would silently return an empty
        // scan instead of failing the query loud — the exact regression this fix prevents.
        FakeFileSystem fs = new FakeFileSystem().failForLocation(new IOException("no StorageProperties for scheme"));
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveFileListingCache.listFromFileSystem("nosuchscheme://host/path", fs));
        Assertions.assertFalse(e instanceof HiveDirectoryListingException,
                "a filesystem-resolution failure must be loud (plain DorisConnectorException), not skippable");
    }

    @Test
    public void listFromFileSystemFailsLoudWhenSchemeImplMissing() {
        // A lazily-surfaced "No FileSystem for scheme X" (the engine-side FS impl for the scheme is absent — broken
        // packaging) is thrown from list(), not forLocation(). It is nonetheless SYSTEMIC (affects every partition).
        // WHY (Rule 9): it must be re-classified LOUD (plain DorisConnectorException), matching the pre-migration
        // FileSystem.get behaviour — this is the exact "No FileSystem for scheme hdfs" error class FIX-HIVEFS keeps
        // loud. If it degraded to the skippable subtype, a broken deployment would scan EMPTY silently.
        FakeFileSystem fs = new FakeFileSystem()
                .failList(new UnsupportedFileSystemException("No FileSystem for scheme \"hdfs\""));
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> HiveFileListingCache.listFromFileSystem("hdfs://host/path", fs));
        Assertions.assertFalse(e instanceof HiveDirectoryListingException,
                "a scheme-not-registered failure must stay loud, not skippable");
    }

    @Test
    public void listFromFileSystemIsSkippableWhenDirectoryMissing() {
        // A resolvable filesystem but a non-existent directory makes list() fail — a LOCAL failure of one partition
        // directory. It must throw the skippable HiveDirectoryListingException so the scan skips just that partition
        // (pre-cache tolerance of a missing/unreadable partition dir).
        FakeFileSystem fs = new FakeFileSystem().failList(new FileNotFoundException("Path does not exist"));
        Assertions.assertThrows(HiveDirectoryListingException.class,
                () -> HiveFileListingCache.listFromFileSystem("file:///wh/db/t/does-not-exist", fs));
    }

    @Test
    public void resolutionFailureThroughCacheFailsLoudAndIsNotCached() {
        // Drives the REAL production lister THROUGH the cache lookup with a forLocation failure. WHY (Rule 9): pins
        // that (1) a systemic storage-config failure propagates from listDataFiles as the loud plain
        // DorisConnectorException — MetaCacheEntry's manual-miss load rethrows RuntimeException unwrapped, so the
        // type survives the cache boundary — and NOT the skippable subtype; and (2) the failure leaves NO cache
        // entry. (2) kills the mutation "catch -> return emptyList" in the loader, which would cache a poisoned
        // empty listing and silently turn every later scan into 0 rows for the TTL.
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap());
        FakeFileSystem fs = new FakeFileSystem().failForLocation(new IOException("no StorageProperties for scheme"));

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> cache.listDataFiles("db", "t", "nosuchfs://host/path", fs));
        Assertions.assertFalse(e instanceof HiveDirectoryListingException,
                "a filesystem-resolution failure through the cache must be the loud type, not the skippable one");
        Assertions.assertEquals(0L, cache.size(), "a failed load must never leave a cache entry");
    }

    @Test
    public void listFailureThroughCacheIsSkippableAndNotCached() {
        // Drives the REAL production lister THROUGH the cache with a resolvable filesystem but a list() failure, so
        // the listing genuinely fails. WHY (Rule 9): pins that the LOCAL per-directory failure keeps its distinct
        // skippable type (HiveDirectoryListingException, exactly what the scan's per-partition skip catches) across
        // the cache boundary, and leaves no cache entry either.
        HiveFileListingCache cache = new HiveFileListingCache(Collections.emptyMap());
        FakeFileSystem fs = new FakeFileSystem().failList(new FileNotFoundException("Path does not exist"));

        Assertions.assertThrows(HiveDirectoryListingException.class,
                () -> cache.listDataFiles("db", "t", "file:///wh/db/t/does-not-exist", fs));
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
                new FakeConnectorContext(), new HiveReadTransactionManager(),
                new HiveFileListingCache(Collections.emptyMap(), lister));

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
                new FakeConnectorContext(), new HiveReadTransactionManager(),
                new HiveFileListingCache(Collections.emptyMap(), lister));

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
                null, Collections.emptyMap(), new FakeConnectorContext(), new HiveReadTransactionManager(), cache);

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

        long first = md.estimateDataSizeByListingFiles(new FakeSession(), handle);
        long second = md.estimateDataSizeByListingFiles(new FakeSession(), handle);

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
     * {@link #error} when set, to exercise the failure-not-cached path. Lists above the FileSystem seam, so it
     * ignores the injected {@link FileSystem}.
     */
    private static final class CountingLister implements HiveFileListingCache.DirectoryLister {
        final Map<String, Integer> callsPerLocation = new HashMap<>();
        int totalCalls;
        RuntimeException error;
        // When set, error is thrown only for this location (a single bad partition among healthy ones).
        String errorLocation;

        @Override
        public List<HiveFileStatus> list(String location, FileSystem fs) {
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
