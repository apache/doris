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

package org.apache.doris.connector.hms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link CachingHmsClient}: the caching decorator over an {@link HmsClient}.
 *
 * <p>WHY: at the HMS cutover a hive catalog stops routing to the engine-side {@code HiveExternalMetaCache},
 * so the connector must cache these reads itself or every scan regresses to fresh Thrift RPCs. These tests
 * pin the behaviours that make that re-homed cache correct: (1) the four read methods actually cache (loader
 * runs once per key), keyed exactly by their arguments — including the database dimension, so two databases
 * never collide; (2) the per-entry {@code meta.cache.hive.*} knobs turn a cache off; (3)
 * {@link CachingHmsClient#flush} / {@code flushAll} drop the right entries across all four caches (arming
 * REFRESH) and {@code flush} is scoped to one table; and that other methods are a verbatim pass-through and
 * a loader failure is neither swallowed nor cached.</p>
 */
public class CachingHmsClientTest {

    private static Map<String, String> props(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    // ---- getTable ----

    @Test
    public void getTableCachesByDbAndTable() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        HmsTableInfo first = cache.getTable("db", "t1");
        HmsTableInfo second = cache.getTable("db", "t1");
        // WHY: a hit must serve the cached instance without re-hitting the metastore.
        Assertions.assertSame(first, second);
        Assertions.assertEquals(1, delegate.getTableCalls);

        // WHY: a different table is a different key — must NOT serve t1's value.
        HmsTableInfo other = cache.getTable("db", "t2");
        Assertions.assertNotSame(first, other);
        Assertions.assertEquals(2, delegate.getTableCalls);
    }

    @Test
    public void cacheKeysAreScopedByDatabase() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // Same table name, different database, across all four caches. WHY: the db dimension MUST be part of
        // every key — otherwise "db2.t" would be served "db1.t"'s cached metadata (a cross-database mix-up).
        HmsTableInfo t1 = cache.getTable("db1", "t");
        HmsTableInfo t2 = cache.getTable("db2", "t");
        Assertions.assertNotSame(t1, t2);
        Assertions.assertEquals(2, delegate.getTableCalls);

        cache.listPartitionNames("db1", "t", -1);
        cache.listPartitionNames("db2", "t", -1);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);

        cache.getPartitions("db1", "t", Arrays.asList("p=1"));
        cache.getPartitions("db2", "t", Arrays.asList("p=1"));
        Assertions.assertEquals(2, delegate.getPartitionsCalls);

        cache.getTableColumnStatistics("db1", "t", Arrays.asList("c1"));
        cache.getTableColumnStatistics("db2", "t", Arrays.asList("c1"));
        Assertions.assertEquals(2, delegate.getColumnStatsCalls);
    }

    // ---- listPartitionNames ----

    @Test
    public void listPartitionNamesCachesByDbTableAndMaxParts() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        List<String> a = cache.listPartitionNames("db", "t", -1);
        List<String> b = cache.listPartitionNames("db", "t", -1);
        Assertions.assertSame(a, b);
        Assertions.assertEquals(1, delegate.listPartitionNamesCalls);

        // WHY: maxParts is part of the key — a bounded request must never be served the unbounded list.
        cache.listPartitionNames("db", "t", 10);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
    }

    // ---- getPartitions ----

    @Test
    public void getPartitionsSharesPerPartitionEntriesAcrossRequests() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // First request loads BOTH partitions in one delegate round-trip and caches each PER PARTITION.
        List<HmsPartitionInfo> a = cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2"));
        Assertions.assertEquals(2, a.size());
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        // WHY (Rule 9 / the D2 fix): an OVERLAPPING subset request must be served entirely from the shared
        // per-partition entries — no new delegate call. The OLD list-keyed cache re-fetched any distinct
        // request list (this was `getPartitionsCalls == 2` here); a mutation reverting to list keying —
        // storing the whole list under a request-name-list key — makes this re-fetch and go red.
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        Assertions.assertEquals(1, delegate.getPartitionsCalls,
                "p=1 is served from the shared per-partition entry (no re-fetch)");

        // WHY: order-independent too (the old list key was order-sensitive and re-loaded on a reversed list);
        // both partitions are already cached, so a reversed request still hits.
        List<HmsPartitionInfo> rev = cache.getPartitions("db", "t", Arrays.asList("p=2", "p=1"));
        Assertions.assertEquals(2, rev.size());
        Assertions.assertEquals(1, delegate.getPartitionsCalls, "reversed order still hits the shared entries");

        // WHY: only a genuinely new partition triggers a delegate fetch — and ONLY for the miss (p=1 stays
        // cached), proving misses are fetched in one round-trip while hits are served locally.
        cache.getPartitions("db", "t", Arrays.asList("p=1", "p=3"));
        Assertions.assertEquals(2, delegate.getPartitionsCalls, "only the new p=3 is fetched; p=1 stays cached");
        Assertions.assertEquals(Arrays.asList("p=3"), delegate.lastGetPartitionsArg,
                "the delegate is asked for the MISS only, not the whole requested list");
    }

    @Test
    public void getPartitionsOmitsMissingPartitionWithoutNegativeCaching() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.absentPartitionNames.add("p=9"); // HMS has no such partition -> omitted from the response
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        List<HmsPartitionInfo> r = cache.getPartitions("db", "t", Arrays.asList("p=1", "p=9"));
        // WHY: a non-existent partition is OMITTED (get_partitions_by_names parity), never fabricated.
        Assertions.assertEquals(1, r.size(), "the absent partition is omitted, not fabricated");
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        // WHY (Rule 9): the missing p=9 must NOT be negative-cached — a later request re-attempts it (so once
        // the partition is created + REFRESH'd it is picked up). Only p=9 re-fetches; p=1 stays cached.
        cache.getPartitions("db", "t", Arrays.asList("p=1", "p=9"));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "the absent partition is re-attempted (no negative cache); p=1 still hits");
        Assertions.assertEquals(Arrays.asList("p=9"), delegate.lastGetPartitionsArg);
    }

    @Test
    public void getPartitionsStaysCorrectWhenParsedNameDivergesFromStoredValues() {
        // Pathological: the delegate returns a partition whose values do NOT match the requested name's parse
        // (models a value the name-parse cannot round-trip). The decorator keys the STORE by the partition's
        // OWN values but the LOOKUP by the parsed name, so they never match -> the partition is re-fetched
        // every time. WHY (Rule 9 / Rule 12): this pins the safety contract — a parse divergence degrades to a
        // reload (perf), NEVER a wrong or dropped partition. A mutation that keyed the STORE by the parsed name
        // instead would make the lookup "hit" a mis-keyed entry (or drop the partition) -> the size/values
        // asserts go red.
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.forcedValues = Arrays.asList("EXOTIC"); // stored values != parse("p=1") == ["1"]
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        List<HmsPartitionInfo> r1 = cache.getPartitions("db", "t", Arrays.asList("p=1"));
        Assertions.assertEquals(1, r1.size(), "the partition is returned even though its values diverge from the name");
        Assertions.assertEquals(Arrays.asList("EXOTIC"), r1.get(0).getValues());
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        List<HmsPartitionInfo> r2 = cache.getPartitions("db", "t", Arrays.asList("p=1"));
        Assertions.assertEquals(1, r2.size());
        Assertions.assertEquals("EXOTIC", r2.get(0).getValues().get(0));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "divergence degrades to a reload, never a wrong/dropped result");
    }

    // ---- column statistics ----

    @Test
    public void columnStatisticsCacheByRequestedColumnList() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        List<String> cols = Arrays.asList("c1", "c2");
        List<HmsColumnStatistics> a = cache.getTableColumnStatistics("db", "t", cols);
        List<HmsColumnStatistics> b = cache.getTableColumnStatistics("db", "t", new ArrayList<>(cols));
        // WHY: same requested column set+order hits.
        Assertions.assertSame(a, b);
        Assertions.assertEquals(1, delegate.getColumnStatsCalls);
        // WHY: the delegate's real stats must survive the cache, not the interface's empty-list default.
        Assertions.assertEquals(1, a.size());
        Assertions.assertEquals("c1", a.get(0).getColumnName());

        // WHY: a different requested column set is a distinct entry (RPC-argument granularity).
        cache.getTableColumnStatistics("db", "t", Arrays.asList("c1"));
        Assertions.assertEquals(2, delegate.getColumnStatsCalls);
    }

    @Test
    public void emptyColumnStatisticsResultIsCached() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // The fake returns an empty (no-stats) list for an empty column request.
        cache.getTableColumnStatistics("db", "t", Collections.emptyList());
        cache.getTableColumnStatistics("db", "t", Collections.emptyList());
        // WHY: an empty "no stats" result is a real cached value (only null is treated as a miss) — it must
        // NOT be re-fetched, or a table without column stats would hit HMS on every planner probe.
        Assertions.assertEquals(1, delegate.getColumnStatsCalls);
    }

    // ---- per-entry property knobs ----

    @Test
    public void perEntryPropertiesControlCaching() {
        // table cache disabled via enable=false; partition_names disabled via ttl-second=0; partition left on.
        Map<String, String> properties = props(
                "meta.cache.hive.table.enable", "false",
                "meta.cache.hive.partition_names.ttl-second", "0");
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, properties);

        cache.getTable("db", "t");
        cache.getTable("db", "t");
        // WHY: enable=false must bypass caching entirely — every call reloads.
        Assertions.assertEquals(2, delegate.getTableCalls);

        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        // WHY: ttl-second=0 also disables the cache (a distinct knob from enable).
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);

        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        // WHY: an unconfigured entry stays enabled by default — proves the knobs are read PER entry.
        Assertions.assertEquals(1, delegate.getPartitionsCalls);
    }

    // ---- flush(db, table) ----

    @Test
    public void flushDropsOnlyThatTablesEntries() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // Populate ALL four caches for BOTH t1 and t2 (t2 must live in the three predicate-invalidated caches
        // too, not just the table cache, so an over-broad flush of them is detectable).
        cache.getTable("db", "t1");
        cache.listPartitionNames("db", "t1", -1);
        cache.getPartitions("db", "t1", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t1", Arrays.asList("c1"));
        cache.getTable("db", "t2");
        cache.listPartitionNames("db", "t2", -1);
        cache.getPartitions("db", "t2", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t2", Arrays.asList("c1"));
        Assertions.assertEquals(2, delegate.getTableCalls);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(2, delegate.getPartitionsCalls);
        Assertions.assertEquals(2, delegate.getColumnStatsCalls);

        cache.flush("db", "t1");

        // WHY: t1 must reload across all four caches after its flush.
        cache.getTable("db", "t1");
        cache.listPartitionNames("db", "t1", -1);
        cache.getPartitions("db", "t1", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t1", Arrays.asList("c1"));
        Assertions.assertEquals(3, delegate.getTableCalls);
        Assertions.assertEquals(3, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(3, delegate.getPartitionsCalls);
        Assertions.assertEquals(3, delegate.getColumnStatsCalls);

        // WHY: flush is scoped to ONE table — t2's entries in ALL four caches must survive (no reload). This
        // pins the matches() per-table scoping of the three predicate caches, not just the table cache's
        // exact-key invalidation: an over-broad flush that wiped every table would reload t2 here.
        cache.getTable("db", "t2");
        cache.listPartitionNames("db", "t2", -1);
        cache.getPartitions("db", "t2", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t2", Arrays.asList("c1"));
        Assertions.assertEquals(3, delegate.getTableCalls);
        Assertions.assertEquals(3, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(3, delegate.getPartitionsCalls);
        Assertions.assertEquals(3, delegate.getColumnStatsCalls);
    }

    // ---- flushDb() ----

    @Test
    public void flushDbDropsOnlyThatDatabasesEntries() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // Populate all four caches for db1.t1, plus db1.t2 (a SECOND table in the same db) and db2.t1 (a table in
        // ANOTHER db). flushDb("db1") must drop EVERY db1 table (t1 AND t2) across all four caches, while db2 lives.
        cache.getTable("db1", "t1");
        cache.listPartitionNames("db1", "t1", -1);
        cache.getPartitions("db1", "t1", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db1", "t1", Arrays.asList("c1"));
        cache.getTable("db1", "t2");
        cache.getTable("db2", "t1");
        Assertions.assertEquals(3, delegate.getTableCalls);

        cache.flushDb("db1");

        // WHY: every db1 table reloads across all four caches — this pins the matchesDb() db scoping (not the
        // per-table matches()): t2 reloading proves the whole database was dropped, not just one table.
        cache.getTable("db1", "t1");
        cache.listPartitionNames("db1", "t1", -1);
        cache.getPartitions("db1", "t1", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db1", "t1", Arrays.asList("c1"));
        cache.getTable("db1", "t2");
        Assertions.assertEquals(5, delegate.getTableCalls, "flushDb must drop EVERY table in the database (t1 and t2)");
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(2, delegate.getPartitionsCalls);
        Assertions.assertEquals(2, delegate.getColumnStatsCalls);

        // WHY: flushDb is scoped to ONE database — db2's entry must survive (no reload). An over-broad flushDb that
        // wiped every db would reload db2 here -> red.
        cache.getTable("db2", "t1");
        Assertions.assertEquals(5, delegate.getTableCalls, "flushDb must NOT drop another database's entries");
    }

    // ---- flushAll() ----

    @Test
    public void flushAllDropsEverything() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // Populate all four caches so flushAll's independent invalidateAll() call on each is exercised.
        cache.getTable("db", "t");
        cache.listPartitionNames("db", "t", -1);
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t", Arrays.asList("c1"));
        Assertions.assertEquals(1, delegate.getTableCalls);
        Assertions.assertEquals(1, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(1, delegate.getPartitionsCalls);
        Assertions.assertEquals(1, delegate.getColumnStatsCalls);

        cache.flushAll();

        // WHY: flushAll drops ALL four caches — every one reloads (not just the table cache).
        cache.getTable("db", "t");
        cache.listPartitionNames("db", "t", -1);
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t", Arrays.asList("c1"));
        Assertions.assertEquals(2, delegate.getTableCalls);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(2, delegate.getPartitionsCalls);
        Assertions.assertEquals(2, delegate.getColumnStatsCalls);
    }

    // ---- pass-through delegation ----

    @Test
    public void nonCachedMethodsDelegate() throws IOException {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        cache.listDatabases();
        Assertions.assertEquals(1, delegate.listDatabasesCalls);

        cache.dropTable("db", "t");
        Assertions.assertEquals(1, delegate.dropTableCalls);

        cache.close();
        Assertions.assertEquals(1, delegate.closeCalls);
    }

    // ---- loader failures ----

    @Test
    public void loaderExceptionPropagatesAndIsNotCached() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.getTableError = new HmsClientException("boom");
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        HmsClientException e = Assertions.assertThrows(HmsClientException.class,
                () -> cache.getTable("db", "t"));
        Assertions.assertEquals("boom", e.getMessage());
        Assertions.assertEquals(1, delegate.getTableCalls);

        // WHY: a failed load must NOT be cached — after recovery, the next call reloads and succeeds.
        delegate.getTableError = null;
        HmsTableInfo ok = cache.getTable("db", "t");
        Assertions.assertNotNull(ok);
        Assertions.assertEquals(2, delegate.getTableCalls);
    }

    @Test
    public void nullDelegateRejected() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new CachingHmsClient(null, Collections.emptyMap()));
    }

    /**
     * A minimal {@link HmsClient} that counts calls and returns a fresh instance per call, so reference
     * identity distinguishes a cache hit (same instance) from a reload (new instance).
     */
    private static final class RecordingHmsClient implements HmsClient {
        int getTableCalls;
        int listPartitionNamesCalls;
        int getPartitionsCalls;
        int getColumnStatsCalls;
        int listDatabasesCalls;
        int dropTableCalls;
        int closeCalls;
        RuntimeException getTableError;
        // Partition names the fake has NO partition for (mirrors HMS omitting non-existent partitions).
        final Set<String> absentPartitionNames = new HashSet<>();
        // When set, every returned partition carries these exact values regardless of the requested name
        // (used to model a value the name-parse cannot round-trip, exercising the store-by-real-values path).
        List<String> forcedValues;
        // The partition-name list the decorator actually asked the delegate for on the LAST getPartitions call
        // (so a test can assert the decorator fetches only the MISSES, not the whole requested list).
        List<String> lastGetPartitionsArg;

        @Override
        public HmsTableInfo getTable(String dbName, String tableName) {
            getTableCalls++;
            if (getTableError != null) {
                throw getTableError;
            }
            return HmsTableInfo.builder().dbName(dbName).tableName(tableName).build();
        }

        @Override
        public List<String> listPartitionNames(String dbName, String tableName, int maxParts) {
            listPartitionNamesCalls++;
            return new ArrayList<>(Arrays.asList("p=1", "p=2"));
        }

        @Override
        public List<HmsPartitionInfo> getPartitions(String dbName, String tableName, List<String> partNames) {
            getPartitionsCalls++;
            lastGetPartitionsArg = new ArrayList<>(partNames);
            List<HmsPartitionInfo> out = new ArrayList<>();
            for (String name : partNames) {
                if (absentPartitionNames.contains(name)) {
                    continue; // no such partition -> omitted (get_partitions_by_names parity)
                }
                // The partition's OWN values must correspond to its name ("k=v/..." -> ["v", ...]) so the
                // decorator can key it per-partition the same way it parses the lookup name; forcedValues
                // overrides this to model a value the name-parse cannot round-trip.
                List<String> values = forcedValues != null ? forcedValues : valuesOf(name);
                out.add(new HmsPartitionInfo(values, "loc/" + name, null, null, null, null));
            }
            return out;
        }

        // "p=1" -> ["1"]; "k1=a/k2=b" -> ["a", "b"] (simple split; test names carry no escaped characters).
        private static List<String> valuesOf(String partitionName) {
            List<String> values = new ArrayList<>();
            for (String seg : partitionName.split("/")) {
                int eq = seg.indexOf('=');
                values.add(eq >= 0 ? seg.substring(eq + 1) : seg);
            }
            return values;
        }

        @Override
        public List<HmsColumnStatistics> getTableColumnStatistics(String dbName, String tableName,
                List<String> columns) {
            getColumnStatsCalls++;
            if (columns.isEmpty()) {
                return Collections.emptyList();
            }
            return new ArrayList<>(Arrays.asList(new HmsColumnStatistics("c1", 1L, 0L, 4.0)));
        }

        @Override
        public List<String> listDatabases() {
            listDatabasesCalls++;
            return Collections.emptyList();
        }

        @Override
        public void dropTable(String dbName, String tableName) {
            dropTableCalls++;
        }

        @Override
        public void close() {
            closeCalls++;
        }

        // Unused abstract methods — trivial stubs.
        @Override
        public HmsDatabaseInfo getDatabase(String dbName) {
            return null;
        }

        @Override
        public List<String> listTables(String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(String dbName, String tableName) {
            return false;
        }

        @Override
        public Map<String, String> getDefaultColumnValues(String dbName, String tableName) {
            return Collections.emptyMap();
        }

        @Override
        public HmsPartitionInfo getPartition(String dbName, String tableName, List<String> values) {
            return null;
        }
    }
}
