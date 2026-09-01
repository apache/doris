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

import org.apache.doris.connector.cache.CatalogMetaCache;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests {@link CachingHmsClient}: the caching decorator over an {@link HmsClient}.
 *
 * <p>WHY: at the HMS cutover a hive catalog stops routing to the engine-side {@code HiveExternalMetaCache},
 * so the connector must cache these reads itself or every scan regresses to fresh Thrift RPCs. These tests
 * pin the behaviours that make that re-homed cache correct: (1) the four read methods actually cache (loader
 * runs once per key), keyed exactly by their arguments — including the database dimension, so two databases
 * never collide; (2) the per-entry {@code meta.cache.hive.*} knobs turn a cache off; (3)
 * the shared catalog owner drops the right entries across all four caches and scopes invalidation correctly;
 * and that other methods are a verbatim pass-through and
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

    private static CachingHmsClient cache(HmsClient delegate, Map<String, String> properties) {
        return new CachingHmsClient(new CatalogMetaCache(), delegate, properties);
    }

    // ---- getTable ----

    @Test
    public void getTableCachesByDbAndTable() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        List<String> a = cache.listPartitionNames("db", "t", -1);
        List<String> b = cache.listPartitionNames("db", "t", -1);
        Assertions.assertSame(a, b);
        Assertions.assertEquals(1, delegate.listPartitionNamesCalls);

        // WHY: maxParts is part of the key — a bounded request must never be served the unbounded list.
        cache.listPartitionNames("db", "t", 10);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
    }

    // ---- getTableFresh (SHOW CREATE TABLE — must bypass the table cache) ----

    @Test
    public void getTableFreshAlwaysHitsDelegate() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        // WHY: SHOW CREATE TABLE must see the latest schema (a column added externally after the cache filled)
        // even while DESC serves the stale cached table. Every fresh call goes to the metastore. (test_hive_meta_cache.)
        cache.getTableFresh("db", "t1");
        cache.getTableFresh("db", "t1");
        Assertions.assertEquals(2, delegate.getTableCalls);
    }

    @Test
    public void getTableFreshDoesNotPopulateCache() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        // Fresh call must NOT write the table cache: a following cached getTable must still MISS (delegate call #2)
        // and only THEN populate — proving fresh bypasses the cache in both directions.
        cache.getTableFresh("db", "t1");   // delegate #1, no populate
        cache.getTable("db", "t1");        // cache miss -> delegate #2 + populate
        cache.getTable("db", "t1");        // cache hit -> no delegate call
        Assertions.assertEquals(2, delegate.getTableCalls);
    }

    @Test
    public void getTableFreshDefaultOnNonCachingClientIsPlainGet() {
        // A bare HmsClient (no caching decorator) inherits the interface default: fresh == the raw getTable.
        RecordingHmsClient raw = new RecordingHmsClient();
        raw.getTableFresh("db", "t1");
        raw.getTable("db", "t1");
        Assertions.assertEquals(2, raw.getTableCalls);
    }

    // ---- listPartitionNamesFresh (SHOW PARTITIONS / partitions TVF — must bypass the names cache) ----

    @Test
    public void listPartitionNamesFreshAlwaysHitsDelegate() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        // WHY: SHOW PARTITIONS must see partitions added externally after the cache filled. Every fresh call
        // goes to the metastore — never served from partitionNamesCache. (test_hive_use_meta_cache_true sql09.)
        cache.listPartitionNamesFresh("db", "t", -1);
        cache.listPartitionNamesFresh("db", "t", -1);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
    }

    @Test
    public void listPartitionNamesFreshDoesNotPopulateCache() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        // Fresh call must NOT write the names cache: a following cached listPartitionNames must still MISS
        // (delegate call #2) and only THEN populate — proving fresh bypasses the cache in both directions.
        cache.listPartitionNamesFresh("db", "t", -1);   // delegate #1, no populate
        cache.listPartitionNames("db", "t", -1);        // cache miss -> delegate #2 + populate
        cache.listPartitionNames("db", "t", -1);         // cache hit -> no delegate call
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
    }

    @Test
    public void listPartitionNamesFreshDefaultOnNonCachingClientIsPlainListing() {
        // A bare HmsClient (no caching decorator) inherits the interface default: fresh == the raw listing.
        // Guards the C4 foot-gun — a non-decorating client has nothing to bypass, so the two must be identical.
        RecordingHmsClient raw = new RecordingHmsClient();
        raw.listPartitionNamesFresh("db", "t", -1);
        raw.listPartitionNames("db", "t", -1);
        Assertions.assertEquals(2, raw.listPartitionNamesCalls);
    }

    // ---- getPartitions ----

    @Test
    public void getPartitionsSharesPerPartitionEntriesAcrossRequests() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
    public void getPartitionsPreservesOrderAcrossHitsAndMisses() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());
        cache.getPartitions("db", "t", Collections.singletonList("p=2"));

        List<HmsPartitionInfo> result = cache.getPartitions(
                "db", "t", Arrays.asList("p=1", "p=2", "p=3"));

        Assertions.assertEquals(Arrays.asList("1", "2", "3"), result.stream()
                .map(partition -> partition.getValues().get(0))
                .collect(java.util.stream.Collectors.toList()));
        Assertions.assertEquals(Arrays.asList("p=1", "p=3"), delegate.lastGetPartitionsArg);
        Assertions.assertEquals(2, delegate.getPartitionsCalls);
    }

    @Test
    public void getPartitionsStatsKeepLogicalRequestAndPhysicalMissDimensions() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        HmsPartitionBatchStats cold = cache.getPartitionsWithStats(
                "db", "t", Arrays.asList("p=1", "p=2")).getStats();
        Assertions.assertEquals(2, cold.getRequestedItems());
        Assertions.assertEquals(1, cold.getTransportInvocations());
        Assertions.assertEquals(2, cold.getTransportItems());

        HmsPartitionBatchStats hit = cache.getPartitionsWithStats(
                "db", "t", Collections.singletonList("p=1")).getStats();
        Assertions.assertEquals(1, hit.getRequestedItems());
        Assertions.assertEquals(0, hit.getTransportInvocations());
        Assertions.assertEquals(0, hit.getTransportItems());

        HmsPartitionBatchStats mixed = cache.getPartitionsWithStats(
                "db", "t", Arrays.asList("p=1", "p=3")).getStats();
        Assertions.assertEquals(2, mixed.getRequestedItems());
        Assertions.assertEquals(1, mixed.getTransportInvocations());
        Assertions.assertEquals(1, mixed.getTransportItems());
    }

    @Test
    public void failedPartitionStatsKeepLogicalRequestAndPhysicalMissDimensions() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());
        cache.getPartitions("db", "t", Collections.singletonList("p=1"));
        delegate.getPartitionsError = new HmsClientException("failed", HmsPartitionBatchStats.builder()
                .requestedItems(1)
                .transportInvocations(1)
                .transportItems(1)
                .largestBatchSize(1)
                .smallestBatchSize(1)
                .build());

        HmsClientException failure = Assertions.assertThrows(HmsClientException.class,
                () -> cache.getPartitionsWithStats("db", "t", Arrays.asList("p=1", "p=2")));

        Assertions.assertSame(delegate.getPartitionsError, failure);
        Assertions.assertEquals(2, failure.getPartitionBatchStats().getRequestedItems());
        Assertions.assertEquals(1, failure.getPartitionBatchStats().getTransportInvocations());
        Assertions.assertEquals(1, failure.getPartitionBatchStats().getTransportItems());
    }

    @Test
    public void getPartitionsRejectsMissingPartitionWithoutPartialCaching() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.absentPartitionNames.add("p=9");
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        Assertions.assertThrows(HmsClientException.class,
                () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=9")));
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        Assertions.assertThrows(HmsClientException.class,
                () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=9")));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "an invalid response must publish neither positive nor negative cache entries");
        Assertions.assertEquals(Arrays.asList("p=1", "p=9"), delegate.lastGetPartitionsArg);
    }

    @Test
    public void getExistingPartitionsReturnsAndCachesOnlyPresentSubset() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.absentPartitionNames.add("p=2");
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        List<HmsPartitionInfo> first = cache.getExistingPartitions(
                "db", "t", Arrays.asList("p=1", "p=2", "p=3"));
        Assertions.assertEquals(Arrays.asList("1", "3"), first.stream()
                .map(partition -> partition.getValues().get(0)).collect(java.util.stream.Collectors.toList()));

        List<HmsPartitionInfo> second = cache.getExistingPartitions("db", "t", Arrays.asList("p=1", "p=3"));
        Assertions.assertEquals(2, second.size());
        Assertions.assertEquals(1, delegate.getPartitionsCalls,
                "present partitions from a partial freshness response must be cached");
    }

    @Test
    public void getExistingPartitionsRetainsPhysicalBatchStatsWhenNamesBecomeStale() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.absentPartitionNames.add("p=2");
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        HmsPartitionBatchResult result = cache.getExistingPartitionsWithStats(
                "db", "t", Arrays.asList("p=1", "p=2", "p=3"));

        Assertions.assertEquals(Arrays.asList("1", "3"), result.getPartitions().stream()
                .map(partition -> partition.getValues().get(0)).collect(java.util.stream.Collectors.toList()));
        Assertions.assertEquals(3, result.getStats().getRequestedItems());
        Assertions.assertEquals(1, result.getStats().getTransportInvocations());
        Assertions.assertEquals(3, result.getStats().getTransportItems());
    }

    @Test
    public void getPartitionsRejectsUnexpectedIdentityWithoutCachingIt() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.forcedValues = Arrays.asList("EXOTIC");
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

        Assertions.assertThrows(HmsClientException.class,
                () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        Assertions.assertThrows(HmsClientException.class,
                () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "an unexpected partition identity must never enter the cache");
    }

    @Test
    public void getPartitionsInvalidationRacingInFlightFetchDoesNotRecacheStale() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CatalogMetaCache owner = new CatalogMetaCache();
        CachingHmsClient cache = new CachingHmsClient(owner, delegate, Collections.emptyMap());

        // Model a REFRESH TABLE landing DURING the cold-cache delegate RPC: the bulk-load handle captures the
        // framework publication fence before the RPC, so the per-partition publish must be dropped rather than
        // re-cache the pre-refresh partition. The in-flight query still returns the freshly-fetched partition.
        delegate.onGetPartitions = () -> owner.invalidateTable("db", "t");
        List<HmsPartitionInfo> r = cache.getPartitions("db", "t", Arrays.asList("p=1"));
        Assertions.assertEquals(1, r.size(), "the in-flight query still returns the delegate's partition");
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        // WHY (Rule 9 / R3): the racing flush must have prevented the stale put, so a follow-up read is a MISS
        // and re-fetches — it is NOT served the pre-refresh partition up to the TTL. MUTATION: the pre-R3 raw
        // partitionsCache.put re-caches the stale partition here, so the next read hits and getPartitionsCalls
        // stays 1 -> red. (getTable/listPartitionNames/getTableColumnStatistics kept the guard via get(); only
        // getPartitions' per-partition put had lost it.)
        delegate.onGetPartitions = null;
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "a flush racing the in-flight fetch must not leave the stale partition cached (guarded put)");
    }

    @Test
    public void coldPartitionBatchInvalidationFencesInFlightFetch() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CatalogMetaCache owner = new CatalogMetaCache();
        CachingHmsClient cache = new CachingHmsClient(owner, delegate,
                props("meta.cache.hive.partition_names.ttl-second", "0"));
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch releaseFetch = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        delegate.onGetPartitions = () -> {
            fetchStarted.countDown();
            try {
                Assertions.assertTrue(releaseFetch.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };

        try {
            Future<List<HmsPartitionInfo>> inFlight = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1")));
            Assertions.assertTrue(fetchStarted.await(10, TimeUnit.SECONDS));

            // Both the partition-name collection and p=1 are cold. The batch invalidation must still advance
            // the resolved table prefix captured by the bulk handle before the HMS RPC.
            owner.invalidatePartitions("db", "t", Collections.singletonList(Arrays.asList("1")));
            releaseFetch.countDown();
            Assertions.assertEquals(1, inFlight.get(10, TimeUnit.SECONDS).size());

            delegate.onGetPartitions = null;
            cache.getPartitions("db", "t", Arrays.asList("p=1"));
            Assertions.assertEquals(2, delegate.getPartitionsCalls,
                    "the pre-invalidation HMS result must not publish into the long-lived partition cache");
        } finally {
            releaseFetch.countDown();
            executor.shutdownNow();
            cache.close();
            owner.close();
        }
    }

    @Test
    public void concurrentColdPartitionRequestsShareOneBulkLoadEvenWhenEntriesEvict() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch loadEntered = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        CountDownLatch waiterRegistered = new CountDownLatch(1);
        AtomicInteger registrations = new AtomicInteger();
        CachingHmsClient cache = new CachingHmsClient(new CatalogMetaCache(), delegate,
                props("meta.cache.hive.partition.capacity", "1")) {
            @Override
            void afterPartitionLoadRegistrationForTest() {
                if (registrations.incrementAndGet() == 2) {
                    waiterRegistered.countDown();
                }
            }
        };
        delegate.onGetPartitions = () -> {
            loadEntered.countDown();
            try {
                Assertions.assertTrue(releaseLoad.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> owner = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(loadEntered.await(10, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> waiter = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(waiterRegistered.await(10, TimeUnit.SECONDS));

            releaseLoad.countDown();

            Assertions.assertEquals(2, owner.get(10, TimeUnit.SECONDS).size());
            Assertions.assertEquals(2, waiter.get(10, TimeUnit.SECONDS).size());
            Assertions.assertEquals(1, delegate.getPartitionsCalls,
                    "the waiter must consume the current in-flight result even after capacity eviction");
            Assertions.assertEquals(0, cache.inFlightPartitionLoadCountForTest());
        } finally {
            releaseLoad.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void partiallyOverlappingColdRequestsOnlyLoadUnownedPartitions() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());
        CountDownLatch firstLoadEntered = new CountDownLatch(1);
        CountDownLatch bothLoadsEntered = new CountDownLatch(2);
        CountDownLatch releaseLoads = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            firstLoadEntered.countDown();
            bothLoadsEntered.countDown();
            try {
                Assertions.assertTrue(releaseLoads.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> first = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(firstLoadEntered.await(10, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> second = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=2", "p=3")));
            Assertions.assertTrue(bothLoadsEntered.await(10, TimeUnit.SECONDS));

            releaseLoads.countDown();

            Assertions.assertEquals(2, first.get(10, TimeUnit.SECONDS).size());
            Assertions.assertEquals(2, second.get(10, TimeUnit.SECONDS).size());
            Assertions.assertTrue(delegate.getPartitionsArgs.contains(Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(delegate.getPartitionsArgs.contains(Collections.singletonList("p=3")));
            Assertions.assertEquals(2, delegate.getPartitionsArgs.size());
        } finally {
            releaseLoads.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void waiterRetriesWhenInvalidationRacesTheOwnedBulkLoad() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CatalogMetaCache owner = new CatalogMetaCache();
        CountDownLatch firstLoadEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstLoad = new CountDownLatch(1);
        CountDownLatch waiterRegistered = new CountDownLatch(1);
        AtomicInteger registrations = new AtomicInteger();
        AtomicInteger loads = new AtomicInteger();
        CachingHmsClient cache = new CachingHmsClient(owner, delegate, Collections.emptyMap()) {
            @Override
            void afterPartitionLoadRegistrationForTest() {
                if (registrations.incrementAndGet() == 2) {
                    waiterRegistered.countDown();
                }
            }
        };
        delegate.onGetPartitions = () -> {
            if (loads.incrementAndGet() == 1) {
                firstLoadEntered.countDown();
                try {
                    Assertions.assertTrue(releaseFirstLoad.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> first = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(firstLoadEntered.await(10, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> second = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(waiterRegistered.await(10, TimeUnit.SECONDS));

            owner.invalidateTable("db", "t");
            releaseFirstLoad.countDown();

            Assertions.assertEquals(1, first.get(10, TimeUnit.SECONDS).size());
            Assertions.assertEquals(1, second.get(10, TimeUnit.SECONDS).size());
            Assertions.assertEquals(2, delegate.getPartitionsArgs.size(),
                    "the invalidated waiter must start a new load instead of consuming stale in-flight data");
        } finally {
            releaseFirstLoad.countDown();
            executor.shutdownNow();
        }
    }

    // ---- column statistics ----

    @Test
    public void columnStatisticsCacheByRequestedColumnList() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
        CachingHmsClient cache = cache(delegate, properties);

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

    @Test
    public void legacyTtlPropertiesControlCaching() {
        // The legacy fe-core catalog knobs must still work after the SPI cutover: schema.cache.ttl-second maps
        // onto the table/schema cache; partition.cache.ttl-second onto the partition-NAME list (legacy's
        // partition_values), NOT the per-partition objects cache. Mirrors HiveExternalMetaCache's compat map;
        // this is what test_hive_meta_cache's schema-cache and partition-cache sections exercise.
        Map<String, String> properties = props(
                "schema.cache.ttl-second", "0",
                "partition.cache.ttl-second", "0");
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = cache(delegate, properties);

        cache.getTable("db", "t");
        cache.getTable("db", "t");
        // WHY: schema.cache.ttl-second=0 must disable the table/schema cache (backs DESC) — every call reloads.
        Assertions.assertEquals(2, delegate.getTableCalls);

        cache.listPartitionNames("db", "t", -1);
        cache.listPartitionNames("db", "t", -1);
        // WHY: partition.cache.ttl-second=0 must disable the partition-name list — a newly-added partition is
        // then visible without REFRESH.
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);

        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        // WHY: the per-partition objects cache has NO legacy knob (fe-core mapped partition.cache only to the
        // partition-values list), so it stays enabled — pins the faithful legacy mapping.
        Assertions.assertEquals(1, delegate.getPartitionsCalls);
    }

    // ---- shared-owner invalidation ----

    @Test
    public void tableInvalidationDropsOnlyThatTablesEntries() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CatalogMetaCache owner = new CatalogMetaCache();
        CachingHmsClient cache = new CachingHmsClient(owner, delegate, Collections.emptyMap());

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

        owner.invalidateTable("db", "t1");

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

    // ---- database invalidation ----

    @Test
    public void databaseInvalidationDropsOnlyThatDatabasesEntries() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CatalogMetaCache owner = new CatalogMetaCache();
        CachingHmsClient cache = new CachingHmsClient(owner, delegate, Collections.emptyMap());

        // Populate all four caches for db1.t1, plus db1.t2 (a SECOND table in the same db) and db2.t1 (a table in
        // ANOTHER db). Invalidating db1 must drop EVERY db1 table (t1 AND t2) across all four caches, while db2 lives.
        cache.getTable("db1", "t1");
        cache.listPartitionNames("db1", "t1", -1);
        cache.getPartitions("db1", "t1", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db1", "t1", Arrays.asList("c1"));
        cache.getTable("db1", "t2");
        cache.getTable("db2", "t1");
        Assertions.assertEquals(3, delegate.getTableCalls);

        owner.invalidateDatabase("db1");

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

    // ---- catalog invalidation ----

    @Test
    public void catalogInvalidationDropsEverything() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CatalogMetaCache owner = new CatalogMetaCache();
        CachingHmsClient cache = new CachingHmsClient(owner, delegate, Collections.emptyMap());

        // Populate all four caches so flushAll's independent invalidateAll() call on each is exercised.
        cache.getTable("db", "t");
        cache.listPartitionNames("db", "t", -1);
        cache.getPartitions("db", "t", Arrays.asList("p=1"));
        cache.getTableColumnStatistics("db", "t", Arrays.asList("c1"));
        Assertions.assertEquals(1, delegate.getTableCalls);
        Assertions.assertEquals(1, delegate.listPartitionNamesCalls);
        Assertions.assertEquals(1, delegate.getPartitionsCalls);
        Assertions.assertEquals(1, delegate.getColumnStatsCalls);

        owner.invalidateCatalog();

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
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
        CachingHmsClient cache = cache(delegate, Collections.emptyMap());

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
                () -> cache(null, Collections.emptyMap()));
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
        HmsClientException getPartitionsError;
        // Partition names the fake has NO partition for (mirrors HMS omitting non-existent partitions).
        final Set<String> absentPartitionNames = new HashSet<>();
        // When set, every returned partition carries these exact values regardless of the requested name
        // (used to model a value the name-parse cannot round-trip, exercising the store-by-real-values path).
        List<String> forcedValues;
        // The partition-name list the decorator actually asked the delegate for on the LAST getPartitions call
        // (so a test can assert the decorator fetches only the MISSES, not the whole requested list).
        List<String> lastGetPartitionsArg;
        final List<List<String>> getPartitionsArgs = Collections.synchronizedList(new ArrayList<>());
        // Optional hook fired INSIDE getPartitions (after counting, before returning) to model a concurrent
        // mutation (e.g. a REFRESH flush) racing the in-flight cold-cache RPC.
        Runnable onGetPartitions;

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
            getPartitionsArgs.add(new ArrayList<>(partNames));
            if (onGetPartitions != null) {
                onGetPartitions.run();
            }
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

        @Override
        public HmsPartitionBatchResult getPartitionsWithStats(
                String dbName, String tableName, List<String> partNames) {
            if (getPartitionsError != null) {
                throw getPartitionsError;
            }
            long startNanos = System.nanoTime();
            List<HmsPartitionInfo> partitions = getPartitions(dbName, tableName, partNames);
            HmsPartitionBatchStats stats = HmsPartitionBatchStats.builder()
                    .requestedItems(partNames.size())
                    .transportInvocations(1)
                    .transportItems(partNames.size())
                    .largestBatchSize(partNames.size())
                    .smallestBatchSize(partNames.size())
                    .logicalElapsedNanos(System.nanoTime() - startNanos)
                    .build();
            return new HmsPartitionBatchResult(partitions, stats);
        }

        @Override
        public HmsPartitionBatchResult getExistingPartitionsWithStats(
                String dbName, String tableName, List<String> partNames) {
            return getPartitionsWithStats(dbName, tableName, partNames);
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
