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

import org.apache.doris.connector.spi.ConnectorMetadataAccessObserver;
import org.apache.doris.connector.spi.ConnectorOperationAbortedException;
import org.apache.doris.connector.spi.ConnectorOperationControl;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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

    // ---- getTableFresh (SHOW CREATE TABLE — must bypass the table cache) ----

    @Test
    public void getTableFreshAlwaysHitsDelegate() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // WHY: SHOW CREATE TABLE must see the latest schema (a column added externally after the cache filled)
        // even while DESC serves the stale cached table. Every fresh call goes to the metastore. (test_hive_meta_cache.)
        cache.getTableFresh("db", "t1");
        cache.getTableFresh("db", "t1");
        Assertions.assertEquals(2, delegate.getTableCalls);
    }

    @Test
    public void getTableFreshDoesNotPopulateCache() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

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
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // WHY: SHOW PARTITIONS must see partitions added externally after the cache filled. Every fresh call
        // goes to the metastore — never served from partitionNamesCache. (test_hive_use_meta_cache_true sql09.)
        cache.listPartitionNamesFresh("db", "t", -1);
        cache.listPartitionNamesFresh("db", "t", -1);
        Assertions.assertEquals(2, delegate.listPartitionNamesCalls);
    }

    @Test
    public void listPartitionNamesFreshDoesNotPopulateCache() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

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
        Assertions.assertEquals(Arrays.asList("2"), rev.get(0).getValues());
        Assertions.assertEquals(Arrays.asList("1"), rev.get(1).getValues());
        Assertions.assertEquals(1, delegate.getPartitionsCalls, "reversed order still hits the shared entries");

        // WHY: only a genuinely new partition triggers a delegate fetch — and ONLY for the miss (p=1 stays
        // cached), proving misses are fetched in one round-trip while hits are served locally.
        cache.getPartitions("db", "t", Arrays.asList("p=1", "p=3"));
        Assertions.assertEquals(2, delegate.getPartitionsCalls, "only the new p=3 is fetched; p=1 stays cached");
        Assertions.assertEquals(Arrays.asList("p=3"), delegate.lastGetPartitionsArg,
                "the delegate is asked for the MISS only, not the whole requested list");
    }

    @Test
    public void getPartitionsPropagatesRequestObserverToCacheMiss() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());
        ConnectorMetadataAccessObserver observer = event -> { };
        HmsPartitionRequest request = HmsPartitionRequest.builder()
                .database("db")
                .table("t")
                .partitionNames(Arrays.asList("p=1", "p=2"))
                .source(HmsPartitionAccessSource.QUERY)
                .metadataAccessObserver(observer)
                .build();

        cache.getPartitions(request);

        Assertions.assertSame(observer, delegate.lastMetadataAccessObserver);
        Assertions.assertEquals(Arrays.asList("p=1", "p=2"), delegate.lastGetPartitionsArg);
    }

    @Test
    public void concurrentColdRequestsShareOneBulkLoad() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch bothRequestsFoundColdMisses = new CountDownLatch(1);
        AtomicInteger coldRequests = new AtomicInteger();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap()) {
            @Override
            void afterPartitionLoadRegistrationForTest() {
                if (coldRequests.incrementAndGet() == 2) {
                    bothRequestsFoundColdMisses.countDown();
                }
            }
        };
        CountDownLatch firstLoadEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstLoad = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            firstLoadEntered.countDown();
            try {
                releaseFirstLoad.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> first = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(firstLoadEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> second = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(bothRequestsFoundColdMisses.await(5, TimeUnit.SECONDS));
            releaseFirstLoad.countDown();

            Assertions.assertEquals(2, first.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(2, second.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(1, delegate.getPartitionsCalls,
                    "the waiter must re-check the per-partition cache after the in-flight bulk load");
        } finally {
            releaseFirstLoad.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void partiallyOverlappingColdRequestsOnlyLoadTheUnownedPartition() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());
        CountDownLatch firstLoadEntered = new CountDownLatch(1);
        CountDownLatch loadsEntered = new CountDownLatch(2);
        CountDownLatch releaseLoads = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            firstLoadEntered.countDown();
            loadsEntered.countDown();
            await(releaseLoads);
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> first = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(firstLoadEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> second = executor.submit(
                    () -> cache.getPartitions("db", "t", Arrays.asList("p=2", "p=3")));
            Assertions.assertTrue(loadsEntered.await(5, TimeUnit.SECONDS));
            releaseLoads.countDown();
            Assertions.assertEquals(2, first.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(2, second.get(5, TimeUnit.SECONDS).size());
            Assertions.assertTrue(delegate.getPartitionsArgs.contains(Arrays.asList("p=1", "p=2")));
            Assertions.assertTrue(delegate.getPartitionsArgs.contains(Collections.singletonList("p=3")));
        } finally {
            releaseLoads.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void waiterRetriesAfterItsOwnerIsCancelled() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch waiterRegistered = new CountDownLatch(1);
        AtomicInteger registrations = new AtomicInteger();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap()) {
            @Override
            void afterPartitionLoadRegistrationForTest() {
                if (registrations.incrementAndGet() == 2) {
                    waiterRegistered.countDown();
                }
            }
        };
        CountDownLatch ownerEntered = new CountDownLatch(1);
        CountDownLatch cancelOwner = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            if (delegate.getPartitionsCalls == 1) {
                ownerEntered.countDown();
                await(cancelOwner);
                throw new ConnectorOperationAbortedException(
                        ConnectorOperationAbortedException.Reason.CANCELLED, "owner cancelled");
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> owner = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(ownerEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> waiter = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(waiterRegistered.await(5, TimeUnit.SECONDS));
            cancelOwner.countDown();
            Assertions.assertInstanceOf(ConnectorOperationAbortedException.class,
                    Assertions.assertThrows(ExecutionException.class,
                            () -> owner.get(5, TimeUnit.SECONDS)).getCause());
            Assertions.assertEquals(1, waiter.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(2, delegate.getPartitionsCalls);
        } finally {
            cancelOwner.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void cancelledSlotWaiterDoesNotLeakThePermit() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch ownerEntered = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        CountDownLatch slotWaitStarted = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            if (delegate.getPartitionsCalls == 1) {
                ownerEntered.countDown();
                await(releaseOwner);
            }
        };
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap(), 1) {
            @Override
            void beforePartitionLoadSlotWaitForTest() {
                slotWaitStarted.countDown();
            }
        };
        AtomicBoolean cancelled = new AtomicBoolean();
        ConnectorOperationControl control = new ConnectorOperationControl() {
            @Override
            public void checkActive() {
                if (cancelled.get()) {
                    throw new ConnectorOperationAbortedException(
                            ConnectorOperationAbortedException.Reason.CANCELLED, "slot waiter cancelled");
                }
            }

            @Override
            public long remainingTimeMillis() {
                return TimeUnit.SECONDS.toMillis(30);
            }
        };
        HmsPartitionRequest waiting = HmsPartitionRequest.builder().database("db").table("t")
                .partitionNames(Collections.singletonList("p=2")).operationControl(control).build();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> owner = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(ownerEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> waiter = executor.submit(() -> cache.getPartitions(waiting));
            Assertions.assertTrue(slotWaitStarted.await(5, TimeUnit.SECONDS));
            cancelled.set(true);
            Assertions.assertInstanceOf(ConnectorOperationAbortedException.class,
                    Assertions.assertThrows(ExecutionException.class,
                            () -> waiter.get(5, TimeUnit.SECONDS)).getCause());
            releaseOwner.countDown();
            Assertions.assertEquals(1, owner.get(5, TimeUnit.SECONDS).size());
            Future<List<HmsPartitionInfo>> next = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=3")));
            Assertions.assertEquals(1, next.get(5, TimeUnit.SECONDS).size());
        } finally {
            releaseOwner.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void observerErrorAfterSlotAcquisitionDoesNotLeakThePermit() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch ownerEntered = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        CountDownLatch waiterQueued = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            if (delegate.getPartitionsCalls == 1) {
                ownerEntered.countDown();
                await(releaseOwner);
            }
        };
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap(), 1) {
            @Override
            void beforePartitionLoadSlotWaitForTest() {
                waiterQueued.countDown();
            }
        };
        HmsPartitionRequest failing = HmsPartitionRequest.builder().database("db").table("t")
                .partitionNames(Collections.singletonList("p=2"))
                .metadataAccessObserver(event -> {
                    throw new AssertionError("observer");
                }).build();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<List<HmsPartitionInfo>> owner = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(ownerEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> waiter = executor.submit(() -> cache.getPartitions(failing));
            Assertions.assertTrue(waiterQueued.await(5, TimeUnit.SECONDS));
            releaseOwner.countDown();
            Assertions.assertEquals(1, owner.get(5, TimeUnit.SECONDS).size());
            Assertions.assertInstanceOf(AssertionError.class,
                    Assertions.assertThrows(ExecutionException.class,
                            () -> waiter.get(5, TimeUnit.SECONDS)).getCause());
            Future<List<HmsPartitionInfo>> next = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=3")));
            Assertions.assertEquals(1, next.get(5, TimeUnit.SECONDS).size());
        } finally {
            releaseOwner.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void pureWaiterReleasesItsSlotBeforeWaitingForTheOwner() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch ownerEntered = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        CountDownLatch unrelatedEntered = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            if (delegate.lastGetPartitionsArg.equals(Collections.singletonList("p=1"))) {
                ownerEntered.countDown();
                await(releaseOwner);
            } else {
                unrelatedEntered.countDown();
            }
        };
        AtomicInteger registrations = new AtomicInteger();
        CountDownLatch waiterRegistered = new CountDownLatch(1);
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap(), 2) {
            @Override
            void afterPartitionLoadRegistrationForTest() {
                if (registrations.incrementAndGet() == 2) {
                    waiterRegistered.countDown();
                }
            }
        };
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<List<HmsPartitionInfo>> owner = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(ownerEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> waiter = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=1")));
            Assertions.assertTrue(waiterRegistered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> unrelated = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=2")));
            Assertions.assertTrue(unrelatedEntered.await(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, unrelated.get(5, TimeUnit.SECONDS).size());
            releaseOwner.countDown();
            Assertions.assertEquals(1, owner.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(1, waiter.get(5, TimeUnit.SECONDS).size());
        } finally {
            releaseOwner.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void loadSlotWaitersKeepFifoOrderAcrossControlPolling() throws Exception {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CountDownLatch ownerEntered = new CountDownLatch(1);
        CountDownLatch releaseOwner = new CountDownLatch(1);
        delegate.onGetPartitions = () -> {
            if (delegate.getPartitionsCalls == 1) {
                ownerEntered.countDown();
                await(releaseOwner);
            }
        };
        CountDownLatch firstQueued = new CountDownLatch(1);
        CountDownLatch bothQueued = new CountDownLatch(2);
        AtomicBoolean queued = new AtomicBoolean();
        AtomicInteger checks = new AtomicInteger();
        CountDownLatch polled = new CountDownLatch(1);
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap(), 1) {
            @Override
            void beforePartitionLoadSlotWaitForTest() {
                if (queued.compareAndSet(false, true)) {
                    firstQueued.countDown();
                }
                bothQueued.countDown();
            }
        };
        ConnectorOperationControl control = new ConnectorOperationControl() {
            @Override
            public void checkActive() {
                if (queued.get() && checks.incrementAndGet() == 2) {
                    polled.countDown();
                }
            }

            @Override
            public long remainingTimeMillis() {
                return TimeUnit.SECONDS.toMillis(30);
            }
        };
        HmsPartitionRequest firstRequest = HmsPartitionRequest.builder().database("db").table("t")
                .partitionNames(Collections.singletonList("p=1")).operationControl(control).build();
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<List<HmsPartitionInfo>> owner = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=0")));
            Assertions.assertTrue(ownerEntered.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> first = executor.submit(() -> cache.getPartitions(firstRequest));
            Assertions.assertTrue(firstQueued.await(5, TimeUnit.SECONDS));
            Future<List<HmsPartitionInfo>> second = executor.submit(
                    () -> cache.getPartitions("db", "t", Collections.singletonList("p=2")));
            Assertions.assertTrue(bothQueued.await(5, TimeUnit.SECONDS));
            Assertions.assertTrue(polled.await(5, TimeUnit.SECONDS));
            releaseOwner.countDown();
            Assertions.assertEquals(1, owner.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(1, first.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(1, second.get(5, TimeUnit.SECONDS).size());
            Assertions.assertEquals(Arrays.asList("p=0", "p=1", "p=2"),
                    delegate.getPartitionsArgs.stream().map(names -> names.get(0))
                            .collect(java.util.stream.Collectors.toList()));
        } finally {
            releaseOwner.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void coordinationWindowBoundsInFlightPartitions() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        AtomicInteger maxInFlight = new AtomicInteger();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.singletonMap(
                HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "128")) {
            @Override
            void afterPartitionLoadRegistrationForTest() {
                maxInFlight.accumulateAndGet(inFlightPartitionLoadCountForTest(), Math::max);
            }
        };
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 1025; i++) {
            names.add("p=" + i);
        }
        Assertions.assertEquals(names.size(), cache.getPartitions("db", "t", names).size());
        Assertions.assertEquals(128, maxInFlight.get());
        Assertions.assertEquals(0, cache.inFlightPartitionLoadCountForTest());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void fallbackStateIsSharedAcrossCacheWindows() {
        List<Integer> attempts = new ArrayList<>();
        HmsPartitionBatchLoader rawLoader = HmsPartitionBatchLoader.builder()
                .maxBatchSize(4).fallbackTimeoutMillis(30)
                .fetcher((db, table, names, control) -> {
                    attempts.add(names.size());
                    if (names.size() > 2) {
                        throw new HmsRemoteCallException("remote",
                                new shade.doris.hive.org.apache.thrift.TException("too many partitions"));
                    }
                    List<HmsPartitionInfo> result = new ArrayList<>();
                    for (String name : names) {
                        result.add(new HmsPartitionInfo(HmsPartitionIdentity.fromName(name),
                                "loc/" + name, null, null, null, null));
                    }
                    return result;
                }).build();
        RecordingHmsClient delegate = new RecordingHmsClient() {
            @Override
            public List<HmsPartitionInfo> getPartitions(HmsPartitionRequest request) {
                return rawLoader.load(request);
            }
        };
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.singletonMap(
                HmsClientConfig.PARTITION_BATCH_SIZE_KEY, "4"));

        Assertions.assertEquals(8, cache.getPartitions("db", "t", Arrays.asList(
                "p=1", "p=2", "p=3", "p=4", "p=5", "p=6", "p=7", "p=8")).size());
        Assertions.assertEquals(Arrays.asList(4, 2, 2, 2, 2), attempts);
    }

    @Test
    public void getPartitionsRejectsMissingPartitionWithoutPartialCaching() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        delegate.absentPartitionNames.add("p=9"); // HMS has no such partition -> omitted from the response
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        HmsPartitionResultException failure = Assertions.assertThrows(HmsPartitionResultException.class,
                () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=9")));
        Assertions.assertTrue(failure.getMismatchTypes()
                .contains(HmsPartitionResultException.MismatchType.MISSING_RESULT));
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        // The successful p=1 from an incomplete response must not leak into the cache: retry the whole logical
        // request, rather than silently combining a stale partial result with a future response.
        Assertions.assertThrows(HmsPartitionResultException.class,
                () -> cache.getPartitions("db", "t", Arrays.asList("p=1", "p=9")));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "an incomplete response must not populate any partition cache entry");
        Assertions.assertEquals(Arrays.asList("p=1", "p=9"), delegate.lastGetPartitionsArg);
    }

    @Test
    public void getPartitionsRejectsUnexpectedStoredValues() {
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

        HmsPartitionResultException first = Assertions.assertThrows(HmsPartitionResultException.class,
                () -> cache.getPartitions("db", "t", Arrays.asList("p=1")));
        Assertions.assertTrue(first.getMismatchTypes()
                .contains(HmsPartitionResultException.MismatchType.UNEXPECTED_RESULT));
        Assertions.assertTrue(first.getMismatchTypes()
                .contains(HmsPartitionResultException.MismatchType.MISSING_RESULT));
        Assertions.assertEquals(1, delegate.getPartitionsCalls);

        Assertions.assertThrows(HmsPartitionResultException.class,
                () -> cache.getPartitions("db", "t", Arrays.asList("p=1")));
        Assertions.assertEquals(2, delegate.getPartitionsCalls,
                "an unexpected result must not be cached under either identity");
    }

    @Test
    public void getPartitionsFlushRacingInFlightFetchDoesNotRecacheStale() {
        RecordingHmsClient delegate = new RecordingHmsClient();
        CachingHmsClient cache = new CachingHmsClient(delegate, Collections.emptyMap());

        // Model a REFRESH TABLE (flush) landing DURING the cold-cache delegate RPC: getPartitions captures the
        // invalidation generation BEFORE the RPC, the flush bumps it mid-RPC, so the per-partition guarded put
        // must be dropped rather than re-cache the pre-refresh partition. The in-flight query still returns the
        // freshly-fetched partition (only the CACHE put is guarded).
        delegate.onGetPartitions = () -> cache.flush("db", "t");
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
        CachingHmsClient cache = new CachingHmsClient(delegate, properties);

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
    private static class RecordingHmsClient implements HmsClient {
        int getTableCalls;
        int listPartitionNamesCalls;
        int getPartitionsCalls;
        final AtomicInteger getPartitionsCallCounter = new AtomicInteger();
        final List<List<String>> getPartitionsArgs = Collections.synchronizedList(new ArrayList<>());
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
        ConnectorMetadataAccessObserver lastMetadataAccessObserver;
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
            getPartitionsCalls = getPartitionsCallCounter.incrementAndGet();
            lastGetPartitionsArg = new ArrayList<>(partNames);
            getPartitionsArgs.add(lastGetPartitionsArg);
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
        public List<HmsPartitionInfo> getPartitions(HmsPartitionRequest request) {
            lastMetadataAccessObserver = request.getMetadataAccessObserver();
            return getPartitions(request.getDbName(), request.getTableName(), request.getPartitionNames());
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
