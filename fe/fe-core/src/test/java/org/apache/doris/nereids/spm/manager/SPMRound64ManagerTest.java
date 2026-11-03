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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.BaselineStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The follow-up review of the matching-path cache miss: the read was already bounded by
 * the caller's rewrite budget, but N concurrent cold queries of the SAME evicted hash
 * still issued N separate synchronous by-hash reads and stampeded the internal table.
 *
 * The fix COALESCES them: the first attempt loads while the hash is marked IN FLIGHT
 * (still bounded by its budget, still matching from the loaded rows), and every
 * concurrent attempt of that hash degrades immediately - no read, no wait, and no index
 * change - while the completed load serves the following statements from the cache. The
 * marker is per hash and is released on every exit path, so a failed load cannot wedge
 * the hash.
 */
public class SPMRound64ManagerTest {

    private final BaselineTestDoubles.TestBaselineStore store =
            new BaselineTestDoubles.TestBaselineStore();
    private final BaselineTestDoubles.TestClusterGateway gateway =
            new BaselineTestDoubles.TestClusterGateway();
    private BaselineManager manager;

    private BaselineManager newManager() {
        BaselineManager created = new BaselineManager(store, gateway, System::currentTimeMillis);
        BaselineManagerTestAccess.clear(created);
        return created;
    }

    private static BaselinePlan row(long id, String digest, long hash, BaselineStatus status,
            long updateTime) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(hash);
        plan.setPlanSql("SELECT k FROM t1 WHERE k = _spm_const_var(1)");
        plan.setSource(BaselineSource.USER);
        plan.setStatus(status);
        plan.setCreateTime(updateTime);
        plan.setUpdateTime(updateTime);
        return plan;
    }

    /**
     * N concurrent misses of ONE hash coalesce into ONE internal read: the loader matches
     * from the rows it read, the concurrent attempt degrades WITHOUT waiting and WITHOUT
     * a read, and the next statement is served from the cache the loader filled.
     */
    @Test
    public void testConcurrentMissesOfOneHashCoalesceIntoOneRead() throws Exception {
        store.durableOverride = true;
        manager = newManager();
        BaselinePlan enabled = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        manager.applyRefreshedBaselines(Map.of(1L, enabled));

        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch readerRelease = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            readerEntered.countDown();
            try {
                readerRelease.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L));
        };

        AtomicReference<List<BaselinePlan>> loaderResult = new AtomicReference<>();
        Thread loader = new Thread(() ->
                loaderResult.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
        loader.start();
        Assertions.assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "the first miss must be reading");

        // the concurrent miss of the SAME hash: degrade immediately, do not read, do not wait
        AtomicReference<List<BaselinePlan>> concurrentResult = new AtomicReference<>();
        Thread concurrent = new Thread(() ->
                concurrentResult.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
        concurrent.start();
        concurrent.join(TimeUnit.SECONDS.toMillis(3));
        Assertions.assertFalse(concurrent.isAlive(),
                "a coalesced miss must not wait for the in-flight load");
        Assertions.assertTrue(concurrentResult.get().isEmpty(),
                "the coalesced attempt degrades: this statement runs without SPM");
        Assertions.assertEquals(1, reads.get(),
                "only the first attempt may read the internal table");
        Assertions.assertEquals(1, BaselineManagerTestAccess.inflightMissLoadCount(manager),
                "the hash stays marked while its load is in flight");
        Assertions.assertTrue(manager.hasBaselines(),
                "a coalesced attempt must not touch the hash index");

        readerRelease.countDown();
        loader.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertEquals(1, loaderResult.get().size(),
                "the loader still matches from the rows it read");
        Assertions.assertEquals(1, reads.get());
        Assertions.assertEquals(0, BaselineManagerTestAccess.inflightMissLoadCount(manager),
                "the marker is released when the load ends");
        Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                "the completed load serves the next statements");
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
        Assertions.assertEquals(1, reads.get(), "the next statement is a cache hit");
    }

    /**
     * The coalescing is PER HASH: while one hash's load is in flight, a miss of another
     * hash still reads and matches on its own.
     */
    @Test
    public void testMissOfAnotherHashLoadsIndependently() throws Exception {
        store.durableOverride = true;
        manager = newManager();
        BaselinePlan enabledA = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        BaselinePlan enabledB = row(2L, "digest_b", 0xB2L, BaselineStatus.ENABLED, 2_000L);
        manager.applyRefreshedBaselines(Map.of(1L, enabledA, 2L, enabledB));

        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch readerRelease = new CountDownLatch(1);
        AtomicInteger readsA = new AtomicInteger();
        AtomicInteger readsB = new AtomicInteger();
        store.byHashReader = hash -> {
            if (hash == 0xA1L) {
                readsA.incrementAndGet();
                readerEntered.countDown();
                try {
                    readerRelease.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L));
            }
            readsB.incrementAndGet();
            return List.of(row(2L, "digest_b", 0xB2L, BaselineStatus.ENABLED, 2_000L));
        };

        AtomicReference<List<BaselinePlan>> resultA = new AtomicReference<>();
        Thread loaderA = new Thread(() ->
                resultA.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
        loaderA.start();
        Assertions.assertTrue(readerEntered.await(5, TimeUnit.SECONDS));

        // another hash is NOT coalesced: it reads and matches on its own
        Assertions.assertEquals(1,
                manager.findCandidateBaselines("digest_b", 0xB2L).size());
        Assertions.assertEquals(1, readsB.get());
        Assertions.assertEquals(1, readsA.get(), "the other hash's load is independent");

        readerRelease.countDown();
        loaderA.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertEquals(1, resultA.get().size());
        Assertions.assertEquals(0, BaselineManagerTestAccess.inflightMissLoadCount(manager));
    }

    /**
     * A FAILED load releases the marker (try/finally): the very next statement reads
     * again and matches - a fault of one read must not wedge the hash in flight forever.
     */
    @Test
    public void testFailedLoadReleasesTheInflightMarker() {
        store.durableOverride = true;
        manager = newManager();
        manager.applyRefreshedBaselines(Map.of(1L,
                row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L)));
        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            if (reads.incrementAndGet() == 1) {
                throw new RuntimeException("internal read timed out");
            }
            return List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L));
        };

        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                "a failed read degrades this attempt");
        Assertions.assertEquals(0, BaselineManagerTestAccess.inflightMissLoadCount(manager),
                "the marker of the failed load must be released");
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                "nothing is cached for a failed read");

        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size(),
                "the next statement retries the read and matches");
        Assertions.assertEquals(2, reads.get());
    }
}
