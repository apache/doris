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

import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.nereids.spm.manager.BaselinePersistence.SeqReservation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Round-92 review fix: {@code refreshFromInternalTable} must NOT hold {@code writerLock} across
 * its paginated snapshot read.
 *
 * <p>One page is one internal query with its own timeout, so a large or slow baseline table held
 * the writers' monitor for minutes and blocked every GLOBAL CREATE / DROP / ALTER (and the
 * forwarded-DDL reconciliation) on that FE. The read is now side-effect free and runs outside the
 * monitor; only the tombstone repair and the publication are serialized, and a store generation /
 * state version change in between discards the snapshot.
 */
public class SPMRound92ManagerTest {

    /**
     * The read itself does not hold the writers' monitor: a writer arriving while a snapshot page
     * is still in flight acquires it immediately (before the fix it waited for the whole read).
     */
    @Test
    public void testSnapshotReadDoesNotHoldTheWriterLock() throws Exception {
        boolean schemaDb = FeConstants.enableInternalSchemaDb;
        FeConstants.enableInternalSchemaDb = true;
        try {
            BlockingStore store = new BlockingStore();
            BaselineManager manager = manager(store);
            Object writerLock = BaselineManagerTestAccess.writerLock(manager);

            AtomicReference<Throwable> refreshError = new AtomicReference<>();
            Thread refresh = new Thread(() -> {
                try {
                    manager.refreshFromInternalTable();
                } catch (Throwable t) {
                    refreshError.set(t);
                }
            });
            refresh.start();
            try {
                Assertions.assertTrue(store.readStarted.await(5, TimeUnit.SECONDS),
                        "the refresh must reach the snapshot read");
                CountDownLatch acquired = new CountDownLatch(1);
                Thread writer = new Thread(() -> {
                    synchronized (writerLock) {
                        acquired.countDown();
                    }
                });
                writer.start();
                Assertions.assertTrue(acquired.await(2, TimeUnit.SECONDS),
                        "a writer must not be blocked by the refresh snapshot read");
                writer.join(2000);
            } finally {
                store.releaseRead.countDown();
                refresh.join(5000);
            }
            Assertions.assertFalse(refresh.isAlive(), "the refresh must terminate");
            if (refreshError.get() != null) {
                throw new AssertionError("refresh failed", refreshError.get());
            }
        } finally {
            FeConstants.enableInternalSchemaDb = schemaDb;
        }
    }

    /** Control for the generation guard below: an undisturbed read IS published. */
    @Test
    public void testSnapshotIsPublishedWhenTheStoreDidNotMove() throws Exception {
        boolean schemaDb = FeConstants.enableInternalSchemaDb;
        FeConstants.enableInternalSchemaDb = true;
        try {
            BlockingStore store = new BlockingStore();
            BaselineManager manager = manager(store);
            store.rows.put(7L, remoteBaseline(7L));

            long versionBefore = manager.getStateVersion();
            runRefreshWhileReading(store, manager, () -> {
            });

            Assertions.assertTrue(manager.getStateVersion() > versionBefore,
                    "an undisturbed refresh must publish its snapshot");
        } finally {
            FeConstants.enableInternalSchemaDb = schemaDb;
        }
    }

    /**
     * The generation guard: a snapshot whose read straddled an invalidation / forwarded-DDL fence
     * must be DISCARDED - it may still contain rows a DROP already deleted.
     */
    @Test
    public void testSnapshotIsDiscardedWhenTheStoreGenerationMoved() throws Exception {
        boolean schemaDb = FeConstants.enableInternalSchemaDb;
        FeConstants.enableInternalSchemaDb = true;
        try {
            BlockingStore store = new BlockingStore();
            BaselineManager manager = manager(store);
            store.rows.put(7L, remoteBaseline(7L));

            long versionBefore = manager.getStateVersion();
            runRefreshWhileReading(store, manager,
                    () -> BaselineManagerTestAccess.bumpStoreGeneration(manager));

            Assertions.assertEquals(versionBefore, manager.getStateVersion(),
                    "a snapshot read before the fence must not be published");
            Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xB7L),
                    "a snapshot read before the fence must not publish its stale row");
        } finally {
            FeConstants.enableInternalSchemaDb = schemaDb;
        }
    }

    // ==================== helpers ====================

    /** Runs one refresh, applies {@code whileReading} once its snapshot page is in flight. */
    private static void runRefreshWhileReading(BlockingStore store, BaselineManager manager,
            Runnable whileReading) throws Exception {
        AtomicReference<Throwable> refreshError = new AtomicReference<>();
        Thread refresh = new Thread(() -> {
            try {
                manager.refreshFromInternalTable();
            } catch (Throwable t) {
                refreshError.set(t);
            }
        });
        refresh.start();
        try {
            Assertions.assertTrue(store.readStarted.await(5, TimeUnit.SECONDS),
                    "the refresh must reach the snapshot read");
            whileReading.run();
        } finally {
            store.releaseRead.countDown();
            refresh.join(5000);
        }
        Assertions.assertFalse(refresh.isAlive(), "the refresh must terminate");
        if (refreshError.get() != null) {
            throw new AssertionError("refresh failed", refreshError.get());
        }
    }

    /** A durable store whose snapshot read can be held open, like a slow internal query. */
    private static final class BlockingStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final CountDownLatch readStarted = new CountDownLatch(1);
        final CountDownLatch releaseRead = new CountDownLatch(1);

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public boolean hashReadsAvailable() {
            return true;
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            BaselinePlan found = rows.get(id);
            return found == null ? List.of() : List.of(found);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            List<BaselinePlan> found = new ArrayList<>();
            for (BaselinePlan plan : rows.values()) {
                if (plan.getBindSqlHash() == hash) {
                    found.add(plan);
                }
            }
            return found;
        }

        @Override
        public Map<Long, BaselinePlan> readSnapshot() {
            readStarted.countDown();
            try {
                if (!releaseRead.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the test never released the snapshot read");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new ConcurrentHashMap<>(rows);
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            rows.remove(plan.getId());
        }

        @Override
        public boolean appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            return true;
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids) {
            return new HashSet<>();
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids, int timeoutSeconds) {
            return readDroppedIdentities(ids);
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            return null;
        }

        @Override
        public Map<Long, SeqReservation> readLatestReservations(Collection<Long> compactIds,
                int timeoutSeconds) {
            return Map.of();
        }
    }

    /** Builds a baseline as it would come back from a refresh (no transient trees needed). */
    private static BaselinePlan remoteBaseline(long id) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("SELECT 1 FROM t WHERE k = ?");
        plan.setBindSqlDigest("digest_" + id);
        plan.setBindSqlHash(0xB7L);
        plan.setPlanSql("SELECT 1 FROM t WHERE k = _spm_const_var(1)");
        plan.setCost(1.0D);
        plan.setQueryTimeMs(-1L);
        plan.setStatus(BaselineStatus.ENABLED);
        plan.setCreateTime(1_000L);
        plan.setUpdateTime(1_000L);
        return plan;
    }

    private static BaselineManager manager(BaselinePersistence store) {
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), () -> 1_700_000_000_000L);
        BaselineManagerTestAccess.clear(manager);
        // clear() disables the durable path for unit tests; the refresh needs it ON
        BaselineManagerTestAccess.enableDurableRefresh(manager);
        return manager;
    }
}
