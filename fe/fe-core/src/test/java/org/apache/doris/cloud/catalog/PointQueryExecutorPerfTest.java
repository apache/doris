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

package org.apache.doris.cloud.catalog;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.VersionHelper;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.PointQueryExecutor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.ShortCircuitQueryContext;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import mockit.Mock;
import mockit.MockUp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end performance test for PointQueryExecutor.updateCloudPartitionVersions().
 *
 * Mocks the MetaService RPC layer (VersionHelper.getVersionFromMeta) with configurable
 * latency, then exercises the actual updateCloudPartitionVersions() code path under
 * high concurrency. Compares RPC counts with cache disabled vs enabled.
 */
public class PointQueryExecutorPerfTest {
    private static final long PARTITION_ID = 10001;
    private static final long TABLE_ID = 1001;
    private static final long DB_ID = 100;

    // Counter for MetaService RPC calls
    private static final AtomicInteger msRpcCount = new AtomicInteger(0);
    private static volatile long msRpcLatencyMs = 5;
    private static volatile long msVersionToReturn = 42L;

    private CloudPartition partition;

    @Before
    public void setUp() throws Exception {
        msRpcCount.set(0);
        partition = CloudPartitionTest.createPartition(PARTITION_ID, DB_ID, TABLE_ID);

        // -- Mock VersionHelper.getVersionFromMeta (the MetaService RPC) --
        new MockUp<VersionHelper>(VersionHelper.class) {
            @Mock
            public Cloud.GetVersionResponse getVersionFromMeta(Cloud.GetVersionRequest req) {
                msRpcCount.incrementAndGet();
                if (msRpcLatencyMs > 0) {
                    try {
                        Thread.sleep(msRpcLatencyMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return Cloud.GetVersionResponse.newBuilder()
                        .setStatus(Cloud.MetaServiceResponseStatus.newBuilder()
                                .setCode(Cloud.MetaServiceCode.OK).build())
                        .setVersion(msVersionToReturn)
                        .build();
            }
        };

        // -- Create Objenesis OlapTable and mock its getPartition --
        final OlapTable mockTable = new org.objenesis.ObjenesisStd().newInstance(OlapTable.class);
        new MockUp<OlapTable>(OlapTable.class) {
            @Mock
            public Partition getPartition(long partitionId) {
                return partition;
            }
        };

        // -- Mock OlapScanNode --
        new MockUp<OlapScanNode>(OlapScanNode.class) {
            @Mock
            public Collection<Long> getSelectedPartitionIds() {
                return ImmutableList.of(PARTITION_ID);
            }

            @Mock
            public OlapTable getOlapTable() {
                return mockTable;
            }
        };

        // -- Mock ShortCircuitQueryContext.sanitize --
        new MockUp<ShortCircuitQueryContext>(ShortCircuitQueryContext.class) {
            @Mock
            public void sanitize() {
                // no-op
            }
        };

        PointQueryVersionCache.getInstance().clear();
    }

    @After
    public void tearDown() {
        PointQueryVersionCache.getInstance().clear();
    }

    /**
     * Create a PointQueryExecutor and directly invoke updateCloudPartitionVersions().
     * This is the exact code path that was optimized.
     */
    private void invokeUpdateCloudPartitionVersions(long cacheTtlMs) throws Exception {
        // Set up ConnectContext for this thread
        SessionVariable sv = new SessionVariable();
        sv.enableSnapshotPointQuery = true;
        sv.pointQueryVersionCacheTtlMs = cacheTtlMs;
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(sv);
        ctx.setThreadLocalInfo();

        // Build minimal PointQueryExecutor
        org.objenesis.ObjenesisStd objenesis = new org.objenesis.ObjenesisStd();
        ShortCircuitQueryContext queryCtx = objenesis.newInstance(ShortCircuitQueryContext.class);
        OlapScanNode scanNode = objenesis.newInstance(OlapScanNode.class);
        OlapTable table = objenesis.newInstance(OlapTable.class);

        setField(queryCtx, "scanNode", scanNode);
        setField(queryCtx, "tbl", table);
        setField(queryCtx, "serializedDescTable", ByteString.copyFrom(new byte[]{0}));
        setField(queryCtx, "serializedOutputExpr", ByteString.copyFrom(new byte[]{0}));
        setField(queryCtx, "serializedQueryOptions", ByteString.copyFrom(new byte[]{0}));
        setField(queryCtx, "cacheID", java.util.UUID.randomUUID());
        setField(queryCtx, "schemaVersion", 0);

        PointQueryExecutor executor = new PointQueryExecutor(queryCtx, 1024 * 1024);

        // Directly invoke updateCloudPartitionVersions() via reflection
        Method method = PointQueryExecutor.class.getDeclaredMethod("updateCloudPartitionVersions");
        method.setAccessible(true);
        method.invoke(executor);
    }

    private static void setField(Object obj, String fieldName, Object value) throws Exception {
        Class<?> clz = obj.getClass();
        while (clz != null) {
            try {
                Field f = clz.getDeclaredField(fieldName);
                f.setAccessible(true);
                f.set(obj, value);
                return;
            } catch (NoSuchFieldException e) {
                clz = clz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    // ======================== Main Test ========================

    @Test
    public void testHighConcurrencyCacheEffectiveness() throws Exception {
        int numThreads = 100;
        msRpcLatencyMs = 5; // 5ms per MetaService RPC

        // ========== WITHOUT CACHE (ttl=0) ==========
        msRpcCount.set(0);
        PointQueryVersionCache.getInstance().clear();
        long noCacheWallMs = runConcurrent(numThreads, 0);
        int noCacheRpcs = msRpcCount.get();

        // ========== WITH CACHE (ttl=500ms) ==========
        msRpcCount.set(0);
        PointQueryVersionCache.getInstance().clear();
        long cachedWallMs = runConcurrent(numThreads, 500);
        int cachedRpcs = msRpcCount.get();

        // ========== Print Results ==========
        double reductionPct = (1.0 - (double) cachedRpcs / Math.max(1, noCacheRpcs)) * 100;
        double reductionFactor = (double) noCacheRpcs / Math.max(1, cachedRpcs);

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  PointQueryExecutor.updateCloudPartitionVersions() Perf Test ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  Concurrent threads:      %-34d║%n", numThreads);
        System.out.printf("║  MetaService RPC latency: %-34s║%n", msRpcLatencyMs + "ms");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf("║  %-28s %-14s %-14s ║%n", "Metric", "No Cache", "With Cache");
        System.out.println("║  ──────────────────────── ────────────── ────────────── ║");
        System.out.printf("║  %-28s %-14d %-14d ║%n", "MetaService RPCs", noCacheRpcs, cachedRpcs);
        System.out.printf("║  %-28s %-14s %-14s ║%n", "Wall time",
                noCacheWallMs + "ms", cachedWallMs + "ms");
        System.out.printf("║  %-28s %-14s %-14s ║%n", "RPC reduction", "-",
                String.format("%.1f%% (%.0fx)", reductionPct, reductionFactor));
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();

        // ========== Assertions ==========
        Assertions.assertTrue(noCacheRpcs >= numThreads,
                "Without cache, expected >= " + numThreads + " MS RPCs, got " + noCacheRpcs);
        Assertions.assertTrue(cachedRpcs <= 5,
                "With cache, expected <= 5 MS RPCs, got " + cachedRpcs);
        Assertions.assertTrue(reductionFactor >= 20,
                "Expected >= 20x RPC reduction, got " + String.format("%.1fx", reductionFactor));
    }

    private long runConcurrent(int numThreads, long cacheTtlMs) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicReference<Exception> error = new AtomicReference<>();

        long startNs = System.nanoTime();

        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    barrier.await();
                    invokeUpdateCloudPartitionVersions(cacheTtlMs);
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await();
        long wallMs = (System.nanoTime() - startNs) / 1_000_000;

        if (error.get() != null) {
            throw new RuntimeException("Thread failed", error.get());
        }

        return wallMs;
    }
}
