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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.thrift.TDeltaLakeDeletionVectorDesc;
import org.apache.doris.thrift.TDeltaLakeFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

class DeltaLakeScanPlanMemoTest {
    private final DeltaLakeScanPlanMemo.Key key = key("table");
    private final DeltaLakeScanPlanMemo.Plan plan = new DeltaLakeScanPlanMemo.Plan(List.of(range("a", 0)), null);

    @Test
    void coalescesConcurrentLoadsWithoutLettingACancelledWaiterCancelTheOwner() throws Exception {
        DeltaLakeScanPlanMemo memo = new DeltaLakeScanPlanMemo();
        AtomicInteger loads = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch waiting = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<DeltaLakeScanPlanMemo.Outcome> owner = executor.submit(() -> memo.get(key,
                    new ConnectorScanContext(), () -> {
                        loads.incrementAndGet();
                        started.countDown();
                        await(release);
                        return plan;
                    }));
            await(started);
            ConnectorScanContext cancelled = new ConnectorScanContext();
            Future<?> waiter = executor.submit(() -> {
                waiting.countDown();
                return memo.get(key, cancelled, () -> {
                    Assertions.fail("waiter must not load");
                    return plan;
                });
            });
            await(waiting);
            Assertions.assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));
            cancelled.cancel();
            Assertions.assertInstanceOf(CancellationException.class,
                    Assertions.assertThrows(ExecutionException.class, () -> waiter.get(10, TimeUnit.SECONDS)).getCause());
            Future<DeltaLakeScanPlanMemo.Outcome> survivor = executor.submit(() -> memo.get(key,
                    new ConnectorScanContext(), () -> {
                        Assertions.fail("concurrent identical scan must share the load");
                        return plan;
                    }));
            // A different key must make progress while the first owner's IO is blocked.
            Assertions.assertFalse(memo.get(key("another-table"), new ConnectorScanContext(), () -> plan).reused());
            release.countDown();
            Assertions.assertFalse(owner.get(10, TimeUnit.SECONDS).reused());
            Assertions.assertSame(plan, survivor.get(10, TimeUnit.SECONDS).plan());
            Assertions.assertTrue(survivor.get().reused());
            Assertions.assertEquals(1, loads.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void interruptedWaitersLeaveAndOwnerFailuresReachOtherWaitersWithoutBeingCached() throws Exception {
        DeltaLakeScanPlanMemo memo = new DeltaLakeScanPlanMemo();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch waiting = new CountDownLatch(1);
        AtomicReference<Thread> waiterThread = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        IllegalStateException failure = new IllegalStateException("checkpoint IO failed");
        ExecutorService executor = Executors.newFixedThreadPool(3);
        try {
            Future<?> owner = executor.submit(() -> memo.get(key, new ConnectorScanContext(), () -> {
                started.countDown();
                await(release);
                throw failure;
            }));
            await(started);
            Future<?> waiter = executor.submit(() -> {
                waiterThread.set(Thread.currentThread());
                waiting.countDown();
                try {
                    return memo.get(key, new ConnectorScanContext(), () -> plan);
                } finally {
                    interrupted.set(Thread.currentThread().isInterrupted());
                }
            });
            await(waiting);
            Assertions.assertThrows(TimeoutException.class, () -> waiter.get(100, TimeUnit.MILLISECONDS));
            waiterThread.get().interrupt();
            Assertions.assertInstanceOf(CancellationException.class,
                    Assertions.assertThrows(ExecutionException.class, () -> waiter.get(10, TimeUnit.SECONDS)).getCause());
            Assertions.assertTrue(interrupted.get());
            Future<?> other = executor.submit(() -> memo.get(key, new ConnectorScanContext(), () -> plan));
            Assertions.assertThrows(TimeoutException.class, () -> other.get(100, TimeUnit.MILLISECONDS));
            release.countDown();
            for (Future<?> task : List.of(owner, other)) {
                Assertions.assertSame(failure,
                        Assertions.assertThrows(ExecutionException.class, () -> task.get(10, TimeUnit.SECONDS)).getCause());
            }
            Assertions.assertFalse(memo.get(key, new ConnectorScanContext(), () -> plan).reused());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void failedAndCancelledLoadsAreRetriedInsteadOfPublishingPartialPlans() {
        DeltaLakeScanPlanMemo memo = new DeltaLakeScanPlanMemo();
        IllegalStateException failure = new IllegalStateException("failed after enumerating one file");
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> memo.get(key, new ConnectorScanContext(), () -> {
                    throw failure;
                })));
        ConnectorScanContext cancelled = new ConnectorScanContext();
        Assertions.assertThrows(CancellationException.class, () -> memo.get(key, cancelled, () -> {
            cancelled.cancel();
            return plan;
        }));
        Assertions.assertFalse(memo.get(key, new ConnectorScanContext(), () -> plan).reused());
        Assertions.assertTrue(memo.get(key, new ConnectorScanContext(), () -> plan).reused());
        Thread.currentThread().interrupt();
        try {
            Assertions.assertThrows(CancellationException.class,
                    () -> memo.get(key, new ConnectorScanContext(), () -> plan));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void boundsEntriesTotalRangesAndRangePayloadWithoutTruncatingResults() {
        DeltaLakeScanPlanMemo oneEntry = new DeltaLakeScanPlanMemo(1, 10, 1 << 20);
        Assertions.assertFalse(oneEntry.get(key, new ConnectorScanContext(), () -> plan).reused());
        for (int i = 0; i < 2; i++) {
            Assertions.assertFalse(oneEntry.get(key("extra"), new ConnectorScanContext(), () -> plan).reused());
        }
        DeltaLakeScanPlanMemo twoRanges = new DeltaLakeScanPlanMemo(10, 2, 1 << 20);
        DeltaLakeScanPlanMemo.Plan oversized = new DeltaLakeScanPlanMemo.Plan(
                List.of(range("a", 0), range("b", 0), range("c", 0)), null);
        for (int i = 0; i < 2; i++) {
            DeltaLakeScanPlanMemo.Outcome result = twoRanges.get(key, new ConnectorScanContext(), () -> oversized);
            Assertions.assertFalse(result.reused());
            Assertions.assertEquals(3, result.plan().ranges().size());
        }
        for (String table : List.of("first", "second")) {
            Assertions.assertFalse(twoRanges.get(key(table), new ConnectorScanContext(), () -> plan).reused());
        }
        Assertions.assertFalse(twoRanges.get(key, new ConnectorScanContext(), () -> plan).reused());
        Assertions.assertFalse(twoRanges.get(key, new ConnectorScanContext(), () -> plan).reused());

        DeltaLakeScanPlanMemo bytes = new DeltaLakeScanPlanMemo(10, 100, 1024);
        for (DeltaLakeScanRange range : List.of(range("a".repeat(2048), 0), range("a", 2048))) {
            DeltaLakeScanPlanMemo.Plan largePayload = new DeltaLakeScanPlanMemo.Plan(List.of(range), null);
            for (int i = 0; i < 2; i++) {
                Assertions.assertFalse(bytes.get(key, new ConnectorScanContext(), () -> largePayload).reused());
            }
        }
        Assertions.assertFalse(bytes.get(key, new ConnectorScanContext(), () -> plan).reused());
        Assertions.assertTrue(bytes.get(key, new ConnectorScanContext(), () -> plan).reused());
        DeltaLakeScanPlanMemo.Plan empty = new DeltaLakeScanPlanMemo.Plan(List.of(), null);
        Assertions.assertFalse(oneEntry.get(key("empty"), new ConnectorScanContext(), () -> empty).reused());
        Assertions.assertFalse(oneEntry.get(key("empty"), new ConnectorScanContext(), () -> empty).reused());
    }

    @Test
    void distinguishesSnapshotCoordinatesAndTypedPredicatesEvenWhenTextMatches() {
        DeltaLakeScanPlanMemo memo = new DeltaLakeScanPlanMemo();
        AtomicInteger loads = new AtomicInteger();
        Supplier<DeltaLakeScanPlanMemo.Plan> loader = () -> {
            loads.incrementAndGet();
            return plan;
        };
        ConnectorLiteral bigint = new ConnectorLiteral(ConnectorType.of("BIGINT"), 1L);
        ConnectorLiteral integer = new ConnectorLiteral(ConnectorType.of("INT"), 1L);
        Assertions.assertEquals(bigint.toString(), integer.toString());
        List<DeltaLakeScanPlanMemo.Key> keys = List.of(key,
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "path", "other-id", 0,
                        Optional.empty(), Set.of(), false, false, 128, "UTC"),
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "path", "id", 1,
                        Optional.empty(), Set.of(), false, false, 128, "UTC"),
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "other-path", "id", 0,
                        Optional.empty(), Set.of(), false, false, 128, "UTC"),
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "path", "id", 0,
                        Optional.of(bigint), Set.of(), false, false, 128, "UTC"),
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "path", "id", 0,
                        Optional.of(integer), Set.of(), false, false, 128, "UTC"),
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "path", "id", 0,
                        Optional.empty(), Set.of(), false, false, 256, "UTC"),
                new DeltaLakeScanPlanMemo.Key("u", "db", "table", "path", "id", 0,
                        Optional.empty(), Set.of(), false, false, 128, "Asia/Shanghai"));
        for (DeltaLakeScanPlanMemo.Key item : keys) {
            Assertions.assertFalse(memo.get(item, new ConnectorScanContext(), loader).reused());
        }
        for (DeltaLakeScanPlanMemo.Key item : keys) {
            Assertions.assertTrue(memo.get(item, new ConnectorScanContext(), loader).reused());
        }
        Assertions.assertEquals(keys.size(), loads.get());
    }

    private static DeltaLakeScanPlanMemo.Key key(String table) {
        return new DeltaLakeScanPlanMemo.Key("u", "db", table, "path", "id", 0,
                Optional.empty(), Set.of(), false, false, 128, "UTC");
    }

    private static DeltaLakeScanRange range(String path, int inlineBytes) {
        TDeltaLakeFileDesc descriptor = new TDeltaLakeFileDesc().setSchemaId(123);
        descriptor.setDeletionVector(new TDeltaLakeDeletionVectorDesc().setInlineData(new byte[inlineBytes]));
        return new DeltaLakeScanRange(path, 0, 10, 10, 1, 128, Map.of(), descriptor);
    }

    private static void await(CountDownLatch latch) {
        try {
            Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}
