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

package org.apache.doris.connector.mongodb;

import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import org.bson.BsonTimestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

class MongoSnapshotTest {
    @Test
    void independentScansInOneStatementSelectTheTimestampOnlyOnce() {
        var state = new MongoSnapshot.State(30000);
        var source = new MongoSnapshot.Source("replica_set", "000000000000000000000001", 1, 25);
        var count = new AtomicInteger();
        Supplier<BsonTimestamp> select = () -> new BsonTimestamp(1700000000, count.incrementAndGet());
        var first = Assertions.assertDoesNotThrow(() -> state.pin(() -> { }, source, select));
        var second = Assertions.assertDoesNotThrow(() -> state.pin(() -> { }, source, select));
        Assertions.assertEquals(first, second);
        Assertions.assertEquals(1, count.get());
    }

    private static MongoSnapshot.Source source(String kind, String id, long version) {
        return new MongoSnapshot.Source(kind, id, version, 25);
    }

    @Test
    void distinctStatementsNeverReuseTheTimestamp() {
        var first = new MongoSnapshot.State(30000);
        var second = new MongoSnapshot.State(30000);
        var source = source("replica_set", "000000000000000000000001", 1);
        var count = new AtomicInteger();
        Supplier<BsonTimestamp> select = () -> new BsonTimestamp(1700000000, count.incrementAndGet());
        Assertions.assertNotEquals(first.pin(() -> { }, source, select), second.pin(() -> { }, source, select));
        Assertions.assertEquals(2, count.get());
    }

    @Test
    void sourceOrTopologyChangesCannotSelectAnotherSnapshot() {
        var original = source("replica_set", "000000000000000000000001", 1);
        for (var changed : List.of(source("replica_set", "000000000000000000000002", 1),
                source("replica_set", "000000000000000000000001", 2),
                source("mongos", "000000000000000000000001", 0))) {
            var state = new MongoSnapshot.State(30000);
            state.pin(() -> { }, original, () -> new BsonTimestamp(1700000000, 1));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> state.pin(() -> { }, changed, () -> {
                        throw new AssertionError("Source change must not reselect T");
                    }));
        }
    }

    @Test
    void routersWithTheSameClusterIdentityShareThePin() {
        var state = new MongoSnapshot.State(30000);
        var router = source("mongos", "000000000000000000000001", 0);
        var timestamp = state.pin(() -> { }, router, () -> new BsonTimestamp(1700000000, 1));
        Assertions.assertEquals(timestamp, state.pin(() -> { }, router,
                () -> {
                    throw new AssertionError("Another router must reuse the selected T");
                }));
    }

    @Test
    void mixedReadContractsAreRejectedInEitherPlanningOrder() {
        var source = source("replica_set", "000000000000000000000001", 1);
        var ordinaryFirst = new MongoSnapshot.State(30000);
        ordinaryFirst.ordinaryRead();
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ordinaryFirst.pin(() -> { }, source, () -> new BsonTimestamp(1700000000, 1)));
        var snapshotFirst = new MongoSnapshot.State(30000);
        snapshotFirst.pin(() -> { }, source, () -> new BsonTimestamp(1700000000, 1));
        Assertions.assertThrows(IllegalArgumentException.class, snapshotFirst::ordinaryRead);
    }

    @Test
    void failedSelectionIsStickyForTheStatement() {
        var state = new MongoSnapshot.State(30000);
        var source = source("replica_set", "000000000000000000000001", 1);
        var failure = new IllegalStateException("source snapshot unavailable");
        var count = new AtomicInteger();
        Supplier<BsonTimestamp> select = () -> {
            count.incrementAndGet();
            throw failure;
        };
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> state.pin(() -> { }, source, select)));
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> state.pin(() -> { }, source, select)));
        Assertions.assertEquals(1, count.get());
    }

    @Test
    void missingOrZeroServerTimestampCannotBecomeAReadOfLatest() {
        for (BsonTimestamp value : new BsonTimestamp[] {null, new BsonTimestamp(0, 0)}) {
            var state = new MongoSnapshot.State(30000);
            Assertions.assertThrows(RuntimeException.class,
                    () -> state.pin(() -> { }, source("replica_set", "000000000000000000000001", 1), () -> value));
        }
    }

    @Test
    void onlyQualifiedSourceVersionsAndStableIdentitiesAreAccepted() {
        for (int version : new int[] {21, 25}) {
            Assertions.assertDoesNotThrow(() -> new MongoSnapshot.Source(
                    "replica_set", "000000000000000000000001", 1, version));
        }
        for (int version : new int[] {0, 13, 17, 26}) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> new MongoSnapshot.Source(
                    "replica_set", "000000000000000000000001", 1, version));
        }
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> source("standalone", "000000000000000000000001", 1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> source("replica_set", "missing", 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> source("replica_set", "000000000000000000000001", 0));
    }

    @Test
    void concurrentScanPlannersProbeOnceWithoutHoldingTheStateMonitor() throws Exception {
        var state = new MongoSnapshot.State(30000);
        var source = source("replica_set", "000000000000000000000001", 1);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var count = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> state.pin(() -> { }, source, () -> {
                count.incrementAndGet();
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Fixture selection was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return new BsonTimestamp(1700000000, 1);
            }));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            // A source check can acquire the monitor even while selection is blocked in I/O.
            var mismatch = executor.submit(() -> Assertions.assertThrows(IllegalArgumentException.class,
                    () -> state.pin(() -> { }, source("replica_set", "000000000000000000000002", 1),
                            () -> new BsonTimestamp(1700000000, 2))));
            mismatch.get(5, TimeUnit.SECONDS);
            var second = executor.submit(() -> state.pin(() -> { }, source, () -> {
                count.incrementAndGet();
                return new BsonTimestamp(1700000000, 2);
            }));
            release.countDown();
            Assertions.assertEquals(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
            Assertions.assertEquals(1, count.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void cancelledWaiterLeavesTheOtherScansSharedSelectionRunning() throws Exception {
        var state = new MongoSnapshot.State(30000);
        var source = source("replica_set", "000000000000000000000001", 1);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var waiting = new CountDownLatch(2);
        var context = new ConnectorScanContext();
        var executor = Executors.newFixedThreadPool(2);
        try {
            var owner = executor.submit(() -> state.pin(() -> { }, source, () -> {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("Fixture selection was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
                return new BsonTimestamp(1700000000, 1);
            }));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            var waiter = executor.submit(() -> state.pin(() -> {
                context.checkCancelled();
                waiting.countDown();
            }, source, () -> {
                throw new AssertionError("A waiting scan must not select another timestamp");
            }));
            Assertions.assertTrue(waiting.await(5, TimeUnit.SECONDS));
            context.cancel();
            var failure = Assertions.assertThrows(ExecutionException.class, () -> waiter.get(2, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(CancellationException.class, failure.getCause());
            Assertions.assertFalse(owner.isDone());
            release.countDown();
            var selected = owner.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(selected, state.pin(() -> { }, source, () -> {
                throw new AssertionError("Cancelling a waiter must not invalidate the shared pin");
            }));
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void deadlineIsCheckedAfterTimestampSelection() {
        var state = new MongoSnapshot.State(1);
        Assertions.assertThrows(RuntimeException.class, () -> state.pin(() -> { },
                source("replica_set", "000000000000000000000001", 1), () -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                    return new BsonTimestamp(1700000000, 1);
                }));
    }

    @Test
    void interruptedPlanningCannotOpenANewSnapshotProbe() {
        var state = new MongoSnapshot.State(30000);
        Thread.currentThread().interrupt();
        try {
            Assertions.assertThrows(CancellationException.class, () -> state.pin(() -> { },
                    source("replica_set", "000000000000000000000001", 1), () -> {
                        throw new AssertionError("Cancelled planning must not open a source probe");
                    }));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void assertionErrorCompletesTheSharedSnapshotForEveryWaiter() throws Exception {
        assertFatalSnapshotFailure(new AssertionError("controlled snapshot failure"));
    }

    @Test
    void outOfMemoryErrorCompletesTheSharedSnapshotForEveryWaiter() throws Exception {
        assertFatalSnapshotFailure(new OutOfMemoryError("controlled snapshot failure"));
    }

    private static void assertFatalSnapshotFailure(Error fatal) throws Exception {
        var state = new MongoSnapshot.State(30000);
        var identity = source("replica_set", "000000000000000000000001", 1);
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var waiting = new CountDownLatch(1);
        var loads = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(2);
        try {
            var owner = executor.submit(() -> state.pin(() -> { }, identity, () -> {
                loads.incrementAndGet();
                entered.countDown();
                try {
                    Assertions.assertTrue(release.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException();
                }
                throw fatal;
            }));
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            var waiter = executor.submit(() -> {
                waiting.countDown();
                return state.pin(() -> { }, identity, () -> {
                    loads.incrementAndGet();
                    return new BsonTimestamp(1700000000, 2);
                });
            });
            Assertions.assertTrue(waiting.await(5, TimeUnit.SECONDS));
            release.countDown();
            Assertions.assertSame(fatal, Assertions.assertThrows(ExecutionException.class,
                    () -> owner.get(10, TimeUnit.SECONDS)).getCause());
            Assertions.assertSame(fatal, Assertions.assertThrows(ExecutionException.class,
                    () -> waiter.get(10, TimeUnit.SECONDS)).getCause());
            Assertions.assertSame(fatal, Assertions.assertThrows(Error.class,
                    () -> state.pin(() -> { }, identity, () -> new BsonTimestamp(1700000000, 3))));
            Assertions.assertEquals(1, loads.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

}
