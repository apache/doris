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

package org.apache.doris.fluss;

import org.apache.fluss.client.table.scanner.batch.BatchScanner;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SafeKvSnapshotAndLogBatchScannerTest {

    @Test
    public void subscribeFailureClosesBothReadersAndStopsSnapshotWorker() throws Exception {
        WorkerBatchScanner snapshot = new WorkerBatchScanner();
        FailingLogScanner log = new FailingLogScanner();
        SafeKvSnapshotAndLogBatchScanner.ScannerFactory factory =
                new SafeKvSnapshotAndLogBatchScanner.ScannerFactory() {
                    @Override
                    public BatchScanner createSnapshotScanner(
                            TableBucket tableBucket, long snapshotId, int[] projectedFields) {
                        return snapshot;
                    }

                    @Override
                    public LogScanner createLogScanner(int[] projectedFields) {
                        return log;
                    }
                };

        try {
            Assertions.assertTrue(snapshot.awaitStarted());
            Assertions.assertThrows(IllegalStateException.class, () ->
                    SafeKvSnapshotAndLogBatchScanner.acquireScanners(
                            factory, new TableBucket(1L, 0), 7L, 10L, 20L, new int[] {0}));

            Assertions.assertTrue(snapshot.closed.get(), "snapshot reader was leaked");
            Assertions.assertTrue(snapshot.awaitStopped(), "snapshot worker was leaked");
            Assertions.assertFalse(
                    snapshot.worker.isAlive(), "snapshot worker is still alive after failure");
            Assertions.assertTrue(log.closed.get(), "partially initialized log reader was leaked");
        } finally {
            snapshot.close();
            log.close();
        }
    }

    private static final class WorkerBatchScanner implements BatchScanner {
        private final AtomicBoolean closed = new AtomicBoolean();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch stopped = new CountDownLatch(1);
        private final Thread worker;

        private WorkerBatchScanner() {
            worker = new Thread(() -> {
                started.countDown();
                try {
                    while (!closed.get()) {
                        Thread.sleep(1_000L);
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                } finally {
                    stopped.countDown();
                }
            }, "fake-fluss-snapshot-worker");
            worker.start();
        }

        private boolean awaitStarted() throws InterruptedException {
            return started.await(5, TimeUnit.SECONDS);
        }

        private boolean awaitStopped() throws InterruptedException {
            return stopped.await(5, TimeUnit.SECONDS);
        }

        @Override
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) {
            return null;
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                worker.interrupt();
            }
            try {
                worker.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while stopping test worker", e);
            }
        }
    }

    private static final class FailingLogScanner implements LogScanner {
        private final AtomicBoolean closed = new AtomicBoolean();

        @Override
        public ScanRecords poll(Duration timeout) {
            return ScanRecords.EMPTY;
        }

        @Override
        public void subscribe(int bucket, long offset) {
            throw new IllegalStateException("injected subscribe failure");
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            throw new IllegalStateException("injected subscribe failure");
        }

        @Override
        public void unsubscribe(long partitionId, int bucket) {
        }

        @Override
        public void unsubscribe(int bucket) {
        }

        @Override
        public void wakeup() {
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
