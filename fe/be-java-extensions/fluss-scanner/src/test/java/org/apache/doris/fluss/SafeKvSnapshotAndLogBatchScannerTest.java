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
import java.util.concurrent.atomic.AtomicInteger;

public class SafeKvSnapshotAndLogBatchScannerTest {

    @Test
    public void subscribeFailureNeverStartsTheAsynchronousSnapshotReader() {
        AtomicBoolean snapshotCreated = new AtomicBoolean();
        FailingLogScanner log = new FailingLogScanner();
        SafeKvSnapshotAndLogBatchScanner.ScannerFactory factory =
                new SafeKvSnapshotAndLogBatchScanner.ScannerFactory() {
                    @Override
                    public BatchScanner createSnapshotScanner(
                            TableBucket tableBucket, long snapshotId, int[] projectedFields) {
                        snapshotCreated.set(true);
                        throw new AssertionError(
                                "snapshot acquisition must follow successful log subscription");
                    }

                    @Override
                    public LogScanner createLogScanner(int[] projectedFields) {
                        return log;
                    }
                };

        Assertions.assertThrows(IllegalStateException.class, () ->
                SafeKvSnapshotAndLogBatchScanner.acquireScanners(
                        factory, new TableBucket(1L, 0), 7L, 10L, 20L, new int[] {0}));

        Assertions.assertFalse(snapshotCreated.get(),
                "a later subscription failure must have no asynchronous snapshot reader to cancel");
        Assertions.assertTrue(log.closed.get(), "partially initialized log reader was leaked");
    }

    @Test
    public void snapshotCreationFailureClosesTheAlreadySubscribedLogReader() {
        RecordingLogScanner log = new RecordingLogScanner();
        SafeKvSnapshotAndLogBatchScanner.ScannerFactory factory =
                new SafeKvSnapshotAndLogBatchScanner.ScannerFactory() {
                    @Override
                    public BatchScanner createSnapshotScanner(
                            TableBucket tableBucket, long snapshotId, int[] projectedFields) {
                        throw new IllegalStateException("injected snapshot creation failure");
                    }

                    @Override
                    public LogScanner createLogScanner(int[] projectedFields) {
                        return log;
                    }
                };

        Assertions.assertThrows(IllegalStateException.class, () ->
                SafeKvSnapshotAndLogBatchScanner.acquireScanners(
                        factory, new TableBucket(1L, 0), 7L, 10L, 20L, new int[] {0}));

        Assertions.assertTrue(log.subscribed.get(), "log subscription must precede snapshot creation");
        Assertions.assertTrue(log.closed.get(), "subscribed log reader was leaked");
    }

    @Test
    public void successfulOpenThenEarlyCloseWaitsForLateSnapshotPublication() throws Exception {
        LatePublishingSnapshotScanner snapshot = new LatePublishingSnapshotScanner();
        SafeKvSnapshotAndLogBatchScanner.ScannerFactory factory =
                new SafeKvSnapshotAndLogBatchScanner.ScannerFactory() {
                    @Override
                    public BatchScanner createSnapshotScanner(
                            TableBucket tableBucket, long snapshotId, int[] projectedFields) {
                        return snapshot;
                    }

                    @Override
                    public LogScanner createLogScanner(int[] projectedFields) {
                        throw new AssertionError("the staged log range is empty");
                    }
                };

        // Acquisition has returned successfully, matching a Java scanner that BE can close after
        // prepare_split but before its first getNextBatch call.
        SafeKvSnapshotAndLogBatchScanner.ScannerResources resources =
                SafeKvSnapshotAndLogBatchScanner.acquireScanners(
                        factory, new TableBucket(1L, 0), 7L, 20L, 20L, new int[] {0});
        Assertions.assertNotNull(resources.snapshotScanner);
        resources.snapshotScanner.close();

        Assertions.assertTrue(snapshot.pollEntered.await(5, TimeUnit.SECONDS),
                "early close did not start a publication waiter");
        Assertions.assertEquals(0, snapshot.closeCalls.get(),
                "closing the SDK scanner before publication consumes its only effective close");

        snapshot.publishNativeReader();
        Assertions.assertTrue(snapshot.nativeReaderClosed.await(5, TimeUnit.SECONDS),
                "the reader published after cancellation was not closed");
        Assertions.assertEquals(1, snapshot.closeCalls.get(),
                "the SDK scanner must be closed exactly once, after publication");
    }

    private static class RecordingLogScanner implements LogScanner {
        final AtomicBoolean closed = new AtomicBoolean();
        final AtomicBoolean subscribed = new AtomicBoolean();

        @Override
        public ScanRecords poll(Duration timeout) {
            return ScanRecords.EMPTY;
        }

        @Override
        public void subscribe(int bucket, long offset) {
            subscribed.set(true);
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            subscribed.set(true);
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

    private static final class FailingLogScanner extends RecordingLogScanner {
        @Override
        public void subscribe(int bucket, long offset) {
            super.subscribe(bucket, offset);
            throw new IllegalStateException("injected subscribe failure");
        }

        @Override
        public void subscribe(long partitionId, int bucket, long offset) {
            super.subscribe(partitionId, bucket, offset);
            throw new IllegalStateException("injected subscribe failure");
        }
    }

    /** Models Fluss 1.0's reader becoming closeable only after its asynchronous publication. */
    private static final class LatePublishingSnapshotScanner implements BatchScanner {
        private final CountDownLatch pollEntered = new CountDownLatch(1);
        private final CountDownLatch published = new CountDownLatch(1);
        private final CountDownLatch nativeReaderClosed = new CountDownLatch(1);
        private final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public CloseableIterator<InternalRow> pollBatch(Duration timeout) throws IOException {
            pollEntered.countDown();
            try {
                if (!published.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    return CloseableIterator.emptyIterator();
                }
                // A ready, empty snapshot is the SDK's null return. The native reader was still
                // allocated and must be closed even though it contains no rows.
                return null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
            if (published.getCount() == 0) {
                nativeReaderClosed.countDown();
            }
        }

        private void publishNativeReader() {
            published.countDown();
        }
    }
}
