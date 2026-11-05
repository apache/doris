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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * One scan's serial HMS producer. Readers claim small directory batches from one bounded result list.
 * Claiming its last slice permits one next metadata batch, overlapping HMS with directory work without
 * waiting for children or accumulating an unbounded queue. Both stages use the existing schedulers.
 */
final class HivePartitionBatchLoader {
    private final ConnectorTaskScheduler scheduler;
    private final ConnectorScanContext scanContext;
    private final List<String> names;
    private final int maxBatchSize;
    private final Function<List<String>, Optional<List<HmsPartitionInfo>>> cached;
    private final Function<List<String>, List<HmsPartitionInfo>> loader;
    // Serializes synchronous cache hits and inline/offline executors without recursive submission.
    private final AtomicInteger pumping = new AtomicInteger();
    // All fields below are guarded by this. I/O, scheduler calls and future completion run outside it.
    private int nextName;
    private List<HmsPartitionInfo> ready = Collections.emptyList();
    private int nextReady;
    private CompletableFuture<Void> available = new CompletableFuture<>();
    private LoadTask current;
    private Throwable failure;
    private int readers;

    HivePartitionBatchLoader(ConnectorTaskScheduler scheduler, ConnectorScanContext scanContext,
            List<String> names, int maxBatchSize, int readers,
            Function<List<String>, Optional<List<HmsPartitionInfo>>> cached,
            Function<List<String>, List<HmsPartitionInfo>> loader) {
        this.scheduler = scheduler;
        this.scanContext = scanContext;
        this.names = names;
        this.maxBatchSize = maxBatchSize;
        this.readers = readers;
        this.cached = cached;
        this.loader = loader;
        scanContext.whenCancelled(() -> fail(new CancellationException("Split generation stopped")));
    }

    CompletableFuture<Void> isBlocked() {
        pump();
        synchronized (this) {
            return failure != null || (ready.isEmpty() && (current != null || nextName < names.size()))
                    ? available : null;
        }
    }

    synchronized boolean isFinished() {
        return failure == null && ready.isEmpty() && current == null && nextName == names.size();
    }

    List<HmsPartitionInfo> take(int size) {
        List<HmsPartitionInfo> result;
        synchronized (this) {
            if (failure != null) {
                throw new CompletionException(failure);
            }
            if (ready.isEmpty()) {
                return Collections.emptyList();
            }
            int end = Math.min(nextReady + size, ready.size());
            // Own a small reference array: a slow reader must not retain an entire completed HMS batch.
            result = new ArrayList<>(ready.subList(nextReady, end));
            nextReady = end;
            if (nextReady == ready.size()) {
                ready = Collections.emptyList();
                nextReady = 0;
                available = new CompletableFuture<>();
            }
        }
        pump();
        return result;
    }

    void readerClosed() {
        boolean last;
        synchronized (this) {
            last = --readers == 0;
        }
        if (last) {
            fail(new CancellationException("All partition readers closed"));
        }
    }

    private void pump() {
        if (pumping.getAndIncrement() != 0) {
            return;
        }
        int missed = 1;
        do {
            LoadTask task = null;
            synchronized (this) {
                if (failure == null && current == null && ready.isEmpty() && nextName < names.size()) {
                    int size = nextName == 0 ? Math.min(16, maxBatchSize) : maxBatchSize;
                    int end = nextName + Math.min(size, names.size() - nextName);
                    task = new LoadTask(names.subList(nextName, end));
                    nextName = end;
                    current = task;
                }
            }
            if (task != null) {
                try {
                    scanContext.checkCancelled();
                    Optional<List<HmsPartitionInfo>> hit = cached.apply(task.batch);
                    if (hit.isPresent()) {
                        publish(task, hit.get());
                    } else {
                        task.admit();
                    }
                } catch (Throwable e) {
                    fail(e);
                }
            }
            missed = pumping.addAndGet(-missed);
        } while (missed != 0);
    }

    private void publish(LoadTask task, List<HmsPartitionInfo> partitions) {
        CompletableFuture<Void> signal = null;
        boolean empty;
        synchronized (this) {
            if (failure != null) {
                return;
            }
            current = null;
            ready = partitions;
            empty = ready.isEmpty();
            if (!empty || nextName == names.size()) {
                signal = available;
            }
        }
        task.complete(null);
        if (signal != null) {
            signal.complete(null);
        }
        if (empty) {
            pump();
        }
    }

    private void fail(Throwable cause) {
        CompletableFuture<Void> signal;
        LoadTask task;
        synchronized (this) {
            if (failure != null) {
                return;
            }
            failure = cause;
            ready = Collections.emptyList();
            if (available.isDone()) {
                available = new CompletableFuture<>();
            }
            signal = available;
            task = current;
            current = null;
        }
        signal.completeExceptionally(cause);
        if (task != null) {
            task.completeExceptionally(cause);
        }
    }

    private final class LoadTask extends CompletableFuture<Void> implements Runnable {
        private final List<String> batch;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicInteger admitting = new AtomicInteger();
        private boolean admitted;
        private final Runnable onSpace = this::admit;

        private LoadTask(List<String> batch) {
            this.batch = batch;
            whenComplete((unused, error) -> {
                scheduler.removeSpaceListener(onSpace);
                scheduler.remove(this);
            });
        }

        private void admit() {
            // onSpace may run before tryExecute returns. Coalesce concurrent and inline retries,
            // and never admit this task twice after one retry has already succeeded.
            if (admitting.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            do {
                if (!isDone() && !admitted) {
                    try {
                        admitted = scheduler.tryExecute(this, HivePartitionBatchLoader.this::fail, onSpace);
                        if (admitted) {
                            scheduler.removeSpaceListener(onSpace);
                        }
                    } catch (RuntimeException e) {
                        fail(e);
                    }
                }
                if (isDone()) {
                    scheduler.removeSpaceListener(onSpace);
                    scheduler.remove(this);
                }
                missed = admitting.addAndGet(-missed);
            } while (missed != 0);
        }

        @Override
        public void run() {
            if (isDone() || !started.compareAndSet(false, true)) {
                return;
            }
            ClassLoader previous = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(HivePartitionBatchLoader.class.getClassLoader());
                scanContext.checkCancelled();
                List<HmsPartitionInfo> partitions = loader.apply(batch);
                scanContext.checkCancelled();
                publish(this, partitions);
            } catch (Throwable e) {
                fail(e);
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }
    }
}
