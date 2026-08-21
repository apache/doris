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

package org.apache.doris.connector.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Keeps catalog generations alive while cached tables loaded through them still have borrowers. */
final class IcebergCatalogResourceTracker {

    private final List<Generation> generations = new ArrayList<>();
    private Generation current = new Generation();
    private int loadsInProgress;
    private boolean closed;

    IcebergCatalogResourceTracker() {
        generations.add(current);
    }

    synchronized LoadGuard beginLoad() {
        if (closed) {
            throw new IllegalStateException("Iceberg catalog resources are already closed");
        }
        loadsInProgress++;
        current.retain();
        return new LoadGuard(this, generations.size() - 1, current);
    }

    /** Loads a resource while retaining every catalog generation crossed by that load. */
    <T> TrackedResource<T> load(Supplier<T> loader) {
        LoadGuard guard = beginLoad();
        try {
            T resource = loader.get();
            return new TrackedResource<>(resource, guard.promote());
        } finally {
            guard.close();
        }
    }

    /** Runs one synchronous catalog operation under the generation active when it starts. */
    <T> T call(Callable<T> operation) throws Exception {
        LoadGuard guard = beginLoad();
        try {
            return operation.call();
        } finally {
            guard.close();
        }
    }

    /** Atomically rotates the resource generation with publication of the corresponding REST delegate. */
    synchronized void rotate(Runnable retiredCleanup, Runnable publishReplacement) {
        if (closed) {
            throw new IllegalStateException("Iceberg catalog resources are already closed");
        }
        Generation retired = current;
        current = new Generation();
        current.retain(loadsInProgress);
        generations.add(current);
        publishReplacement.run();
        retired.retire(retiredCleanup);
    }

    synchronized void close(Runnable currentCleanup) {
        if (closed) {
            return;
        }
        closed = true;
        current.retire(currentCleanup);
    }

    /** Test-visible count of retired cleanup closures still retained by the tracker. */
    synchronized int retainedCleanupCount() {
        int retained = 0;
        for (Generation generation : generations) {
            if (generation.hasCleanup()) {
                retained++;
            }
        }
        return retained;
    }

    private synchronized ResourceLease promote(int firstGeneration) {
        List<Generation> retained = new ArrayList<>();
        retained.add(generations.get(firstGeneration));
        for (int i = firstGeneration + 1; i < generations.size(); i++) {
            // rotate() pre-retains one reference in every new generation for each load already in progress.
            retained.add(generations.get(i));
        }
        completeLoad();
        return new ResourceLease(retained);
    }

    private synchronized void abortLoad(int firstGeneration) {
        for (int i = firstGeneration + 1; i < generations.size(); i++) {
            generations.get(i).release();
        }
        completeLoad();
    }

    private void completeLoad() {
        loadsInProgress--;
        if (loadsInProgress < 0) {
            throw new IllegalStateException("Iceberg catalog load guard completed too many times");
        }
    }

    static final class LoadGuard implements AutoCloseable {
        private final IcebergCatalogResourceTracker tracker;
        private final int firstGeneration;
        private final Generation initialGeneration;
        private final AtomicBoolean transferred = new AtomicBoolean();

        private LoadGuard(IcebergCatalogResourceTracker tracker, int firstGeneration, Generation initialGeneration) {
            this.tracker = tracker;
            this.firstGeneration = firstGeneration;
            this.initialGeneration = initialGeneration;
        }

        ResourceLease promote() {
            if (!transferred.compareAndSet(false, true)) {
                throw new IllegalStateException("Iceberg catalog load guard was already completed");
            }
            return tracker.promote(firstGeneration);
        }

        @Override
        public void close() {
            if (transferred.compareAndSet(false, true)) {
                try {
                    initialGeneration.release();
                } finally {
                    tracker.abortLoad(firstGeneration);
                }
            }
        }
    }

    static final class ResourceLease implements AutoCloseable {
        private final List<Generation> generations;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ResourceLease(List<Generation> generations) {
            this.generations = generations;
        }

        synchronized ResourceLease retain() {
            if (closed.get()) {
                throw new IllegalStateException("Iceberg catalog resource lease is already closed");
            }
            for (Generation generation : generations) {
                generation.retain();
            }
            return new ResourceLease(new ArrayList<>(generations));
        }

        @Override
        public synchronized void close() {
            if (closed.compareAndSet(false, true)) {
                RuntimeException failure = null;
                for (Generation generation : generations) {
                    try {
                        generation.release();
                    } catch (RuntimeException e) {
                        if (failure == null) {
                            failure = e;
                        } else {
                            failure.addSuppressed(e);
                        }
                    }
                }
                if (failure != null) {
                    throw failure;
                }
            }
        }
    }

    static final class TrackedResource<T> implements AutoCloseable {
        private final T resource;
        private final ResourceLease lease;

        private TrackedResource(T resource, ResourceLease lease) {
            this.resource = resource;
            this.lease = lease;
        }

        T resource() {
            return resource;
        }

        ResourceLease retainLease() {
            return lease.retain();
        }

        @Override
        public void close() {
            lease.close();
        }
    }

    private static final class Generation {
        private int references;
        private boolean retired;
        private boolean cleaned;
        private Runnable cleanup;

        private synchronized void retain() {
            if (cleaned) {
                throw new IllegalStateException("Iceberg catalog generation was already cleaned");
            }
            references++;
        }

        private synchronized void retain(int count) {
            if (cleaned) {
                throw new IllegalStateException("Iceberg catalog generation was already cleaned");
            }
            references += count;
        }

        private synchronized void retire(Runnable cleanup) {
            if (retired) {
                return;
            }
            retired = true;
            this.cleanup = cleanup;
            maybeCleanup();
        }

        private synchronized void release() {
            if (references <= 0) {
                throw new IllegalStateException("Iceberg catalog generation released too many times");
            }
            references--;
            maybeCleanup();
        }

        private void maybeCleanup() {
            if (retired && references == 0 && !cleaned) {
                cleaned = true;
                try {
                    cleanup.run();
                } finally {
                    // A REST-recovery cleanup captures the retired delegate and its auth/FileIO graph. The
                    // tracker intentionally keeps Generation markers for in-flight-load indexing, but a cleaned
                    // marker must not retain the heavyweight delegate for the connector's remaining lifetime.
                    cleanup = null;
                }
            }
        }

        private synchronized boolean hasCleanup() {
            return cleanup != null;
        }
    }
}
