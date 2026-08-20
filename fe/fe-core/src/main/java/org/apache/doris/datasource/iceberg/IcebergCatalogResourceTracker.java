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

package org.apache.doris.datasource.iceberg;

import java.util.concurrent.atomic.AtomicBoolean;

/** Keeps one catalog generation alive while tables loaded through it still have owners or borrowers. */
final class IcebergCatalogResourceTracker {
    private Generation current = new Generation();

    synchronized LoadGuard beginLoad() {
        current.retain();
        return new LoadGuard(current);
    }

    synchronized void retireCurrent(Runnable cleanup) {
        Generation retired = current;
        current = new Generation();
        retired.retire(cleanup);
    }

    static final class LoadGuard implements AutoCloseable {
        private final Generation generation;
        private final AtomicBoolean transferred = new AtomicBoolean();

        private LoadGuard(Generation generation) {
            this.generation = generation;
        }

        ResourceLease promote() {
            if (!transferred.compareAndSet(false, true)) {
                throw new IllegalStateException("Iceberg catalog load guard was already completed");
            }
            return new ResourceLease(generation);
        }

        @Override
        public void close() {
            if (transferred.compareAndSet(false, true)) {
                generation.release();
            }
        }
    }

    static final class ResourceLease implements AutoCloseable {
        private final Generation generation;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ResourceLease(Generation generation) {
            this.generation = generation;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                generation.release();
            }
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
                cleanup.run();
            }
        }
    }
}
