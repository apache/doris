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

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** A frozen table's statement-owned manifest reads; only active consumers retain private results. */
final class IcebergManifestReader implements AutoCloseable {
    private final IcebergManifestCache cache;
    private final Table table;
    private final String queryId;
    private final AutoCloseable owner;
    private final boolean encryptedTable;
    private final ConcurrentHashMap<ReadKey, CompletableFuture<ManifestCacheValue>> inFlight =
            new ConcurrentHashMap<>();
    private volatile boolean closed;
    private int active;
    private boolean ownerReleased;

    IcebergManifestReader(IcebergManifestCache cache, Table table, String queryId, AutoCloseable owner) {
        this.cache = cache;
        this.table = table;
        this.queryId = queryId;
        this.owner = owner;
        this.encryptedTable = IcebergEncryptionRead.isEncrypted(table);
    }

    ManifestCacheValue read(ManifestFile manifest) {
        return read(manifest, queryId);
    }

    ManifestCacheValue read(ManifestFile manifest, String statsQueryId) {
        try {
            return readFuture(manifest, statsQueryId).toCompletableFuture().join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException) {
                throw (RuntimeException) failure.getCause();
            }
            if (failure.getCause() instanceof Error) {
                throw (Error) failure.getCause();
            }
            throw failure;
        }
    }

    /** The elected caller does I/O; other callers only attach cancellation-isolated observers. */
    CompletionStage<ManifestCacheValue> readFuture(ManifestFile manifest) {
        return readFuture(manifest, queryId);
    }

    CompletionStage<ManifestCacheValue> readFuture(ManifestFile manifest, String statsQueryId) {
        try (ReadLease ignored = retain()) {
            boolean privateRead = encryptedTable || manifest.keyMetadata() != null;
            // A statement reader is shared by scans with and without optional EXPLAIN accounting.
            cache.recordStatementAccess(manifest, statsQueryId, !privateRead);
            ReadKey key = new ReadKey(manifest);
            CompletableFuture<ManifestCacheValue> elected = new CompletableFuture<>();
            CompletableFuture<ManifestCacheValue> current = inFlight.putIfAbsent(key, elected);
            if (current == null) {
                current = elected;
                ManifestCacheValue value;
                try {
                    value = cache.readForStatement(manifest, table, this, privateRead);
                } catch (RuntimeException | Error failure) {
                    inFlight.remove(key, elected);
                    elected.completeExceptionally(failure);
                    if (failure instanceof Error) {
                        throw (Error) failure;
                    }
                    return elected.thenApply(result -> result);
                }
                // Remove before notifying consumers: callbacks may block or start the next manifest.
                // Completed key-bearing values must never accumulate in a statement-wide memo.
                inFlight.remove(key, elected);
                elected.complete(value);
            }
            return current.thenApply(result -> result);
        }
    }

    /** No I/O or lease cleanup: prefetch invokes this probe while holding its pending-queue lock. */
    ManifestCacheValue cachedOnly(ManifestFile manifest) {
        return cachedOnly(manifest, queryId);
    }

    ManifestCacheValue cachedOnly(ManifestFile manifest, String statsQueryId) {
        checkOpen();
        return encryptedTable || manifest.keyMetadata() != null
                ? null : cache.getCachedManifestValue(manifest, statsQueryId);
    }

    synchronized ReadLease retain() {
        checkOpen();
        active++;
        return new ReadLease();
    }

    private void checkOpen() {
        if (closed) {
            throw new CancellationException("Iceberg manifest reader is closed");
        }
    }

    private void release() {
        boolean releaseOwner;
        synchronized (this) {
            active--;
            releaseOwner = claimOwnerRelease();
        }
        if (releaseOwner) {
            closeOwner();
        }
    }

    private boolean claimOwnerRelease() {
        if (closed && active == 0 && !ownerReleased) {
            ownerReleased = true;
            return true;
        }
        return false;
    }

    private void closeOwner() {
        try {
            owner.close();
        } catch (Exception failure) {
            throw new RuntimeException("Failed to close manifest reader owner", failure);
        }
    }

    int inFlightCount() {
        return inFlight.size();
    }

    @Override
    public void close() {
        boolean releaseOwner;
        synchronized (this) {
            closed = true;
            inFlight.clear();
            releaseOwner = claimOwnerRelease();
        }
        // Future cancellation does not mean the elected callable has exited. Its leases release last.
        if (releaseOwner) {
            closeOwner();
        }
    }

    final class ReadLease implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                release();
            }
        }
    }

    /** A different wrapped key must not borrow an authorized read of the same manifest path. */
    private static final class ReadKey {
        private final IcebergManifestEntryKey identity;
        private final long length;
        private final int specId;
        private final ByteBuffer keyMetadata;

        private ReadKey(ManifestFile manifest) {
            identity = IcebergManifestEntryKey.of(manifest);
            length = manifest.length();
            specId = manifest.partitionSpecId();
            ByteBuffer metadata = manifest.keyMetadata();
            if (metadata == null) {
                keyMetadata = null;
            } else {
                ByteBuffer copy = ByteBuffer.allocate(metadata.remaining());
                copy.put(metadata.duplicate()).flip();
                keyMetadata = copy.asReadOnlyBuffer();
            }
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ReadKey)) {
                return false;
            }
            ReadKey that = (ReadKey) other;
            return length == that.length && specId == that.specId && identity.equals(that.identity)
                    && Objects.equals(keyMetadata, that.keyMetadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identity, length, specId, keyMetadata);
        }
    }
}
