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

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheKey;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HiveFileStatus;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;
import org.apache.doris.fs.FileSystemIOException;

import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/** Bounded, optional cache retention while the consumer pulls files from a partition. */
final class HiveFileListingCache {
    private static final long RESERVATION_CHUNK_BYTES = 16L * 1024;
    private final MetaCacheEntry<FileCacheKey, FileCacheValue> files;
    private final MetaCacheEntry<FileCacheKey, Long> sizes;
    private final long entryMaxBytes;
    private final long pendingMaxBytes;
    private final AtomicLong pendingBytes = new AtomicLong();

    interface Opener {
        HiveFileIterator open() throws FileSystemIOException;
    }

    HiveFileListingCache(MetaCacheEntry<FileCacheKey, FileCacheValue> files,
            MetaCacheEntry<FileCacheKey, Long> sizes, long entryMaxBytes, long pendingMaxBytes) {
        this.files = files;
        this.sizes = sizes;
        this.entryMaxBytes = entryMaxBytes;
        this.pendingMaxBytes = pendingMaxBytes;
    }

    boolean owns(MetaCacheEntry<FileCacheKey, FileCacheValue> entry) {
        return files == entry;
    }

    HiveFileIterator open(FileCacheKey key, Opener opener, BooleanSupplier cancelled) {
        checkCancelled(cancelled);
        FileCacheValue hit = files.getIfPresent(key);
        return hit == null ? new CachingIterator(key, opener, cancelled, true) : HiveFileIterator.of(hit);
    }

    FileCacheValue refresh(FileCacheKey key, Opener opener) throws FileSystemIOException {
        // The managed refresh already records its outcome in MetaCacheEntry.loadAndTrack.
        try (CachingIterator iterator = new CachingIterator(key, opener, () -> false, false)) {
            while (iterator.hasNext()) {
                iterator.next();
            }
            // The inner publication advances the mutation epoch, so the outer refresh cannot
            // republish its old snapshot. Never retain a rejected completed list in a closed cursor.
            return files.peekIfPresent(key);
        }
    }

    long pendingBytes() {
        return pendingBytes.get();
    }

    private static void checkCancelled(BooleanSupplier cancelled) {
        if (cancelled.getAsBoolean()) {
            throw new CancellationException("Hive file listing cancelled");
        }
    }

    private final class CachingIterator implements HiveFileIterator {
        private final Opener opener;
        private final BooleanSupplier cancelled;
        private final boolean trackLoad;
        private final MetaCacheEntry<FileCacheKey, FileCacheValue>.StreamingLoad load;
        private final MetaCacheEntry<FileCacheKey, Long>.StreamingLoad summaryLoad;
        private HiveFileIterator delegate;
        private ArrayList<HiveFileStatus> candidate;
        private long candidateBytes;
        private long reservedBytes;
        private long totalFileBytes;
        private int capacity;
        private boolean closed;
        private boolean delegateClosed;

        CachingIterator(FileCacheKey key, Opener opener, BooleanSupplier cancelled, boolean trackLoad) {
            this.opener = opener;
            this.cancelled = cancelled;
            this.trackLoad = trackLoad;
            load = files.beginStreamingLoad(key);
            summaryLoad = sizes.beginStreamingLoad(key);
            candidateBytes = HiveFileListingSizeEstimator.baseBytes(key);
            if (files.isEffectivelyEnabled() && entryMaxBytes > 0 && reserveCandidate(candidateBytes)) {
                candidate = new ArrayList<>();
            }
        }

        @Override
        public FileCacheValue metadata() {
            return delegate.metadata();
        }

        @Override
        public boolean hasNext() throws FileSystemIOException {
            if (closed) {
                return false;
            }
            try {
                checkCancelled(cancelled);
                if (delegate == null) {
                    delegate = opener.open();
                }
                if (delegate.hasNext()) {
                    return true;
                }
                // EOF alone is insufficient: failed close or cancellation must not publish a prefix.
                delegateClosed = true;
                delegate.close();
                checkCancelled(cancelled);
                if (candidate != null) {
                    FileCacheValue value = new FileCacheValue(candidate);
                    value.setSplittable(metadata().isSplittable());
                    value.setPartitionValues(metadata().getPartitionValues());
                    value.setCacheGeneration(metadata().getCacheGeneration());
                    value.setEstimatedCacheBytes(candidateBytes);
                    load.publish(value);
                    candidate = null;
                } else {
                    load.invalidateIfCurrent();
                }
                if (files.isEffectivelyEnabled()) {
                    summaryLoad.publish(totalFileBytes);
                }
                close();
                if (trackLoad) {
                    load.recordSuccess();
                }
                return false;
            } catch (FileSystemIOException | RuntimeException | Error e) {
                recordFailure(e);
                try {
                    close();
                } catch (Throwable closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }

        @Override
        public HiveFileStatus next() throws FileSystemIOException {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            try {
                HiveFileStatus file = delegate.next();
                totalFileBytes = MetaCacheWeightUtils.saturatedAdd(totalFileBytes, file.getLength());
                collect(file);
                return file;
            } catch (FileSystemIOException | RuntimeException | Error e) {
                recordFailure(e);
                try {
                    close();
                } catch (Throwable closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }

        private void collect(HiveFileStatus file) throws FileSystemIOException {
            if (candidate == null) {
                return;
            }
            long nextBytes = MetaCacheWeightUtils.saturatedAdd(candidateBytes,
                    HiveFileListingSizeEstimator.fileBytes(file));
            int nextCapacity = capacity;
            long peakBytes = nextBytes;
            if (candidate.size() == capacity) {
                nextCapacity = capacity == 0 ? 16 : Math.multiplyExact(capacity, 2);
                long newArray = MetaCacheWeightUtils.estimatedObjectArrayBytes(nextCapacity);
                // Reserve the temporary old+new array peak before growing; charge final spare capacity too.
                peakBytes = MetaCacheWeightUtils.saturatedAdd(nextBytes, newArray);
                nextBytes = MetaCacheWeightUtils.saturatedAdd(nextBytes,
                        8L * (nextCapacity - capacity));
            }
            if (!reserveCandidate(Math.max(nextBytes, peakBytes))) {
                discardCandidate();
                return;
            }
            if (nextCapacity != capacity) {
                candidate.ensureCapacity(nextCapacity);
                capacity = nextCapacity;
            }
            candidate.add(file);
            candidateBytes = nextBytes;
        }

        private void recordFailure(Throwable error) {
            if (trackLoad) {
                load.recordFailure(error);
            }
        }

        private boolean reserveCandidate(long required) {
            if (required > entryMaxBytes) {
                return false;
            }
            if (required <= reservedBytes) {
                return true;
            }
            long extra = Math.max(required - reservedBytes,
                    Math.min(RESERVATION_CHUNK_BYTES, entryMaxBytes - reservedBytes));
            while (true) {
                long current = pendingBytes.get();
                long available = pendingMaxBytes - current;
                if (available < required - reservedBytes) {
                    return false;
                }
                long granted = Math.min(extra, available);
                if (pendingBytes.compareAndSet(current, current + granted)) {
                    reservedBytes += granted;
                    return true;
                }
            }
        }

        private void discardCandidate() {
            if (candidate != null) {
                candidate.clear();
                candidate = null;
            }
            pendingBytes.addAndGet(-reservedBytes);
            reservedBytes = 0;
        }

        @Override
        public void close() throws FileSystemIOException {
            if (closed) {
                return;
            }
            closed = true;
            discardCandidate();
            load.close();
            summaryLoad.close();
            if (delegate != null && !delegateClosed) {
                delegateClosed = true;
                try {
                    delegate.close();
                } catch (FileSystemIOException | RuntimeException | Error e) {
                    recordFailure(e);
                    throw e;
                }
            }
        }
    }
}
