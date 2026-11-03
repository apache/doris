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

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.filesystem.FileSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared complete directory listings; cold cursors and their bounded cache candidates belong to each scan.
 * Only successful EOF may publish. Closing, cancellation and failure discard the candidate, while a candidate
 * that exceeds its entry or catalog staging budget is abandoned without stopping file enumeration.
 */
public class HiveFileListingCache {
    static final String ENGINE = "hive";
    static final String ENTRY_FILE = "file";
    static final String ENTRY_FILE_SUMMARY = "file_summary";
    static final String LEGACY_FILE_META_CACHE_TTL_SECOND = "file.meta.cache.ttl-second";
    static final long DEFAULT_TTL_SECOND = 86400L;
    static final long DEFAULT_FILE_CAPACITY = 10000L;
    private static final long RESERVATION_CHUNK_BYTES = 16L * 1024;

    /** List-based seam retained for existing recording fakes; the production implementation overrides open. */
    @FunctionalInterface
    interface DirectoryLister {
        List<HiveFileStatus> list(String location, FileSystem fs);

        default HiveFileIterator open(String location, FileSystem fs, ConnectorScanContext context) {
            context.checkCancelled();
            List<HiveFileStatus> files = list(location, fs);
            context.checkCancelled();
            return HiveFileIterator.of(files.iterator());
        }
    }

    private final CatalogMetaCache owner;
    private final MetaCache<FileListingKey, FileListingValue> cache;
    private final MetaCache<FileListingKey, Long> sizeCache;
    private final DirectoryLister lister;
    private final long entryMaxBytes;
    private final long pendingMaxBytes;
    private final AtomicLong pendingBytes = new AtomicLong();

    public HiveFileListingCache(HiveCatalogProperties properties) {
        this(CatalogMetaCache.unmanaged(), properties);
    }

    public HiveFileListingCache(CatalogMetaCache owner, HiveCatalogProperties properties) {
        this(owner, properties, new DirectoryLister() {
            @Override
            public List<HiveFileStatus> list(String location, FileSystem fs) {
                return listFromFileSystem(location, fs, properties.isRecursiveDirectories());
            }

            @Override
            public HiveFileIterator open(String location, FileSystem fs, ConnectorScanContext context) {
                return new HiveDirectoryIterator(location, fs, properties.isRecursiveDirectories(), context);
            }
        });
    }

    HiveFileListingCache(HiveCatalogProperties properties, DirectoryLister lister) {
        this(CatalogMetaCache.unmanaged(), properties, lister);
    }

    HiveFileListingCache(CatalogMetaCache owner, HiveCatalogProperties properties, DirectoryLister lister) {
        this.owner = Objects.requireNonNull(owner, "owner can not be null");
        Map<String, String> props = CacheSpec.applyCompatibilityMap(properties.getRaw(),
                Collections.singletonMap(LEGACY_FILE_META_CACHE_TTL_SECOND,
                        CacheSpec.metaCacheTtlKey(ENGINE, ENTRY_FILE)));
        CacheSpec spec = CacheSpec.fromProperties(props, ENGINE, ENTRY_FILE,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_FILE_CAPACITY));
        this.cache = owner.create(MetaCacheDefinition
                .<FileListingKey, FileListingValue>builder("hive-file", spec, FileListingKey::scope)
                .sizeEstimator(HiveFileListingSizeEstimator::estimateEntry)
                .build());
        Map<String, String> summaryProps = CacheSpec.applyCompatibilityMap(properties.getRaw(),
                Collections.singletonMap(LEGACY_FILE_META_CACHE_TTL_SECOND,
                        CacheSpec.metaCacheTtlKey(ENGINE, ENTRY_FILE_SUMMARY)));
        CacheSpec summarySpec = CacheSpec.fromProperties(summaryProps, ENGINE, ENTRY_FILE_SUMMARY,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_FILE_CAPACITY));
        this.sizeCache = owner.create(MetaCacheDefinition
                .<FileListingKey, Long>builder("hive-file-summary", summarySpec, FileListingKey::scope)
                .sizeEstimator((key, value) -> MetaCacheSizeEstimate.complete(JvmSizeUtils.saturatedAdd(
                        HiveFileListingSizeEstimator.estimateKey(key), JvmSizeUtils.instanceSize(Long.class))))
                .build());
        this.lister = Objects.requireNonNull(lister, "lister can not be null");
        this.entryMaxBytes = properties.getFileListingCacheEntryMaxBytes();
        this.pendingMaxBytes = properties.getFileListingCachePendingMaxBytes();
    }

    HiveFileIterator openDataFiles(String dbName, String tableName, String location,
            List<String> partitionValues, FileSystem fs, ConnectorScanContext context) {
        return openDataFiles(dbName, tableName, location, partitionValues, fs, context, new ListingStats());
    }

    HiveFileIterator openDataFiles(String dbName, String tableName, String location,
            List<String> partitionValues, FileSystem fs, ConnectorScanContext context, ListingStats stats) {
        context.checkCancelled();
        FileListingKey key = new FileListingKey(dbName, tableName, location, partitionValues);
        FileListingValue present = cache.getIfPresent(key);
        if (present != null) {
            stats.hits.incrementAndGet();
            HiveFileIterator files = HiveFileIterator.of(present.files.iterator());
            return new HiveFileIterator() {
                @Override
                public boolean hasNext() {
                    context.checkCancelled();
                    return files.hasNext();
                }

                @Override
                public HiveFileStatus next() {
                    context.checkCancelled();
                    HiveFileStatus file = files.next();
                    context.filesEnumerated(1);
                    return file;
                }

                @Override
                public void close() {
                    files.close();
                }
            };
        }
        stats.misses.incrementAndGet();
        return new CachingIterator(key, fs, context, stats);
    }

    /** A metadata-only hint: never starts or waits for a remote listing. */
    long cachedFileCount(String dbName, String tableName, String location, List<String> partitionValues) {
        FileListingValue value = cache.getIfPresent(new FileListingKey(dbName, tableName, location, partitionValues));
        return value == null ? -1 : value.files.size();
    }

    long cachedDataSize(String dbName, String tableName, String location, List<String> partitionValues) {
        FileListingKey key = new FileListingKey(dbName, tableName, location, partitionValues);
        FileListingValue value = cache.getIfPresent(key);
        if (value != null) {
            return value.totalFileBytes;
        }
        Long summary = sizeCache.getIfPresent(key);
        return summary == null ? -1 : summary;
    }

    // Explicit materialization for callers that request a List (primarily cache contract tests).
    public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location, FileSystem fs) {
        return listDataFiles(dbName, tableName, location, Collections.emptyList(), fs);
    }

    public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location,
            List<String> partitionValues, FileSystem fs) {
        return listDataFiles(dbName, tableName, location, partitionValues, fs, new ConnectorScanContext());
    }

    public List<HiveFileStatus> listDataFiles(String dbName, String tableName, String location,
            List<String> partitionValues, FileSystem fs, ConnectorScanContext context) {
        List<HiveFileStatus> files = new ArrayList<>();
        try (HiveFileIterator iterator = openDataFiles(dbName, tableName, location, partitionValues, fs, context)) {
            iterator.forEachRemaining(files::add);
        }
        return files;
    }

    public void invalidateTable(String dbName, String tableName) {
        owner.invalidateTable(dbName, tableName);
    }

    public void invalidatePartitions(String dbName, String tableName, Set<List<String>> partitionValues) {
        for (List<String> values : partitionValues) {
            owner.invalidatePartition(dbName, tableName, values);
        }
    }

    public void invalidateDb(String dbName) {
        owner.invalidateDatabase(dbName);
    }

    public void invalidateAll() {
        owner.invalidateCatalog();
    }

    long size() {
        return cache.size();
    }

    long pendingBytes() {
        return pendingBytes.get();
    }

    static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs) {
        return listFromFileSystem(location, fs, false);
    }

    static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs, boolean recursive) {
        return listFromFileSystem(location, fs, recursive, new ConnectorScanContext());
    }

    static List<HiveFileStatus> listFromFileSystem(String location, FileSystem fs, boolean recursive,
            ConnectorScanContext context) {
        List<HiveFileStatus> files = new ArrayList<>();
        try (HiveFileIterator iterator = new HiveDirectoryIterator(location, fs, recursive, context)) {
            iterator.forEachRemaining(file -> {
                context.filesEnumerated(1);
                files.add(file);
            });
        }
        return files;
    }

    private boolean reserve(long bytes) {
        long current = pendingBytes.get();
        while (bytes <= pendingMaxBytes - current) {
            if (pendingBytes.compareAndSet(current, current + bytes)) {
                return true;
            }
            current = pendingBytes.get();
        }
        return false;
    }

    private final class CachingIterator implements HiveFileIterator {
        private final FileListingKey key;
        private final FileSystem fs;
        private final ConnectorScanContext context;
        private final ListingStats stats;
        private final long keyBytes;
        private HiveFileIterator delegate;
        private MetaCache.BulkLoad<FileListingKey, FileListingValue> load;
        private MetaCache.BulkLoad<FileListingKey, Long> summaryLoad;
        private ArrayList<HiveFileStatus> candidate;
        private long candidateBytes;
        // Includes unused chunk credit, which is not part of the published entry's weight.
        private long reservedBytes;
        private long totalFileBytes;
        private int capacity;
        private boolean closed;

        private CachingIterator(FileListingKey key, FileSystem fs, ConnectorScanContext context, ListingStats stats) {
            this.key = key;
            this.fs = fs;
            this.context = context;
            this.stats = stats;
            boolean collect = cache.cacheSpec().isCacheEnabled() && entryMaxBytes > 0 && pendingMaxBytes > 0;
            this.keyBytes = collect ? HiveFileListingSizeEstimator.estimateKey(key) : 0;
            try {
                // Both fences precede the first request, even when a list candidate is disabled or abandoned.
                ScopePath table = ScopePath.table(key.dbName, key.tableName);
                if (sizeCache.cacheSpec().isCacheEnabled()) {
                    summaryLoad = sizeCache.beginBulkLoad(table);
                }
                if (!collect) {
                    return;
                }
                load = cache.beginBulkLoad(table);
                long baseBytes = keyBytes + HiveFileListingSizeEstimator.emptyValueBytes();
                if (baseBytes > entryMaxBytes || !reserveCandidate(baseBytes)) {
                    stats.abandoned.incrementAndGet();
                    discardCandidate();
                    return;
                }
                candidateBytes = baseBytes;
                candidate = new ArrayList<>(0);
                stats.peakBytes.accumulateAndGet(candidateBytes, Math::max);
            } catch (RuntimeException | Error e) {
                closeAfterFailure(e);
                throw e;
            }
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            try {
                context.checkCancelled();
                if (delegate == null) {
                    delegate = lister.open(key.location, fs, context);
                }
                if (delegate.hasNext()) {
                    return true;
                }
                delegate.close();
                delegate = null;
                context.checkCancelled();
                if (candidate != null) {
                    FileListingValue value = new FileListingValue(candidate, candidateBytes - keyBytes, totalFileBytes);
                    if (load.publish(key, value)) {
                        stats.published.incrementAndGet();
                    }
                }
                if (summaryLoad != null && summaryLoad.publish(key, totalFileBytes)) {
                    stats.summariesPublished.incrementAndGet();
                }
                close();
                return false;
            } catch (RuntimeException | Error e) {
                closeAfterFailure(e);
                throw e;
            }
        }

        @Override
        public HiveFileStatus next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                HiveFileStatus file = delegate.next();
                context.checkCancelled();
                context.filesEnumerated(1);
                stats.files.incrementAndGet();
                totalFileBytes += file.getLength();
                collect(file);
                return file;
            } catch (RuntimeException | Error e) {
                closeAfterFailure(e);
                throw e;
            }
        }

        private void collect(HiveFileStatus file) {
            if (candidate == null) {
                return;
            }
            int nextCapacity = capacity;
            long oldArrayBytes = 0;
            long arrayGrowth = 0;
            if (candidate.size() == capacity) {
                nextCapacity = Math.max(16, Math.multiplyExact(capacity, 2));
                oldArrayBytes = capacity == 0 ? 0 : JvmSizeUtils.objectArraySize(capacity);
                arrayGrowth = JvmSizeUtils.objectArraySize(nextCapacity) - oldArrayBytes;
            }
            long nextBytes = candidateBytes + arrayGrowth + HiveFileListingSizeEstimator.fileBytes(file);
            // Account for both arrays during expansion. No allocation is made after admission fails.
            long peakBytes = nextBytes + oldArrayBytes;
            if (peakBytes > entryMaxBytes || !reserveCandidate(peakBytes)) {
                stats.abandoned.incrementAndGet();
                discardCandidate();
                return;
            }
            candidate.ensureCapacity(nextCapacity);
            candidate.add(file);
            capacity = nextCapacity;
            candidateBytes = nextBytes;
            if (oldArrayBytes > 0) {
                reservedBytes -= oldArrayBytes;
                pendingBytes.addAndGet(-oldArrayBytes);
            }
            stats.peakBytes.accumulateAndGet(peakBytes, Math::max);
        }

        private boolean reserveCandidate(long requiredBytes) {
            if (requiredBytes <= reservedBytes) {
                return true;
            }
            // Keep unused credit until this cursor closes, avoiding a shared atomic update for each file.
            long missing = requiredBytes - reservedBytes;
            long chunk = Math.min(entryMaxBytes - reservedBytes, Math.max(missing, RESERVATION_CHUNK_BYTES));
            if (reserve(chunk)) {
                reservedBytes += chunk;
                return true;
            }
            // A file can still fit when the catalog has less than a whole chunk left.
            if (chunk == missing || !reserve(missing)) {
                return false;
            }
            reservedBytes += missing;
            return true;
        }

        private void discardCandidate() {
            candidate = null;
            pendingBytes.addAndGet(-reservedBytes);
            reservedBytes = 0;
            candidateBytes = 0;
            if (load != null) {
                load.close();
                load = null;
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                if (delegate != null) {
                    delegate.close();
                }
            } finally {
                delegate = null;
                try {
                    discardCandidate();
                } finally {
                    if (summaryLoad != null) {
                        summaryLoad.close();
                        summaryLoad = null;
                    }
                }
            }
        }

        private void closeAfterFailure(Throwable failure) {
            try {
                close();
            } catch (RuntimeException e) {
                failure.addSuppressed(e);
            }
        }
    }

    static final class ListingStats {
        private final AtomicLong hits = new AtomicLong();
        private final AtomicLong misses = new AtomicLong();
        private final AtomicLong files = new AtomicLong();
        private final AtomicLong published = new AtomicLong();
        private final AtomicLong summariesPublished = new AtomicLong();
        private final AtomicLong abandoned = new AtomicLong();
        private final AtomicLong peakBytes = new AtomicLong();

        private Map<String, String> lastReported = Collections.emptyMap();

        synchronized Map<String, String> drain() {
            Map<String, String> metrics = new LinkedHashMap<>();
            metrics.put("CacheHits", Long.toString(hits.get()));
            metrics.put("CacheMisses", Long.toString(misses.get()));
            metrics.put("FilesListed", Long.toString(files.get()));
            metrics.put("ListingsPublished", Long.toString(published.get()));
            metrics.put("SizeSummariesPublished", Long.toString(summariesPublished.get()));
            metrics.put("CandidatesAbandonedForBudget", Long.toString(abandoned.get()));
            metrics.put("PeakCandidateBytes", Long.toString(peakBytes.get()));
            if (metrics.equals(lastReported) || hits.get() == 0 && misses.get() == 0) {
                return Collections.emptyMap();
            }
            // Publish cumulative progress only when it changed since the previous collection.
            lastReported = Collections.unmodifiableMap(metrics);
            return lastReported;
        }
    }

    static final class FileListingKey {
        final String dbName;
        final String tableName;
        final String location;
        final List<String> partitionValues;

        FileListingKey(String dbName, String tableName, String location, List<String> partitionValues) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.location = location;
            this.partitionValues = partitionValues == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(partitionValues));
        }

        ScopePath scope() {
            if (partitionValues.isEmpty()) {
                return ScopePath.table(dbName, tableName);
            }
            return ScopePath.partition(dbName, tableName, partitionValues);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FileListingKey)) {
                return false;
            }
            FileListingKey that = (FileListingKey) o;
            return Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(location, that.location)
                    && Objects.equals(partitionValues, that.partitionValues);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, location, partitionValues);
        }
    }

    static final class FileListingValue {
        final List<HiveFileStatus> files;
        final MetaCacheSizeEstimate sizeEstimate;
        final long totalFileBytes;

        FileListingValue(List<HiveFileStatus> ownedFiles, long valueBytes, long totalFileBytes) {
            this.files = Collections.unmodifiableList(ownedFiles);
            this.sizeEstimate = MetaCacheSizeEstimate.complete(valueBytes);
            this.totalFileBytes = totalFileBytes;
        }
    }
}
