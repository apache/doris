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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/fs/TransactionScopeCachingDirectoryLister.java
// and modified by Doris

package org.apache.doris.fs;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.fs.remote.RemoteFile;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * Caches directory content (including listings that were started concurrently).
 * {@link TransactionScopeCachingDirectoryLister} assumes that all listings
 * are performed by same user within single transaction, therefore any failure can
 * be shared between concurrent listings.
 */
public class TransactionScopeCachingDirectoryLister implements DirectoryLister {
    private final long transactionId;

    @VisibleForTesting
    public Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder> getCache() {
        return cache;
    }

    //TODO use a cache key based on Path & SchemaTableName and iterate over the cache keys
    // to deal more efficiently with cache invalidation scenarios for partitioned tables.
    private final Cache<TransactionDirectoryListingCacheKey, FetchingValueHolder> cache;
    private final DirectoryLister delegate;

    public TransactionScopeCachingDirectoryLister(DirectoryLister delegate, long transactionId,
                                                  Cache<TransactionDirectoryListingCacheKey,
                                                          FetchingValueHolder> cache) {
        this.delegate = Objects.requireNonNull(delegate, "delegate is null");
        this.transactionId = transactionId;
        this.cache = Objects.requireNonNull(cache, "cache is null");
    }

    @Override
    public RemoteIterator<RemoteFile> listFiles(FileSystem fs, boolean recursive, TableIf table, String location)
            throws FileSystemIOException {
        return listInternal(fs, recursive, table, new TransactionDirectoryListingCacheKey(transactionId, location));
    }

    private RemoteIterator<RemoteFile> listInternal(FileSystem fs, boolean recursive, TableIf table,
                                                    TransactionDirectoryListingCacheKey cacheKey)
            throws FileSystemIOException {
        FetchingValueHolder cachedValueHolder;
        try {
            cachedValueHolder = cache.get(cacheKey,
                    () -> new FetchingValueHolder(createListingRemoteIterator(fs, recursive, table, cacheKey)));
        } catch (ExecutionException | UncheckedExecutionException e) {
            Throwable throwable = e.getCause();
            Throwables.throwIfInstanceOf(throwable, FileSystemIOException.class);
            Throwables.throwIfUnchecked(throwable);
            throw new RuntimeException("Failed to list directory: " + cacheKey.getPath(), throwable);
        }

        if (cachedValueHolder.isFullyCached()) {
            return new SimpleRemoteIterator(cachedValueHolder.getCachedFiles());
        }

        return cachingRemoteIterator(cachedValueHolder, cacheKey);
    }

    private RemoteIterator<RemoteFile> createListingRemoteIterator(FileSystem fs, boolean recursive,
                                                                   TableIf table,
                                                                   TransactionDirectoryListingCacheKey cacheKey)
            throws FileSystemIOException {
        return delegate.listFiles(fs, recursive, table, cacheKey.getPath());
    }


    private RemoteIterator<RemoteFile> cachingRemoteIterator(FetchingValueHolder cachedValueHolder,
                                                             TransactionDirectoryListingCacheKey cacheKey) {
        return new RemoteIterator<RemoteFile>() {
            private int fileIndex;

            @Override
            public boolean hasNext()
                    throws FileSystemIOException {
                try {
                    boolean hasNext = cachedValueHolder.getCachedFile(fileIndex).isPresent();
                    // Update cache weight of cachedValueHolder for a given path.
                    // The cachedValueHolder acts as an invalidation guard.
                    // If a cache invalidation happens while this iterator goes over the files from the specified path,
                    // the eventually outdated file listing will not be added anymore to the cache.
                    cache.asMap().replace(cacheKey, cachedValueHolder, cachedValueHolder);
                    return hasNext;
                } catch (Exception exception) {
                    // invalidate cached value to force retry of directory listing
                    cache.invalidate(cacheKey);
                    throw exception;
                }
            }

            @Override
            public RemoteFile next()
                    throws FileSystemIOException {
                // force cache entry weight update in case next file is cached
                Preconditions.checkState(hasNext());
                return cachedValueHolder.getCachedFile(fileIndex++).orElseThrow(NoSuchElementException::new);
            }
        };
    }

    @VisibleForTesting
    boolean isCached(String location) {
        return isCached(new TransactionDirectoryListingCacheKey(transactionId, location));
    }

    @VisibleForTesting
    boolean isCached(TransactionDirectoryListingCacheKey cacheKey) {
        FetchingValueHolder cached = cache.getIfPresent(cacheKey);
        return cached != null && cached.isFullyCached();
    }

    static class FetchingValueHolder {

        private final List<RemoteFile> cachedFiles = ListUtils.synchronizedList(new ArrayList<RemoteFile>());

        @GuardedBy("this")
        @Nullable
        private RemoteIterator<RemoteFile> fileIterator;
        @GuardedBy("this")
        @Nullable
        private Exception exception;

        public FetchingValueHolder(RemoteIterator<RemoteFile> fileIterator) {
            this.fileIterator = Objects.requireNonNull(fileIterator, "fileIterator is null");
        }

        public synchronized boolean isFullyCached() {
            return fileIterator == null && exception == null;
        }

        public long getCacheFileCount() {
            return cachedFiles.size();
        }

        public Iterator<RemoteFile> getCachedFiles() {
            Preconditions.checkState(isFullyCached());
            return cachedFiles.iterator();
        }

        public Optional<RemoteFile> getCachedFile(int index)
                throws FileSystemIOException {
            int filesSize = cachedFiles.size();
            Preconditions.checkArgument(index >= 0 && index <= filesSize,
                    "File index (%s) out of bounds [0, %s]", index, filesSize);

            // avoid fileIterator synchronization (and thus blocking) for already cached files
            if (index < filesSize) {
                return Optional.of(cachedFiles.get(index));
            }

            return fetchNextCachedFile(index);
        }

        private synchronized Optional<RemoteFile> fetchNextCachedFile(int index)
                throws FileSystemIOException {
            if (exception != null) {
                throw new FileSystemIOException("Exception while listing directory", exception);
            }

            if (index < cachedFiles.size()) {
                // file was fetched concurrently
                return Optional.of(cachedFiles.get(index));
            }

            try {
                if (fileIterator == null || !fileIterator.hasNext()) {
                    // no more files
                    fileIterator = null;
                    return Optional.empty();
                }

                RemoteFile fileStatus = fileIterator.next();
                cachedFiles.add(fileStatus);
                return Optional.of(fileStatus);
            } catch (Exception exception) {
                fileIterator = null;
                this.exception = exception;
                throw exception;
            }
        }
    }
}
