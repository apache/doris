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

package org.apache.doris.fs;

import org.apache.doris.common.CacheFactory;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.filesystem.FileSystem;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class FileSystemCache {

    private static final Logger LOG = LogManager.getLogger(FileSystemCache.class);

    private final LoadingCache<FileSystemCacheKey, FileSystemHolder> fileSystemCache;
    private final Function<FileSystemCacheKey, FileSystem> loader;

    public FileSystemCache() {
        this(
                Config.max_remote_file_system_cache_num,
                OptionalLong.of(Config.external_cache_expire_time_seconds_after_access),
                FileSystemCache::loadFileSystem);
    }

    @VisibleForTesting
    FileSystemCache(long maxSize, OptionalLong expireAfterAccessSec, Function<FileSystemCacheKey, FileSystem> loader) {
        this.loader = Objects.requireNonNull(loader, "loader");
        if (maxSize == 0) {
            fileSystemCache = null;
            return;
        }
        // no need to set refreshAfterWrite, because the FileSystem is created once and never changed
        CacheFactory fsCacheFactory = new CacheFactory(
                expireAfterAccessSec,
                OptionalLong.empty(),
                maxSize,
                false,
                null);
        fileSystemCache = fsCacheFactory.buildCacheWithSyncRemovalListener(
                key -> new FileSystemHolder(key, loader.apply(key)), (key, holder, cause) -> {
                    if (holder != null) {
                        holder.markEvicted();
                    }
                });
    }

    private static FileSystem loadFileSystem(FileSystemCacheKey key) {
        try {
            return FileSystemFactory.getFileSystemWithEffectiveProperties(key.effectiveProperties);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create filesystem for key: " + key, e);
        }
    }

    public FileSystemLease getFileSystem(FileSystemCacheKey key) {
        if (fileSystemCache == null) {
            return new DirectFileSystemLease(key, loader.apply(key));
        }
        while (true) {
            FileSystemHolder holder = fileSystemCache.get(key);
            FileSystemLease lease = holder.acquire();
            if (lease != null) {
                return lease;
            }
            fileSystemCache.asMap().remove(key, holder);
        }
    }

    @VisibleForTesting
    void cleanUp() {
        if (fileSystemCache != null) {
            fileSystemCache.cleanUp();
        }
    }

    private static final class FileSystemHolder {
        private final FileSystemCacheKey key;
        private final FileSystem fileSystem;
        private int referenceCount = 0;
        private boolean evicted = false;
        private boolean closed = false;

        private FileSystemHolder(FileSystemCacheKey key, FileSystem fileSystem) {
            this.key = Objects.requireNonNull(key, "key");
            this.fileSystem = Objects.requireNonNull(fileSystem, "fileSystem");
        }

        private synchronized FileSystemLease acquire() {
            if (evicted || closed) {
                return null;
            }
            referenceCount++;
            return new CachedFileSystemLease(this);
        }

        private synchronized void release() {
            Preconditions.checkState(referenceCount > 0, "FileSystem lease has been released more than once");
            referenceCount--;
            closeIfIdle();
        }

        private synchronized void markEvicted() {
            evicted = true;
            closeIfIdle();
        }

        private void closeIfIdle() {
            if (!evicted || referenceCount != 0 || closed) {
                return;
            }
            closed = true;
            try {
                fileSystem.close();
            } catch (IOException e) {
                LOG.warn("Failed to close evicted FileSystem for key: {}", key, e);
            }
        }

        private FileSystem fileSystem() {
            return fileSystem;
        }
    }

    public interface FileSystemLease extends AutoCloseable {
        FileSystem fileSystem();

        @Override
        void close();
    }

    private static final class CachedFileSystemLease implements FileSystemLease {
        private final FileSystemHolder holder;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private CachedFileSystemLease(FileSystemHolder holder) {
            this.holder = holder;
        }

        @Override
        public FileSystem fileSystem() {
            return holder.fileSystem();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            holder.release();
        }
    }

    private static final class DirectFileSystemLease implements FileSystemLease {
        private final FileSystemCacheKey key;
        private final FileSystem fileSystem;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private DirectFileSystemLease(FileSystemCacheKey key, FileSystem fileSystem) {
            this.key = key;
            this.fileSystem = fileSystem;
        }

        @Override
        public FileSystem fileSystem() {
            return fileSystem;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                fileSystem.close();
            } catch (IOException e) {
                LOG.warn("Failed to close uncached FileSystem for key: {}", key, e);
            }
        }
    }

    public static class FileSystemCacheKey {
        // eg: hdfs://nameservices1
        private final String fsIdent;
        private final StorageProperties properties;
        private final Map<String, String> effectiveProperties;
        private final boolean s3SkipListForDeterministicPath;
        private final int s3HeadRequestMaxPaths;

        public FileSystemCacheKey(String fsIdent, StorageProperties properties) {
            this.fsIdent = fsIdent;
            this.properties = properties;
            boolean isS3Compatible = properties instanceof AbstractS3CompatibleProperties;
            this.s3SkipListForDeterministicPath = isS3Compatible
                    && Config.s3_skip_list_for_deterministic_path;
            this.s3HeadRequestMaxPaths = isS3Compatible ? Config.s3_head_request_max_paths : 0;
            this.effectiveProperties = Collections.unmodifiableMap(FileSystemFactory.withRuntimeFileSystemProperties(
                    StoragePropertiesConverter.toMap(properties),
                    s3SkipListForDeterministicPath, s3HeadRequestMaxPaths));
        }

        public StorageProperties getProperties() {
            return properties;
        }

        Map<String, String> getEffectiveProperties() {
            return effectiveProperties;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof FileSystemCacheKey)) {
                return false;
            }
            FileSystemCacheKey o = (FileSystemCacheKey) obj;
            return fsIdent.equals(o.fsIdent)
                            && properties.equals(o.properties)
                            && s3SkipListForDeterministicPath == o.s3SkipListForDeterministicPath
                            && s3HeadRequestMaxPaths == o.s3HeadRequestMaxPaths;
        }

        @Override
        public int hashCode() {
            return Objects.hash(properties, fsIdent, s3SkipListForDeterministicPath, s3HeadRequestMaxPaths);
        }
    }
}
