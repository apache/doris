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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheKey;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.FileCacheValue;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.HiveFileStatus;
import org.apache.doris.datasource.metacache.CacheSpec;
import org.apache.doris.datasource.metacache.MetaCacheEntry;
import org.apache.doris.fs.FileSystemIOException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class HiveFileListingCacheTest {
    @Test
    void changingEitherBoundRetiresTheCatalogCacheGroup() {
        Env env = Mockito.mock(Env.class);
        ExternalMetaCacheMgr manager = Mockito.mock(
                ExternalMetaCacheMgr.class);
        Mockito.when(env.getExtMetaCacheMgr()).thenReturn(manager);
        HMSExternalCatalog catalog = new HMSExternalCatalog() {
            @Override
            public void resetToUninitialized(boolean invalidCache) {
                // No live HMS client is needed to verify the property-specific cache retirement.
            }
        };
        try (MockedStatic<Env> mocked =
                Mockito.mockStatic(Env.class)) {
            mocked.when(Env::getCurrentEnv).thenReturn(env);
            catalog.notifyPropertiesUpdated(Collections.singletonMap(
                    HMSExternalCatalog.FILE_LISTING_CACHE_ENTRY_MAX_BYTES, "0"));
            catalog.notifyPropertiesUpdated(Collections.singletonMap(
                    HMSExternalCatalog.FILE_LISTING_CACHE_PENDING_MAX_BYTES, "0"));
            Mockito.verify(manager, Mockito.times(2))
                    .removeCatalogByEngine(catalog.getId(), HiveExternalMetaCache.ENGINE);
        }
    }

    @Test
    void catalogBoundsValidateOnCreateAndAlter() throws Exception {
        for (String key : new String[] {HMSExternalCatalog.FILE_LISTING_CACHE_ENTRY_MAX_BYTES,
                HMSExternalCatalog.FILE_LISTING_CACHE_PENDING_MAX_BYTES}) {
            Map<String, String> properties = new HashMap<>();
            properties.put("type", "hms");
            properties.put("hive.metastore.uris", "thrift://localhost:9083");
            properties.put(key, "0");
            HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, properties, "");
            catalog.checkProperties();
            for (String value : new String[] {"-1", "bad", "9223372036854775808"}) {
                DdlException error = Assertions.assertThrows(
                        DdlException.class,
                        () -> catalog.validatePropertiesBeforeUpdate(properties, Collections.singletonMap(key, value)));
                Assertions.assertTrue(error.getMessage().contains(key));
            }
        }
    }

    @Test
    void retainsOnlyCompleteListsAndWarmHitsPreserveFiles() throws Exception {
        try (Fixture f = new Fixture(1 << 20, 1 << 20)) {
            try (HiveFileIterator cursor = f.open(30)) {
                Assertions.assertNotNull(cursor.next());
                Assertions.assertNull(f.files.peekIfPresent(f.key));
                Assertions.assertNull(f.sizes.peekIfPresent(f.key));
                Assertions.assertTrue(f.cache.pendingBytes() > 0);
                drain(cursor);
            }
            FileCacheValue published = f.files.peekIfPresent(f.key);
            Assertions.assertEquals(30, published.getFiles().size());
            Assertions.assertEquals(300L, f.sizes.peekIfPresent(f.key));
            Assertions.assertEquals(0, f.cache.pendingBytes());
            try (HiveFileIterator hit = f.open(99)) {
                Assertions.assertEquals(30, drain(hit));
            }
            Assertions.assertEquals(1, f.opens.get());
            Assertions.assertEquals(30, published.getFiles().size(), "Closing a cache hit cannot clear shared files");
            Assertions.assertEquals(1, f.files.stats().getLoadSuccessCount());
            Assertions.assertEquals(0, f.files.stats().getLoadFailureCount());
            Assertions.assertTrue(f.files.stats().getTotalLoadTimeNanos() > 0);
            Assertions.assertTrue(f.files.stats().getLastLoadSuccessTimeMs() > 0);
        }
    }

    @Test
    void oversizedCandidatesAreReleasedWhileListingContinuesAndSummarySurvives() throws Exception {
        try (Fixture f = new Fixture(3000, 3000); HiveFileIterator cursor = f.open(100)) {
            int consumed = 0;
            while (cursor.hasNext()) {
                cursor.next();
                consumed++;
                if (consumed == 20) {
                    Assertions.assertEquals(0, f.cache.pendingBytes());
                    Assertions.assertNull(f.sizes.peekIfPresent(f.key));
                }
            }
            Assertions.assertEquals(100, consumed);
            Assertions.assertNull(f.files.peekIfPresent(f.key));
            Assertions.assertEquals(1000L, f.sizes.peekIfPresent(f.key));
            Assertions.assertEquals(0, f.cache.pendingBytes());
            Assertions.assertEquals(1, f.files.stats().getLoadSuccessCount());
        }
    }

    @Test
    void aggregateBudgetAndEarlyCloseReleaseAllCredits() throws Exception {
        try (Fixture f = new Fixture(10000, 10000);
                HiveFileIterator first = f.open(20); HiveFileIterator second = f.open(20)) {
            first.next();
            Assertions.assertTrue(f.cache.pendingBytes() <= 10000);
            Assertions.assertEquals(20, drain(second));
            Assertions.assertNull(f.files.peekIfPresent(f.key));
            first.close();
            Assertions.assertEquals(0, f.cache.pendingBytes());
            Assertions.assertEquals(1, f.files.stats().getLoadSuccessCount());
            Assertions.assertEquals(0, f.files.stats().getLoadFailureCount());
        }
    }

    @Test
    void invalidationOrRetirementDuringListingCannotResurrectEntries() throws Exception {
        try (Fixture f = new Fixture(100000, 100000)) {
            try (HiveFileIterator cursor = f.open(20)) {
                cursor.next();
                f.files.invalidateIf(key -> true);
                f.sizes.invalidateAll();
                Assertions.assertEquals(19, drain(cursor));
            }
            Assertions.assertNull(f.files.peekIfPresent(f.key));
            Assertions.assertNull(f.sizes.peekIfPresent(f.key));
            try (HiveFileIterator cursor = f.open(20)) {
                cursor.next();
                f.files.close();
                f.sizes.close();
                Assertions.assertEquals(19, drain(cursor));
            }
            Assertions.assertEquals(0, f.cache.pendingBytes());
        }
    }

    @Test
    void cancellationReadFailureAndCloseFailurePublishNothing() throws Exception {
        for (int failure = 0; failure < 4; failure++) {
            try (Fixture f = new Fixture(100000, 100000)) {
                int mode = failure;
                f.failure = mode;
                try (HiveFileIterator cursor = f.open(20)) {
                    cursor.next();
                    if (mode == 0) {
                        f.cancelled.set(true);
                        Assertions.assertThrows(CancellationException.class, () -> drain(cursor));
                    } else {
                        Assertions.assertThrows(FileSystemIOException.class, () -> drain(cursor));
                    }
                }
                Assertions.assertNull(f.files.peekIfPresent(f.key));
                Assertions.assertNull(f.sizes.peekIfPresent(f.key));
                Assertions.assertEquals(0, f.cache.pendingBytes());
                Assertions.assertEquals(1, f.closes.get());
                Assertions.assertEquals(0, f.files.stats().getLoadSuccessCount());
                Assertions.assertEquals(1, f.files.stats().getLoadFailureCount());
                Assertions.assertTrue(f.files.stats().getLastLoadFailureTimeMs() > 0);
                Assertions.assertTrue(f.files.stats().getTotalLoadTimeNanos() > 0);
                if (mode == 3) {
                    Assertions.assertTrue(f.files.stats().getLastError().contains("page failure"));
                }
            }
        }
    }

    @Test
    void completedOversizeRefreshRetiresOldListButNotConcurrentReplacement() throws Exception {
        try (Fixture f = new Fixture(3000, 3000)) {
            Assertions.assertEquals(1, drain(f.open(1)));
            Assertions.assertNotNull(f.files.peekIfPresent(f.key));
            Assertions.assertNull(f.cache.refresh(f.key, () -> f.delegate(50)));
            Assertions.assertNull(f.files.peekIfPresent(f.key));
            Assertions.assertEquals(500L, f.sizes.peekIfPresent(f.key));
            try (HiveFileIterator stale = f.open(50)) {
                stale.next();
                FileCacheValue replacement = new FileCacheValue();
                f.files.put(f.key, replacement);
                f.sizes.put(f.key, 1234L);
                drain(stale);
                Assertions.assertSame(replacement, f.files.peekIfPresent(f.key));
                Assertions.assertEquals(1234L, f.sizes.peekIfPresent(f.key));
            }
        }
    }

    private static int drain(HiveFileIterator iterator) throws Exception {
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static class Fixture implements AutoCloseable {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final MetaCacheEntry<FileCacheKey, FileCacheValue> files = new MetaCacheEntry<>("files",
                key -> {
                    throw new AssertionError("Unexpected eager loader");
                }, CacheSpec.of(true, 600, 10), executor);
        final MetaCacheEntry<FileCacheKey, Long> sizes = new MetaCacheEntry<>("sizes",
                key -> {
                    throw new AssertionError("Unexpected summary load");
                }, CacheSpec.of(true, 600, 10), executor);
        final FileCacheKey key = new FileCacheKey(1, 2, "hdfs://test/p=1", "parquet",
                Collections.singletonList("1"));
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final AtomicBoolean cancelled = new AtomicBoolean();
        final HiveFileListingCache cache;
        int failure = -1;

        Fixture(long entryBytes, long pendingBytes) {
            cache = new HiveFileListingCache(files, sizes, entryBytes, pendingBytes);
        }

        HiveFileIterator open(int count) {
            return cache.open(key, () -> delegate(count), cancelled::get);
        }

        HiveFileIterator delegate(int count) {
            opens.incrementAndGet();
            FileCacheValue metadata = new FileCacheValue();
            metadata.setPartitionValues(Collections.singletonList("1"));
            metadata.setSplittable(true);
            return new HiveFileIterator() {
                int index;
                boolean closed;

                @Override
                public FileCacheValue metadata() {
                    return metadata;
                }

                @Override
                public boolean hasNext() throws FileSystemIOException {
                    if ((failure == 1 || failure == 3) && index == 3) {
                        throw new FileSystemIOException("page failure");
                    }
                    return index < count;
                }

                @Override
                public HiveFileStatus next() throws FileSystemIOException {
                    HiveFileStatus file = new HiveFileStatus();
                    try {
                        file.setPath(LocationPath.of("hdfs://test/p=1/file-" + index++));
                    } catch (Exception e) {
                        throw new FileSystemIOException(e.getMessage(), e);
                    }
                    file.setLength(10);
                    return file;
                }

                @Override
                public void close() throws FileSystemIOException {
                    Assertions.assertFalse(closed, "Raw listing must be closed once");
                    closed = true;
                    closes.incrementAndGet();
                    if (failure == 2 || failure == 3) {
                        throw new FileSystemIOException("close failure");
                    }
                }
            };
        }

        @Override
        public void close() {
            files.close();
            sizes.close();
            executor.shutdownNow();
        }
    }
}
