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

package org.apache.doris.connector.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class CatalogCacheDomainTest {
    private static final CacheSpec SPEC = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100);

    private MetaCache<String, String> entry(CatalogMetaCache domain) {
        return domain.create(MetaCacheDefinition.<String, String>builder("table", SPEC,
                key -> ScopePath.table("db", key)).build());
    }

    @Test
    void sameNameInstancesShareInvalidationButNotRetirement() {
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            CatalogMetaCache old = root.openDomain("hive");
            CatalogMetaCache replacement = root.openDomain("hive");
            MetaCache<String, String> first = entry(old);
            MetaCache<String, String> second = entry(replacement);
            first.put("a", "old");
            second.put("a", "new");
            Assertions.assertEquals(2, root.allEntries().size());
            old.close();
            Assertions.assertEquals("new", second.getIfPresent("a"));
            Assertions.assertEquals(1, root.allEntries().size());
            Assertions.assertThrows(IllegalStateException.class, () -> old.openDomain("iceberg"));
            root.invalidateTable("db", "a");
            Assertions.assertNull(second.getIfPresent("a"));
        }
    }

    @Test
    void constructionRollbackDoesNotRetireTheRootOrSiblings() {
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> sibling = entry(root.openDomain("iceberg"));
            sibling.put("a", "live");
            CatalogMetaCache failing = root.openDomain("hive");
            entry(failing).put("a", "partial");
            Assertions.assertThrows(IllegalArgumentException.class, () -> failing.create(MetaCacheDefinition
                    .<String, String>builder("bad", CacheSpec.of(true, -1L, -1L),
                            key -> ScopePath.table("db", key)).build()));
            failing.close();
            Assertions.assertFalse(root.isClosed());
            Assertions.assertEquals("live", sibling.getIfPresent("a"));
            Assertions.assertEquals(1, root.allEntries().size());
        }
    }

    @Test
    void localAndRemoteCoordinatesDoNotCollideAndCanBeFencedTogether() {
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            CatalogMetaCache local = root.openDomain("fe", ScopePath.Coordinates.LOCAL, getClass().getClassLoader());
            CatalogMetaCache remote = root.openDomain("hive", ScopePath.Coordinates.REMOTE, getClass().getClassLoader());
            MetaCache<String, String> fe = entry(local);
            MetaCache<String, String> source = entry(remote);
            fe.put("a", "local");
            source.put("a", "remote");
            local.invalidateTable("db", "a");
            Assertions.assertNull(fe.getIfPresent("a"));
            Assertions.assertEquals("remote", source.getIfPresent("a"));
            fe.put("a", "local2");
            root.invalidateScopes(Arrays.asList(ScopePath.table("db", "a").in(ScopePath.Coordinates.LOCAL),
                    ScopePath.table("db", "a").in(ScopePath.Coordinates.REMOTE)));
            Assertions.assertNull(fe.getIfPresent("a"));
            Assertions.assertNull(source.getIfPresent("a"));
        }
    }

    @Test
    void oneRemoteInvalidationFencesAnInFlightSiblingLoad() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            CatalogMetaCache hive = root.openDomain("hive", ScopePath.Coordinates.REMOTE, getClass().getClassLoader());
            MetaCache<String, String> sibling = entry(hive.openDomain("iceberg"));
            Future<String> old = worker.submit(() -> sibling.get("a", key -> {
                started.countDown();
                try {
                    Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return "old";
            }));
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            hive.invalidateTable("db", "a");
            sibling.put("a", "new");
            finish.countDown();
            Assertions.assertEquals("old", old.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals("new", sibling.getIfPresent("a"));
        } finally {
            finish.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    void bulkLoadsProjectTheirParentIntoTheDomainCoordinates() {
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            CatalogMetaCache domain = root.openDomain("hive", ScopePath.Coordinates.REMOTE,
                    getClass().getClassLoader());
            MetaCache<String, String> cache = entry(domain);
            try (MetaCache.BulkLoad<String, String> load = cache.beginBulkLoad(ScopePath.database("db"))) {
                Assertions.assertTrue(load.publish("a", "first"));
                domain.invalidateDatabase("db");
                Assertions.assertFalse(load.publish("b", "stale"));
                Assertions.assertNull(cache.getIfPresent("a"));
                Assertions.assertNull(cache.getIfPresent("b"));
            }
        }
    }

    @Test
    void disabledCacheStillFencesPublicationSideEffectsBeforeLoading() throws Exception {
        ExecutorService worker = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger actions = new java.util.concurrent.atomic.AtomicInteger();
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            MetaCache<String, String> cache = root.create(MetaCacheDefinition.<String, String>builder("disabled",
                    CacheSpec.of(false, -1, 10), key -> ScopePath.table("db", key)).build());
            Future<String> load = worker.submit(() -> cache.getWithPublicationAction("a", key -> {
                started.countDown();
                try {
                    Assertions.assertTrue(finish.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                return "old";
            }, (value, commit) -> commit.accept(actions::incrementAndGet)));
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
            root.invalidateTable("db", "a");
            finish.countDown();
            Assertions.assertEquals("old", load.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(0, actions.get());
            Assertions.assertNull(cache.getIfPresent("a"));
        } finally {
            finish.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    void pluginCallbacksRestoreTheirRegisteringClassLoader() {
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        ClassLoader plugin = new ClassLoader(original) { };
        AtomicReference<ClassLoader> loaded = new AtomicReference<>();
        AtomicReference<ClassLoader> removed = new AtomicReference<>();
        try (CatalogMetaCache root = CatalogMetaCache.unmanaged()) {
            CatalogMetaCache domain = root.openDomain("hive", ScopePath.Coordinates.REMOTE, plugin);
            MetaCache<String, String> cache = domain.create(MetaCacheDefinition.<String, String>builder("table", SPEC,
                    key -> ScopePath.table("db", key))
                    .loader(key -> {
                        loaded.set(Thread.currentThread().getContextClassLoader());
                        return "v";
                    }).removalListener((key, value, cause) ->
                            removed.set(Thread.currentThread().getContextClassLoader())).build());
            cache.get("a");
            domain.close();
            Assertions.assertSame(plugin, loaded.get());
            Assertions.assertSame(plugin, removed.get());
            Assertions.assertSame(original, Thread.currentThread().getContextClassLoader());
        }
    }
}
