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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

class ConnectorCacheOwnerTest {
    @Test
    void retiresOnlyItsOwnInstancesEvenWhenCatalogIdsMatch() {
        ConnectorCacheOwner serving = new ConnectorCacheOwner();
        ConnectorCacheOwner checkpoint = new ConnectorCacheOwner();
        try (CatalogMetaCache first = serving.create(() -> cache(911L));
                CatalogMetaCache second = checkpoint.create(() -> cache(911L))) {
            MetaCache<String, String> servingEntry = entry(first);
            MetaCache<String, String> checkpointEntry = entry(second);
            servingEntry.put("key", "serving");
            checkpointEntry.put("key", "checkpoint");
            checkpoint.retire();
            Assertions.assertEquals("serving", servingEntry.getIfPresent("key"));
            Assertions.assertThrows(IllegalStateException.class, () -> checkpointEntry.getIfPresent("key"));
            Assertions.assertThrows(IllegalStateException.class, () -> checkpoint.create(() -> cache(911L)));
        } finally {
            serving.retire();
            checkpoint.retire();
        }
    }

    @Test
    void rejectsRegistrationFromConstructionThatOutlivesRetirement() throws Exception {
        ConnectorCacheOwner owner = new ConnectorCacheOwner();
        CountDownLatch constructionStarted = new CountDownLatch(1);
        CountDownLatch finishConstruction = new CountDownLatch(1);
        ExecutorService constructor = Executors.newSingleThreadExecutor();
        try {
            Future<CatalogMetaCache> result = constructor.submit(() -> owner.create(() -> {
                constructionStarted.countDown();
                try {
                    if (!finishConstruction.await(10L, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("construction was not released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
                return cache(912L);
            }));
            Assertions.assertTrue(constructionStarted.await(10L, TimeUnit.SECONDS));
            owner.retire();
            finishConstruction.countDown();
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> result.get(10L, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(IllegalStateException.class, failure.getCause());
            Assertions.assertTrue(MetaCacheGovernance.catalogCaches(912L).isEmpty());
        } finally {
            finishConstruction.countDown();
            constructor.shutdownNow();
            Assertions.assertTrue(constructor.awaitTermination(10L, TimeUnit.SECONDS));
            owner.retire();
        }
    }

    private static CatalogMetaCache cache(long id) {
        return CatalogMetaCache.managed(id, "ownership-test", Collections.emptyMap());
    }

    private static MetaCache<String, String> entry(CatalogMetaCache cache) {
        return cache.create(MetaCacheDefinition.<String, String>builder("entry",
                CacheSpec.of(true, -1L, 100L), key -> ScopePath.catalog()).build());
    }
}
