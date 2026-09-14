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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Process-wide bridge used because connector-cache is parent-first for every connector plugin. */
public final class MetaCacheGovernance {
    private static final Object CONFIG_LOCK = new Object();
    private static final ConcurrentHashMap<Long, Set<CatalogMetaCache>> CATALOG_CACHES =
            new ConcurrentHashMap<>();
    private static volatile MetaCacheBudgetManager budgetManager =
            new MetaCacheBudgetManager(OptionalLong.empty());
    private static volatile OptionalLong configuredGlobalMaxWeight = OptionalLong.empty();

    private MetaCacheGovernance() {
    }

    public static void configureGlobalMaxWeight(OptionalLong globalMaxWeight) {
        synchronized (CONFIG_LOCK) {
            OptionalLong requested = globalMaxWeight == null ? OptionalLong.empty() : globalMaxWeight;
            if (same(configuredGlobalMaxWeight, requested)) {
                return;
            }
            if (!CATALOG_CACHES.isEmpty()) {
                throw new IllegalStateException(
                        "Can not change external metadata cache global weight after catalogs are initialized");
            }
            budgetManager = new MetaCacheBudgetManager(requested);
            configuredGlobalMaxWeight = requested;
        }
    }

    static MetaCacheBudgetManager budgetManager() {
        return budgetManager;
    }

    static void register(CatalogMetaCache cache) {
        CATALOG_CACHES.compute(cache.catalogId(), (ignored, caches) -> {
            Set<CatalogMetaCache> updated = caches == null ? ConcurrentHashMap.newKeySet() : caches;
            updated.add(cache);
            return updated;
        });
    }

    static void unregister(CatalogMetaCache cache) {
        CATALOG_CACHES.computeIfPresent(cache.catalogId(), (ignored, caches) -> {
            caches.remove(cache);
            return caches.isEmpty() ? null : caches;
        });
    }

    public static List<CatalogMetaCache> catalogCaches(long catalogId) {
        Set<CatalogMetaCache> caches = CATALOG_CACHES.get(catalogId);
        return caches == null ? Collections.emptyList() : new ArrayList<>(caches);
    }

    public static OptionalLong globalMaxWeight() {
        return configuredGlobalMaxWeight;
    }

    public static long globalEstimatedWeight() {
        return budgetManager.getGlobalUsedWeight();
    }

    private static boolean same(OptionalLong left, OptionalLong right) {
        return left.isPresent() == right.isPresent()
                && (!left.isPresent() || left.getAsLong() == right.getAsLong());
    }
}
