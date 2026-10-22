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

package org.apache.doris.datasource.metacache;

import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Catalog scoped entry container.
 */
final class CatalogMetaCacheRuntime {
    private final CatalogMetaCache owner;
    private final boolean ownsOwner;
    private final Map<String, MetaCache<?, ?>> entries = new ConcurrentHashMap<>();

    CatalogMetaCacheRuntime(long catalogId, String engine, Map<String, String> catalogProperties) {
        this(CatalogMetaCache.managed(catalogId, engine, catalogProperties), true);
    }

    CatalogMetaCacheRuntime(CatalogMetaCache owner) {
        this(owner, false);
    }

    private CatalogMetaCacheRuntime(CatalogMetaCache owner, boolean ownsOwner) {
        this.owner = Objects.requireNonNull(owner, "owner");
        this.ownsOwner = ownsOwner;
    }

    CatalogMetaCache owner() {
        return owner;
    }

    MetaCache<?, ?> get(String entryName) {
        return entries.get(entryName);
    }

    void put(String entryName, MetaCache<?, ?> entry) {
        entries.put(Objects.requireNonNull(entryName, "entryName"), Objects.requireNonNull(entry, "entry"));
    }

    Map<String, MetaCacheEntryStats> stats() {
        Map<String, MetaCacheEntryStats> result = Maps.newHashMap();
        owner.entries().forEach((name, entry) -> result.put(name, stats(entry)));
        return result;
    }

    void invalidateAll() {
        owner.invalidateCatalog();
    }

    void close() {
        // An entry group borrows the catalog's FE owner. Reconfiguring schema entries must not close
        // sibling naming/object adapters; only ExternalCatalog retires that shared owner.
        if (ownsOwner) {
            owner.close();
        } else {
            entries.values().forEach(owner::remove);
        }
        entries.clear();
    }

    private MetaCacheEntryStats stats(MetaCache<?, ?> entry) {
        return MetaCacheEntryStats.from(entry);
    }
}
