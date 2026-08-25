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

import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

/**
 * Engine-level abstraction for external metadata cache.
 * It defines a unified access path (engine -> catalog -> entry), scoped
 * invalidation APIs, and a common stats output shape.
 */
public interface ExternalMetaCache {
    /**
     * @return engine type name, for example: hive/iceberg/paimon.
     */
    String engine();

    /**
     * Additional engine aliases accepted by the manager.
     */
    Collection<String> aliases();

    /** Validate cache properties in this engine's canonical namespace. */
    default void validateCatalogProperties(Map<String, String> catalogProperties) {
    }

    /**
     * Drop the properties in this engine's namespace that runtime initialization would ignore
     * (unknown entries, obsolete or unparsable options), returning what the engine will honor.
     */
    default Map<String, String> sanitizeCatalogPropertiesForRuntime(Map<String, String> catalogProperties) {
        return catalogProperties;
    }

    /**
     * Validate cache properties with the semantics initialization applies to persisted state:
     * entry weights must fit their catalog bound, but the catalog bound is not compared with this
     * FE's local global bound (runtime clamps it instead).
     */
    default void validateCatalogPropertiesForRuntime(Map<String, String> catalogProperties) {
    }

    /**
     * Bind the callback that (re)prepares a catalog group under the manager's lifecycle fence.
     * A lookup that finds no group (the catalog was retired by a concurrent cache-policy ALTER
     * after the caller prepared it) uses it once before failing.
     */
    default void bindCatalogPreparer(LongConsumer catalogPreparer) {
    }

    /**
     * Initialize all registered entries for one catalog under current engine.
     * Entry instances are created eagerly at this stage.
     *
     * @param catalogId catalog id
     * @param catalogProperties catalog-level properties used for resolving
     *                          entry cache configuration
     */
    void initCatalog(long catalogId, Map<String, String> catalogProperties);

    /**
     * Get one cache entry under an engine and catalog.
     *
     * <p>This is a low-level extension API. Prefer typed engine operations when
     * available.
     */
    <K, V> MetaCacheEntry<K, V> entry(long catalogId, String entryName, Class<K> keyType, Class<V> valueType);

    /**
     * Validate that the catalog has been initialized in current engine cache.
     */
    void checkCatalogInitialized(long catalogId);

    /**
     * Returns whether the catalog is currently initialized in this engine cache.
     */
    boolean isCatalogInitialized(long catalogId);

    /**
     * Typed schema cache access that hides entry-name and class plumbing from callers.
     */
    @SuppressWarnings("unchecked")
    default <K extends SchemaCacheKey> Optional<SchemaCacheValue> getSchemaValue(long catalogId, K key) {
        return Optional.ofNullable(entry(catalogId, "schema", (Class<K>) key.getClass(), SchemaCacheValue.class)
                .get(key));
    }

    /**
     * Invalidate all entries under one catalog in current engine cache.
     */
    void invalidateCatalog(long catalogId);

    /**
     * The catalog id was permanently dropped and will never be reused. Unlike
     * {@link #invalidateCatalog}, which also serves same-id policy rebuilds, this hook lets an
     * engine release side state (such as monotonic generation counters) that must survive
     * rebuilds but would otherwise accumulate for the FE lifetime. It is invoked even when the
     * engine's entry group was already retired.
     */
    default void onCatalogPermanentlyRemoved(long catalogId) {
    }

    /**
     * Invalidate cached data under one catalog but keep the catalog entry group initialized.
     * This is used by refresh flows where catalog lifecycle remains initialized.
     *
     * <p>The default implementation falls back to full catalog invalidation.
     */
    default void invalidateCatalogEntries(long catalogId) {
        invalidateCatalog(catalogId);
    }

    /**
     * Invalidate all entries related to a database.
     */
    void invalidateDb(long catalogId, String dbName);

    /**
     * Invalidate all entries related to a table.
     */
    void invalidateTable(long catalogId, String dbName, String tableName);

    /**
     * Invalidate all entries related to specific partitions.
     */
    void invalidatePartitions(long catalogId, String dbName, String tableName, List<String> partitions);

    /**
     * Return stats of all entries under one catalog.
     */
    Map<String, MetaCacheEntryStats> stats(long catalogId);

    /**
     * Release resources owned by current engine cache.
     */
    void close();
}
