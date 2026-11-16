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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.MetaCacheSizeEstimators;
import org.apache.doris.connector.cache.ScopePath;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Second-level memo for the time-travel schema-at-snapshot read (FIX-B-MC2). Restores the cross-query
 * cache hit that the legacy catalog-level {@code PaimonExternalMetaCache} (keyed by
 * {@code (NameMapping, schemaId)}) provided and that the SPI cutover dropped (review tag CACHE-P1).
 *
 * <p>This memo lives on the long-lived per-catalog {@link PaimonConnector} (NOT on the per-query
 * {@link PaimonConnectorMetadata}, which is rebuilt by {@code getMetadata} every query) and is injected
 * into the metadata so the at-snapshot resolve can consult/populate it. It is cleared wholesale on
 * REFRESH CATALOG (the connector is rebuilt → a fresh empty memo).
 *
 * <p>The historical snapshot generation is part of the cache key because fast-forward can
 * replace a schema file with a different schema at the same ID. Keep only schema fields and
 * keys here, and apply the effective table options when building the relation.
 */
final class PaimonSchemaAtMemo {

    /** Default best-effort bound; the keyspace (table, branch, schemaId) is naturally tiny. */
    static final int DEFAULT_MAX_SIZE = 10000;

    private final CatalogMetaCache owner;
    private final MetaCache<MemoKey, PaimonCatalogOps.PaimonSchemaSnapshot> cache;

    PaimonSchemaAtMemo(int maxSize) {
        this(CatalogMetaCache.unmanaged(), maxSize);
    }

    PaimonSchemaAtMemo(CatalogMetaCache owner, int maxSize) {
        this.owner = owner;
        CacheSpec spec = CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, maxSize);
        this.cache = owner.create(MetaCacheDefinition
                .<MemoKey, PaimonCatalogOps.PaimonSchemaSnapshot>builder(
                        "paimon-schema-at", spec,
                        key -> ScopePath.table(key.databaseName, key.tableName))
                // A wide schema's fields would exhaust the visit budget of a whole-value walk.
                .sizeEstimator(MetaCacheSizeEstimators.reflectivePerElement(
                        snapshot -> Collections.singletonList(snapshot.fields())))
                .build());
    }

    /**
     * Returns the schema-at-snapshot for {@code (handle, schemaId)}, loading it via {@code loader} (the
     * {@link PaimonCatalogOps#schemaAt} read) only on a miss.
     *
     * <p>The loader runs OUTSIDE any lock (no I/O under a lock; not {@code computeIfAbsent}). A concurrent
     * same-key miss may load twice — harmless because the value is immutable and identical, and it equals
     * the pre-fix per-query double load. A loader exception propagates before any insert, so failures are
     * never negative-cached.
     */
    PaimonCatalogOps.PaimonSchemaSnapshot getOrLoad(PaimonTableHandle handle, long schemaId,
            Supplier<PaimonCatalogOps.PaimonSchemaSnapshot> loader) {
        return getOrLoad(handle, schemaId, Collections.emptyMap(), loader);
    }

    PaimonCatalogOps.PaimonSchemaSnapshot getOrLoad(PaimonTableHandle handle, long schemaId,
            Map<String, String> generation, Supplier<PaimonCatalogOps.PaimonSchemaSnapshot> loader) {
        MemoKey key = new MemoKey(handle, schemaId, generation);
        return cache.get(key, ignored -> loader.get());
    }

    /** Test-only: current number of cached entries. */
    int size() {
        return Math.toIntExact(cache.size());
    }

    /**
     * Drop every memoized schema for {@code (db, table)} across all schemaIds / sys-tables / branches. Wired
     * onto {@code REFRESH TABLE} and — via the generic {@code PluginDrivenExternalCatalog} DDL hook — onto a
     * Doris-issued DROP/CREATE of the same name, so a drop+recreate that reuses a schemaId (e.g. schema 0)
     * with different content does not serve a stale time-travel schema. The memo value is immutable, so
     * dropping an entry only forces a re-read (the pre-memo behavior), never a stale/wrong value.
     */
    void invalidate(String databaseName, String tableName) {
        owner.invalidateTable(databaseName, tableName);
    }

    /**
     * Drop every memoized schema for database {@code databaseName} across all its tables / schemaIds /
     * sys-tables / branches. Wired onto {@code REFRESH DATABASE} and — via the generic
     * {@code PluginDrivenExternalCatalog} dropDb hook — onto a Doris-issued {@code DROP DATABASE} of the
     * same name (incl. its FORCE table cascade), so a drop+recreate of a table in that db that reuses a
     * schemaId does not serve a stale time-travel schema. Db-scoped analogue of {@link #invalidate}.
     */
    void invalidateDb(String databaseName) {
        owner.invalidateDatabase(databaseName);
    }

    /** Drop the whole memo. Wired onto {@code REFRESH CATALOG} (alongside the connector rebuild). */
    void invalidateAll() {
        owner.invalidateCatalog();
    }

    /**
     * Cache key = table and branch identity, schema ID and its snapshot generation when available.
     *
     * <p>The four identity fields MIRROR {@link PaimonTableHandle#equals}/{@link PaimonTableHandle#hashCode}
     * (PaimonTableHandle:233-240). They are stored as extracted values rather than a retained
     * {@link PaimonTableHandle} reference ON PURPOSE: a handle carries its loaded paimon {@code Table}
     * (set via {@code setPaimonTable}), so keying on the handle would pin that {@code Table} in the cache
     * for its lifetime. If {@code PaimonTableHandle}'s identity ever gains a field, mirror it here too.
     */
    static final class MemoKey {
        private final String databaseName;
        private final String tableName;
        private final String sysTableName;
        private final String branchName;
        private final long schemaId;
        private final Map<String, String> generation;

        MemoKey(PaimonTableHandle handle, long schemaId, Map<String, String> generation) {
            this.databaseName = handle.getDatabaseName();
            this.tableName = handle.getTableName();
            this.sysTableName = handle.getSysTableName();
            this.branchName = handle.getBranchName();
            this.schemaId = schemaId;
            this.generation = Map.copyOf(generation);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MemoKey)) {
                return false;
            }
            MemoKey that = (MemoKey) o;
            return schemaId == that.schemaId
                    && generation.equals(that.generation)
                    && databaseName.equals(that.databaseName)
                    && tableName.equals(that.tableName)
                    && Objects.equals(sysTableName, that.sysTableName)
                    && Objects.equals(branchName, that.branchName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(databaseName, tableName, sysTableName, branchName, schemaId, generation);
        }
    }
}
