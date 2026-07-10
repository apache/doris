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

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
 * <p><b>Value =</b> the raw {@link PaimonCatalogOps.PaimonSchemaSnapshot} (fields + partition-key +
 * primary-key name lists) — the exact output of the {@link PaimonCatalogOps#schemaAt} schema-file read,
 * which is a <i>pure function</i> of {@code (table-identity, schemaId)} because a committed paimon
 * schemaId's schema content is write-once. The built {@code ConnectorTableSchema} is deliberately NOT
 * cached: it embeds the live {@code coreOptions()} of the table, which are not keyed by schemaId and could
 * go stale — so the metadata rebuilds it fresh per query from the live table while only the schema read is
 * memoized. The single behavioral delta vs the pre-fix path is therefore "the {@code schemaAt} read is
 * skipped on a repeat"; everything else is unchanged.
 *
 * <p><b>No performance regression (by construction):</b> on a miss the loader runs exactly as before plus
 * an O(1) put; on a hit the {@code schemaAt} read is skipped (strictly faster); on overflow/eviction or a
 * concurrent same-key double-load the value is simply re-read (= the pre-fix behavior). The value is
 * immutable, so a cached entry is safe to share across queries and a flush never yields a stale read.
 */
final class PaimonSchemaAtMemo {

    /** Default best-effort bound; the keyspace (table, branch, schemaId) is naturally tiny. */
    static final int DEFAULT_MAX_SIZE = 10000;

    private final Map<MemoKey, PaimonCatalogOps.PaimonSchemaSnapshot> cache = new ConcurrentHashMap<>();
    private final int maxSize;

    PaimonSchemaAtMemo(int maxSize) {
        this.maxSize = maxSize;
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
        MemoKey key = new MemoKey(handle, schemaId);
        PaimonCatalogOps.PaimonSchemaSnapshot hit = cache.get(key);
        if (hit != null) {
            return hit;
        }
        PaimonCatalogOps.PaimonSchemaSnapshot loaded = loader.get();
        // Best-effort size bound (honors the "bounded memo" requirement). The keyspace is
        // (table, branch, schemaId) — naturally tiny — so this valve effectively never fires; values are
        // immutable, so flushing only causes re-reads (= the pre-fix behavior), never a stale/wrong value.
        if (cache.size() >= maxSize) {
            cache.clear();
        }
        PaimonCatalogOps.PaimonSchemaSnapshot prev = cache.putIfAbsent(key, loaded);
        return prev != null ? prev : loaded;
    }

    /** Test-only: current number of cached entries. */
    int size() {
        return cache.size();
    }

    /**
     * Drop every memoized schema for {@code (db, table)} across all schemaIds / sys-tables / branches. Wired
     * onto {@code REFRESH TABLE} and — via the generic {@code PluginDrivenExternalCatalog} DDL hook — onto a
     * Doris-issued DROP/CREATE of the same name, so a drop+recreate that reuses a schemaId (e.g. schema 0)
     * with different content does not serve a stale time-travel schema. The memo value is immutable, so
     * dropping an entry only forces a re-read (the pre-memo behavior), never a stale/wrong value.
     */
    void invalidate(String databaseName, String tableName) {
        cache.keySet().removeIf(key -> key.matches(databaseName, tableName));
    }

    /** Drop the whole memo. Wired onto {@code REFRESH CATALOG} (alongside the connector rebuild). */
    void invalidateAll() {
        cache.clear();
    }

    /**
     * Cache key = the handle's identity (db, table, sysTableName, branchName) plus the pinned schemaId.
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

        MemoKey(PaimonTableHandle handle, long schemaId) {
            this.databaseName = handle.getDatabaseName();
            this.tableName = handle.getTableName();
            this.sysTableName = handle.getSysTableName();
            this.branchName = handle.getBranchName();
            this.schemaId = schemaId;
        }

        /** True if this key belongs to {@code (db, table)} (any schemaId / sys-table / branch). */
        boolean matches(String db, String table) {
            return databaseName.equals(db) && tableName.equals(table);
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
                    && databaseName.equals(that.databaseName)
                    && tableName.equals(that.tableName)
                    && Objects.equals(sysTableName, that.sysTableName)
                    && Objects.equals(branchName, that.branchName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(databaseName, tableName, sysTableName, branchName, schemaId);
        }
    }
}
