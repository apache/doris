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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.cache.MetaCacheEntry;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

/**
 * What one ADBC catalog remembers about the remote source between statements.
 *
 * <p>The engine builds a fresh {@link AdbcConnectorMetadata} per statement, so without this every query paid
 * three remote round trips before planning even started -- list the databases, list one database's tables,
 * read one table's schema -- two of which list <em>every</em> object and therefore cost more the larger the
 * source is. This lives on {@link AdbcConnector} instead, whose lifetime is the catalog's.
 *
 * <p><b>Never answers "there is no such object" from memory.</b> A cached listing is a fine basis for
 * {@code SHOW TABLES}, which is a report on the source and may lag it, but not for deciding that a name does
 * not exist: a table created remotely a second ago would then be missing for as long as the entry lives, and
 * "I just created it and Doris says it isn't there" is the one staleness a user cannot reason their way out
 * of. So a caller that fails to find a name re-reads the listing through {@link #reloadTableNames} /
 * {@link #reloadNamespaces} before concluding anything -- a remote call, but only on the path that was about
 * to raise an error anyway.
 *
 * <p><b>Safe to share between users only because ADBC has no per-user identity.</b> Every key here is a
 * plain object name, so anything cached under one is served to whoever asks next. That is correct exactly
 * while a catalog reaches the source as one fixed principal, which is what this connector does (it declares
 * no {@code SUPPORTS_USER_SESSION}); the day it projects the querying user onto the connection, these keys
 * must carry that identity or one user's metadata will be served to another. Pinned by
 * {@code AdbcConnectorCacheHookTest}.
 */
public final class AdbcMetadataCache {

    private static final String ENGINE = "adbc";

    /**
     * All three entries share one set of knobs ({@code meta.cache.adbc.metadata.*}). They are read together,
     * dropped together and describe the same thing -- the shape of the remote source -- so separate knobs
     * would be three ways to spell one intent.
     */
    static final String ENTRY = "metadata";

    /**
     * Ten minutes, where the shared framework's default is a day. An ADBC source is another live database,
     * not a warehouse of immutable files: its tables are altered by other people's DDL at any time, and
     * nothing tells Doris when. This is the ceiling on how long someone who forgot to REFRESH keeps seeing
     * the old shape, and it is deliberately far below the framework value -- do not "correct" it to 86400.
     */
    private static final long DEFAULT_TTL_SECOND = 600L;

    private static final long DEFAULT_CAPACITY = 1000L;

    /** The database listing is a single value, so it needs a single key. */
    private static final String THE_ONLY_KEY = "";

    private final MetaCacheEntry<String, List<AdbcNamespace>> namespaces;
    private final MetaCacheEntry<AdbcNamespace, List<String>> tableNames;
    private final MetaCacheEntry<TableKey, Schema> tableSchemas;

    /**
     * Built in {@link AdbcConnector}'s constructor, which also runs on an FE replaying the edit log, so this
     * reads properties and nothing else -- no driver, no filesystem, no remote call.
     */
    public AdbcMetadataCache(Map<String, String> properties) {
        CacheSpec spec = cacheSpec(properties);
        this.namespaces = entry("adbc-namespaces", spec);
        this.tableNames = entry("adbc-table-names", spec);
        this.tableSchemas = entry("adbc-table-schema", spec);
    }

    static CacheSpec cacheSpec(Map<String, String> properties) {
        return CacheSpec.fromProperties(properties, propertySpec());
    }

    /**
     * The knobs this cache reads, named once so that whoever validates them at {@code CREATE CATALOG} cannot
     * drift from whoever reads them at runtime -- a validator guarding a key nobody reads would pass every
     * catalog and protect nothing.
     */
    static CacheSpec.PropertySpec propertySpec() {
        return CacheSpec.metaCachePropertySpec(ENGINE, ENTRY,
                CacheSpec.of(true, DEFAULT_TTL_SECOND, DEFAULT_CAPACITY));
    }

    /**
     * Contextual-only with manual miss load, as the iceberg caches are: the remote read runs OUTSIDE
     * Caffeine's compute lock, so a slow source does not stall unrelated keys, the driver's own exception
     * arrives unwrapped, and a load that failed is not remembered as an answer.
     */
    private static <K, V> MetaCacheEntry<K, V> entry(String name, CacheSpec spec) {
        return new MetaCacheEntry<>(name, null, spec, ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    // ========= reads =========

    List<AdbcNamespace> namespaces(Supplier<List<AdbcNamespace>> loader) {
        return namespaces.get(THE_ONLY_KEY, ignored -> loader.get());
    }

    /** Re-reads the database listing, replacing whatever was remembered. See the class note. */
    List<AdbcNamespace> reloadNamespaces(Supplier<List<AdbcNamespace>> loader) {
        namespaces.invalidateKey(THE_ONLY_KEY);
        return namespaces(loader);
    }

    List<String> tableNames(AdbcNamespace namespace, Supplier<List<String>> loader) {
        return tableNames.get(namespace, ignored -> loader.get());
    }

    /** Re-reads one database's table listing, replacing whatever was remembered. See the class note. */
    List<String> reloadTableNames(AdbcNamespace namespace, Supplier<List<String>> loader) {
        tableNames.invalidateKey(namespace);
        return tableNames(namespace, loader);
    }

    Schema tableSchema(AdbcTableHandle handle, Supplier<Schema> loader) {
        return tableSchemas.get(new TableKey(handle), ignored -> loader.get());
    }

    // ========= what each REFRESH forgets =========

    /**
     * {@code REFRESH TABLE}: that table's schema, and its database's table listing.
     *
     * <p>Dropping the listing too is not collateral damage. A table created remotely after the listing was
     * cached is absent from it, and REFRESH TABLE is precisely what a user reaches for to make Doris look at
     * that table again -- if the listing survived, no statement short of REFRESH CATALOG could ever bring the
     * new name in, and the one the user tried would appear to do nothing.
     */
    void invalidateTable(String dbName, String tableName) {
        tableSchemas.invalidateIf(key -> key.is(dbName, tableName));
        tableNames.invalidateIf(namespace -> namespace.dorisDatabaseName().equals(dbName));
    }

    /** {@code REFRESH DATABASE}: that database's table listing and every schema in it. */
    void invalidateDb(String dbName) {
        tableSchemas.invalidateIf(key -> key.isIn(dbName));
        tableNames.invalidateIf(namespace -> namespace.dorisDatabaseName().equals(dbName));
    }

    /**
     * {@code REFRESH CATALOG}: everything, including which databases exist -- the only statement that both
     * names no database to invalidate and is reached for when the catalog's own shape changed.
     */
    void invalidateAll() {
        namespaces.invalidateAll();
        tableNames.invalidateAll();
        tableSchemas.invalidateAll();
    }

    /**
     * One table, identified the way Doris addresses it. The namespace is carried whole rather than reduced to
     * the Doris database name so that two remote namespaces which happen to present the same name cannot
     * share a schema.
     */
    private static final class TableKey {

        private final AdbcNamespace namespace;
        private final String table;
        // Derived from the namespace, so it takes no part in identity; kept because the invalidate hooks
        // match on it and AdbcNamespace#dorisDatabaseName throws for a namespace with no name at all.
        private final String dorisDbName;

        private TableKey(AdbcTableHandle handle) {
            this.namespace = handle.getNamespace();
            this.table = handle.getRemoteTable();
            this.dorisDbName = handle.getDorisDbName();
        }

        private boolean is(String db, String tableName) {
            return dorisDbName.equals(db) && table.equals(tableName);
        }

        private boolean isIn(String db) {
            return dorisDbName.equals(db);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof TableKey)) {
                return false;
            }
            TableKey other = (TableKey) o;
            return namespace.equals(other.namespace) && table.equals(other.table);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, table);
        }

        @Override
        public String toString() {
            return namespace + "." + table;
        }
    }
}
