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
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.MetaCacheDefinition;
import org.apache.doris.connector.cache.ScopePath;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * What one ADBC catalog remembers about the remote source between statements.
 *
 * <p>The engine builds a fresh {@link AdbcConnectorMetadata} per statement, so without this every query paid
 * three remote round trips before planning even started -- list the databases, list one database's tables,
 * read one table's schema -- two of which list <em>every</em> object and therefore cost more the larger the
 * source is. This lives on {@link AdbcConnector} instead, whose lifetime is the catalog's.
 *
 * <p><b>Never answers "there is no such object" from memory.</b> "I just created it and Doris says it isn't
 * there" is the one staleness a user cannot reason their way out of, so nothing here is allowed to produce
 * it. Two rules keep that true, and both are load-bearing:
 *
 * <ul>
 *   <li>A lookup that fails re-reads the listing ({@link #reloadTableNames} / {@link #reloadNamespaces})
 *       before concluding anything -- a remote call, but only on the path that was about to raise an error
 *       anyway.</li>
 *   <li>{@code listDatabaseNames} / {@code listTableNames} are served <b>live</b> and merely refresh what is
 *       remembered. They look like reports, but the engine loads its own name cache from them and then
 *       decides existence from that -- including the last-chance re-list it does for a name it has never
 *       seen ({@code ExternalDatabase.buildTableForInit}). Answering those from a cache would make the
 *       engine's re-check meaningless and is exactly how a newly created table becomes unreachable.</li>
 * </ul>
 *
 * <p>What is left cached is what a query actually repeats: resolving a database and a table name, and
 * reading a schema. The engine caches the listings itself, so nothing is paid twice for them.
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

    private final CatalogMetaCache owner;
    private final MetaCache<String, List<AdbcNamespace>> namespaces;
    private final MetaCache<TableNameKey, List<String>> tableNames;
    private final MetaCache<TableKey, Schema> tableSchemas;

    /**
     * Built in {@link AdbcConnector}'s constructor, which also runs on an FE replaying the edit log, so this
     * reads properties and nothing else -- no driver, no filesystem, no remote call.
     */
    public AdbcMetadataCache(Map<String, String> properties) {
        this(new CatalogMetaCache(), properties);
    }

    AdbcMetadataCache(CatalogMetaCache owner, Map<String, String> properties) {
        this.owner = Objects.requireNonNull(owner, "owner can not be null");
        CacheSpec spec = cacheSpec(properties);
        this.namespaces = owner.create(MetaCacheDefinition
                .<String, List<AdbcNamespace>>builder("adbc-namespaces", spec, ignored -> ScopePath.catalog())
                .build());
        this.tableNames = owner.create(MetaCacheDefinition
                .<TableNameKey, List<String>>builder("adbc-table-names", spec,
                        key -> ScopePath.database(key.dorisDbName))
                .build());
        this.tableSchemas = owner.create(MetaCacheDefinition
                .<TableKey, Schema>builder("adbc-table-schema", spec,
                        key -> ScopePath.table(key.dorisDbName, key.table))
                .build());
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
        return tableNames.get(TableNameKey.forNamespace(namespace), ignored -> loader.get());
    }

    /** Re-reads one database's table listing, replacing whatever was remembered. See the class note. */
    List<String> reloadTableNames(AdbcNamespace namespace, Supplier<List<String>> loader) {
        tableNames.invalidateKey(TableNameKey.forNamespace(namespace));
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
        owner.invalidateTable(dbName, tableName);
        // A table refresh must also forget the parent name listing, so a remotely-created table can be found.
        // This is an ancestor materialization, not a sibling-cache dependency, and therefore cannot be selected
        // by the table's descendant scope.
        tableNames.invalidateKey(TableNameKey.forDatabase(dbName));
    }

    /** {@code REFRESH DATABASE}: that database's table listing and every schema in it. */
    void invalidateDb(String dbName) {
        owner.invalidateDatabase(dbName);
    }

    /**
     * {@code REFRESH CATALOG}: everything, including which databases exist -- the only statement that both
     * names no database to invalidate and is reached for when the catalog's own shape changed.
     */
    void invalidateAll() {
        owner.invalidateCatalog();
    }

    /**
     * A database's listing is addressed by the Doris database name. The remote namespace is carried only so
     * the miss loader can query it; it is deliberately not part of identity because Doris cannot address two
     * remote namespaces that project to the same database name separately.
     */
    private static final class TableNameKey {
        private final String dorisDbName;

        private TableNameKey(String dorisDbName) {
            this.dorisDbName = Objects.requireNonNull(dorisDbName, "dorisDbName can not be null");
        }

        private static TableNameKey forNamespace(AdbcNamespace namespace) {
            return new TableNameKey(namespace.dorisDatabaseName());
        }

        private static TableNameKey forDatabase(String database) {
            return new TableNameKey(database);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof TableNameKey && dorisDbName.equals(((TableNameKey) o).dorisDbName);
        }

        @Override
        public int hashCode() {
            return dorisDbName.hashCode();
        }
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
