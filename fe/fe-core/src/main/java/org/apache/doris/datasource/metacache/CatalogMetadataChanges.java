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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.cache.CacheCallbackContext;
import org.apache.doris.connector.cache.CatalogMetaCache;
import org.apache.doris.connector.cache.MetaCache;
import org.apache.doris.connector.cache.ScopePath;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The engine's semantic change boundary. FE names, remote connector identities and numeric derived state are
 * projected once and detached at one catalog publication barrier. Projection never loads remote metadata.
 * Connector domains own registrations/lifetime; resource scopes own invalidation, independently of domains.
 */
public final class CatalogMetadataChanges {
    private CatalogMetadataChanges() {
    }

    public static ExternalObjectLog table(ExternalTable table, boolean lookup, long updateTime) {
        ExternalObjectLog change = lookup
                ? ExternalObjectLog.createForExplicitRefreshTable(table.getCatalog().getId(),
                        table.getDb().getFullName(), table.getName(), updateTime)
                : ExternalObjectLog.createForRefreshTable(table.getCatalog().getId(),
                        table.getDb().getFullName(), table.getName(), updateTime);
        change.setDbId(table.getDb().getId());
        change.setTableId(table.getId());
        return change.withRemoteNames(table.getRemoteDbName(), table.getRemoteName());
    }

    public static Optional<MissingTableObservation> observeTableLookup(ExternalTable table) {
        ExternalDatabase<?> database = table.getDb();
        return database.observeTableLookup(table)
                .map(observation -> new MissingTableObservation(table, observation));
    }

    /** A read observation repairs only this FE; it is not a committed external DDL journal record. */
    public static final class MissingTableObservation {
        private final ExternalCatalog catalog;
        private final ExternalObjectLog change;
        private final MetaCache.Observation observation;

        private MissingTableObservation(ExternalTable table, MetaCache.Observation observation) {
            this.catalog = table.getCatalog();
            this.change = table(table, true, 0);
            this.observation = observation;
        }

        public void missing() {
            List<ScopePath> scopes = new ArrayList<>();
            try {
                plan(catalog, change, scopes);
            } finally {
                observation.invalidateScopes(scopes);
            }
        }
    }

    public static void apply(ExternalCatalog catalog, ExternalObjectLog change) {
        apply(catalog, java.util.Collections.singletonList(change));
    }

    /** A rename event can span databases; all old/new coordinates share one publication barrier. */
    public static void apply(ExternalCatalog catalog, List<ExternalObjectLog> changes) {
        List<ScopePath> scopes = new ArrayList<>();
        RuntimeException failure = null;
        try {
            for (ExternalObjectLog change : changes) {
                try {
                    updateCachedTable(plan(catalog, change, scopes), change);
                } catch (RuntimeException error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
        } finally {
            CatalogMetaCache owner = catalog.getMetaCacheOwnerIfPresent();
            if (owner != null) {
                owner.invalidateScopes(scopes);
            }
            if (Env.getCurrentEnv().getSortedPartitionsCacheManager() != null) {
                for (ExternalObjectLog change : changes) {
                    invalidateSortedPartitions(catalog, change);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void invalidateSortedPartitions(ExternalCatalog catalog, ExternalObjectLog change) {
        if (change.getTableName() == null) {
            Env.getCurrentEnv().getSortedPartitionsCacheManager().invalidateAll();
        } else {
            Env.getCurrentEnv().getSortedPartitionsCacheManager().invalidateTable(
                    catalog.getName(), change.getDbName(), change.getTableName());
            if (change.getNewTableName() != null) {
                Env.getCurrentEnv().getSortedPartitionsCacheManager().invalidateTable(
                        catalog.getName(), change.getDbName(), change.getNewTableName());
            }
        }
    }

    private static void updateCachedTable(Optional<? extends ExternalTable> table, ExternalObjectLog change) {
        table.ifPresent(value -> {
            if (change.getPartitionNames() == null) {
                value.unsetObjectCreated();
            }
            if (change.getLastUpdateTime() > 0) {
                value.setUpdateTime(change.getLastUpdateTime());
            }
        });
    }

    private static Optional<? extends ExternalTable> plan(
            ExternalCatalog catalog, ExternalObjectLog change, List<ScopePath> scopes) {
        Optional<ExternalDatabase<? extends ExternalTable>> db = catalog.getDbForReplay(change.getDbName());
        Optional<? extends ExternalTable> table = Strings.isNullOrEmpty(change.getTableName())
                ? Optional.empty() : db.flatMap(database -> database.getTableForReplay(change.getTableName()));
        long dbId = change.getDbId() != 0 ? change.getDbId()
                : Util.genIdByName(catalog.getName(), change.getDbName());
        long tableId = change.getTableId() != 0 ? change.getTableId()
                : change.getTableName() == null ? 0L
                : Util.genIdByName(catalog.getName(), change.getDbName(), change.getTableName());
        String remoteDb = change.getRemoteDbName() != null ? change.getRemoteDbName()
                : db.map(ExternalDatabase::getRemoteName).orElse(null);
        String remoteTable = change.getRemoteTableName() != null ? change.getRemoteTableName()
                : table.map(ExternalTable::getRemoteName).orElse(null);
        Connector connector = catalog instanceof PluginDrivenExternalCatalog
                ? ((PluginDrivenExternalCatalog) catalog).getConnectorIfPresent() : null;
        Set<ScopePath> remote = new LinkedHashSet<>();
        boolean database = Strings.isNullOrEmpty(change.getTableName());
        boolean partitions = change.getPartitionNames() != null;
        String canonicalDb = catalog.resolveDatabaseNameForInvalidation(change.getDbName());
        if (!canonicalDb.equals(change.getDbName())) {
            scopes.add(local(ScopePath.database(canonicalDb)));
            scopes.add(id(ScopePath.database(Long.toString(Util.genIdByName(catalog.getName(), canonicalDb)))));
        }
        if (database) {
            scopes.add(local(ScopePath.database(change.getDbName())));
            scopes.add(local(ScopePath.databaseCollection()));
            scopes.add(id(ScopePath.database(Long.toString(dbId))));
            remote.add(remoteDb == null ? ScopePath.catalog() : ScopePath.database(remoteDb));
            remote.add(ScopePath.databaseCollection());
        } else {
            scopes.add(id(ScopePath.table(Long.toString(dbId), Long.toString(tableId))));
            if (!partitions) {
                if (db.isPresent() && (change.isInvalidateTableLookup() || change.getNewTableName() != null)) {
                    addCanonicalTableScopes(catalog, db.get(), dbId, change.getTableName(), scopes);
                }
                scopes.add(local(change.isInvalidateTableLookup()
                        ? ScopePath.table(change.getDbName(), change.getTableName())
                        : ScopePath.tableMetadata(change.getDbName(), change.getTableName())));
                if (change.isInvalidateTableLookup() || change.getNewTableName() != null) {
                    // A first object/row-count load can publish an old spelling after scope planning.
                    // LOCAL scopes share case-folded identities; numeric derived caches need this DB fence.
                    if (catalog.getLowerCaseTableNames() == 2) {
                        scopes.add(id(ScopePath.database(Long.toString(dbId))));
                    }
                    scopes.add(local(ScopePath.tableCollection(change.getDbName())));
                }
            }
            if (remoteDb == null || remoteTable == null) {
                // Old logs carry only local identities. Widen only the remote coordinate, without I/O.
                remote.add(remoteDb == null ? ScopePath.catalog() : ScopePath.database(remoteDb));
            } else {
                remote.add(ScopePath.table(remoteDb, remoteTable));
            }
            if (remoteDb != null && (change.isInvalidateTableLookup() || change.getNewTableName() != null)) {
                remote.add(ScopePath.tableCollection(remoteDb));
            }
            if (change.getNewTableName() != null) {
                if (db.isPresent()) {
                    addCanonicalTableScopes(catalog, db.get(), dbId, change.getNewTableName(), scopes);
                }
                scopes.add(local(ScopePath.table(change.getDbName(), change.getTableName())));
                scopes.add(local(ScopePath.table(change.getDbName(), change.getNewTableName())));
                scopes.add(id(ScopePath.table(Long.toString(dbId), Long.toString(Util.genIdByName(
                        catalog.getName(), change.getDbName(), change.getNewTableName())))));
                remote.add(remoteDb != null && change.getRemoteNewTableName() != null
                        ? ScopePath.table(remoteDb, change.getRemoteNewTableName())
                        : remoteDb == null ? ScopePath.catalog() : ScopePath.database(remoteDb));
            }
        }
        try {
            if (connector != null) {
                new CacheCallbackContext(connector.getClass().getClassLoader()).run(() -> {
                    if (partitions && remoteDb != null && remoteTable != null) {
                        remote.clear();
                        remote.addAll(connector.partitionCacheScopes(
                                remoteDb, remoteTable, change.getPartitionNames()));
                    }
                    List<ScopePath> dependencies = new ArrayList<>();
                    for (ScopePath scope : remote) {
                        if (scope.database() != null) {
                            dependencies.addAll(connector.cacheDependencies(scope));
                        }
                    }
                    remote.addAll(dependencies);
                });
            }
        } catch (RuntimeException | Error failure) {
            // A gateway failed to project its dependencies. Fence every remote cache rather than retain a
            // dependency whose identity is unknown, then report the failure to the caller.
            remote.add(ScopePath.catalog());
            throw failure;
        } finally {
            remote.forEach(scope -> scopes.add(scope.in(ScopePath.Coordinates.REMOTE)));
        }
        return table;
    }

    private static void addCanonicalTableScopes(ExternalCatalog catalog, ExternalDatabase<?> database,
            long dbId, String tableName, List<ScopePath> scopes) {
        // Case-insensitive lookup can retain a differently cased owner with a different deterministic ID.
        String canonical = database.resolveTableNameForInvalidation(tableName);
        scopes.add(local(ScopePath.table(database.getFullName(), canonical)));
        scopes.add(id(ScopePath.table(Long.toString(dbId), Long.toString(Util.genIdByName(
                catalog.getName(), database.getFullName(), canonical)))));
    }

    private static ScopePath local(ScopePath scope) {
        return scope.in(ScopePath.Coordinates.LOCAL);
    }

    private static ScopePath id(ScopePath scope) {
        return scope.in(ScopePath.Coordinates.ID);
    }
}
