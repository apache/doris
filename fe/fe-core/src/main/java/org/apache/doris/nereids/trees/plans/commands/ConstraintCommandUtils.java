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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Shared locking helpers for constraint DDL commands. */
final class ConstraintCommandUtils {
    private ConstraintCommandUtils() {
    }

    static ExternalCatalogSnapshots snapshotExternalCatalogs(List<TableNameInfo> tableNameInfos)
            throws DdlException {
        Map<Long, ExternalCatalogSnapshot> snapshots = new LinkedHashMap<>();
        for (TableNameInfo tableNameInfo : tableNameInfos) {
            CatalogIf<?> catalog = Env.getCurrentEnv().getCatalogMgr()
                    .getCatalogOrDdlException(tableNameInfo.getCtl());
            if (catalog instanceof ExternalCatalog) {
                ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
                snapshots.putIfAbsent(externalCatalog.getId(),
                        new ExternalCatalogSnapshot(tableNameInfo.getCtl(), externalCatalog,
                                externalCatalog.snapshotConstraintMetadata()));
            }
        }
        return new ExternalCatalogSnapshots(snapshots);
    }

    /** Lock external catalog fences and internal databases referenced by a constraint. */
    static LockedDatabases lockCurrentDatabases(List<TableNameInfo> tableNameInfos,
            ExternalCatalogSnapshots externalCatalogSnapshots, List<TableIf> analyzedTables)
            throws DdlException {
        Map<String, TableIf> analyzedExternalTables = new LinkedHashMap<>();
        for (TableIf table : analyzedTables) {
            if (table != null
                    && table.getDatabase().getCatalog() instanceof ExternalCatalog) {
                TableNameInfo tableNameInfo = TableNameInfoUtils.fromCatalogDb(
                        table.getDatabase().getCatalog(), table.getDatabase(), table);
                analyzedExternalTables.put(tableKey(tableNameInfo), table);
            }
        }
        LockedExternalCatalogs lockedExternalCatalogs = externalCatalogSnapshots.lock();
        Map<String, ResolvedDatabase> resolvedByName = new LinkedHashMap<>();
        List<ResolvedDatabase> lockOrder = new ArrayList<>();
        LockedDatabases lockedDatabases = null;
        int lockedDatabaseCount = 0;
        try {
            for (TableNameInfo tableNameInfo : tableNameInfos) {
                String databaseKey = databaseKey(tableNameInfo);
                if (!resolvedByName.containsKey(databaseKey)) {
                    CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = Env.getCurrentEnv()
                            .getCatalogMgr().getCatalogOrDdlException(tableNameInfo.getCtl());
                    if (catalog instanceof ExternalCatalog) {
                        externalCatalogSnapshots.requireSame(tableNameInfo.getCtl(), catalog);
                        continue;
                    }
                    DatabaseIf<? extends TableIf> database =
                            catalog.getDbOrDdlException(tableNameInfo.getDb());
                    resolvedByName.put(databaseKey,
                            new ResolvedDatabase(databaseKey, tableNameInfo, catalog, database));
                }
            }
            Map<String, TableIf> resolvedTables = new LinkedHashMap<>(analyzedExternalTables);
            for (TableNameInfo tableNameInfo : tableNameInfos) {
                ResolvedDatabase resolvedDatabase = resolvedByName.get(databaseKey(tableNameInfo));
                if (resolvedDatabase != null) {
                    resolvedTables.put(tableKey(tableNameInfo),
                            resolvedDatabase.database.getTableNullable(tableNameInfo.getTbl()));
                }
            }
            lockOrder.addAll(resolvedByName.values());
            lockOrder.sort(Comparator
                    .comparingLong((ResolvedDatabase resolved) -> resolved.database.getId())
                    .thenComparing(resolved -> resolved.databaseKey));
            for (ResolvedDatabase resolved : lockOrder) {
                if (!resolved.database.tryReadLock(
                        Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
                    throw new DdlException(
                            "Failed to acquire database read lock while altering constraint on "
                                    + resolved.tableNameInfo + ". Try again");
                }
                lockedDatabaseCount++;
            }
            lockedDatabases = new LockedDatabases(
                    resolvedByName, resolvedTables, lockOrder, lockedExternalCatalogs);
            for (ResolvedDatabase resolved : lockOrder) {
                if (Env.getCurrentEnv().getCatalogMgr().getCatalog(
                        resolved.tableNameInfo.getCtl()) != resolved.catalog
                        || resolved.catalog.getDbNullable(resolved.tableNameInfo.getDb())
                                != resolved.database) {
                    throw new DdlException(
                            "Database changed while altering constraint on "
                                    + resolved.tableNameInfo);
                }
            }
            return lockedDatabases;
        } catch (DdlException | RuntimeException e) {
            if (lockedDatabases == null) {
                for (int i = lockedDatabaseCount - 1; i >= 0; i--) {
                    lockOrder.get(i).database.readUnlock();
                }
                lockedExternalCatalogs.close();
            } else {
                lockedDatabases.close();
            }
            throw e;
        }
    }

    /** Lock all currently resolved tables in the same deterministic order used by constraint ADD and DROP. */
    static LockedTables lockCurrentTables(
            LockedDatabases lockedDatabases, List<TableNameInfo> tableNameInfos)
            throws DdlException {
        return lockCurrentTables(lockedDatabases, tableNameInfos, true);
    }

    private static LockedTables lockCurrentTables(
            LockedDatabases lockedDatabases, List<TableNameInfo> tableNameInfos,
            boolean requireAllTables) throws DdlException {
        Map<String, TableIf> tablesByName = new LinkedHashMap<>();
        Map<TableIf, Boolean> seenTables = new IdentityHashMap<>();
        List<TableIf> lockOrder = new ArrayList<>();
        for (TableNameInfo tableNameInfo : tableNameInfos) {
            TableIf table = lockedDatabases.getCurrentTable(tableNameInfo);
            if (table == null && requireAllTables) {
                throw new DdlException("Table changed while altering constraint on " + tableNameInfo);
            }
            tablesByName.put(tableKey(tableNameInfo), table);
            if (table != null
                    && !(table.getDatabase() instanceof ExternalDatabase)
                    && seenTables.put(table, Boolean.TRUE) == null) {
                lockOrder.add(table);
            }
        }
        lockOrder.sort(Comparator
                .comparingLong(TableIf::getId)
                .thenComparing(table -> table.getDatabase().getCatalog().getName())
                .thenComparing(table -> table.getDatabase().getFullName())
                .thenComparing(TableIf::getName));
        if (!MetaLockUtils.tryWriteLockTablesIfExist(
                lockOrder, Config.catalog_try_lock_timeout_ms, TimeUnit.MILLISECONDS)) {
            throw new DdlException(
                    "Failed to acquire table locks while altering constraints. Try again");
        }
        return new LockedTables(tablesByName, lockOrder);
    }

    /** Lock all existing tables, allowing name-only cleanup for metadata whose external table disappeared. */
    static LockedTables lockCurrentTablesIfPresent(
            LockedDatabases lockedDatabases, List<TableNameInfo> tableNameInfos)
            throws DdlException {
        return lockCurrentTables(lockedDatabases, tableNameInfos, false);
    }

    private static String databaseKey(TableNameInfo tableNameInfo) {
        return tableNameInfo.getCtl() + "\0" + tableNameInfo.getDb();
    }

    private static String tableKey(TableNameInfo tableNameInfo) {
        return databaseKey(tableNameInfo) + "\0" + tableNameInfo.getTbl();
    }

    static TableNameInfo qualifyTableName(ConnectContext ctx, List<String> nameParts) {
        String catalogName = ctx.getCurrentCatalog() == null
                ? "internal" : ctx.getCurrentCatalog().getName();
        if (nameParts.size() == 1) {
            return new TableNameInfo(catalogName, ctx.getDatabase(), nameParts.get(0));
        }
        if (nameParts.size() == 2) {
            return new TableNameInfo(catalogName, nameParts.get(0), nameParts.get(1));
        }
        return new TableNameInfo(nameParts);
    }

    static boolean sameTables(List<TableNameInfo> first, List<TableNameInfo> second) {
        Set<String> firstKeys = new HashSet<>();
        for (TableNameInfo tableNameInfo : first) {
            firstKeys.add(tableKey(tableNameInfo));
        }
        Set<String> secondKeys = new HashSet<>();
        for (TableNameInfo tableNameInfo : second) {
            secondKeys.add(tableKey(tableNameInfo));
        }
        return firstKeys.equals(secondKeys);
    }

    static final class LockedDatabases implements AutoCloseable {
        private final Map<String, ResolvedDatabase> resolvedByName;
        private final Map<String, TableIf> resolvedTables;
        private final List<ResolvedDatabase> lockOrder;
        private final LockedExternalCatalogs lockedExternalCatalogs;

        private LockedDatabases(Map<String, ResolvedDatabase> resolvedByName,
                Map<String, TableIf> resolvedTables, List<ResolvedDatabase> lockOrder,
                LockedExternalCatalogs lockedExternalCatalogs) {
            this.resolvedByName = resolvedByName;
            this.resolvedTables = resolvedTables;
            this.lockOrder = lockOrder;
            this.lockedExternalCatalogs = lockedExternalCatalogs;
        }

        TableIf getCurrentTable(TableNameInfo tableNameInfo) throws DdlException {
            ResolvedDatabase resolvedDatabase =
                    resolvedByName.get(databaseKey(tableNameInfo));
            if (resolvedDatabase == null) {
                return resolvedTables.get(tableKey(tableNameInfo));
            }
            DatabaseIf<? extends TableIf> database = resolvedDatabase.database;
            TableIf resolvedTable = resolvedTables.get(tableKey(tableNameInfo));
            TableIf currentTable = database.getTableNullable(tableNameInfo.getTbl());
            if (currentTable != resolvedTable) {
                throw new DdlException(
                        "Table changed while altering constraint on " + tableNameInfo);
            }
            return resolvedTable;
        }

        @Override
        public void close() {
            for (int i = lockOrder.size() - 1; i >= 0; i--) {
                lockOrder.get(i).database.readUnlock();
            }
            lockedExternalCatalogs.close();
        }
    }

    static final class LockedTables implements AutoCloseable {
        private final Map<String, TableIf> tablesByName;
        private final List<TableIf> lockOrder;

        private LockedTables(Map<String, TableIf> tablesByName, List<TableIf> lockOrder) {
            this.tablesByName = tablesByName;
            this.lockOrder = lockOrder;
        }

        TableIf get(TableNameInfo tableNameInfo) {
            return tablesByName.get(tableKey(tableNameInfo));
        }

        void requireSame(TableNameInfo tableNameInfo, TableIf expectedTable)
                throws DdlException {
            if (get(tableNameInfo) != expectedTable) {
                throw new DdlException(
                        "Table metadata changed while altering constraint on " + tableNameInfo);
            }
        }

        @Override
        public void close() {
            MetaLockUtils.writeUnlockTables(lockOrder);
        }
    }

    private static final class ResolvedDatabase {
        private final String databaseKey;
        private final TableNameInfo tableNameInfo;
        private final CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog;
        private final DatabaseIf<? extends TableIf> database;

        private ResolvedDatabase(String databaseKey, TableNameInfo tableNameInfo,
                CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog,
                DatabaseIf<? extends TableIf> database) {
            this.databaseKey = databaseKey;
            this.tableNameInfo = tableNameInfo;
            this.catalog = catalog;
            this.database = database;
        }
    }

    static final class ExternalCatalogSnapshots {
        private final Map<Long, ExternalCatalogSnapshot> snapshots;

        private ExternalCatalogSnapshots(Map<Long, ExternalCatalogSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        private LockedExternalCatalogs lock() throws DdlException {
            List<ExternalCatalogSnapshot> lockOrder = new ArrayList<>(snapshots.values());
            lockOrder.sort(Comparator.comparingLong(snapshot -> snapshot.catalog.getId()));
            List<ExternalCatalog.ConstraintMetadataReadGuard> guards = new ArrayList<>();
            try {
                for (ExternalCatalogSnapshot snapshot : lockOrder) {
                    requireSame(snapshot.catalogName, snapshot.catalog);
                    guards.add(snapshot.catalog.lockConstraintMetadata(snapshot.sequence));
                    requireSame(snapshot.catalogName, snapshot.catalog);
                }
                return new LockedExternalCatalogs(guards);
            } catch (DdlException | RuntimeException e) {
                closeGuards(guards);
                throw e;
            }
        }

        private void requireSame(String catalogName, CatalogIf<?> catalog) throws DdlException {
            ExternalCatalogSnapshot snapshot = snapshots.get(catalog.getId());
            if (snapshot == null || snapshot.catalog != catalog
                    || Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName) != catalog) {
                throw new DdlException(
                        "External catalog changed while altering constraints on " + catalogName);
            }
        }
    }

    private static final class ExternalCatalogSnapshot {
        private final String catalogName;
        private final ExternalCatalog catalog;
        private final long sequence;

        private ExternalCatalogSnapshot(String catalogName, ExternalCatalog catalog, long sequence) {
            this.catalogName = catalogName;
            this.catalog = catalog;
            this.sequence = sequence;
        }
    }

    private static final class LockedExternalCatalogs implements AutoCloseable {
        private final List<ExternalCatalog.ConstraintMetadataReadGuard> guards;

        private LockedExternalCatalogs(
                List<ExternalCatalog.ConstraintMetadataReadGuard> guards) {
            this.guards = guards;
        }

        @Override
        public void close() {
            closeGuards(guards);
        }
    }

    private static void closeGuards(
            List<ExternalCatalog.ConstraintMetadataReadGuard> guards) {
        for (int i = guards.size() - 1; i >= 0; i--) {
            guards.get(i).close();
        }
    }
}
