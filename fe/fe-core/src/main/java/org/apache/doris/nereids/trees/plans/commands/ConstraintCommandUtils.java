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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.datasource.CatalogIf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Shared locking helpers for constraint DDL commands. */
final class ConstraintCommandUtils {
    private ConstraintCommandUtils() {
    }

    /** Lock all databases referenced by a constraint in a deterministic order. */
    static LockedDatabases lockCurrentDatabases(List<TableNameInfo> tableNameInfos)
            throws DdlException {
        Map<String, ResolvedDatabase> resolvedByName = new LinkedHashMap<>();
        for (TableNameInfo tableNameInfo : tableNameInfos) {
            String databaseKey = databaseKey(tableNameInfo);
            if (!resolvedByName.containsKey(databaseKey)) {
                CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog = Env.getCurrentEnv()
                        .getCatalogMgr().getCatalogOrDdlException(tableNameInfo.getCtl());
                DatabaseIf<? extends TableIf> database =
                        catalog.getDbOrDdlException(tableNameInfo.getDb());
                resolvedByName.put(databaseKey,
                        new ResolvedDatabase(databaseKey, tableNameInfo, catalog, database));
            }
        }
        List<ResolvedDatabase> lockOrder = new ArrayList<>(resolvedByName.values());
        lockOrder.sort(Comparator
                .comparingLong((ResolvedDatabase resolved) -> resolved.database.getId())
                .thenComparing(resolved -> resolved.databaseKey));
        for (ResolvedDatabase resolved : lockOrder) {
            resolved.database.readLock();
        }
        LockedDatabases lockedDatabases = new LockedDatabases(resolvedByName, lockOrder);
        try {
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
            lockedDatabases.close();
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
            TableIf table = lockedDatabases.get(tableNameInfo)
                    .getTableNullable(tableNameInfo.getTbl());
            if (table == null && requireAllTables) {
                throw new DdlException("Table changed while altering constraint on " + tableNameInfo);
            }
            tablesByName.put(tableKey(tableNameInfo), table);
            if (table != null && seenTables.put(table, Boolean.TRUE) == null) {
                lockOrder.add(table);
            }
        }
        lockOrder.sort(Comparator
                .comparingLong((TableIf table) -> table.getDatabase().getId())
                .thenComparing(table -> table.getDatabase().getCatalog().getName())
                .thenComparing(table -> table.getDatabase().getFullName())
                .thenComparingLong(TableIf::getId)
                .thenComparing(TableIf::getName));
        MetaLockUtils.writeLockTables(lockOrder);
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
        private final List<ResolvedDatabase> lockOrder;

        private LockedDatabases(Map<String, ResolvedDatabase> resolvedByName,
                List<ResolvedDatabase> lockOrder) {
            this.resolvedByName = resolvedByName;
            this.lockOrder = lockOrder;
        }

        DatabaseIf<? extends TableIf> get(TableNameInfo tableNameInfo) {
            return resolvedByName.get(databaseKey(tableNameInfo)).database;
        }

        @Override
        public void close() {
            for (int i = lockOrder.size() - 1; i >= 0; i--) {
                lockOrder.get(i).database.readUnlock();
            }
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
}
