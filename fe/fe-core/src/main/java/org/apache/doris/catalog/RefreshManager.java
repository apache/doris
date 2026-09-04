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

package org.apache.doris.catalog;

import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.persist.OperationType;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Manager for refresh database and table action
public class RefreshManager {
    private static final Logger LOG = LogManager.getLogger(RefreshManager.class);
    private ScheduledThreadPoolExecutor refreshScheduler = ThreadPoolManager.newDaemonScheduledThreadPool(1,
            "catalog-refresh-timer-pool", true);
    // Unit:SECONDS
    private static final int REFRESH_TIME_SEC = 5;
    // key is the id of a catalog, value is an array of length 2, used to store
    // the original refresh time and the current remaining time of the catalog
    private Map<Long, Integer[]> refreshMap = Maps.newConcurrentMap();

    // Refresh catalog
    public void handleRefreshCatalog(String catalogName, boolean invalidCache) throws UserException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogName);
        refreshCatalogInternal(catalog, invalidCache);
        CatalogLog log = CatalogLog.createForRefreshCatalog(catalog.getId(), invalidCache);
        Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_REFRESH_CATALOG, log);
    }

    public void replayRefreshCatalog(CatalogLog log) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog replaying refresh catalog {}", log.getCatalogId());
            return;
        }
        refreshCatalogInternal(catalog, log.isInvalidCache());
    }

    private void refreshCatalogInternal(CatalogIf catalog, boolean invalidCache) {
        if (catalog.isInternalCatalog()) {
            return;
        }
        ((ExternalCatalog) catalog).onRefreshCache(invalidCache);
        LOG.info("refresh catalog {} with invalidCache {}", catalog.getName(), invalidCache);
    }

    // Refresh database
    public void handleRefreshDb(String catalogName, String dbName) throws DdlException {
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();
        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh database in external catalog");
        }
        DatabaseIf db = catalog.getDbOrDdlException(dbName);
        refreshDbInternal((ExternalDatabase) db);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(catalog.getId(), db.getFullName());
        Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
    }

    public void replayRefreshDb(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh db: {}", log.debugForRefreshDb());
            return;
        }
        String localDbName = log.getDbName();
        if (Strings.isNullOrEmpty(localDbName)) {
            LOG.warn("refresh database replay log has no local name: {}", log.debugForRefreshDb());
            return;
        }
        long dbId = Util.genIdByName(catalog.getName(), localDbName);
        Optional<ExternalDatabase<? extends ExternalTable>> db = catalog.getDbForReplay(localDbName);

        if (!db.isPresent()) {
            ExternalMetaCacheMgr cacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
            try {
                invalidateAllConnectorCachesIfPresent(catalog);
            } finally {
                cacheMgr.invalidateDb(log.getCatalogId(), dbId, localDbName);
            }
            LOG.info("database object cache is cold when replaying refresh database; invalidated caches by "
                            + "local name {}: {}",
                    localDbName, log.debugForRefreshDb());
            return;
        }
        refreshDbInternal(db.get());
    }

    private void refreshDbInternal(ExternalDatabase db) {
        // Connector metadata is the row-count source, so invalidate it before engine and row-count caches.
        try {
            if (db.getCatalog() instanceof PluginDrivenExternalCatalog) {
                ((PluginDrivenExternalCatalog) db.getCatalog()).getConnector().invalidateDb(db.getRemoteName());
            }
        } finally {
            try {
                db.resetMetaToUninitialized();
            } finally {
                Env.getCurrentEnv().getExtMetaCacheMgr()
                        .invalidateDbRowCountCache(db.getCatalog().getId(), db.getId());
            }
        }
        LOG.info("refresh database {} in catalog {}", db.getFullName(), db.getCatalog().getName());
    }

    // Refresh table
    public void handleRefreshTable(String catalogName, String dbName, String tableName, boolean ignoreIfNotExists)
            throws DdlException {
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();
        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }

        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }
        long updateTime = System.currentTimeMillis();
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, updateTime);
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                catalog.getId(), db.getFullName(), table.getName(), updateTime);
        env.getEditLog().logRefreshExternalTable(log);
    }

    /** Records a committed remote mutation for replay before any fallible local cache refresh. */
    public void refreshTableAfterExternalMutation(ExternalTable table) {
        ExternalDatabase db = table.getDb();
        long updateTime = System.currentTimeMillis();
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                table.getCatalog().getId(), db.getFullName(), table.getName(), updateTime);
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
        refreshTableInternal(db, table, updateTime);
    }

    public void replayRefreshTable(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        String localDbName = log.getDbName();
        String localTableName = log.getTableName();
        if (Strings.isNullOrEmpty(localDbName) || Strings.isNullOrEmpty(localTableName)) {
            LOG.warn("refresh table replay log has no local name: {}", log.debugForRefreshTable());
            return;
        }
        long dbId = Util.genIdByName(catalog.getName(), localDbName);
        long tableId = Util.genIdByName(catalog.getName(), localDbName, localTableName);
        Optional<ExternalDatabase<? extends ExternalTable>> db = catalog.getDbForReplay(localDbName);
        if (!Strings.isNullOrEmpty(log.getNewTableName())) {
            replayRenameTable(log, catalog, db);
            return;
        }
        if (!db.isPresent()) {
            try {
                invalidateAllConnectorCachesIfPresent(catalog);
            } finally {
                Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTable(
                        log.getCatalogId(), dbId, localDbName, tableId, localTableName);
            }
            LOG.info("database object cache is cold when replaying refresh table; "
                            + "invalidated caches by local names {}.{}: {}",
                    localDbName, localTableName, log.debugForRefreshTable());
            return;
        }
        Optional<? extends ExternalTable> table = db.get().getTableForReplay(localTableName);
        if (!table.isPresent()) {
            try {
                if (catalog instanceof PluginDrivenExternalCatalog) {
                    // A cold table object cannot provide the mapped remote table name.
                    ((PluginDrivenExternalCatalog) catalog).getConnector().invalidateDb(db.get().getRemoteName());
                }
            } finally {
                Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTable(
                        log.getCatalogId(), dbId, db.get().getFullName(), tableId, localTableName);
            }
            LOG.info("table object cache is cold when replaying refresh table; invalidated caches by local name {}: {}",
                    localTableName, log.debugForRefreshTable());
            return;
        }
        refreshTableInternal(db.get(), table.get(), log.getLastUpdateTime());
    }

    private void replayRenameTable(ExternalObjectLog log, ExternalCatalog catalog,
            Optional<ExternalDatabase<? extends ExternalTable>> db) {
        ExternalMetaCacheMgr cacheMgr = Env.getCurrentEnv().getExtMetaCacheMgr();
        String localDbName = log.getDbName();
        String localTableName = log.getTableName();
        long dbId = Util.genIdByName(catalog.getName(), localDbName);
        long sourceTableId = Util.genIdByName(catalog.getName(), localDbName, localTableName);
        long destinationTableId = Util.genIdByName(catalog.getName(), localDbName, log.getNewTableName());
        try {
            if (!db.isPresent()) {
                invalidateAllConnectorCachesIfPresent(catalog);
                LOG.info("database object cache is cold when replaying rename table; invalidated connector caches for "
                                + "{}: {}",
                        localDbName, log.debugForRefreshTable());
            } else {
                ExternalDatabase<? extends ExternalTable> database = db.get();
                try {
                    if (catalog instanceof PluginDrivenExternalCatalog) {
                        // Persisted names are local identities, so database scope is the replay-safe connector key.
                        ((PluginDrivenExternalCatalog) catalog).getConnector()
                                .invalidateDb(database.getRemoteName());
                    }
                } finally {
                    database.invalidateTableRename(localTableName, log.getNewTableName());
                }
            }
        } finally {
            try {
                Env.getCurrentEnv().getConstraintManager().renameTable(
                        new TableNameInfo(catalog.getName(), localDbName, localTableName),
                        new TableNameInfo(catalog.getName(), localDbName, log.getNewTableName()));
            } finally {
                cacheMgr.invalidateTableRename(log.getCatalogId(), dbId, localDbName,
                        sourceTableId, localTableName, destinationTableId, log.getNewTableName());
            }
        }
    }

    private void invalidateAllConnectorCachesIfPresent(ExternalCatalog catalog) {
        if (catalog instanceof PluginDrivenExternalCatalog) {
            ((PluginDrivenExternalCatalog) catalog).invalidateAllConnectorCachesIfPresent();
        }
    }

    public void refreshExternalTableFromEvent(String catalogName, String localDbName, String localTableName,
            long updateTime) throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(localDbName);
        if (db == null) {
            return;
        }

        TableIf table = db.getTableNullable(localTableName);
        if (table == null) {
            return;
        }
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, updateTime);
    }

    public void refreshTableInternal(ExternalDatabase db, ExternalTable table, long updateTime) {
        table.unsetObjectCreated();
        if (updateTime > 0) {
            table.setUpdateTime(updateTime);
        }
        // Connector metadata is the row-count source, so invalidate it before engine and row-count caches.
        try {
            if (table.getCatalog() instanceof PluginDrivenExternalCatalog) {
                ((PluginDrivenExternalCatalog) table.getCatalog()).getConnector()
                        .invalidateTable(db.getRemoteName(), table.getRemoteName());
            }
        } finally {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTable(table);
        }
        LOG.info("refresh table {}, id {} from db {} in catalog {}, update time: {}",
                table.getName(), table.getId(), db.getFullName(), db.getCatalog().getName(), updateTime);
    }

    // Refresh partition
    public void refreshPartitionsFromEvent(String catalogName, String localDbName, String localTableName,
            List<String> partitionNames, long updateTime) throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            return;
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog");
        }
        DatabaseIf db = catalog.getDbNullable(localDbName);
        if (db == null) {
            return;
        }

        TableIf table = db.getTableNullable(localTableName);
        if (table == null) {
            return;
        }

        ExternalTable externalTable = (ExternalTable) table;
        try {
            if (externalTable.getCatalog() instanceof PluginDrivenExternalCatalog) {
                ((PluginDrivenExternalCatalog) externalTable.getCatalog()).getConnector().invalidatePartition(
                        ((ExternalDatabase<?>) db).getRemoteName(), externalTable.getRemoteName(), partitionNames);
            }
            externalTable.setUpdateTime(updateTime);
        } finally {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableRowCountCache(externalTable);
        }
    }

    public void addToRefreshMap(long catalogId, Integer[] sec) {
        LOG.info("Add catalog id={} to scheduled refresh map, interval={}s", catalogId, sec[0]);
        refreshMap.put(catalogId, sec);
    }

    public void removeFromRefreshMap(long catalogId) {
        LOG.info("Remove catalog (id={}) from scheduled refresh map", catalogId);
        refreshMap.remove(catalogId);
    }

    public void start() {
        RefreshTask refreshTask = new RefreshTask();
        this.refreshScheduler.scheduleAtFixedRate(refreshTask, 0, REFRESH_TIME_SEC,
                TimeUnit.SECONDS);
    }

    private class RefreshTask implements Runnable {
        @Override
        public void run() {
            for (Map.Entry<Long, Integer[]> entry : refreshMap.entrySet()) {
                Long catalogId = entry.getKey();
                Integer[] timeGroup = entry.getValue();
                Integer original = timeGroup[0];
                Integer current = timeGroup[1];
                if (current - REFRESH_TIME_SEC > 0) {
                    timeGroup[1] = current - REFRESH_TIME_SEC;
                    refreshMap.put(catalogId, timeGroup);
                } else {
                    CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
                    if (catalog != null) {
                        String catalogName = catalog.getName();
                        LOG.info("Scheduled refresh triggered for catalog {} (id={}), interval={}s, invalidCache=true",
                                catalogName, catalogId, original);
                        /**
                         * Now do not invoke
                         * {@link org.apache.doris.analysis.RefreshCatalogStmt#analyze(Analyzer)} is ok,
                         * because the default value of invalidCache is true.
                         * */
                        try {
                            Env.getCurrentEnv().getRefreshManager().handleRefreshCatalog(catalogName, true);
                            LOG.info("Scheduled refresh completed for catalog {} (id={}), next refresh in {}s",
                                    catalogName, catalogId, original);
                        } catch (Exception e) {
                            LOG.warn("Failed to execute scheduled refresh for catalog {} (id={})",
                                    catalogName, catalogId, e);
                        }

                        // reset
                        timeGroup[1] = original;
                        refreshMap.put(catalogId, timeGroup);
                    } else {
                        LOG.warn("Scheduled refresh skipped: catalog id={} not found, removing from refresh map",
                                catalogId);
                        refreshMap.remove(catalogId);
                    }
                }
            }
        }
    }
}
