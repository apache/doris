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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
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
        String catalogName = catalog.getName();
        if (!catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            ((ExternalCatalog) catalog).resetToUninitialized(invalidCache);
            LOG.info("refresh catalog {} with invalidCache {}", catalogName, invalidCache);
        }
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
        }
        Optional<ExternalDatabase<? extends ExternalTable>> db;
        if (!Strings.isNullOrEmpty(log.getDbName())) {
            db = catalog.getDbForReplay(log.getDbName());
        } else {
            db = catalog.getDbForReplay(log.getDbId());
        }

        if (!db.isPresent()) {
            LOG.warn("failed to find db when replaying refresh db: {}", log.debugForRefreshDb());
        } else {
            refreshDbInternal(db.get());
        }
    }

    private void refreshDbInternal(ExternalDatabase db) {
        db.resetToUninitialized();
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
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, 0);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(catalog.getId(), db.getFullName(),
                table.getName());
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    public void replayRefreshTable(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        Optional<ExternalDatabase<? extends ExternalTable>> db;
        if (!Strings.isNullOrEmpty(log.getDbName())) {
            db = catalog.getDbForReplay(log.getDbName());
        } else {
            db = catalog.getDbForReplay(log.getDbId());
        }
        // See comment in refreshDbInternal for why db and table may be null.
        if (!db.isPresent()) {
            LOG.warn("failed to find db when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        Optional<? extends ExternalTable> table;
        if (!Strings.isNullOrEmpty(log.getTableName())) {
            table = db.get().getTableForReplay(log.getTableName());
        } else {
            table = db.get().getTableForReplay(log.getTableId());
        }
        if (!table.isPresent()) {
            LOG.warn("failed to find table when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        if (!Strings.isNullOrEmpty(log.getNewTableName())) {
            // this is a rename table op
            db.get().unregisterTable(log.getTableName());
            db.get().resetMetaCacheNames();
        } else {
            refreshTableInternal(db.get(), table.get(), log.getLastUpdateTime());
        }
    }

    public void refreshExternalTableFromEvent(String catalogName, String dbName, String tableName,
            long updateTime) throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            throw new DdlException("No catalog found with name: " + catalogName);
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            return;
        }
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, updateTime);
    }

    public void refreshTableInternal(ExternalDatabase db, ExternalTable table, long updateTime) {
        table.unsetObjectCreated();
        if (table instanceof HMSExternalTable && updateTime > 0) {
            ((HMSExternalTable) table).setEventUpdateTime(updateTime);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(table);
        LOG.info("refresh table {}, id {} from db {} in catalog {}",
                table.getName(), table.getId(), db.getFullName(), db.getCatalog().getName());
    }

    // Refresh partition
    public void refreshPartitions(String catalogName, String dbName, String tableName,
            List<String> partitionNames, long updateTime, boolean ignoreIfNotExists)
            throws DdlException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalog == null) {
            if (!ignoreIfNotExists) {
                throw new DdlException("No catalog found with name: " + catalogName);
            }
            return;
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support ExternalCatalog");
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

        Env.getCurrentEnv().getExtMetaCacheMgr().invalidatePartitionsCache((ExternalTable) table, partitionNames);
        ((HMSExternalTable) table).setEventUpdateTime(updateTime);
    }

    public void addToRefreshMap(long catalogId, Integer[] sec) {
        refreshMap.put(catalogId, sec);
    }

    public void removeFromRefreshMap(long catalogId) {
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
                        /**
                         * Now do not invoke
                         * {@link org.apache.doris.analysis.RefreshCatalogStmt#analyze(Analyzer)} is ok,
                         * because the default value of invalidCache is true.
                         * */
                        try {
                            Env.getCurrentEnv().getRefreshManager().handleRefreshCatalog(catalogName, true);
                        } catch (Exception e) {
                            LOG.warn("failed to refresh catalog {}", catalogName, e);
                        }

                        // reset
                        timeGroup[1] = original;
                        refreshMap.put(catalogId, timeGroup);
                    }
                }
            }
        }
    }
}
