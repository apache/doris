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

import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogFactory;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogLog;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.persist.OperationType;
import org.apache.doris.qe.DdlExecutor;

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
    public void handleRefreshCatalog(RefreshCatalogStmt stmt) throws UserException {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalogOrAnalysisException(stmt.getCatalogName());
        CatalogLog log = CatalogFactory.createCatalogLog(catalog.getId(), stmt);
        refreshCatalogInternal(catalog, log.isInvalidCache());
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
            ((ExternalCatalog) catalog).onRefreshCache(invalidCache);
            LOG.info("refresh catalog {} with invalidCache {}", catalogName, invalidCache);
        }
    }

    // Refresh database
    public void handleRefreshDb(RefreshDbStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        String dbName = stmt.getDbName();
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();
        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh database in external catalog");
        }
        DatabaseIf db = catalog.getDbOrDdlException(dbName);
        ((ExternalDatabase) db).setUnInitialized(stmt.isInvalidCache());

        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setInvalidCache(stmt.isInvalidCache());
        Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
    }

    public void replayRefreshDb(ExternalObjectLog log) {
        refreshDbInternal(log.getCatalogId(), log.getDbId(), log.isInvalidCache());
    }

    private void refreshDbInternal(long catalogId, long dbId, boolean invalidCache) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogId);
        Optional<ExternalDatabase<? extends ExternalTable>> db = catalog.getDbForReplay(dbId);
        // Database may not exist if 'use_meta_cache' is true.
        // Because each FE fetch the meta data independently.
        db.ifPresent(e -> {
            e.setUnInitialized(invalidCache);
            LOG.info("refresh database {} in catalog {} with invalidCache {}", e.getFullName(),
                    catalog.getName(), invalidCache);
        });
    }

    // Refresh table
    public void handleRefreshTable(RefreshTableStmt stmt) throws UserException {
        String catalogName = stmt.getCtl();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTblName();
        refreshTable(catalogName, dbName, tableName, false);
    }

    public void refreshTable(String catalogName, String dbName, String tableName, boolean ignoreIfNotExists)
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
        refreshTableInternal(catalog, db, table, 0);

        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    public void replayRefreshTable(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog replaying refresh table {}", log.getCatalogId());
            return;
        }
        Optional<ExternalDatabase<? extends ExternalTable>> db = catalog.getDbForReplay(log.getDbId());
        // See comment in refreshDbInternal for why db and table may be null.
        if (!db.isPresent()) {
            LOG.warn("failed to find db replaying refresh table {}", log.getDbId());
            return;
        }
        Optional<? extends ExternalTable> table = db.get().getTableForReplay(log.getTableId());
        if (!table.isPresent()) {
            LOG.warn("failed to find table replaying refresh table {}", log.getTableId());
            return;
        }
        refreshTableInternal(catalog, db.get(), table.get(), log.getLastUpdateTime());
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
        refreshTableInternal(catalog, db, table, updateTime);
    }

    private void refreshTableInternal(CatalogIf catalog, DatabaseIf db, TableIf table, long updateTime) {
        if (table instanceof ExternalTable) {
            ((ExternalTable) table).unsetObjectCreated();
        }
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateTableCache(catalog.getId(), db.getFullName(), table.getName());
        if (table instanceof HMSExternalTable && updateTime > 0) {
            ((HMSExternalTable) table).setEventUpdateTime(updateTime);
        }
        LOG.info("refresh table {} from db {} in catalog {}", table.getName(), db.getFullName(), catalog.getName());
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

        Env.getCurrentEnv().getExtMetaCacheMgr().invalidatePartitionsCache(
                catalog.getId(), db.getFullName(), table.getName(), partitionNames);
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
                        RefreshCatalogStmt refreshCatalogStmt = new RefreshCatalogStmt(catalogName, null);
                        try {
                            DdlExecutor.execute(Env.getCurrentEnv(), refreshCatalogStmt);
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
