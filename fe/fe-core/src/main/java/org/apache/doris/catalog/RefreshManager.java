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
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.qe.DdlExecutor;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
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

    public void handleRefreshTable(RefreshTableStmt stmt) throws UserException {
        String catalogName = stmt.getCtl();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTblName();
        Env env = Env.getCurrentEnv();

        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();

        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }

        // Process external catalog table refresh
        env.getCatalogMgr().refreshExternalTable(dbName, tableName, catalogName, false);
        LOG.info("Successfully refresh table: {} from db: {}", tableName, dbName);
    }

    public void handleRefreshDb(RefreshDbStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        String dbName = stmt.getDbName();
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();

        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }

        // Process external catalog db refresh
        refreshExternalCtlDb(dbName, catalog, stmt.isInvalidCache());
        LOG.info("Successfully refresh db: {}", dbName);
    }

    private void refreshExternalCtlDb(String dbName, CatalogIf catalog, boolean invalidCache) throws DdlException {
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Database");
        }

        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
        }
        ((ExternalDatabase) db).setUnInitialized(invalidCache);
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setInvalidCache(invalidCache);
        Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
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
