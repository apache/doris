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
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
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
        if (catalog instanceof ExternalCatalog) {
            try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                    ((ExternalCatalog) catalog).beginConstraintMetadataMutation()) {
                refreshCatalogInternal(catalog, invalidCache);
                CatalogLog log = CatalogLog.createForRefreshCatalog(catalog.getId(), invalidCache);
                Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_REFRESH_CATALOG, log);
            }
        } else {
            refreshCatalogInternal(catalog, invalidCache);
            CatalogLog log = CatalogLog.createForRefreshCatalog(catalog.getId(), invalidCache);
            Env.getCurrentEnv().getEditLog().logCatalogLog(OperationType.OP_REFRESH_CATALOG, log);
        }
    }

    public void replayRefreshCatalog(CatalogLog log) {
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog replaying refresh catalog {}", log.getCatalogId());
            return;
        }
        if (catalog instanceof ExternalCatalog) {
            try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                    ((ExternalCatalog) catalog).beginConstraintMetadataMutation()) {
                refreshCatalogInternal(catalog, log.isInvalidCache());
            }
        } else {
            refreshCatalogInternal(catalog, log.isInvalidCache());
        }
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
        ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
        try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                externalCatalog.beginConstraintMetadataMutation()) {
            DatabaseIf db = catalog.getDbOrDdlException(dbName);
            refreshDbInternal((ExternalDatabase) db);
            ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(
                    catalog.getId(), db.getFullName());
            Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
        }
    }

    public void replayRefreshDb(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh db: {}", log.debugForRefreshDb());
            return;
        }
        try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                catalog.beginConstraintMetadataMutation()) {
            boolean hasDbName = !Strings.isNullOrEmpty(log.getDbName());
            String localDbName = hasDbName
                    ? log.getDbName()
                    : catalog.getDbNameForReplay(log.getDbId()).orElse(null);
            Optional<ExternalDatabase<? extends ExternalTable>> db =
                    localDbName == null ? Optional.empty() : catalog.getDbForReplay(localDbName);

            if (!db.isPresent()) {
                if (localDbName != null) {
                    Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDb(catalog.getId(), localDbName);
                    invalidateAllConnectorCachesIfPresent(catalog);
                    LOG.info("database object cache is cold when replaying refresh database; invalidated caches by "
                                    + "local name {} from {}: {}",
                            localDbName, hasDbName ? "edit log" : "retained ID mapping", log.debugForRefreshDb());
                    return;
                }
                LOG.warn("failed to find db when replaying refresh db: {}", log.debugForRefreshDb());
            } else {
                refreshDbInternal(db.get());
            }
        }
    }

    private void refreshDbInternal(ExternalDatabase db) {
        db.resetMetaToUninitialized();
        // Also drop any connector-side caches for every table in this db (e.g. hive's metastore + directory-listing
        // caches) so a subsequent read reflects the latest external state — otherwise REFRESH DATABASE would reset
        // only the engine-side meta and leave the connector serving stale partition/file listings up to its TTL.
        // Connector-agnostic (generic SPI no-op default); keyed by the REMOTE db name. Mirrors refreshTableInternal.
        if (db.getCatalog() instanceof PluginDrivenExternalCatalog) {
            ((PluginDrivenExternalCatalog) db.getCatalog()).getConnector().invalidateDb(db.getRemoteName());
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
        try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                ((ExternalCatalog) catalog).beginConstraintMetadataMutation()) {
            DatabaseIf db = catalog.getDbNullable(dbName);
            if (db == null) {
                if (!ignoreIfNotExists) {
                    throw new DdlException(
                            "Database " + dbName + " does not exist in catalog " + catalog.getName());
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
            Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
        }
    }

    public void replayRefreshTable(ExternalObjectLog log) {
        ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr().getCatalog(log.getCatalogId());
        if (catalog == null) {
            LOG.warn("failed to find catalog when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        try (ExternalCatalog.ConstraintMetadataMutationGuard ignored =
                catalog.beginConstraintMetadataMutation()) {
            replayRefreshTableInternal(catalog, log);
        }
    }

    private void replayRefreshTableInternal(ExternalCatalog catalog, ExternalObjectLog log) {
        boolean hasDbName = !Strings.isNullOrEmpty(log.getDbName());
        String localDbName = hasDbName
                ? log.getDbName()
                : catalog.getDbNameForReplay(log.getDbId()).orElse(null);
        Optional<ExternalDatabase<? extends ExternalTable>> db =
                localDbName == null ? Optional.empty() : catalog.getDbForReplay(localDbName);
        // See comment in refreshDbInternal for why db and table may be null.
        if (!db.isPresent()) {
            if (!catalog.isInitialized()) {
                LOG.info("catalog is uninitialized when replaying refresh table; skip cache invalidation: {}",
                        log.debugForRefreshTable());
                return;
            }
            if (localDbName != null) {
                if (hasDbName && !Strings.isNullOrEmpty(log.getTableName())) {
                    Env.getCurrentEnv().getExtMetaCacheMgr()
                            .invalidateTable(catalog.getId(), localDbName, log.getTableName());
                    LOG.info("database object cache is cold when replaying refresh table; "
                                    + "invalidated caches by local names {}.{} from edit log: {}",
                            localDbName, log.getTableName(), log.debugForRefreshTable());
                } else {
                    // Doris 2.1/3.0 edit logs contain only database/table IDs. Once the database object is cold,
                    // its table ID map is unreachable, so invalidate the database scope conservatively.
                    Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDb(catalog.getId(), localDbName);
                    LOG.info("database object cache is cold when replaying legacy ID-only refresh table; "
                                    + "invalidated database caches by local name {} from retained ID mapping: {}",
                            localDbName, log.debugForRefreshTable());
                }
                invalidateAllConnectorCachesIfPresent(catalog);
                return;
            }
            LOG.warn("failed to find db when replaying refresh table: {}", log.debugForRefreshTable());
            return;
        }
        String localTableName;
        String tableNameSource;
        boolean hasTableName = !Strings.isNullOrEmpty(log.getTableName());
        if (hasTableName) {
            localTableName = log.getTableName();
            tableNameSource = "edit log";
        } else {
            localTableName = db.get().getTableNameForReplay(log.getTableId()).orElse(null);
            tableNameSource = "retained ID mapping";
        }
        Optional<? extends ExternalTable> table = hasTableName
                ? db.get().getTableForReplay(log.getTableName())
                : db.get().getTableForReplay(log.getTableId());
        if (!Strings.isNullOrEmpty(log.getNewTableName())) {
            if (localTableName == null) {
                LOG.warn("failed to resolve local table name when replaying rename table, skip rename: {}",
                        log.debugForRefreshTable());
                return;
            }
            // Keep connector caches aligned with the FE rename replay. A hot object provides the precise remote
            // source key; if the object is cold, invalidate the connector's database cache because the retained
            // FE ID mapping contains only the local table name.
            if (catalog instanceof PluginDrivenExternalCatalog) {
                Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
                String remoteDbName = db.get().getRemoteName();
                if (table.isPresent()) {
                    connector.invalidateTable(remoteDbName, table.get().getRemoteName());
                } else {
                    connector.invalidateDb(remoteDbName);
                }
                connector.invalidateTable(remoteDbName, log.getNewTableName());
            }
            db.get().unregisterTable(localTableName);
            db.get().resetMetaCacheNames();
            Env.getCurrentEnv().getConstraintManager().renameTable(
                    new TableNameInfo(catalog.getName(), db.get().getFullName(), localTableName),
                    new TableNameInfo(catalog.getName(), db.get().getFullName(), log.getNewTableName()));
            return;
        }
        if (!table.isPresent()) {
            if (localTableName == null) {
                LOG.warn("failed to resolve local table name when replaying refresh table, skip refresh: {}",
                        log.debugForRefreshTable());
                return;
            }
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateTable(catalog.getId(), db.get().getFullName(), localTableName);
            if (catalog instanceof PluginDrivenExternalCatalog) {
                // The retained ID mapping cannot recover a mapped remote table name without loading metadata.
                // Invalidate the connector's database cache to preserve replay correctness without remote I/O.
                ((PluginDrivenExternalCatalog) catalog).getConnector().invalidateDb(db.get().getRemoteName());
            }
            LOG.info("table object cache is cold when replaying refresh table; "
                            + "invalidated engine caches by local name {} from {}: {}",
                    localTableName, tableNameSource, log.debugForRefreshTable());
            return;
        }
        // Legacy partition-bearing refresh logs are conservatively replayed as a full table invalidation now that
        // connector plugins own their partition caches.
        refreshTableInternal(db.get(), table.get(), log.getLastUpdateTime());
    }

    private void invalidateAllConnectorCachesIfPresent(ExternalCatalog catalog) {
        if (catalog instanceof PluginDrivenExternalCatalog) {
            ((PluginDrivenExternalCatalog) catalog).invalidateAllConnectorCachesIfPresent();
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
        if (updateTime > 0) {
            table.setUpdateTime(updateTime);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(table);
        // FIX-4: also drop any connector-side per-table cache (e.g. paimon's latest-snapshot cache) so the
        // next read reflects the latest external state. Connector-agnostic (generic SPI no-op default); keyed
        // by the REMOTE db/table names the connector uses.
        if (table.getCatalog() instanceof PluginDrivenExternalCatalog) {
            ((PluginDrivenExternalCatalog) table.getCatalog()).getConnector()
                    .invalidateTable(db.getRemoteName(), table.getRemoteName());
        }
        LOG.info("refresh table {}, id {} from db {} in catalog {}, update time: {}",
                table.getName(), table.getId(), db.getFullName(), db.getCatalog().getName(), updateTime);
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

        ExternalTable externalTable = (ExternalTable) table;
        if (externalTable.getCatalog() instanceof PluginDrivenExternalCatalog) {
            // The connector owns the partition cache (pull-through); invalidate by name. Mirrors
            // refreshTableInternal's connector hook.
            ((PluginDrivenExternalCatalog) externalTable.getCatalog()).getConnector().invalidatePartition(
                    ((ExternalDatabase<?>) db).getRemoteName(), externalTable.getRemoteName(), partitionNames);
        }
        externalTable.setUpdateTime(updateTime);
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
