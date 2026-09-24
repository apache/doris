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
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveExternalMetaCache;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalMetaCache;
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
        replayRefreshSafely("refresh catalog " + log.getCatalogId(),
                () -> refreshCatalogInternal(catalog, log.isInvalidCache()));
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
        // Local DB-object eviction also resets metadata; only an explicit refresh retires access.
        invalidateLanceTableAccess(catalog);
        refreshDbInternal((ExternalDatabase) db);

        ExternalObjectLog log = ExternalObjectLog.createForRefreshDb(catalog.getId(), db.getFullName());
        Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
    }

    public void replayRefreshDb(ExternalObjectLog log) {
        replayRefreshSafely("refresh db " + log.getCatalogId(), () -> {
            ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                    .getCatalog(log.getCatalogId());
            if (catalog == null) {
                LOG.warn("failed to find catalog when replaying refresh db: {}", log.debugForRefreshDb());
                return;
            }
            invalidateLanceTableAccess(catalog);
            Optional<ExternalDatabase<? extends ExternalTable>> db;
            if (!Strings.isNullOrEmpty(log.getDbName())) {
                db = catalog.getDbForReplay(log.getDbName());
            } else {
                db = catalog.getDbForReplay(log.getDbId());
            }

            if (!db.isPresent()) {
                LOG.warn("failed to find db when replaying refresh db: {}", log.debugForRefreshDb());
                // No canonical identity is available: retire the catalog scope so engine entries and
                // row counts cannot survive the committed refresh.
                Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalog(catalog.getId());
                invalidatePaimonCatalogForUnresolvedReplay(catalog);
            } else {
                refreshDbInternal(db.get());
            }
        });
    }

    private void invalidateLanceTableAccess(CatalogIf catalog) {
        // Access entries outlive the bounded DB/table object caches. Replay must invalidate by
        // catalog identity before its cache-only object lookup can return early, including ID logs.
        if (catalog instanceof LanceExternalCatalog) {
            ((LanceExternalCatalog) catalog).invalidateTableAccessCache();
        }
    }

    private void refreshDbInternal(ExternalDatabase db) {
        db.resetMetaToUninitialized();
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
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(catalog.getId(), db.getFullName(),
                table.getName(), updateTime);
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    public void refreshTableAfterCommit(ExternalTable table) {
        long updateTime = System.currentTimeMillis();
        try {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateRowCountCache(table);
            refreshTableInternal((ExternalDatabase) table.getDatabase(), table, updateTime);
        } catch (RuntimeException e) {
            // The external transaction is already committed. Still notify follower FEs even if a
            // cache layer failed, so peers do not keep serving the pre-commit state.
            LOG.warn("Failed to refresh table cache after committing external insert for {}",
                    table.getNameWithFullQualifiers(), e);
        }
        ExternalObjectLog log = ExternalObjectLog.createForRefreshTable(
                table.getCatalog().getId(), table.getDatabase().getFullName(), table.getName(), updateTime);
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }

    public void replayRefreshTable(ExternalObjectLog log) {
        replayRefreshSafely("refresh table " + log.getCatalogId(), () -> {
            ExternalCatalog catalog = (ExternalCatalog) Env.getCurrentEnv().getCatalogMgr()
                    .getCatalog(log.getCatalogId());
            if (catalog == null) {
                LOG.warn("failed to find catalog when replaying refresh table: {}", log.debugForRefreshTable());
                return;
            }
            invalidateLanceTableAccess(catalog);
            Optional<ExternalDatabase<? extends ExternalTable>> db;
            if (!Strings.isNullOrEmpty(log.getDbName())) {
                db = catalog.getDbForReplay(log.getDbName());
            } else {
                db = catalog.getDbForReplay(log.getDbId());
            }
            // See comment in refreshDbInternal for why db and table may be null.
            if (!db.isPresent()) {
                LOG.warn("failed to find db when replaying refresh table: {}", log.debugForRefreshTable());
                // No canonical identity is available: retire the catalog scope so engine entries and
                // row counts cannot survive the committed refresh.
                Env.getCurrentEnv().getExtMetaCacheMgr().invalidateCatalog(catalog.getId());
                invalidatePaimonCatalogForUnresolvedReplay(catalog);
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
                // Only a genuinely unresolved name (for example a lost case-insensitive mapping)
                // needs the conservative database-wide retirement; an ordinary cold miss for a
                // known name must not evict unrelated cached siblings.
                if (!Strings.isNullOrEmpty(log.getTableName())
                        && !db.get().hasLocalTableName(log.getTableName())) {
                    db.get().retireAllTableObjectsWithoutEngineInvalidation();
                }
                // The independent row-count cache can outlive the table object, so fence the
                // canonical database scope before acknowledging the committed refresh.
                Env.getCurrentEnv().getExtMetaCacheMgr()
                        .invalidateRowCountCache(catalog.getId(), db.get().getId());
                invalidatePaimonCatalogForUnresolvedReplay(catalog);
                return;
            }
            if (!Strings.isNullOrEmpty(log.getNewTableName())) {
                // this is a rename table op
                db.get().unregisterTable(log.getTableName());
                db.get().resetMetaCacheNames();
            } else {
                List<String> modifiedPartNames = log.getPartitionNames();
                List<String> newPartNames = log.getNewPartitionNames();
                if (catalog instanceof HMSExternalCatalog
                        && ((modifiedPartNames != null && !modifiedPartNames.isEmpty())
                        || (newPartNames != null && !newPartNames.isEmpty()))) {
                    // Partition-level cache invalidation, only for hive catalog. Fence the held table
                    // before the hive(...) lookup can lazily initialize the group and throw; otherwise
                    // a failed acquisition skips the row-count fence and the full-table fallback.
                    Env.getCurrentEnv().getExtMetaCacheMgr().invalidateRowCountCache(table.get());
                    try {
                        HiveExternalMetaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                                .hive(catalog.getId());
                        cache.refreshAffectedPartitionsCache((HMSExternalTable) table.get(), modifiedPartNames,
                                newPartNames);
                        // Close the admission window the opening fence left open: a load admitted after
                        // it can publish the pre-insert value from the still-resident file list.
                        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateRowCountCache(table.get());
                    } catch (RuntimeException e) {
                        // The insert is already committed. A partial or failed selective refresh must not
                        // leave stale Hive partition/file entries, so fall back to the same conservative
                        // full-table invalidation the leader uses.
                        LOG.warn("failed to refresh affected partitions when replaying refresh table for {}",
                                table.get().getNameWithFullQualifiers(), e);
                        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(table.get());
                    }
                    if (table.get() instanceof HMSExternalTable && log.getLastUpdateTime() > 0) {
                        ((HMSExternalTable) table.get()).setUpdateTime(log.getLastUpdateTime());
                    }
                    LOG.info("replay refresh partitions for table {}, "
                                    + "modified partitions count: {}, "
                                    + "new partitions count: {}",
                            table.get().getName(), modifiedPartNames == null ? 0 : modifiedPartNames.size(),
                            newPartNames == null ? 0 : newPartNames.size());
                } else {
                    // Full table cache invalidation
                    refreshTableInternal(db.get(), table.get(), log.getLastUpdateTime());
                }
            }
        });
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
        // Whole-table events are already committed remotely. Fence the row count by cached identity
        // before any database/table reload can fail and make the not-found path return.
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateRowCountCache(catalog.getId(), dbName, tableName);
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            // Cold database: widen so independently resident engine entries are retired too.
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateTableByNameOrWider(catalog.getId(), dbName, tableName);
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateTableByNameOrWider(catalog.getId(), dbName, tableName);
            return;
        }
        refreshTableInternal((ExternalDatabase) db, (ExternalTable) table, updateTime);
    }

    public void refreshTableInternal(ExternalDatabase db, ExternalTable table, long updateTime) {
        table.unsetObjectCreated();
        // Iceberg partition evolution can change partition specs across FEs.
        // Clear related-table validation cache to avoid stale partitioned/unpartitioned judgment.
        if (table instanceof IcebergExternalTable) {
            ((IcebergExternalTable) table).setIsValidRelatedTableCached(false);
        }
        if (updateTime > 0) {
            table.setUpdateTime(updateTime);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(table);
        LOG.info("refresh table {}, id {} from db {} in catalog {}, update time: {}",
                table.getName(), table.getId(), db.getFullName(), db.getCatalog().getName(), updateTime);
    }

    /**
     * Run a replay-time cache invalidation without letting a failure escape into EditLog's fatal
     * replay handler. The refresh record is already committed, so a cache-cleanup failure must be
     * logged and swallowed instead of taking the FE down.
     */
    private void replayRefreshSafely(String operation, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            LOG.warn("failed to replay {}: {}", operation, e.getMessage(), e);
        }
    }

    private void invalidatePaimonCatalogForUnresolvedReplay(ExternalCatalog catalog) {
        if (catalog instanceof PaimonExternalCatalog) {
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateCatalogByEngine(catalog.getId(), PaimonExternalMetaCache.ENGINE);
        }
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
        // Partition events are already committed remotely. Fence the row count by cached identity
        // before any database/table reload can fail and make the ignored-not-found path return.
        Env.getCurrentEnv().getExtMetaCacheMgr()
                .invalidateRowCountCache(catalog.getId(), dbName, tableName);
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateTableByNameOrWider(catalog.getId(), dbName, tableName);
            if (!ignoreIfNotExists) {
                throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
            }
            return;
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateTableByNameOrWider(catalog.getId(), dbName, tableName);
            if (!ignoreIfNotExists) {
                throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
            }
            return;
        }

        ExternalTable externalTable = (ExternalTable) table;
        HiveExternalMetaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().hive(externalTable.getCatalog().getId());
        try {
            for (String partitionName : partitionNames) {
                cache.invalidatePartitionCache(externalTable, partitionName);
            }
        } finally {
            // Close the admission window opened by the pre-fence above, even when the selective
            // invalidation fails partway.
            Env.getCurrentEnv().getExtMetaCacheMgr()
                    .invalidateRowCountCache(catalog.getId(), dbName, tableName);
        }
        ((HMSExternalTable) table).setUpdateTime(updateTime);
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
