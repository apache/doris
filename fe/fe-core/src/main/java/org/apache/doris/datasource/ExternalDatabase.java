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

package org.apache.doris.datasource;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaTable;
import org.apache.doris.datasource.infoschema.ExternalMysqlDatabase;
import org.apache.doris.datasource.infoschema.ExternalMysqlTable;
import org.apache.doris.datasource.metacache.MetaCache;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterCatalogExecutor;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Base class of external database.
 *
 * @param <T> External table type is ExternalTable or its subclass.
 */
public abstract class ExternalDatabase<T extends ExternalTable>
        implements DatabaseIf<T>, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalDatabase.class);

    protected MonitoredReentrantReadWriteLock rwLock = new MonitoredReentrantReadWriteLock(true);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "dbProperties")
    protected DatabaseProperty dbProperties = new DatabaseProperty();
    @SerializedName(value = "initialized")
    protected boolean initialized = false;
    // Cache of table name to table id.
    protected Map<String, Long> tableNameToId = Maps.newConcurrentMap();
    @SerializedName(value = "idToTbl")
    protected Map<Long, T> idToTbl = Maps.newConcurrentMap();
    @SerializedName(value = "lastUpdateTime")
    protected long lastUpdateTime;
    protected final InitDatabaseLog.Type dbLogType;
    protected ExternalCatalog extCatalog;
    protected boolean invalidCacheInInit = true;

    private MetaCache<T> metaCache;

    /**
     * Create external database.
     *
     * @param extCatalog The catalog this database belongs to.
     * @param id Database id.
     * @param name Database name.
     */
    public ExternalDatabase(ExternalCatalog extCatalog, long id, String name, InitDatabaseLog.Type dbLogType) {
        this.extCatalog = extCatalog;
        this.id = id;
        this.name = name;
        this.dbLogType = dbLogType;
    }

    public void setExtCatalog(ExternalCatalog extCatalog) {
        this.extCatalog = extCatalog;
    }

    public void setTableExtCatalog(ExternalCatalog extCatalog) {
        for (T table : idToTbl.values()) {
            table.setCatalog(extCatalog);
        }
    }

    public void setUnInitialized(boolean invalidCache) {
        this.initialized = false;
        this.invalidCacheInInit = invalidCache;
        if (extCatalog.getUseMetaCache().isPresent()) {
            if (extCatalog.getUseMetaCache().get() && metaCache != null) {
                metaCache.invalidateAll();
            } else if (!extCatalog.getUseMetaCache().get()) {
                for (T table : idToTbl.values()) {
                    table.unsetObjectCreated();
                }
            }
        }
        if (invalidCache) {
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateDbCache(extCatalog.getId(), name);
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public final synchronized void makeSureInitialized() {
        extCatalog.makeSureInitialized();
        if (!initialized) {
            if (extCatalog.getUseMetaCache().get()) {
                if (metaCache == null) {
                    metaCache = Env.getCurrentEnv().getExtMetaCacheMgr().buildMetaCache(
                            name,
                            OptionalLong.of(86400L),
                            OptionalLong.of(Config.external_cache_expire_time_minutes_after_access * 60L),
                            Config.max_meta_object_cache_num,
                            ignored -> listTableNames(),
                            tableName -> Optional.ofNullable(
                                    buildTableForInit(tableName,
                                            Util.genIdByName(extCatalog.getName(), name, tableName), extCatalog)),
                            (key, value, cause) -> value.ifPresent(ExternalTable::unsetObjectCreated));
                }
                setLastUpdateTime(System.currentTimeMillis());
            } else {
                if (!Env.getCurrentEnv().isMaster()) {
                    // Forward to master and wait the journal to replay.
                    int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeout();
                    MasterCatalogExecutor remoteExecutor = new MasterCatalogExecutor(waitTimeOut * 1000);
                    try {
                        remoteExecutor.forward(extCatalog.getId(), id);
                    } catch (Exception e) {
                        Util.logAndThrowRuntimeException(LOG,
                                String.format("failed to forward init external db %s operation to master", name), e);
                    }
                    return;
                }
                init();
            }
            initialized = true;
        }
    }

    public void replayInitDb(InitDatabaseLog log, ExternalCatalog catalog) {
        Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
        Map<Long, T> tmpIdToTbl = Maps.newConcurrentMap();
        for (int i = 0; i < log.getRefreshCount(); i++) {
            Optional<T> table = getTableForReplay(log.getRefreshTableIds().get(i));
            // When upgrade cluster with this pr: https://github.com/apache/doris/pull/27666
            // Maybe there are some create table events will be skipped
            // if the cluster has any hms catalog(s) with hms event listener enabled.
            // So we need add a validation here to avoid table(s) not found, this is just a temporary solution
            // because later we will remove all the logics about InitCatalogLog/InitDatabaseLog.
            if (table.isPresent()) {
                tmpTableNameToId.put(table.get().getName(), table.get().getId());
                tmpIdToTbl.put(table.get().getId(), table.get());
            }
        }
        for (int i = 0; i < log.getCreateCount(); i++) {
            T table = buildTableForInit(log.getCreateTableNames().get(i), log.getCreateTableIds().get(i), catalog);
            tmpTableNameToId.put(table.getName(), table.getId());
            tmpIdToTbl.put(table.getId(), table);
        }
        tableNameToId = tmpTableNameToId;
        idToTbl = tmpIdToTbl;
        lastUpdateTime = log.getLastUpdateTime();
        initialized = true;
    }

    private void init() {
        InitDatabaseLog initDatabaseLog = new InitDatabaseLog();
        initDatabaseLog.setType(dbLogType);
        initDatabaseLog.setCatalogId(extCatalog.getId());
        initDatabaseLog.setDbId(id);
        List<String> tableNames = listTableNames();
        if (tableNames != null) {
            Map<String, Long> tmpTableNameToId = Maps.newConcurrentMap();
            Map<Long, T> tmpIdToTbl = Maps.newHashMap();
            for (String tableName : tableNames) {
                long tblId;
                if (tableNameToId != null && tableNameToId.containsKey(tableName)) {
                    tblId = tableNameToId.get(tableName);
                    tmpTableNameToId.put(tableName, tblId);
                    T table = idToTbl.get(tblId);
                    tmpIdToTbl.put(tblId, table);
                    initDatabaseLog.addRefreshTable(tblId);
                } else {
                    tblId = Env.getCurrentEnv().getNextId();
                    tmpTableNameToId.put(tableName, tblId);
                    T table = buildTableForInit(tableName, tblId, extCatalog);
                    tmpIdToTbl.put(tblId, table);
                    initDatabaseLog.addCreateTable(tblId, tableName);
                }
            }
            tableNameToId = tmpTableNameToId;
            idToTbl = tmpIdToTbl;
        }

        lastUpdateTime = System.currentTimeMillis();
        initDatabaseLog.setLastUpdateTime(lastUpdateTime);
        Env.getCurrentEnv().getEditLog().logInitExternalDb(initDatabaseLog);
    }

    private List<String> listTableNames() {
        List<String> tableNames;
        if (name.equals(InfoSchemaDb.DATABASE_NAME)) {
            tableNames = ExternalInfoSchemaDatabase.listTableNames();
        } else if (name.equals(MysqlDb.DATABASE_NAME)) {
            tableNames = ExternalMysqlDatabase.listTableNames();
        } else {
            tableNames = extCatalog.listTableNames(null, name);
        }
        return tableNames;
    }

    protected abstract T buildTableForInit(String tableName, long tblId, ExternalCatalog catalog);

    public Optional<T> getTableForReplay(long tableId) {
        if (extCatalog.getUseMetaCache().get()) {
            if (!isInitialized()) {
                return Optional.empty();
            }
            return metaCache.getMetaObjById(tableId);
        } else {
            return Optional.ofNullable(idToTbl.get(tableId));
        }
    }

    @Override
    public void readLock() {
        this.rwLock.readLock().lock();
    }

    @Override
    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    @Override
    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    @Override
    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    @Override
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at external db[" + id + "]", e);
            return false;
        }
    }

    @Override
    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    @Override
    public boolean writeLockIfExist() {
        writeLock();
        return true;
    }

    @Override
    public <E extends Exception> void writeLockOrException(E e) throws E {
        writeLock();
    }

    @Override
    public void writeLockOrDdlException() throws DdlException {
        writeLock();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getFullName() {
        return name;
    }

    @Override
    public DatabaseProperty getDbProperties() {
        return dbProperties;
    }

    @Override
    public boolean isTableExist(String tableName) {
        return extCatalog.tableExist(ConnectContext.get().getSessionContext(), name, tableName);
    }

    // ATTN: this method only returned cached tables.
    @Override
    public List<T> getTables() {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            List<T> tables = Lists.newArrayList();
            Set<String> tblNames = getTableNamesWithLock();
            for (String tblName : tblNames) {
                T tbl = getTableNullable(tblName);
                if (tbl != null) {
                    tables.add(tbl);
                }
            }
            return tables;
        } else {
            return Lists.newArrayList(idToTbl.values());
        }
    }

    @Override
    public List<T> getViews() {
        throw new NotImplementedException("getViews() is not implemented");
    }

    @Override
    public List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        throw new NotImplementedException("getTablesOnIdOrderIfExist() is not implemented");
    }

    @Override
    public List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        throw new NotImplementedException("getTablesOnIdOrderOrThrowException() is not implemented");
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            return Sets.newHashSet(metaCache.listNames());
        } else {
            return Sets.newHashSet(tableNameToId.keySet());
        }
    }

    @Override
    public T getTableNullable(String tableName) {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            // must use full qualified name to generate id.
            // otherwise, if 2 databases have the same table name, the id will be the same.
            return metaCache.getMetaObj(tableName, Util.genIdByName(getQualifiedName(tableName))).orElse(null);
        } else {
            if (!tableNameToId.containsKey(tableName)) {
                return null;
            }
            return idToTbl.get(tableNameToId.get(tableName));
        }
    }

    @Override
    public T getTableNullable(long tableId) {
        makeSureInitialized();
        if (extCatalog.getUseMetaCache().get()) {
            return metaCache.getMetaObjById(tableId).orElse(null);
        } else {
            return idToTbl.get(tableId);
        }
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @SuppressWarnings("rawtypes")
    public static ExternalDatabase read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalDatabase.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        tableNameToId = Maps.newConcurrentMap();
        Map<Long, T> tmpIdToTbl = Maps.newConcurrentMap();
        for (Object obj : idToTbl.values()) {
            if (obj instanceof LinkedTreeMap) {
                String clazzType = ((LinkedTreeMap<?, ?>) obj).get("clazz").toString();
                switch (clazzType) {
                    case "ExternalInfoSchemaTable":
                        ExternalInfoSchemaTable infoSchemaTable = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(obj),
                                ExternalInfoSchemaTable.class);
                        tmpIdToTbl.put(infoSchemaTable.getId(), (T) infoSchemaTable);
                        tableNameToId.put(infoSchemaTable.getName(), infoSchemaTable.getId());
                        break;
                    case "ExternalMysqlTable":
                        ExternalMysqlTable mysqlTable = GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(obj),
                                ExternalMysqlTable.class);
                        tmpIdToTbl.put(mysqlTable.getId(), (T) mysqlTable);
                        tableNameToId.put(mysqlTable.getName(), mysqlTable.getId());
                        break;
                    default:
                        break;
                }
            } else {
                Preconditions.checkState(obj instanceof ExternalTable);
                tmpIdToTbl.put(((ExternalTable) obj).getId(), (T) obj);
                tableNameToId.put(((ExternalTable) obj).getName(), ((ExternalTable) obj).getId());
            }
        }
        idToTbl = tmpIdToTbl;
        rwLock = new MonitoredReentrantReadWriteLock(true);
    }

    @Override
    public void unregisterTable(String tableName) {
        makeSureInitialized();
        if (LOG.isDebugEnabled()) {
            LOG.debug("create table [{}]", tableName);
        }

        if (extCatalog.getUseMetaCache().get()) {
            if (isInitialized()) {
                metaCache.invalidate(tableName, Util.genIdByName(getQualifiedName(tableName)));
            }
        } else {
            Long tableId = tableNameToId.remove(tableName);
            if (tableId == null) {
                LOG.warn("table [{}] does not exist when drop", tableName);
                return;
            }
            idToTbl.remove(tableId);
        }
        setLastUpdateTime(System.currentTimeMillis());
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(
                extCatalog.getId(), getFullName(), tableName);
    }

    @Override
    public CatalogIf getCatalog() {
        return extCatalog;
    }

    // Only used for sync hive metastore event
    @Override
    public boolean registerTable(TableIf tableIf) {
        makeSureInitialized();
        long tableId = tableIf.getId();
        String tableName = tableIf.getName();
        if (LOG.isDebugEnabled()) {
            LOG.debug("create table [{}]", tableName);
        }
        if (extCatalog.getUseMetaCache().get()) {
            if (isInitialized()) {
                metaCache.updateCache(tableName, (T) tableIf, Util.genIdByName(getQualifiedName(tableName)));
            }
        } else {
            if (!tableNameToId.containsKey(tableName)) {
                tableNameToId.put(tableName, tableId);
                idToTbl.put(tableId, buildTableForInit(tableName, tableId, extCatalog));
            }
        }
        setLastUpdateTime(System.currentTimeMillis());
        return true;
    }

    public String getQualifiedName(String tblName) {
        return String.join(".", extCatalog.getName(), name, tblName);
    }
}
