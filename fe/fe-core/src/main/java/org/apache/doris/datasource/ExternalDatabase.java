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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.infoschema.ExternalInfoSchemaDatabase;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterCatalogExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class of external database.
 *
 * @param <T> External table type is ExternalTable or its subclass.
 */
public abstract class ExternalDatabase<T extends ExternalTable> implements DatabaseIf<T> {
    private static final Logger LOG = LogManager.getLogger(ExternalDatabase.class);

    protected ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    protected long id;

    protected String name;
    protected DatabaseProperty dbProperties = new DatabaseProperty();

    protected volatile boolean initialized = false;

    protected volatile long lastUpdateTime;
    protected final ExternalCatalog.Type ctlType;

    protected ExternalCatalog extCatalog;

    protected boolean invalidCacheInInit = true;

    protected Map<Long, T> idToTbl = Maps.newConcurrentMap();
    protected Map<String, Long> tableNameToId = Maps.newConcurrentMap();

    /**
     * Create external database.
     *
     * @param extCatalog The catalog this database belongs to.
     * @param id Database id.
     * @param name Database name.
     */
    public ExternalDatabase(ExternalCatalog extCatalog, long id, String name, ExternalCatalog.Type type) {
        this.extCatalog = extCatalog;
        this.id = id;
        this.name = name;
        this.ctlType = type;
    }

    public void setUnInitialized(boolean invalidCache) {
        this.initialized = false;
        this.invalidCacheInInit = invalidCache;
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
            initForMaster();
        }
    }

    // only master should invoke this method.
    protected void initForMaster() {
        List<String> tableNames;
        if (name.equals(InfoSchemaDb.DATABASE_NAME)) {
            tableNames = ExternalInfoSchemaDatabase.listTableNames();
        } else {
            tableNames = extCatalog.listTableNames(null, name);
        }

        MetaIdMappingsLog log = new MetaIdMappingsLog();
        log.setCatalogId(extCatalog.getId());
        log.setFromInitDb(true);
        log.setLastUpdateTime(System.currentTimeMillis());
        if (CollectionUtils.isNotEmpty(tableNames)) {
            for (String tableName : tableNames) {
                MetaIdMappingsLog.MetaIdMapping metaIdMapping = new MetaIdMappingsLog.MetaIdMapping(
                        MetaIdMappingsLog.OPERATION_TYPE_ADD,
                        MetaIdMappingsLog.META_OBJECT_TYPE_TABLE,
                        name, tableName, ExternalMetaIdMgr.nextMetaId());
                log.addMetaIdMapping(metaIdMapping);
            }
        }
        Env.getCurrentEnv().getExternalMetaIdMgr().replayMetaIdMappingsLog(log);
        Env.getCurrentEnv().getEditLog().logMetaIdMappingsLog(log);
    }

    public void initForAllNodes(long lastUpdateTime) {
        ExternalMetaIdMgr metaIdMgr = Env.getCurrentEnv().getExternalMetaIdMgr();
        ExternalMetaIdMgr.DbMetaIdMgr dbMetaIdMgr = metaIdMgr.getDbMetaIdMgr(extCatalog.getId(), name);
        if (dbMetaIdMgr != null) {
            Map<String, ExternalMetaIdMgr.TblMetaIdMgr> tblNameToMgr = dbMetaIdMgr.getTblNameToMgr();
            for (String tableName : tblNameToMgr.keySet()) {
                T table = idToTbl.getOrDefault(tableNameToId.getOrDefault(tableName, -1L), null);
                if (table == null) {
                    table = newExternalTable(tableName, tblNameToMgr.get(tableName).getTblId(), extCatalog);
                }
                idToTbl.put(table.getId(), table);
                tableNameToId.put(tableName, table.getId());
            }
        }
        this.lastUpdateTime = lastUpdateTime;
        this.initialized = true;
    }

    protected abstract T newExternalTable(String tableName, long tblId, ExternalCatalog catalog);

    public T getTableForReplay(long tableId) {
        return idToTbl.get(tableId);
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

    @Override
    public List<T> getTables() {
        makeSureInitialized();
        return Lists.newArrayList(idToTbl.values());
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
        return Sets.newHashSet();
    }

    @Override
    public T getTableNullable(String tableName) {
        makeSureInitialized();
        if (!tableNameToId.containsKey(tableName)) {
            return null;
        }
        return idToTbl.get(tableNameToId.get(tableName));
    }

    @Override
    public T getTableNullable(long tableId) {
        makeSureInitialized();
        return idToTbl.get(tableId);
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    @Override
    public void unregisterTable(String tableName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("create table [{}]", tableName);
        }
        Long tableId = tableNameToId.remove(tableName);
        if (tableId == null) {
            LOG.warn("table [{}] does not exist when drop", tableName);
            return;
        }
        idToTbl.remove(tableId);
        setLastUpdateTime(System.currentTimeMillis());
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(
                extCatalog.getId(), getFullName(), tableName);
    }

    @Override
    public CatalogIf getCatalog() {
        return extCatalog;
    }

    // Only used for sync hive metastore event
    public boolean registerTable(TableIf tableIf) {
        long tableId = tableIf.getId();
        String tableName = tableIf.getName();
        if (LOG.isDebugEnabled()) {
            LOG.debug("create table [{}]", tableName);
        }
        tableNameToId.put(tableName, tableId);
        idToTbl.put(tableId, newExternalTable(tableName, tableId, extCatalog));
        setLastUpdateTime(System.currentTimeMillis());
        return true;
    }

    @Override
    public Map<Long, TableIf> getIdToTable() {
        return new HashMap<>(idToTbl);
    }
}
