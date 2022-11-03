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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitDatabaseLog;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base class of external database.
 *
 * @param <T> External table type is ExternalTable or its subclass.
 */
public class ExternalDatabase<T extends ExternalTable> implements DatabaseIf<T>, Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(ExternalDatabase.class);

    protected ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "dbProperties")
    protected DatabaseProperty dbProperties = new DatabaseProperty();
    @SerializedName(value = "initialized")
    protected boolean initialized = false;
    protected ExternalCatalog extCatalog;

    /**
     * No args constructor for persist.
     */
    public ExternalDatabase() {
        initialized = false;
    }

    /**
     * Create external database.
     *
     * @param extCatalog The catalog this database belongs to.
     * @param id Database id.
     * @param name Database name.
     */
    public ExternalDatabase(ExternalCatalog extCatalog, long id, String name) {
        this.extCatalog = extCatalog;
        this.id = id;
        this.name = name;
    }

    public void setExtCatalog(ExternalCatalog extCatalog) {
        this.extCatalog = extCatalog;
    }

    public void setTableExtCatalog(ExternalCatalog extCatalog) {}

    public void setUnInitialized() {
        this.initialized = false;
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void makeSureInitialized() {}

    public T getTableForReplay(long tableId) {
        throw new NotImplementedException();
    }

    public void replayInitDb(InitDatabaseLog log, ExternalCatalog catalog) {
        throw new NotImplementedException();
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
        throw new NotImplementedException();
    }

    @Override
    public List<T> getTablesOnIdOrder() {
        throw new NotImplementedException();
    }

    @Override
    public List<T> getViews() {
        throw new NotImplementedException();
    }

    @Override
    public List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        throw new NotImplementedException();
    }

    @Override
    public List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        throw new NotImplementedException();
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        throw new NotImplementedException();
    }

    @Override
    public T getTableNullable(String tableName) {
        throw new NotImplementedException();
    }

    @Override
    public T getTableNullable(long tableId) {
        throw new NotImplementedException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ExternalDatabase read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalDatabase.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {}
}
