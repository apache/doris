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

import org.apache.doris.alter.AlterCancelException;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalSchemaCache;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.statistics.AnalysisTaskInfo;
import org.apache.doris.statistics.AnalysisTaskScheduler;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * External table represent tables that are not self-managed by Doris.
 * Such as tables from hive, iceberg, es, etc.
 */
@Getter
public class ExternalTable implements TableIf, Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(ExternalTable.class);

    @SerializedName(value = "id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected TableType type = null;
    @SerializedName(value = "timestamp")
    protected long timestamp;
    @SerializedName(value = "dbName")
    protected String dbName;

    protected boolean objectCreated = false;
    protected ExternalCatalog catalog;
    protected ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * No args constructor for persist.
     */
    public ExternalTable() {
        this.objectCreated = false;
    }

    /**
     * Create external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param catalog ExternalCatalog this table belongs to.
     * @param dbName Name of the db the this table belongs to.
     * @param type Table type.
     */
    public ExternalTable(long id, String name, ExternalCatalog catalog, String dbName, TableType type) {
        this.id = id;
        this.name = name;
        this.catalog = catalog;
        this.dbName = dbName;
        this.type = type;
        this.objectCreated = false;
    }

    public void setCatalog(ExternalCatalog catalog) {
        this.catalog = catalog;
    }

    public boolean isView() {
        return false;
    }

    protected void makeSureInitialized() {
        throw new NotImplementedException();
    }

    @Override
    public void readLock() {
        this.rwLock.readLock().lock();
    }

    @Override
    public boolean tryReadLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.readLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try read lock at table[" + name + "]", e);
            return false;
        }
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
    public boolean writeLockIfExist() {
        writeLock();
        return true;
    }

    @Override
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at table[" + name + "]", e);
            return false;
        }
    }

    @Override
    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    @Override
    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    @Override
    public <E extends Exception> void writeLockOrException(E e) throws E {
        writeLock();
    }

    @Override
    public void writeLockOrDdlException() throws DdlException {
        writeLockOrException(new DdlException("unknown table, tableName=" + name));
    }

    @Override
    public void writeLockOrMetaException() throws MetaNotFoundException {
        writeLockOrException(new MetaNotFoundException("unknown table, tableName=" + name));
    }

    @Override
    public void writeLockOrAlterCancelException() throws AlterCancelException {
        writeLockOrException(new AlterCancelException("unknown table, tableName=" + name));
    }

    @Override
    public boolean tryWriteLockOrMetaException(long timeout, TimeUnit unit) throws MetaNotFoundException {
        return tryWriteLockOrException(timeout, unit, new MetaNotFoundException("unknown table, tableName=" + name));
    }

    @Override
    public <E extends Exception> boolean tryWriteLockOrException(long timeout, TimeUnit unit, E e) throws E {
        if (tryWriteLock(timeout, unit)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean tryWriteLockIfExist(long timeout, TimeUnit unit) {
        if (tryWriteLock(timeout, unit)) {
            return true;
        }
        return false;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TableType getType() {
        return type;
    }

    @Override
    public List<Column> getFullSchema() {
        ExternalSchemaCache cache = Env.getCurrentEnv().getExtMetaCacheMgr().getSchemaCache(catalog);
        return cache.getSchema(dbName, name);
    }

    @Override
    public List<Column> getBaseSchema() {
        return getFullSchema();
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return getFullSchema();
    }


    @Override
    public void setNewFullSchema(List<Column> newSchema) {
    }

    @Override
    public Column getColumn(String name) {
        List<Column> schema = getFullSchema();
        for (Column column : schema) {
            if (name.equals(column.getName())) {
                return column;
            }
        }
        return null;
    }

    @Override
    public String getEngine() {
        return getType().toEngineName();
    }

    @Override
    public String getMysqlType() {
        return getType().toMysqlType();
    }

    @Override
    public long getRowCount() {
        return 0;
    }

    @Override
    public long getAvgRowLength() {
        return 0;
    }

    @Override
    public long getDataLength() {
        return 0;
    }

    @Override
    public long getCreateTime() {
        return 0;
    }

    @Override
    public long getUpdateTime() {
        return 0;
    }

    @Override
    public long getLastCheckTime() {
        return 0;
    }

    @Override
    public String getComment() {
        return "";
    }

    @Override
    public String getComment(boolean escapeQuota) {
        return "";
    }

    public TTableDescriptor toThrift() {
        return null;
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisTaskScheduler scheduler, AnalysisTaskInfo info) {
        throw new NotImplementedException();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ExternalTable read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ExternalTable.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        rwLock = new ReentrantReadWriteLock(true);
        objectCreated = false;
    }
}
