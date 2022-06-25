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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * External table represent tables that are not self-managed by Doris.
 * Such as tables from hive, iceberg, es, etc.
 */
public class ExternalTable implements TableIf {

    private static final Logger LOG = LogManager.getLogger(ExternalTable.class);

    protected long id;
    protected String name;
    protected ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock(true);
    protected TableType type = null;
    protected List<Column> fullSchema = null;

    /**
     * Create external table.
     *
     * @param id Table id.
     * @param name Table name.
     */
    public ExternalTable(long id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * Create external table.
     *
     * @param id Table id.
     * @param name Table name.
     * @param type Table type.
     */
    public ExternalTable(long id, String name, TableType type) {
        this.id = id;
        this.name = name;
        this.type = type;
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
        throw new NotImplementedException();
    }

    @Override
    public List<Column> getBaseSchema() {
        throw new NotImplementedException();
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        throw new NotImplementedException();
    }

    @Override
    public void setNewFullSchema(List<Column> newSchema) {
        this.fullSchema = newSchema;
    }

    @Override
    public Column getColumn(String name) {
        throw new NotImplementedException();
    }

    @Override
    public String getMysqlType() {
        throw new NotImplementedException();
    }

    @Override
    public String getEngine() {
        throw new NotImplementedException();
    }

    @Override
    public String getComment() {
        throw new NotImplementedException();
    }

    @Override
    public long getCreateTime() {
        throw new NotImplementedException();
    }

    @Override
    public long getUpdateTime() {
        throw new NotImplementedException();
    }

    @Override
    public long getRowCount() {
        throw new NotImplementedException();
    }

    @Override
    public long getDataLength() {
        throw new NotImplementedException();
    }

    @Override
    public long getAvgRowLength() {
        throw new NotImplementedException();
    }

    public long getLastCheckTime() {
        throw new NotImplementedException();
    }
}
