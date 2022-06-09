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

import org.apache.doris.catalog.*;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.ExternalDataSource;
import org.apache.doris.qe.ConnectContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ExternalDatabase implements DatabaseIf {

    private static final Logger LOG = LogManager.getLogger(Database.class);

    protected long id;
    protected String name;
    private ReentrantReadWriteLock rwLock;

    protected ExternalDataSource extDataSource;
    protected DatabaseProperty dbProperties;

    public ExternalDatabase(ExternalDataSource extDataSource, long id, String name) {
        this.extDataSource = extDataSource;
        this.rwLock = new ReentrantReadWriteLock(true);
        this.id = id;
        this.name = name;
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
        return extDataSource.tableExist(ConnectContext.get().getSessionContext(), name, tableName);
    }

    @Override
    public List<TableIf> getTables() {
        return null;
    }

    @Override
    public List<TableIf> getTablesOnIdOrder() {
        return null;
    }

    @Override
    public List<TableIf> getViews() {
        return null;
    }

    @Override
    public List<TableIf> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        return null;
    }

    @Override
    public List<TableIf> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        return null;
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        return null;
    }

    @Override
    public TableIf getTableNullable(String tableName) {
        return null;
    }

    @Override
    public Optional<TableIf> getTable(String tableName) {
        return Optional.empty();
    }

    @Override
    public Optional<TableIf> getTable(long tableId) {
        return Optional.empty();
    }

    @Override
    public <E extends Exception> TableIf getTableOrException(String tableName, Function<String, E> e) throws E {
        return null;
    }

    @Override
    public <E extends Exception> TableIf getTableOrException(long tableId, Function<Long, E> e) throws E {
        return null;
    }

    @Override
    public TableIf getTableOrMetaException(String tableName) throws MetaNotFoundException {
        return null;
    }

    @Override
    public TableIf getTableOrMetaException(long tableId) throws MetaNotFoundException {
        return null;
    }

    @Override
    public <T extends TableIf> T getTableOrMetaException(String tableName, TableIf.TableType tableType)
            throws MetaNotFoundException {
        return null;
    }

    @Override
    public <T extends TableIf> T getTableOrMetaException(long tableId, TableIf.TableType tableType)
            throws MetaNotFoundException {
        return null;
    }

    @Override
    public TableIf getTableOrDdlException(String tableName) throws DdlException {
        return null;
    }

    @Override
    public TableIf getTableOrDdlException(long tableId) throws DdlException {
        return null;
    }

    @Override
    public TableIf getTableOrAnalysisException(String tableName) throws AnalysisException {
        return null;
    }

    @Override
    public OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException {
        return null;
    }

    @Override
    public TableIf getTableOrAnalysisException(long tableId) throws AnalysisException {
        return null;
    }
}
