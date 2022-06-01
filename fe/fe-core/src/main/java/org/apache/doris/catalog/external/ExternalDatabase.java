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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.DatabaseProperty;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
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

    private long id;
    private String name;
    private ReentrantReadWriteLock rwLock;

    private ExternalDataSource extDataSource;

    public ExternalDatabase() {

    }

    @Override
    public void readLock() {

    }

    @Override
    public void readUnlock() {

    }

    @Override
    public void writeLock() {

    }

    @Override
    public void writeUnlock() {

    }

    @Override
    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        return true;
    }

    @Override
    public boolean isWriteLockHeldByCurrentThread() {
        return false;
    }

    @Override
    public boolean writeLockIfExist() {
        return true;
    }

    @Override
    public <E extends Exception> void writeLockOrException(E e) throws E {

    }

    @Override
    public void writeLockOrDdlException() throws DdlException {

    }

    @Override
    public long getId() {
        return 0;
    }

    @Override
    public String getFullName() {
        return null;
    }

    @Override
    public DatabaseProperty getDbProperties() {
        return null;
    }

    @Override
    public boolean isTableExist(String tableName) {
        return extDataSource.tableExist(ConnectContext.get().getSessionContext(), name, tableName);
    }

    @Override
    public List<Table> getTables() {
        return null;
    }

    @Override
    public List<Table> getTablesOnIdOrder() {
        return null;
    }

    @Override
    public List<Table> getViews() {
        return null;
    }

    @Override
    public List<Table> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        return null;
    }

    @Override
    public List<Table> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        return null;
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        return null;
    }

    @Override
    public Table getTableNullable(String tableName) {
        return null;
    }

    @Override
    public Optional<Table> getTable(String tableName) {
        return Optional.empty();
    }

    @Override
    public Optional<Table> getTable(long tableId) {
        return Optional.empty();
    }

    @Override
    public <E extends Exception> Table getTableOrException(String tableName, Function<String, E> e) throws E {
        return null;
    }

    @Override
    public <E extends Exception> Table getTableOrException(long tableId, Function<Long, E> e) throws E {
        return null;
    }

    @Override
    public Table getTableOrMetaException(String tableName) throws MetaNotFoundException {
        return null;
    }

    @Override
    public Table getTableOrMetaException(long tableId) throws MetaNotFoundException {
        return null;
    }

    @Override
    public <T extends Table> T getTableOrMetaException(String tableName, Table.TableType tableType)
            throws MetaNotFoundException {
        return null;
    }

    @Override
    public <T extends Table> T getTableOrMetaException(long tableId, Table.TableType tableType)
            throws MetaNotFoundException {
        return null;
    }

    @Override
    public Table getTableOrDdlException(String tableName) throws DdlException {
        return null;
    }

    @Override
    public Table getTableOrDdlException(long tableId) throws DdlException {
        return null;
    }

    @Override
    public Table getTableOrAnalysisException(String tableName) throws AnalysisException {
        return null;
    }

    @Override
    public OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException {
        return null;
    }

    @Override
    public Table getTableOrAnalysisException(long tableId) throws AnalysisException {
        return null;
    }
}
