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

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Database interface.
 * TODO:
 * I just copied some common interface from the origin Database class.
 * Maybe changed later.
 */
public interface DatabaseIf<T extends TableIf> {
    Logger LOG = LogManager.getLogger(DatabaseIf.class);

    void readLock();

    void readUnlock();

    void writeLock();

    void writeUnlock();

    boolean tryWriteLock(long timeout, TimeUnit unit);

    boolean isWriteLockHeldByCurrentThread();

    boolean writeLockIfExist();

    <E extends Exception> void writeLockOrException(E e) throws E;

    void writeLockOrDdlException() throws DdlException;

    long getId();

    String getFullName();

    DatabaseProperty getDbProperties();

    boolean isTableExist(String tableName);

    List<T> getTables();

    default List<T> getTablesIgnoreException() {
        try {
            return getTables();
        } catch (Exception e) {
            LOG.warn("failed to get tables for db {}", getFullName(), e);
            return Lists.newArrayList();
        }
    }

    List<T> getViews();

    default List<T> getViewsOrEmpty() {
        try {
            return getViews();
        } catch (Exception e) {
            LOG.warn("failed to get views for db {}", getFullName(), e);
            return Lists.newArrayList();
        }
    }

    List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList);

    List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException;

    Set<String> getTableNamesWithLock();

    default Set<String> getTableNamesOrEmptyWithLock() {
        try {
            return getTableNamesWithLock();
        } catch (Exception e) {
            LOG.warn("failed to get table names for db {}", getFullName(), e);
            return Sets.newHashSet();
        }
    }

    T getTableNullable(String tableName);

    default T getTableNullableIfException(String tableName) {
        try {
            return getTableNullable(tableName);
        } catch (Exception e) {
            LOG.warn("failed to get table {} in database {}", tableName, getFullName(), e);
            return null;
        }
    }

    T getTableNullable(long tableId);

    default Optional<T> getTable(String tableName) {
        return Optional.ofNullable(getTableNullable(tableName));
    }

    default Optional<T> getTable(long tableId) {
        return Optional.ofNullable(getTableNullable(tableId));
    }

    default <E extends Exception> T getTableOrException(String tableName, java.util.function.Function<String, E> e)
            throws E {
        T table = getTableNullable(tableName);
        if (table == null) {
            throw e.apply(tableName);
        }
        return table;
    }

    default <E extends Exception> T getTableOrException(long tableId, Function<Long, E> e) throws E {
        T table = getTableNullable(tableId);
        if (table == null) {
            throw e.apply(tableId);
        }
        return table;
    }

    default T getTableOrMetaException(String tableName) throws MetaNotFoundException {
        return getTableOrException(tableName, t -> new MetaNotFoundException("table not found, tableName=" + t,
                                                        ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    default T getTableOrMetaException(long tableId) throws MetaNotFoundException {
        return getTableOrException(tableId, t -> new MetaNotFoundException("table not found, tableId=" + t,
                                                        ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    default T getTableOrMetaException(String tableName, TableIf.TableType tableType) throws MetaNotFoundException {
        T table = getTableOrMetaException(tableName);
        TableType type = Objects.requireNonNull(table.getType());
        if (type != tableType && type.getParentType() != tableType) {
            throw new MetaNotFoundException(
                    "table type is not " + tableType + ", tableName=" + tableName + ", type=" + type);
        }
        return table;
    }

    default T getTableOrMetaException(String tableName, List<TableIf.TableType> tableTypes)
            throws MetaNotFoundException {
        T table = getTableOrMetaException(tableName);
        TableType type = Objects.requireNonNull(table.getType());
        if (!tableTypes.contains(type) && !tableTypes.contains(type.getParentType())) {
            throw new MetaNotFoundException(
                    "Type of " + tableName + " doesn't match, expected data tables=" + tableTypes);
        }
        return table;
    }

    default T getTableOrMetaException(String tableName, TableIf.TableType... tableTypes)
            throws MetaNotFoundException {
        return getTableOrMetaException(tableName, Arrays.asList(tableTypes));
    }

    default T getTableOrMetaException(long tableId, TableIf.TableType tableType) throws MetaNotFoundException {
        T table = getTableOrMetaException(tableId);
        TableType type = Objects.requireNonNull(table.getType());
        if (type != tableType && type.getParentType() != tableType) {
            throw new MetaNotFoundException(
                    "table type is not " + tableType + ", tableId=" + tableId + ", type=" + type);
        }
        return table;
    }

    default T getTableOrMetaException(long tableId, List<TableIf.TableType> tableTypes)
            throws MetaNotFoundException {
        T table = getTableOrMetaException(tableId);
        TableType type = Objects.requireNonNull(table.getType());
        if (!tableTypes.contains(type) && !tableTypes.contains(type.getParentType())) {
            throw new MetaNotFoundException(
                    "Type of " + tableId + " doesn't match, expected data tables=" + tableTypes);
        }
        return table;
    }

    default T getTableOrDdlException(String tableName) throws DdlException {
        return getTableOrException(tableName, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t),
                                                            ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    default T getTableOrDdlException(String tableName, TableIf.TableType tableType) throws DdlException {
        T table = getTableOrDdlException(tableName);
        TableType type = Objects.requireNonNull(table.getType());
        if (type != tableType && type.getParentType() != tableType) {
            throw new DdlException(
                    "table type is not " + tableType + ", tableName=" + tableName + ", type=" + type);
        }
        return table;
    }

    default T getTableOrDdlException(long tableId) throws DdlException {
        return getTableOrException(tableId, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t),
                                                            ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    default T getTableOrDdlException(long tableId, TableIf.TableType tableType) throws DdlException {
        T table = getTableOrDdlException(tableId);
        TableType type = Objects.requireNonNull(table.getType());
        if (type != tableType && type.getParentType() != tableType) {
            throw new DdlException(
                    "table type is not " + tableType + ", tableId=" + tableId + ", type=" + type);
        }
        return table;
    }

    default T getTableOrAnalysisException(String tableName) throws AnalysisException {
        return getTableOrException(tableName,
                t -> new AnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t),
                                ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    default T getTableOrAnalysisException(long tableId) throws AnalysisException {
        return getTableOrException(tableId,
                t -> new AnalysisException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t),
                                ErrorCode.ERR_BAD_TABLE_ERROR));
    }

    default OlapTable getOlapTableOrDdlException(String tableName) throws DdlException {
        T table = getTableOrDdlException(tableName);
        if (!(table instanceof OlapTable)) {
            throw new DdlException(ErrorCode.ERR_NOT_OLAP_TABLE.formatErrorMsg(tableName));
        }
        return (OlapTable) table;
    }

    default OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException {
        T table = getTableOrAnalysisException(tableName);
        if (!(table instanceof OlapTable)) {
            throw new AnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE.formatErrorMsg(tableName));
        }
        return (OlapTable) table;
    }

    /**
     * register table to memory
     * @param table created table
     * @return true if add to memory
     */
    boolean registerTable(TableIf table);

    /**
     * unregister table from memory
     * @param tableName table name
     */
    void unregisterTable(String tableName);

    CatalogIf getCatalog();

    default long getLastUpdateTime() {
        return -1L;
    }
}
