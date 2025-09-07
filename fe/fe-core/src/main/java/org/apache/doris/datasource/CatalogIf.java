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

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.trees.plans.commands.CreateTableCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * The interface of Catalog
 */
public interface CatalogIf<T extends DatabaseIf> {
    Logger LOG = LogManager.getLogger(CatalogIf.class);

    // Type of this catalog
    String getType();

    long getId();

    // Name of this catalog
    String getName();

    List<String> getDbNames();

    default String getErrorMsg() {
        return "";
    }

    default boolean isInternalCatalog() {
        return this instanceof InternalCatalog;
    }

    // Will be used when querying the information_schema table
    // Unable to get db for uninitialized catalog to avoid query timeout
    default List<String> getDbNamesOrEmpty() {
        try {
            return getDbNames();
        } catch (Exception e) {
            LOG.warn("failed to get db names in catalog {}", getName(), e);
            return Lists.newArrayList();
        }
    }

    List<Long> getDbIds();

    @Nullable
    T getDbNullable(String dbName);

    @Nullable
    T getDbNullable(long dbId);

    Map<String, String> getProperties();

    default void notifyPropertiesUpdated(Map<String, String> updatedProps) {
        if (this instanceof ExternalCatalog) {
            ((ExternalCatalog) this).resetToUninitialized(false);
        }
    }

    void modifyCatalogName(String name);

    void modifyCatalogProps(Map<String, String> props);

    default Optional<T> getDb(String dbName) {
        return Optional.ofNullable(getDbNullable(dbName));
    }

    default Optional<T> getDb(long dbId) {
        return Optional.ofNullable(getDbNullable(dbId));
    }

    default <E extends Exception> T getDbOrException(String dbName, Function<String, E> e) throws E {
        T db = getDbNullable(dbName);
        if (db == null) {
            throw e.apply(dbName);
        }
        return db;
    }

    default <E extends Exception> T getDbOrException(long dbId, Function<Long, E> e) throws E {
        T db = getDbNullable(dbId);
        if (db == null) {
            throw e.apply(dbId);
        }
        return db;
    }

    default T getDbOrMetaException(String dbName) throws MetaNotFoundException {
        return getDbOrException(dbName,
                s -> new MetaNotFoundException("unknown databases, dbName=" + s, ErrorCode.ERR_BAD_DB_ERROR));
    }

    default T getDbOrMetaException(long dbId) throws MetaNotFoundException {
        return getDbOrException(dbId,
                s -> new MetaNotFoundException("unknown databases, dbId=" + s, ErrorCode.ERR_BAD_DB_ERROR));
    }

    default T getDbOrDdlException(String dbName) throws DdlException {
        return getDbOrException(dbName,
                s -> new DdlException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    default T getDbOrDdlException(long dbId) throws DdlException {
        return getDbOrException(dbId,
                s -> new DdlException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    default T getDbOrAnalysisException(String dbName) throws AnalysisException {
        return getDbOrException(dbName,
                s -> new AnalysisException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    default T getDbOrAnalysisException(long dbId) throws AnalysisException {
        return getDbOrException(dbId,
                s -> new AnalysisException(ErrorCode.ERR_BAD_DB_ERROR.formatErrorMsg(s), ErrorCode.ERR_BAD_DB_ERROR));
    }

    // Called when catalog is dropped
    default void onClose() {
        Env.getCurrentEnv().getRefreshManager().removeFromRefreshMap(getId());
    }

    String getComment();

    default void setComment(String comment) {
    }

    default long getLastUpdateTime() {
        return -1L;
    }

    default CatalogLog constructEditLog() {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(getId());
        log.setCatalogName(getName());
        log.setResource("");
        log.setComment(getComment());
        log.setProps(getProperties());
        return log;
    }

    TableName getTableNameByTableId(Long tableId);

    // Return a copy of all db collection.
    Collection<DatabaseIf<? extends TableIf>> getAllDbs();

    boolean enableAutoAnalyze();

    void createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException;

    void dropDb(String dbName, boolean ifExists, boolean force) throws DdlException;

    /**
     * @return if org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo.ifNotExists is true,
     * return true if table exists,
     * return false otherwise
     */
    boolean createTable(CreateTableCommand command) throws UserException;

    /**
     * @return if org.apache.doris.analysis.CreateTableStmt.ifNotExists is true, return true if table exists,
     * return false otherwise
     */
    boolean createTable(CreateTableStmt stmt) throws UserException;

    void dropTable(String dbName, String tableName, boolean isView, boolean isMtmv, boolean ifExists,
            boolean mustTemporary, boolean force) throws DdlException;

    default void renameTable(String dbName, String oldTableName, String newTableName) throws DdlException {
        throw new UnsupportedOperationException("Not support rename table operation");
    }

    void truncateTable(String dbName, String tableName, PartitionNames partitionNames, boolean forceDrop,
            String rawTruncateSql)
            throws DdlException;

    // Convert from remote database name to local database name, overridden by subclass if necessary
    default String fromRemoteDatabaseName(String remoteDatabaseName) {
        return remoteDatabaseName;
    }

    // Convert from remote table name to local table name, overridden by subclass if necessary
    default String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
        return remoteTableName;
    }

    // Create or replace branch operations, overridden by subclass if necessary
    default void createOrReplaceBranch(TableIf dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        throw new UserException("Not support create or replace branch operation");
    }

    // Create or replace tag operation, overridden by subclass if necessary
    default void createOrReplaceTag(TableIf dorisTable, CreateOrReplaceTagInfo tagInfo) throws UserException {
        throw new UserException("Not support create or replace tag operation");
    }

    default void replayOperateOnBranchOrTag(String dbName, String tblName) {

    }

    default void dropBranch(TableIf dorisTable, DropBranchInfo branchInfo) throws UserException {
        throw new UserException("Not support drop branch operation");
    }

    default void dropTag(TableIf dorisTable, DropTagInfo tagInfo) throws UserException {
        throw new UserException("Not support drop tag operation");
    }

    // schema change operations:
    // - Adding, deleting, modify and renaming columns
    // - Reordering top-level columns fields

    default void addColumn(TableIf table, Column column, ColumnPosition columnPosition) throws UserException {
        throw new UserException("Not support add column operation");
    }

    default void addColumns(TableIf table, List<Column> columns) throws UserException {
        throw new UserException("Not support add columns operation");
    }

    default void dropColumn(TableIf table, String name) throws UserException {
        throw new UserException("Not support drop column operation");
    }

    default void renameColumn(TableIf table, String oldName, String newName) throws UserException {
        throw new UserException("Not support rename column operation");
    }

    default void modifyColumn(TableIf table, Column column, ColumnPosition columnPosition) throws UserException {
        throw new UserException("Not support update column operation");
    }

    default void reorderColumns(TableIf table, List<String> newOrder) throws UserException {
        throw new UserException("Not support reorder columns operation");
    }
}
