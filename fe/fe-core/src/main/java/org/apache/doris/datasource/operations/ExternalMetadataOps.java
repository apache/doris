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

package org.apache.doris.datasource.operations;

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.trees.plans.commands.info.CreateMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;

import org.apache.iceberg.view.View;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * all external metadata operations use this interface
 */
public interface ExternalMetadataOps {

    /**
     * create db in external metastore
     * @param dbName
     * @param properties
     * @return false means db does not exist and is created this time
     * @throws DdlException
     */
    default boolean createDb(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException {
        boolean res = createDbImpl(dbName, ifNotExists, properties);
        if (!res) {
            afterCreateDb();
        }
        return res;
    }

    /**
     * create db in external metastore for nereids
     *
     * @param dbName the remote name that will be created in remote metastore
     * @param ifNotExists
     * @param properties
     * @return false means db does not exist and is created this time
     * @throws DdlException
     */
    boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties) throws DdlException;

    default void afterCreateDb() {
    }


    /**
     * drop db in external metastore
     *
     * @param dbName the local db name in Doris
     * @param ifExists
     * @param force
     * @throws DdlException
     */
    default void dropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        dropDbImpl(dbName, ifExists, force);
        afterDropDb(dbName);
    }

    void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException;

    void afterDropDb(String dbName);

    /**
     *
     * @param createMTMVInfo
     * @return return false means table does not exist and is created this time
     * @throws UserException
     */
    default boolean createTable(CreateMTMVInfo createMTMVInfo) throws UserException {
        boolean res = createTableImpl(createMTMVInfo);
        if (!res) {
            afterCreateTable(createMTMVInfo.getDbName(), createMTMVInfo.getTableName());
        }
        return res;
    }

    /**
     * @param createTableInfo
     * @return return false means table does not exist and is created this time
     * @throws UserException
     */
    default boolean createTable(CreateTableInfo createTableInfo) throws UserException {
        boolean res = createTableImpl(createTableInfo);
        if (!res) {
            afterCreateTable(createTableInfo.getDbName(), createTableInfo.getTableName());
        }
        return res;
    }

    /**
     *
     * @param stmt
     * @return return false means table does not exist and is created this time
     * @throws UserException
     */
    default boolean createTable(CreateTableStmt stmt) throws UserException {
        boolean res = createTableImpl(stmt);
        if (!res) {
            afterCreateTable(stmt.getDbName(), stmt.getTableName());
        }
        return res;
    }

    boolean createTableImpl(CreateMTMVInfo createMTMVInfo) throws UserException;

    boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException;

    boolean createTableImpl(CreateTableStmt stmt) throws UserException;

    default void afterCreateTable(String dbName, String tblName) {
    }

    default void dropTable(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        dropTableImpl(dorisTable, ifExists);
        afterDropTable(dorisTable.getDbName(), dorisTable.getName());
    }

    void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException;

    default void afterDropTable(String dbName, String tblName) {
    }

    /**
     * rename table in external metastore
     * @param dbName
     * @param oldName
     * @param newName
     * @throws DdlException
     */
    default void renameTable(String dbName, String oldName, String newName) throws DdlException {
        renameTableImpl(dbName, oldName, newName);
        afterRenameTable(dbName, oldName, newName);
    }

    default void renameTableImpl(String dbName, String oldName, String newName) throws DdlException {
        throw new UnsupportedOperationException("Rename table operation is not supported for this table type.");
    }

    default void afterRenameTable(String dbName, String oldName, String newName) {
        throw new UnsupportedOperationException("After rename table operation is not supported for this table type.");
    }

    /**
     * truncate table in external metastore
     *
     * @param dorisTable
     * @param partitions
     */
    default void truncateTable(ExternalTable dorisTable, List<String> partitions) throws DdlException {
        truncateTableImpl(dorisTable, partitions);
        afterTruncateTable(dorisTable.getDbName(), dorisTable.getName());
    }

    void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) throws DdlException;

    default void afterTruncateTable(String dbName, String tblName) {
    }

    /**
     * create or replace branch in external metastore
     *
     * @param dorisTable
     * @param branchInfo
     * @throws UserException
     */
    default void createOrReplaceBranch(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        createOrReplaceBranchImpl(dorisTable, branchInfo);
        afterOperateOnBranchOrTag(dorisTable.getDbName(), dorisTable.getName());
    }

    void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException;

    default void afterOperateOnBranchOrTag(String dbName, String tblName) {
    }

    /**
     * create or replace tag in external metastore
     *
     * @param dorisTable
     * @param tagInfo
     * @throws UserException
     */
    default void createOrReplaceTag(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException {
        createOrReplaceTagImpl(dorisTable, tagInfo);
        afterOperateOnBranchOrTag(dorisTable.getDbName(), dorisTable.getName());
    }

    void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo)
            throws UserException;

    /**
     * drop tag in external metastore
     *
     * @param dorisTable
     * @param tagInfo
     * @throws UserException
     */
    default void dropTag(ExternalTable dorisTable, DropTagInfo tagInfo)
            throws UserException {
        dropTagImpl(dorisTable, tagInfo);
        afterOperateOnBranchOrTag(dorisTable.getDbName(), dorisTable.getName());
    }

    void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException;

    /**
     * drop branch in external metastore
     *
     * @param dorisTable
     * @param branchInfo
     * @throws UserException
     */
    default void dropBranch(ExternalTable dorisTable, DropBranchInfo branchInfo)
            throws UserException {
        dropBranchImpl(dorisTable, branchInfo);
        afterOperateOnBranchOrTag(dorisTable.getDbName(), dorisTable.getName());
    }

    void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException;

    /**
     * add column for external table
     *
     * @param dorisTable
     * @param column
     * @param position
     * @throws UserException
     */
    default void addColumn(ExternalTable dorisTable, Column column, ColumnPosition position)
            throws UserException {
        throw new UnsupportedOperationException("Add column operation is not supported for this table type.");
    }

    /**
     * add columns for external table
     *
     * @param dorisTable
     * @param columns
     * @throws UserException
     */
    default void addColumns(ExternalTable dorisTable, List<Column> columns)
            throws UserException {
        throw new UnsupportedOperationException("Add columns operation is not supported for this table type.");
    }

    /**
     * drop column for external table
     *
     * @param dorisTable
     * @param columnName
     * @throws UserException
     */
    default void dropColumn(ExternalTable dorisTable, String columnName)
            throws UserException {
        throw new UnsupportedOperationException("Drop column operation is not supported for this table type.");
    }

    /**
     * rename column for external table
     *
     * @param dorisTable
     * @param oldName
     * @param newName
     * @throws UserException
     */
    default void renameColumn(ExternalTable dorisTable, String oldName, String newName)
            throws UserException {
        throw new UnsupportedOperationException("Rename column operation is not supported for this table type.");
    }

    /**
     * update column for external table
     *
     * @param dorisTable
     * @param column
     * @param position
     * @throws UserException
     */
    default void modifyColumn(ExternalTable dorisTable, Column column, ColumnPosition position)
            throws UserException {
        throw new UnsupportedOperationException("Modify column operation is not supported for this table type.");
    }

    /**
     * reorder columns for external table
     *
     * @param dorisTable
     * @param newOrder
     * @throws UserException
     */
    default void reorderColumns(ExternalTable dorisTable, List<String> newOrder)
            throws UserException {
        throw new UnsupportedOperationException("Reorder columns operation is not supported for this table type.");
    }

    /**
     *
     * @return
     */
    List<String> listDatabaseNames();

    /**
     *
     * @param db
     * @return
     */
    List<String> listTableNames(String db);

    /**
     *
     * @param dbName
     * @param tblName
     * @return
     */
    boolean tableExist(String dbName, String tblName);

    boolean databaseExist(String dbName);

    default Object loadTable(String dbName, String tblName) {
        throw new UnsupportedOperationException("Load table is not supported.");
    }

    /**
     * close the connection, eg, to hms
     */
    void close();

    /**
     * load an iceberg view.
     * @param dbName
     * @param viewName
     * @return
     */
    default View loadView(String dbName, String viewName) {
        throw new UnsupportedOperationException("Load view is not supported.");
    }

    /**
     * Check if an Iceberg view exists.
     * @param dbName
     * @param viewName
     * @return
     */
    default boolean viewExists(String dbName, String viewName) {
        throw new UnsupportedOperationException("View is not supported.");
    }

    /**
     * List all views under a specific database.
     * @param db
     * @return
     */
    default List<String> listViewNames(String db) {
        return Collections.emptyList();
    }

}
