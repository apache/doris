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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import java.util.List;

/**
 * all external metadata operations use this interface
 */
public interface ExternalMetadataOps {

    /**
     * create db in external metastore
     * @param stmt
     * @throws DdlException
     */
    default void createDb(CreateDbStmt stmt) throws DdlException {
        createDbImpl(stmt);
        afterCreateDb(stmt.getFullDbName());
    }

    void createDbImpl(CreateDbStmt stmt) throws DdlException;

    default void afterCreateDb(String dbName) {
    }

    /**
     * drop db in external metastore
     * @param stmt
     * @throws DdlException
     */
    default void dropDb(DropDbStmt stmt) throws DdlException {
        dropDbImpl(stmt.getDbName(), stmt.isSetIfExists(), stmt.isForceDrop());
        afterDropDb(stmt.getCtlName());
    }

    default void dropDb(String ctlName, String dbName, boolean ifExists, boolean force) throws DdlException {
        dropDbImpl(dbName, ifExists, force);
        afterDropDb(ctlName);
    }

    /**
     * drop db in external metastore for nereids
     * @param dbName
     * @param ifExists
     * @param force
     * @throws DdlException
     */
    void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException;

    void afterDropDb(String dbName);

    /**
     *
     * @param stmt
     * @return if set isExists is true, return true if table exists, otherwise return false
     * @throws UserException
     */
    default boolean createTable(CreateTableStmt stmt) throws UserException {
        boolean res = createTableImpl(stmt);
        if (!res) {
            afterCreateTable(stmt.getDbName(), stmt.getTableName());
        }
        return res;
    }

    boolean createTableImpl(CreateTableStmt stmt) throws UserException;

    default void afterCreateTable(String dbName, String tblName) {
    }

    /**
     *
     * @param stmt
     * @throws DdlException
     */
    default void dropTable(DropTableStmt stmt) throws DdlException {
        dropTableImpl(stmt);
        afterDropTable(stmt.getDbName(), stmt.getTableName());
    }

    default void dropTable(String dbName, String tableName, boolean ifExists) throws DdlException {
        dropTableImpl(dbName, tableName, ifExists);
        afterDropTable(dbName, tableName);
    }

    void dropTableImpl(DropTableStmt stmt) throws DdlException;

    void dropTableImpl(String dbName, String tableName, boolean ifExists) throws DdlException;

    default void afterDropTable(String dbName, String tblName) {
    }

    /**
     *
     * @param dbName
     * @param tblName
     * @param partitions
     */
    default void truncateTable(String dbName, String tblName, List<String> partitions) throws DdlException {
        truncateTableImpl(dbName, tblName, partitions);
        afterTruncateTable(dbName, tblName);
    }

    void truncateTableImpl(String dbName, String tblName, List<String> partitions) throws DdlException;

    default void afterTruncateTable(String dbName, String tblName) {
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
}
