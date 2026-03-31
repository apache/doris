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

package org.apache.doris.datasource.jdbc;

import org.apache.doris.thrift.TOdbcTableType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for formatting database, table, and column names according to
 * the target JDBC/ODBC database type conventions (quoting, case rules).
 *
 * <p>Extracted from {@code JdbcTable} so that both {@code JdbcExternalTable}
 * and any remaining ODBC code paths can share the same logic.
 */
public final class JdbcNameUtil {

    private JdbcNameUtil() {
    }

    // ========= Core formatting =========

    /**
     * Formats a name (database, table, or schema) by wrapping each component
     * with the specified characters, optionally converting case.
     *
     * <p>The name is expected in {@code "schemaName.tableName"} format.
     * If there is no '.', the entire string is treated as one component.
     */
    public static String formatName(String name, String wrapStart, String wrapEnd,
            boolean toUpperCase, boolean toLowerCase) {
        int index = name.indexOf(".");
        if (index == -1) {
            String newName = toUpperCase ? name.toUpperCase() : name;
            newName = toLowerCase ? newName.toLowerCase() : newName;
            return wrapStart + newName + wrapEnd;
        } else {
            String schemaName = toUpperCase ? name.substring(0, index).toUpperCase() : name.substring(0, index);
            schemaName = toLowerCase ? schemaName.toLowerCase() : schemaName;
            String tableName = toUpperCase ? name.substring(index + 1).toUpperCase() : name.substring(index + 1);
            tableName = toLowerCase ? tableName.toLowerCase() : tableName;
            return wrapStart + schemaName + wrapEnd + "." + wrapStart + tableName + wrapEnd;
        }
    }

    /**
     * Formats a database/table/column name according to the database type quoting rules.
     */
    public static String databaseProperName(TOdbcTableType tableType, String name) {
        switch (tableType) {
            case MYSQL:
            case OCEANBASE:
            case GBASE:
                return formatName(name, "`", "`", false, false);
            case SQLSERVER:
                return formatName(name, "[", "]", false, false);
            case POSTGRESQL:
            case CLICKHOUSE:
            case TRINO:
            case PRESTO:
            case OCEANBASE_ORACLE:
            case SAP_HANA:
                return formatName(name, "\"", "\"", false, false);
            case ORACLE:
            case DB2:
                return formatName(name, "\"", "\"", true, false);
            default:
                return name;
        }
    }

    /**
     * Wraps a remote name (already in the correct case) with the appropriate quotes for the database type.
     */
    public static String properNameWithRemoteName(TOdbcTableType tableType, String remoteName) {
        switch (tableType) {
            case MYSQL:
            case OCEANBASE:
            case GBASE:
                return formatNameWithRemoteName(remoteName, "`", "`");
            case SQLSERVER:
                return formatNameWithRemoteName(remoteName, "[", "]");
            case POSTGRESQL:
            case CLICKHOUSE:
            case TRINO:
            case PRESTO:
            case OCEANBASE_ORACLE:
            case ORACLE:
            case SAP_HANA:
            case DB2:
                return formatNameWithRemoteName(remoteName, "\"", "\"");
            default:
                return remoteName;
        }
    }

    public static String formatNameWithRemoteName(String remoteName, String wrapStart, String wrapEnd) {
        return wrapStart + remoteName + wrapEnd;
    }

    // ========= Composite name builders =========

    /**
     * Build the properly quoted full table name (database.table) using remote names.
     *
     * @param tableType       the target database type
     * @param remoteDatabaseName remote database name (may be null for legacy internal tables)
     * @param remoteTableName    remote table name (may be null for legacy internal tables)
     * @param externalTableName  fallback name when remote names are not available
     */
    public static String getProperRemoteFullTableName(TOdbcTableType tableType, String remoteDatabaseName,
            String remoteTableName, String externalTableName) {
        if (remoteDatabaseName == null || remoteTableName == null) {
            return databaseProperName(tableType, externalTableName);
        } else {
            return properNameWithRemoteName(tableType, remoteDatabaseName) + "."
                    + properNameWithRemoteName(tableType, remoteTableName);
        }
    }

    /**
     * Build the properly quoted column name, looking up the remote name from the mapping.
     *
     * @param tableType         the target database type
     * @param columnName        the local column name
     * @param remoteColumnNames mapping from local column name to remote column name (may be null)
     */
    public static String getProperRemoteColumnName(TOdbcTableType tableType, String columnName,
            Map<String, String> remoteColumnNames) {
        if (remoteColumnNames == null || remoteColumnNames.isEmpty() || !remoteColumnNames.containsKey(columnName)) {
            return databaseProperName(tableType, columnName);
        } else {
            return properNameWithRemoteName(tableType, remoteColumnNames.get(columnName));
        }
    }

    // ========= SQL builders =========

    /**
     * Build a parameterized INSERT SQL for the given table and columns.
     */
    public static String getInsertSql(TOdbcTableType tableType, String remoteDatabaseName, String remoteTableName,
            String externalTableName, Map<String, String> remoteColumnNames, List<String> insertCols) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(getProperRemoteFullTableName(tableType, remoteDatabaseName, remoteTableName, externalTableName));
        sb.append("(");
        List<String> transformedCols = insertCols.stream()
                .map(col -> getProperRemoteColumnName(tableType, col, remoteColumnNames))
                .collect(Collectors.toList());
        sb.append(String.join(",", transformedCols));
        sb.append(")");
        sb.append(" VALUES (");
        sb.append(String.join(", ", Collections.nCopies(insertCols.size(), "?")));
        sb.append(")");
        return sb.toString();
    }
}
