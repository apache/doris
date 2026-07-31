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

package org.apache.doris.connector.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Database-specific identifier quoting for JDBC SQL generation.
 *
 * <p>Different databases use different quoting characters for identifiers
 * (table names, column names). This utility provides the correct quoting
 * for each supported database type.</p>
 */
public final class JdbcIdentifierQuoter {

    private JdbcIdentifierQuoter() {
    }

    /**
     * Quotes an identifier (column name, table name) for the given database type.
     *
     * <p>The identifier is wrapped with the appropriate quoting characters.
     * Oracle and DB2 identifiers are also uppercased (per their convention).</p>
     */
    public static String quoteIdentifier(JdbcDbType dbType, String name) {
        switch (dbType) {
            case MYSQL:
            case OCEANBASE:
            case GBASE:
                return "`" + name + "`";
            case SQLSERVER:
                return "[" + name + "]";
            case ORACLE:
            case DB2:
                return "\"" + name.toUpperCase() + "\"";
            case POSTGRESQL:
            case CLICKHOUSE:
            case TRINO:
            case PRESTO:
            case OCEANBASE_ORACLE:
            case SAP_HANA:
                return "\"" + name + "\"";
            default:
                return name;
        }
    }

    /**
     * Quotes an identifier that is already in the correct remote case
     * (no case conversion needed).
     */
    public static String quoteRemoteIdentifier(JdbcDbType dbType, String remoteName) {
        switch (dbType) {
            case MYSQL:
            case OCEANBASE:
            case GBASE:
                return "`" + remoteName + "`";
            case SQLSERVER:
                return "[" + remoteName + "]";
            case POSTGRESQL:
            case CLICKHOUSE:
            case TRINO:
            case PRESTO:
            case OCEANBASE_ORACLE:
            case ORACLE:
            case SAP_HANA:
            case DB2:
                return "\"" + remoteName + "\"";
            default:
                return remoteName;
        }
    }

    /**
     * Builds the fully qualified table name: "db"."table" with proper quoting.
     */
    public static String quoteFullTableName(JdbcDbType dbType,
            String remoteDbName, String remoteTableName) {
        return quoteRemoteIdentifier(dbType, remoteDbName)
                + "." + quoteRemoteIdentifier(dbType, remoteTableName);
    }

    /**
     * Builds a parameterized INSERT SQL for the given table and columns.
     *
     * @param dbType            the target database type for identifier quoting
     * @param remoteDbName      the remote database name
     * @param remoteTableName   the remote table name
     * @param remoteColumnNames mapping from local column name to remote column name (may be null)
     * @param insertCols        ordered list of local column names to insert
     * @return INSERT INTO "db"."table"("col1","col2") VALUES (?, ?)
     */
    public static String buildInsertSql(JdbcDbType dbType, String remoteDbName,
            String remoteTableName, Map<String, String> remoteColumnNames,
            List<String> insertCols) {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(quoteFullTableName(dbType, remoteDbName, remoteTableName));
        sb.append("(");
        List<String> quotedCols = insertCols.stream()
                .map(col -> {
                    String remoteName = (remoteColumnNames != null && remoteColumnNames.containsKey(col))
                            ? remoteColumnNames.get(col) : col;
                    return quoteRemoteIdentifier(dbType, remoteName);
                })
                .collect(Collectors.toList());
        sb.append(String.join(",", quotedCols));
        sb.append(")");
        sb.append(" VALUES (");
        sb.append(String.join(", ", Collections.nCopies(insertCols.size(), "?")));
        sb.append(")");
        return sb.toString();
    }
}
