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

package org.apache.doris.connector.jdbc.client;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.jdbc.JdbcDbType;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * GBase-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcGbaseClient}.
 */
public class JdbcGbaseConnectorClient extends JdbcConnectorClient {

    public JdbcGbaseConnectorClient(
            String catalogName, JdbcDbType dbType, String jdbcUrl,
            boolean onlySpecifiedDatabase,
            Map<String, Boolean> includeDatabaseMap,
            Map<String, Boolean> excludeDatabaseMap,
            boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        super(catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                includeDatabaseMap, excludeDatabaseMap,
                enableMappingVarbinary, enableMappingTimestampTz);
    }

    @Override
    public List<String> getDatabaseNameList() {
        Connection conn = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<>();
        try {
            conn = getConnection();
            if (onlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String current = conn.getCatalog();
                names.add(current);
            } else {
                rs = conn.getMetaData().getCatalogs();
                while (rs.next()) {
                    names.add(rs.getString("TABLE_CAT"));
                }
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to get database name list from GBase", e);
        } finally {
            closeResources(rs, conn);
        }
        return filterDatabaseNames(names);
    }

    @Override
    protected void processTable(String remoteDbName, String remoteTableName,
            String[] tableTypes, Consumer<ResultSet> consumer) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            rs = meta.getTables(remoteDbName, null, remoteTableName, tableTypes);
            consumer.accept(rs);
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to process table", e);
        } finally {
            closeResources(rs, conn);
        }
    }

    @Override
    protected ResultSet getRemoteColumns(DatabaseMetaData meta, String catalog,
            String remoteDbName, String remoteTableName) throws SQLException {
        return meta.getColumns(remoteDbName, null, remoteTableName, null);
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("information_schema");
        set.add("performance_schema");
        set.add("gbase");
        set.add("gctmpdb");
        set.add("sys");
        return set;
    }

    @Override
    public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
        Connection conn = null;
        ResultSet rs = null;
        List<JdbcFieldInfo> schema = new ArrayList<>();
        try {
            conn = getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            String cat = getCatalogName(conn);
            rs = getRemoteColumns(meta, cat, remoteDbName, remoteTableName);
            while (rs.next()) {
                schema.add(new JdbcFieldInfo(rs));
            }
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get JDBC columns info for " + remoteDbName + "." + remoteTableName, e);
        } finally {
            closeResources(rs, conn);
        }
        return schema;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        int sqlType = fieldInfo.getDataType();
        switch (sqlType) {
            case Types.BOOLEAN:
                return ConnectorType.of("BOOLEAN");
            case Types.TINYINT:
                return ConnectorType.of("TINYINT");
            case Types.SMALLINT:
                return ConnectorType.of("SMALLINT");
            case Types.INTEGER:
                return ConnectorType.of("INT");
            case Types.BIGINT:
                return ConnectorType.of("BIGINT");
            case Types.REAL:
            case Types.FLOAT:
                return ConnectorType.of("FLOAT");
            case Types.DOUBLE:
                return ConnectorType.of("DOUBLE");
            case Types.NUMERIC:
            case Types.DECIMAL: {
                int precision = fieldInfo.requiredColumnSize();
                int scale = fieldInfo.requiredDecimalDigits();
                return createDecimalOrString(precision, scale);
            }
            case Types.DATE:
                return ConnectorType.of("DATEV2");
            case Types.TIMESTAMP: {
                int scale = fieldInfo.getDecimalDigits().orElse(0);
                scale = Math.min(scale, JDBC_DATETIME_SCALE);
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case Types.CHAR:
                return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.TIME:
                return ConnectorType.of("STRING");
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }
}
