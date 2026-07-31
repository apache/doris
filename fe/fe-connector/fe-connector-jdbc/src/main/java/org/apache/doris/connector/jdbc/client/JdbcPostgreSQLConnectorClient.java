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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * PostgreSQL-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcPostgreSQLClient}.
 */
public class JdbcPostgreSQLConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcPostgreSQLConnectorClient.class);

    public JdbcPostgreSQLConnectorClient(
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
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "PARTITIONED TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("information_schema");
        set.add("pg_catalog");
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
                int sqlType = rs.getInt("DATA_TYPE");
                if (sqlType == Types.ARRAY) {
                    int arrayDim = getArrayDimensions(conn, remoteDbName, rs.getString("COLUMN_NAME"),
                            remoteTableName);
                    schema.add(new JdbcFieldInfo(rs, arrayDim));
                } else {
                    schema.add(new JdbcFieldInfo(rs));
                }
            }
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get JDBC columns info for " + remoteDbName + "." + remoteTableName, e);
        } finally {
            closeResources(rs, conn);
        }
        return schema;
    }

    private int getArrayDimensions(Connection conn, String schemaName, String columnName, String tableName)
            throws SQLException {
        String query = "SELECT a.attndims FROM pg_attribute a "
                + "JOIN pg_class c ON a.attrelid = c.oid "
                + "JOIN pg_namespace n ON c.relnamespace = n.oid "
                + "WHERE n.nspname = ? AND c.relname = ? AND a.attname = ?";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(query);
            pstmt.setString(1, schemaName);
            pstmt.setString(2, tableName);
            pstmt.setString(3, columnName);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("attndims");
            }
        } finally {
            closeResources(rs, pstmt);
        }
        return 1;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String pgType = fieldInfo.getDataTypeName().orElse("unknown");

        if (pgType.startsWith("_")) {
            return mapArrayType(pgType, fieldInfo);
        }

        switch (pgType) {
            case "bool":
                return ConnectorType.of("BOOLEAN");
            case "int2":
            case "smallserial":
                return ConnectorType.of("SMALLINT");
            case "int4":
            case "serial":
                return ConnectorType.of("INT");
            case "int8":
            case "bigserial":
                return ConnectorType.of("BIGINT");
            case "float4":
                return ConnectorType.of("FLOAT");
            case "float8":
                return ConnectorType.of("DOUBLE");
            case "numeric": {
                int precision = fieldInfo.getColumnSize().orElse(0);
                int scale = fieldInfo.getDecimalDigits().orElse(0);
                return createDecimalOrString(precision, scale);
            }
            case "date":
                return ConnectorType.of("DATEV2");
            case "timestamp": {
                int scale = computeTimestampScale(fieldInfo);
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "timestamptz": {
                int scale = computeTimestampScale(fieldInfo);
                return enableMappingTimestampTz
                        ? ConnectorType.of("TIMESTAMPTZ", scale, -1)
                        : ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "bpchar":
                return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
            case "varchar":
            case "text":
            case "json":
            case "jsonb":
            case "uuid":
            case "time":
            case "timetz":
            case "money":
            case "inet":
            case "cidr":
            case "macaddr":
            case "macaddr8":
            case "varbit":
            case "xml":
            case "hstore":
            case "interval":
                return ConnectorType.of("STRING");
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
                return ConnectorType.of("STRING");
            case "bytea":
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            case "bit":
                if (fieldInfo.getColumnSize().orElse(0) == 1) {
                    return ConnectorType.of("BOOLEAN");
                }
                return ConnectorType.of("STRING");
            case "tsvector":
            case "tsquery":
                return ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private ConnectorType mapArrayType(String pgType, JdbcFieldInfo fieldInfo) {
        String innerTypeName = pgType.substring(1);
        ConnectorType innerType = mapPgInnerType(innerTypeName, fieldInfo);
        int dims = fieldInfo.getArrayDimensions().orElse(1);
        ConnectorType result = innerType;
        for (int i = 0; i < dims; i++) {
            result = ConnectorType.arrayOf(result);
        }
        return result;
    }

    private ConnectorType mapPgInnerType(String innerType, JdbcFieldInfo fieldInfo) {
        switch (innerType) {
            case "int2":
                return ConnectorType.of("SMALLINT");
            case "int4":
                return ConnectorType.of("INT");
            case "int8":
                return ConnectorType.of("BIGINT");
            case "float4":
                return ConnectorType.of("FLOAT");
            case "float8":
                return ConnectorType.of("DOUBLE");
            case "numeric":
                return createDecimalOrString(fieldInfo.requiredColumnSize(), fieldInfo.requiredDecimalDigits());
            case "date":
                return ConnectorType.of("DATEV2");
            case "timestamp": {
                int scale = computeTimestampScale(fieldInfo);
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "timestamptz": {
                int scale = computeTimestampScale(fieldInfo);
                return enableMappingTimestampTz
                        ? ConnectorType.of("TIMESTAMPTZ", scale, -1)
                        : ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "bool":
                return ConnectorType.of("BOOLEAN");
            case "bpchar":
                return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
            case "varchar":
            case "text":
            case "json":
            case "jsonb":
            case "uuid":
                return ConnectorType.of("STRING");
            default:
                return ConnectorType.of("STRING");
        }
    }

    @Override
    public long getRowCount(String dbName, String tableName) {
        String sql = "SELECT reltuples FROM pg_class "
                + "WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '" + dbName + "') "
                + "AND relname = '" + tableName + "'";
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long count = rs.getLong(1);
                if (!rs.wasNull() && count >= 0) {
                    return count;
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to get row count for {}.{}: {}", dbName, tableName, e.getMessage());
        }
        return -1;
    }

    private int computeTimestampScale(JdbcFieldInfo fieldInfo) {
        int scale = fieldInfo.getDecimalDigits().orElse(0);
        return Math.min(scale, JDBC_DATETIME_SCALE);
    }
}
