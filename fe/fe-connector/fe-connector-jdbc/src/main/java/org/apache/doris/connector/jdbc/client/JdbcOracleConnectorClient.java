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
import org.apache.doris.connector.jdbc.JdbcDbType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Oracle-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcOracleClient}.
 */
public class JdbcOracleConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcOracleConnectorClient.class);

    public JdbcOracleConnectorClient(
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
    protected String getTestQuery() {
        return "SELECT 1 FROM dual";
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("ctxsys");
        set.add("flows_files");
        set.add("mdsys");
        set.add("outln");
        set.add("sys");
        set.add("system");
        set.add("xdb");
        set.add("xs$null");
        return set;
    }

    @Override
    protected String getAdditionalTablesQuery(String remoteDbName, String remoteTableName, String[] tableTypes) {
        StringBuilder sb = new StringBuilder(
                "SELECT SYNONYM_NAME AS TABLE_NAME FROM ALL_SYNONYMS WHERE OWNER = '");
        sb.append(remoteDbName).append("'");
        if (remoteTableName != null) {
            sb.append(" AND SYNONYM_NAME = '").append(remoteTableName).append("'");
        }
        return sb.toString();
    }

    @Override
    public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
        // First try to get columns directly
        List<JdbcFieldInfo> columns = super.getJdbcColumnsInfo(remoteDbName, remoteTableName);
        if (!columns.isEmpty()) {
            return columns;
        }
        // If no columns found, the table might be a synonym — resolve and retry
        String[] resolved = resolveSynonym(remoteDbName, remoteTableName);
        if (resolved != null) {
            return getJdbcColumnsInfo(resolved[0], resolved[1]);
        }
        return columns;
    }

    /**
     * Resolves an Oracle synonym to its base table owner and name.
     * Returns [TABLE_OWNER, TABLE_NAME] or null if not a synonym.
     */
    private String[] resolveSynonym(String schema, String tableName) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql = "SELECT TABLE_OWNER, TABLE_NAME FROM ALL_SYNONYMS WHERE OWNER = '"
                    + schema + "' AND SYNONYM_NAME = '" + tableName + "'";
            rs = stmt.executeQuery(sql);
            if (rs.next()) {
                return new String[] {rs.getString("TABLE_OWNER"), rs.getString("TABLE_NAME")};
            }
        } catch (SQLException e) {
            LOG.debug("Failed to resolve synonym {}.{}: {}", schema, tableName, e.getMessage());
        } finally {
            closeResources(rs, stmt, conn);
        }
        return null;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String oracleType = fieldInfo.getDataTypeName().orElse("unknown").toUpperCase();

        // Handle INTERVAL variants, e.g. "INTERVAL YEAR(2) TO MONTH",
        // "INTERVAL DAY(2) TO SECOND(6)". Must check prefix because Oracle includes precision.
        if (oracleType.startsWith("INTERVAL")) {
            return ConnectorType.of("STRING");
        }

        // Handle TIMESTAMP variants with parenthesized precision, e.g. "TIMESTAMP(6)",
        // "TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE"
        if (oracleType.startsWith("TIMESTAMP")) {
            if (oracleType.contains("TIME ZONE") && !oracleType.contains("LOCAL TIME ZONE")) {
                // TIMESTAMP WITH TIME ZONE has no direct Doris equivalent; map to STRING
                // so the value is at least readable (the old path used Type.UNSUPPORTED and
                // relied on legacy planner fallback, which no longer exists for plugin-driven tables).
                return ConnectorType.of("STRING");
            }
            int scale = fieldInfo.getDecimalDigits().orElse(0);
            scale = Math.min(scale, JDBC_DATETIME_SCALE);
            if (enableMappingTimestampTz && oracleType.contains("LOCAL TIME ZONE")) {
                return ConnectorType.of("TIMESTAMPTZ", scale, -1);
            }
            return ConnectorType.of("DATETIMEV2", scale, -1);
        }

        switch (oracleType) {
            case "NUMBER": {
                int precision = fieldInfo.getColumnSize().orElse(0);
                int scale = fieldInfo.getDecimalDigits().orElse(0);
                if (precision == 0 && scale == 0) {
                    return ConnectorType.of("STRING");
                }
                if (scale <= 0) {
                    int totalDigits = precision - scale;
                    if (totalDigits < 3) {
                        return ConnectorType.of("TINYINT");
                    } else if (totalDigits < 5) {
                        return ConnectorType.of("SMALLINT");
                    } else if (totalDigits < 10) {
                        return ConnectorType.of("INT");
                    } else if (totalDigits < 19) {
                        return ConnectorType.of("BIGINT");
                    } else if (totalDigits <= MAX_DECIMAL128_PRECISION) {
                        return ConnectorType.of("LARGEINT");
                    } else {
                        return ConnectorType.of("STRING");
                    }
                }
                if (scale <= precision) {
                    return createDecimalOrString(precision, scale);
                }
                return createDecimalOrString(scale, scale);
            }
            case "FLOAT":
                return ConnectorType.of("DOUBLE");
            case "DATE":
                return ConnectorType.of("DATETIMEV2", 0, -1);
            case "CHAR":
            case "NCHAR":
            case "VARCHAR2":
            case "NVARCHAR2":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "CLOB":
                return ConnectorType.of("STRING");
            case "BLOB":
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    @Override
    public long getRowCount(String dbName, String tableName) {
        String sql = "SELECT NUM_ROWS FROM ALL_TABLES WHERE "
                + "OWNER = '" + dbName + "' AND TABLE_NAME = '" + tableName + "'";
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
}
