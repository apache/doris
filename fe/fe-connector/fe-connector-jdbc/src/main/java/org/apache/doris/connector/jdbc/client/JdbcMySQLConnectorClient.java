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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * MySQL-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcMySQLClient}.
 */
public class JdbcMySQLConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcMySQLConnectorClient.class);

    private final boolean convertDateToNull;
    private boolean isDoris = false;

    public JdbcMySQLConnectorClient(
            String catalogName, JdbcDbType dbType, String jdbcUrl,
            boolean onlySpecifiedDatabase,
            Map<String, Boolean> includeDatabaseMap,
            Map<String, Boolean> excludeDatabaseMap,
            boolean enableMappingVarbinary,
            boolean enableMappingTimestampTz) {
        super(catalogName, dbType, jdbcUrl, onlySpecifiedDatabase,
                includeDatabaseMap, excludeDatabaseMap,
                enableMappingVarbinary, enableMappingTimestampTz);
        this.convertDateToNull = isConvertDatetimeToNull(jdbcUrl);
    }

    @Override
    protected void postInitialize() {
        detectDoris();
    }

    private void detectDoris() {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW VARIABLES LIKE 'version_comment'");
            if (rs.next()) {
                String versionComment = rs.getString("Value");
                isDoris = versionComment.toLowerCase().contains("doris");
            }
        } catch (Exception e) {
            LOG.warn("Failed to detect if remote MySQL is Doris: {}", e.getMessage());
        } finally {
            closeResources(rs, stmt, conn);
        }
    }

    public boolean isDoris() {
        return isDoris;
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
            throw new DorisConnectorException("Failed to get database name list from MySQL", e);
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
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            rs = databaseMetaData.getTables(remoteDbName, null, remoteTableName, tableTypes);
            consumer.accept(rs);
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to process table", e);
        } finally {
            closeResources(rs, conn);
        }
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "SYSTEM VIEW"};
    }

    @Override
    protected ResultSet getRemoteColumns(DatabaseMetaData meta, String catalog,
            String remoteDbName, String remoteTableName) throws SQLException {
        return meta.getColumns(remoteDbName, null, remoteTableName, null);
    }

    @Override
    protected String getCatalogName(Connection conn) throws SQLException {
        return null;
    }

    @Override
    public String getTableComment(String remoteDbName, String remoteTableName) {
        String sql = "SELECT TABLE_COMMENT FROM INFORMATION_SCHEMA.TABLES"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            ps.setString(1, remoteDbName);
            ps.setString(2, remoteTableName);
            rs = ps.executeQuery();
            if (rs.next()) {
                String comment = rs.getString("TABLE_COMMENT");
                return comment != null ? comment : "";
            }
            return "";
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get table comment for " + remoteDbName + "." + remoteTableName, e);
        } finally {
            closeResources(rs, ps, conn);
        }
    }

    @Override
    public long getRowCount(String dbName, String tableName) {
        String sql = "SELECT MAX(row_count) AS cnt FROM ("
                + "SELECT TABLE_ROWS AS row_count FROM INFORMATION_SCHEMA.TABLES "
                + "WHERE TABLE_SCHEMA = '" + dbName + "' AND TABLE_NAME = '" + tableName + "' "
                + "UNION ALL "
                + "SELECT CARDINALITY AS row_count FROM INFORMATION_SCHEMA.STATISTICS "
                + "WHERE TABLE_SCHEMA = '" + dbName + "' AND TABLE_NAME = '" + tableName + "' "
                + "AND CARDINALITY IS NOT NULL) t";
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

    @Override
    public List<String> getPrimaryKeys(String remoteDbName, String remoteTableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<String> primaryKeys = new ArrayList<>();
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            rs = databaseMetaData.getPrimaryKeys(remoteDbName, null, remoteTableName);
            while (rs.next()) {
                primaryKeys.add(rs.getString("COLUMN_NAME"));
            }
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get primary keys for " + remoteDbName + "." + remoteTableName, e);
        } finally {
            closeResources(rs, conn);
        }
        return primaryKeys;
    }

    @Override
    public List<JdbcFieldInfo> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
        Connection conn = null;
        ResultSet rs = null;
        List<JdbcFieldInfo> schema = new ArrayList<>();
        try {
            conn = getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String cat = getCatalogName(conn);
            rs = getRemoteColumns(databaseMetaData, cat, remoteDbName, remoteTableName);

            Map<String, String> dataTypeOverrides = Collections.emptyMap();
            if (isDoris) {
                dataTypeOverrides = getColumnsDataTypeUseQuery(remoteDbName, remoteTableName);
            }

            while (rs.next()) {
                if (dataTypeOverrides.isEmpty()) {
                    schema.add(new JdbcFieldInfo(rs));
                } else {
                    schema.add(new JdbcFieldInfo(rs, dataTypeOverrides));
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

    private Map<String, String> getColumnsDataTypeUseQuery(String remoteDbName, String remoteTableName) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        Map<String, String> fieldToType = new HashMap<>();

        String query = "SHOW FULL COLUMNS FROM `" + remoteTableName + "` FROM `" + remoteDbName + "`";
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                fieldToType.put(rs.getString("Field"), rs.getString("Type"));
            }
        } catch (SQLException e) {
            throw new DorisConnectorException(
                    "Failed to get column data types for " + remoteDbName + "." + remoteTableName, e);
        } finally {
            closeResources(rs, stmt, conn);
        }
        return fieldToType;
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("information_schema");
        set.add("performance_schema");
        set.add("mysql");
        set.add("sys");
        return set;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String rawTypeName = fieldInfo.getDataTypeName().orElse("unknown");
        if (isDoris) {
            return dorisTypeToConnectorType(rawTypeName, fieldInfo);
        }
        String[] typeFields = rawTypeName.split(" ");
        String mysqlType = typeFields[0];

        if (typeFields.length > 1 && "UNSIGNED".equals(typeFields[1])) {
            return mapUnsignedType(mysqlType, fieldInfo);
        }
        return mapSignedType(mysqlType, fieldInfo);
    }

    private ConnectorType dorisTypeToConnectorType(String rawType, JdbcFieldInfo fieldInfo) {
        if (rawType == null || rawType.isEmpty()) {
            return ConnectorType.of("UNSUPPORTED");
        }
        String upperType = rawType.toUpperCase();

        if (upperType.startsWith("ARRAY")) {
            // ARRAY<INNER_TYPE> -> extract inner type and recursively convert
            String innerType = upperType.substring(6, upperType.length() - 1).trim();
            return ConnectorType.arrayOf(dorisTypeToConnectorType(innerType, null));
        }

        int openParen = upperType.indexOf("(");
        String baseType = (openParen == -1) ? upperType : upperType.substring(0, openParen);

        switch (baseType) {
            case "BOOL":
            case "BOOLEAN":
                return ConnectorType.of("BOOLEAN");
            case "TINYINT":
                return ConnectorType.of("TINYINT");
            case "SMALLINT":
                return ConnectorType.of("SMALLINT");
            case "INT":
                return ConnectorType.of("INT");
            case "BIGINT":
                return ConnectorType.of("BIGINT");
            case "LARGEINT":
                return ConnectorType.of("LARGEINT");
            case "FLOAT":
                return ConnectorType.of("FLOAT");
            case "DOUBLE":
                return ConnectorType.of("DOUBLE");
            case "DECIMAL":
            case "DECIMALV3": {
                if (openParen == -1) {
                    if (fieldInfo != null) {
                        int precision = fieldInfo.requiredColumnSize();
                        int scale = fieldInfo.requiredDecimalDigits();
                        return createDecimalOrString(precision, scale);
                    }
                    return ConnectorType.of("DECIMALV3", -1, -1);
                }
                String[] params = upperType.substring(openParen + 1, upperType.length() - 1).split(",");
                int precision = Integer.parseInt(params[0].trim());
                int scale = Integer.parseInt(params[1].trim());
                return ConnectorType.of("DECIMALV3", precision, scale);
            }
            case "DATE":
            case "DATEV2":
                return ConnectorType.of("DATEV2");
            case "DATETIME":
            case "DATETIMEV2": {
                int scale;
                if (openParen != -1) {
                    scale = Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                } else if (fieldInfo != null) {
                    scale = computeDatetimeScale(fieldInfo.requiredColumnSize());
                } else {
                    scale = 0;
                }
                if (scale > 6) {
                    scale = 6;
                }
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "CHAR":
            case "CHARACTER": {
                if (openParen == -1) {
                    if (fieldInfo != null) {
                        return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
                    }
                    return ConnectorType.of("CHAR");
                }
                int len = Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                return ConnectorType.of("CHAR", len, -1);
            }
            case "VARCHAR": {
                if (openParen == -1) {
                    if (fieldInfo != null) {
                        return ConnectorType.of("VARCHAR", fieldInfo.requiredColumnSize(), -1);
                    }
                    return ConnectorType.of("VARCHAR");
                }
                int len = Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                return ConnectorType.of("VARCHAR", len, -1);
            }
            case "STRING":
            case "TEXT":
            case "JSON":
                return ConnectorType.of("STRING");
            case "HLL":
                return ConnectorType.of("HLL");
            case "BITMAP":
                return ConnectorType.of("BITMAP");
            case "QUANTILE_STATE":
                return ConnectorType.of("QUANTILE_STATE");
            case "VARBINARY":
                return ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private ConnectorType mapUnsignedType(String mysqlType, JdbcFieldInfo fieldInfo) {
        switch (mysqlType) {
            case "TINYINT":
                return ConnectorType.of("SMALLINT");
            case "SMALLINT":
            case "MEDIUMINT":
                return ConnectorType.of("INT");
            case "INT":
                return ConnectorType.of("BIGINT");
            case "BIGINT":
                return ConnectorType.of("LARGEINT");
            case "DECIMAL": {
                int precision = fieldInfo.requiredColumnSize() + 1;
                int scale = fieldInfo.requiredDecimalDigits();
                return createDecimalOrString(precision, scale);
            }
            case "DOUBLE":
                return ConnectorType.of("DOUBLE");
            case "FLOAT":
                return ConnectorType.of("FLOAT");
            default:
                throw new DorisConnectorException("Unknown UNSIGNED type of MySQL: " + mysqlType);
        }
    }

    private ConnectorType mapSignedType(String mysqlType, JdbcFieldInfo fieldInfo) {
        switch (mysqlType) {
            case "BOOLEAN":
                return ConnectorType.of("BOOLEAN");
            case "TINYINT":
                return ConnectorType.of("TINYINT");
            case "SMALLINT":
            case "YEAR":
                return ConnectorType.of("SMALLINT");
            case "MEDIUMINT":
            case "INT":
                return ConnectorType.of("INT");
            case "BIGINT":
                return ConnectorType.of("BIGINT");
            case "DATE":
                if (convertDateToNull) {
                    fieldInfo.setAllowNull(true);
                }
                return ConnectorType.of("DATEV2");
            case "TIMESTAMP": {
                int scale = computeDatetimeScale(fieldInfo.requiredColumnSize());
                if (convertDateToNull) {
                    fieldInfo.setAllowNull(true);
                }
                return enableMappingTimestampTz
                        ? ConnectorType.of("TIMESTAMPTZ", scale, -1)
                        : ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "DATETIME": {
                int scale = computeDatetimeScale(fieldInfo.requiredColumnSize());
                if (convertDateToNull) {
                    fieldInfo.setAllowNull(true);
                }
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "FLOAT":
                return ConnectorType.of("FLOAT");
            case "DOUBLE":
                return ConnectorType.of("DOUBLE");
            case "DECIMAL": {
                int precision = fieldInfo.requiredColumnSize();
                int scale = fieldInfo.requiredDecimalDigits();
                return createDecimalOrString(precision, scale);
            }
            case "CHAR":
                return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
            case "VARCHAR":
                return ConnectorType.of("VARCHAR", fieldInfo.requiredColumnSize(), -1);
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "BINARY":
            case "VARBINARY":
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            case "BIT":
                return fieldInfo.requiredColumnSize() == 1
                        ? ConnectorType.of("BOOLEAN")
                        : ConnectorType.of("STRING");
            case "JSON":
            case "TIME":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "STRING":
            case "SET":
            case "ENUM":
                return ConnectorType.of("STRING");
            case "HLL":
                return ConnectorType.of("HLL");
            case "BITMAP":
                return ConnectorType.of("BITMAP");
            case "QUANTILE_STATE":
                return ConnectorType.of("QUANTILE_STATE");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private static int computeDatetimeScale(int columnSize) {
        int scale = columnSize > 19 ? columnSize - 20 : 0;
        return Math.min(scale, JDBC_DATETIME_SCALE);
    }

    private static boolean isConvertDatetimeToNull(String url) {
        String lower = url.toLowerCase();
        return lower.contains("zerodatetimebehavior=converttonull")
                || lower.contains("zerodatetimebehavior=convert_to_null");
    }
}
