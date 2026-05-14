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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * ClickHouse-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcClickHouseClient}.
 */
public class JdbcClickHouseConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcClickHouseConnectorClient.class);

    private Boolean isNewDriver;
    private Boolean databaseTermIsCatalog;

    public JdbcClickHouseConnectorClient(
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
            isNewClickHouseDriver(conn);
            if (onlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                boolean useSchema = !databaseTermIsCatalog;
                String current = useSchema ? conn.getSchema() : conn.getCatalog();
                names.add(current);
            } else {
                if (databaseTermIsCatalog) {
                    rs = conn.getMetaData().getCatalogs();
                } else {
                    rs = conn.getMetaData().getSchemas(null, null);
                }
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to get database name list from ClickHouse", e);
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
            isNewClickHouseDriver(conn);
            boolean useSchema = !databaseTermIsCatalog;
            DatabaseMetaData meta = conn.getMetaData();
            if (useSchema) {
                rs = meta.getTables(null, remoteDbName, remoteTableName, tableTypes);
            } else {
                rs = meta.getTables(remoteDbName, null, remoteTableName, tableTypes);
            }
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
        boolean useSchema = databaseTermIsCatalog != null && !databaseTermIsCatalog;
        if (useSchema) {
            return meta.getColumns(catalog, remoteDbName, remoteTableName, null);
        }
        return meta.getColumns(remoteDbName, null, remoteTableName, null);
    }

    @Override
    protected String getCatalogName(Connection conn) throws SQLException {
        isNewClickHouseDriver(conn);
        boolean useSchema = !databaseTermIsCatalog;
        if (useSchema) {
            return conn.getCatalog();
        }
        return null;
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "SYSTEM TABLE"};
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("information_schema");
        return set;
    }

    private boolean isNewClickHouseDriver(Connection conn) throws SQLException {
        if (isNewDriver == null) {
            String driverVersion = conn.getMetaData().getDriverVersion();
            isNewDriver = driverVersion != null && !driverVersion.startsWith("0.3")
                    && !driverVersion.startsWith("0.4");
            if (!isNewDriver) {
                // Old driver uses schema mode (not catalog), matching old JdbcClickHouseClient
                databaseTermIsCatalog = false;
            } else {
                // New driver checks the JDBC URL for databaseterm parameter
                databaseTermIsCatalog = "catalog".equalsIgnoreCase(getDatabaseTermFromUrl());
            }
        }
        return isNewDriver;
    }

    private String getDatabaseTermFromUrl() {
        if (jdbcUrl != null && jdbcUrl.toLowerCase().contains("databaseterm=schema")) {
            return "schema";
        }
        return "catalog";
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String chType = fieldInfo.getDataTypeName().orElse("unknown");
        return mapClickHouseType(chType, fieldInfo);
    }

    private ConnectorType mapClickHouseType(String chType, JdbcFieldInfo fieldInfo) {
        // Unwrap Nullable / LowCardinality wrappers
        if (chType.startsWith("Nullable(") && chType.endsWith(")")) {
            fieldInfo.setAllowNull(true);
            return mapClickHouseType(chType.substring(9, chType.length() - 1), fieldInfo);
        }
        if (chType.startsWith("LowCardinality(") && chType.endsWith(")")) {
            return mapClickHouseType(chType.substring(15, chType.length() - 1), fieldInfo);
        }

        // Decimal
        if (chType.startsWith("Decimal(") || chType.startsWith("Decimal32(")
                || chType.startsWith("Decimal64(") || chType.startsWith("Decimal128(")
                || chType.startsWith("Decimal256(")) {
            return parseDecimalType(chType, fieldInfo);
        }

        // DateTime64
        if (chType.startsWith("DateTime64(")) {
            return parseDateTimeType(chType);
        }

        // DateTime('timezone') — DateTime with timezone parameter, second precision
        if (chType.startsWith("DateTime(")) {
            return ConnectorType.of("DATETIMEV2", 0, -1);
        }

        // Array
        if (chType.startsWith("Array(") && chType.endsWith(")")) {
            String inner = chType.substring(6, chType.length() - 1);
            return ConnectorType.arrayOf(mapClickHouseType(inner, fieldInfo));
        }

        switch (chType) {
            case "Bool":
                return ConnectorType.of("BOOLEAN");
            case "Int8":
                return ConnectorType.of("TINYINT");
            case "Int16":
            case "UInt8":
                return ConnectorType.of("SMALLINT");
            case "Int32":
            case "UInt16":
                return ConnectorType.of("INT");
            case "Int64":
            case "UInt32":
                return ConnectorType.of("BIGINT");
            case "Int128":
            case "UInt64":
                return ConnectorType.of("LARGEINT");
            case "UInt128":
            case "UInt256":
            case "Int256":
                return ConnectorType.of("STRING");
            case "Float32":
                return ConnectorType.of("FLOAT");
            case "Float64":
                return ConnectorType.of("DOUBLE");
            case "Date":
            case "Date32":
                return ConnectorType.of("DATEV2");
            case "DateTime":
                return ConnectorType.of("DATETIMEV2", 0, -1);
            case "String":
            case "UUID":
            case "IPv4":
            case "IPv6":
                return ConnectorType.of("STRING");
            default:
                if (chType.startsWith("FixedString(") || chType.startsWith("Enum8(")
                        || chType.startsWith("Enum16(")) {
                    return ConnectorType.of("STRING");
                }
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private ConnectorType parseDecimalType(String chType, JdbcFieldInfo fieldInfo) {
        // Parse precision/scale from type string directly, because JDBC metadata
        // may not carry decimal digits for inner types of Array columns.
        // ClickHouse driver reports all Decimal variants as "Decimal(P, S)" format.
        int openParen = chType.indexOf('(');
        int closeParen = chType.lastIndexOf(')');
        String inner = chType.substring(openParen + 1, closeParen);
        String[] parts = inner.split(",");
        int p;
        int s;
        if (parts.length == 2) {
            // Decimal(P, S)
            p = Integer.parseInt(parts[0].trim());
            s = Integer.parseInt(parts[1].trim());
        } else {
            // Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)
            s = Integer.parseInt(parts[0].trim());
            if (chType.startsWith("Decimal32")) {
                p = 9;
            } else if (chType.startsWith("Decimal64")) {
                p = 18;
            } else if (chType.startsWith("Decimal128")) {
                p = 38;
            } else {
                p = 76;
            }
        }
        return createDecimalOrString(p, s);
    }

    private ConnectorType parseDateTimeType(String chType) {
        // DateTime64(precision) or DateTime64(precision, 'timezone')
        String inner = chType.substring(11, chType.indexOf(')'));
        String[] parts = inner.split(",");
        int scale = Integer.parseInt(parts[0].trim());
        scale = Math.min(scale, JDBC_DATETIME_SCALE);
        return ConnectorType.of("DATETIMEV2", scale, -1);
    }
}
