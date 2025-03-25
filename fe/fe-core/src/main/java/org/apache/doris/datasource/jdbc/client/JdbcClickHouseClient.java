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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class JdbcClickHouseClient extends JdbcClient {

    private final Boolean databaseTermIsCatalog;

    protected JdbcClickHouseClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
        try (Connection conn = getConnection()) {
            String jdbcUrl = conn.getMetaData().getURL();
            if (!isNewClickHouseDriver(getJdbcDriverVersion())) {
                this.databaseTermIsCatalog = false;
            } else {
                this.databaseTermIsCatalog = "catalog".equalsIgnoreCase(getDatabaseTermFromUrl(jdbcUrl));
            }
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to initialize JdbcClickHouseClient: %s", e.getMessage());
        }
    }

    @Override
    public List<String> getDatabaseNameList() {
        Connection conn = null;
        ResultSet rs = null;
        List<String> remoteDatabaseNames = Lists.newArrayList();
        try {
            conn = getConnection();
            if (isOnlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                if (databaseTermIsCatalog) {
                    remoteDatabaseNames.add(conn.getCatalog());
                } else {
                    remoteDatabaseNames.add(conn.getSchema());
                }
            } else {
                if (databaseTermIsCatalog) {
                    rs = conn.getMetaData().getCatalogs();
                } else {
                    rs = conn.getMetaData().getSchemas(conn.getCatalog(), null);
                }
                while (rs.next()) {
                    remoteDatabaseNames.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, conn);
        }
        return filterDatabaseNames(remoteDatabaseNames);
    }

    @Override
    protected void processTable(String remoteDbName, String remoteTableName, String[] tableTypes,
            Consumer<ResultSet> resultSetConsumer) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            if (databaseTermIsCatalog) {
                rs = databaseMetaData.getTables(remoteDbName, null, remoteTableName, tableTypes);
            } else {
                rs = databaseMetaData.getTables(null, remoteDbName, remoteTableName, tableTypes);
            }
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    @Override
    protected ResultSet getRemoteColumns(DatabaseMetaData databaseMetaData, String catalogName, String remoteDbName,
            String remoteTableName) throws SQLException {
        if (databaseTermIsCatalog) {
            return databaseMetaData.getColumns(remoteDbName, null, remoteTableName, null);
        } else {
            return databaseMetaData.getColumns(catalogName, remoteDbName, remoteTableName, null);
        }
    }

    @Override
    protected String getCatalogName(Connection conn) throws SQLException {
        if (databaseTermIsCatalog) {
            return null;
        } else {
            return conn.getCatalog();
        }
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "SYSTEM TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {

        String ckType = fieldSchema.getDataTypeName().orElse("unknown");

        if (ckType.startsWith("LowCardinality")) {
            fieldSchema.setAllowNull(true);
            ckType = ckType.substring(15, ckType.length() - 1);
            if (ckType.startsWith("Nullable")) {
                ckType = ckType.substring(9, ckType.length() - 1);
            }
        } else if (ckType.startsWith("Nullable")) {
            fieldSchema.setAllowNull(true);
            ckType = ckType.substring(9, ckType.length() - 1);
        }

        if (ckType.startsWith("Decimal")) {
            String[] accuracy = ckType.substring(8, ckType.length() - 1).split(", ");
            int precision = Integer.parseInt(accuracy[0]);
            int scale = Integer.parseInt(accuracy[1]);
            return createDecimalOrStringType(precision, scale);
        }

        if ("String".contains(ckType)
                || ckType.startsWith("Enum")
                || ckType.startsWith("IPv")
                || "UUID".contains(ckType)
                || ckType.startsWith("FixedString")) {
            return ScalarType.createStringType();
        }

        if (ckType.startsWith("DateTime")) {
            // DateTime with second precision
            if (ckType.startsWith("DateTime(") || ckType.equals("DateTime")) {
                return ScalarType.createDatetimeV2Type(0);
            } else {
                // DateTime64 with millisecond precision
                // Datetime64(6) / DateTime64(6, 'Asia/Shanghai')
                String[] accuracy = ckType.substring(11, ckType.length() - 1).split(", ");
                int precision = Integer.parseInt(accuracy[0]);
                if (precision > 6) {
                    precision = JDBC_DATETIME_SCALE;
                }
                return ScalarType.createDatetimeV2Type(precision);
            }
        }

        if (ckType.startsWith("Array")) {
            String cktype = ckType.substring(6, ckType.length() - 1);
            fieldSchema.setDataTypeName(Optional.of(cktype));
            Type type = jdbcTypeToDoris(fieldSchema);
            return ArrayType.create(type, true);
        }

        switch (ckType) {
            case "Bool":
                return Type.BOOLEAN;
            case "Int8":
                return Type.TINYINT;
            case "Int16":
            case "UInt8":
                return Type.SMALLINT;
            case "Int32":
            case "UInt16":
                return Type.INT;
            case "Int64":
            case "UInt32":
                return Type.BIGINT;
            case "Int128":
            case "UInt64":
                return Type.LARGEINT;
            case "Int256":
            case "UInt128":
            case "UInt256":
                return ScalarType.createStringType();
            case "Float32":
                return Type.FLOAT;
            case "Float64":
                return Type.DOUBLE;
            case "Date":
            case "Date32":
                return ScalarType.createDateV2Type();
            default:
                return Type.UNSUPPORTED;
        }
    }

    /**
     * Determine whether the driver version is greater than or equal to 0.5.0.
     */
    private static boolean isNewClickHouseDriver(String driverVersion) {
        if (driverVersion == null) {
            throw new JdbcClientException("Driver version cannot be null");
        }
        try {
            String[] versionParts = driverVersion.split("\\.");
            int majorVersion = Integer.parseInt(versionParts[0]);
            int minorVersion = Integer.parseInt(versionParts[1]);
            // Determine whether it is greater than or equal to 0.5.x
            return (majorVersion > 0) || (majorVersion == 0 && minorVersion >= 5);
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            throw new JdbcClientException("Invalid clickhouse driver version format: " + driverVersion, e);
        }
    }

    /**
     * Extract databaseterm parameters from the jdbc url.
     */
    private String getDatabaseTermFromUrl(String jdbcUrl) {
        if (jdbcUrl != null && jdbcUrl.toLowerCase().contains("databaseterm=schema")) {
            return "schema";
        }
        return "catalog";
    }

    /**
     * Get the driver version.
     */
    public String getJdbcDriverVersion() {
        try (Connection conn = getConnection()) {
            return conn.getMetaData().getDriverVersion();
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to get jdbc driver version", e);
        }
    }
}
