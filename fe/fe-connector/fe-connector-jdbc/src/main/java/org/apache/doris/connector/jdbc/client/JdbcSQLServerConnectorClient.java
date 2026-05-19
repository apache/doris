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
import java.sql.Statement;
import java.util.Map;

/**
 * SQL Server-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcSQLServerClient}.
 */
public class JdbcSQLServerConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcSQLServerConnectorClient.class);

    public JdbcSQLServerConnectorClient(
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
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String rawType = fieldInfo.getDataTypeName().orElse("unknown").toLowerCase();
        // SQL Server JDBC driver decorates type names for IDENTITY columns,
        // e.g., "int identity", "decimal() identity". Strip parenthesized parts
        // and suffixes to get the base type name.
        String ssType = rawType.replaceAll("[\\s(].*", "");
        switch (ssType) {
            case "bit":
                return ConnectorType.of("BOOLEAN");
            case "tinyint":
            case "smallint":
                return ConnectorType.of("SMALLINT");
            case "int":
                return ConnectorType.of("INT");
            case "bigint":
                return ConnectorType.of("BIGINT");
            case "real":
                return ConnectorType.of("FLOAT");
            case "float":
                return ConnectorType.of("DOUBLE");
            case "money":
                return ConnectorType.of("DECIMALV3", 19, 4);
            case "smallmoney":
                return ConnectorType.of("DECIMALV3", 10, 4);
            case "decimal":
            case "numeric": {
                int precision = fieldInfo.requiredColumnSize();
                int scale = fieldInfo.requiredDecimalDigits();
                return createDecimalOrString(precision, scale);
            }
            case "date":
                return ConnectorType.of("DATEV2");
            case "datetime":
            case "datetime2":
            case "smalldatetime": {
                int scale = fieldInfo.getDecimalDigits().orElse(0);
                scale = Math.min(scale, JDBC_DATETIME_SCALE);
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "char":
            case "nchar":
            case "varchar":
            case "nvarchar":
            case "text":
            case "ntext":
            case "time":
            case "datetimeoffset":
            case "uniqueidentifier":
            case "timestamp":
                return ConnectorType.of("STRING");
            case "binary":
            case "varbinary":
            case "image":
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    @Override
    public long getRowCount(String dbName, String tableName) {
        String sql = "SELECT sum(rows) FROM sys.partitions "
                + "WHERE object_id = (SELECT object_id('" + dbName + "." + tableName + "')) "
                + "AND index_id IN (0, 1)";
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
