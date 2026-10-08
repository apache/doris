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

import org.apache.doris.connector.jdbc.JdbcDbType;
import org.apache.doris.connector.spi.ConnectorType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL Server-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcSQLServerClient}.
 */
public class JdbcSQLServerConnectorClient extends JdbcConnectorClient {

    private static final Logger LOG = LogManager.getLogger(JdbcSQLServerConnectorClient.class);

    // TYPE_NAME of an IDENTITY column decorates the base type: "int identity", "decimal() identity",
    // "numeric(18, 0) identity", "decimal(18,0) IDENTITY(1,1)". IDENTITY is only allowed on these base types.
    private static final Pattern IDENTITY_TYPE_NAME = Pattern.compile(
            "^(tinyint|smallint|int|bigint|decimal|numeric)\\s*(\\([^)]*\\))?\\s+identity(\\s*\\([^)]*\\))?$",
            Pattern.CASE_INSENSITIVE);

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

    /**
     * The base type of an IDENTITY column's TYPE_NAME, or the name unchanged.
     * <p>
     * The decoration is trusted only when {@code DATA_TYPE} is the code of the named base type: an alias type
     * may legally be named like that ({@code CREATE TYPE dbo.[int identity] FROM varchar(10)}, or
     * {@code dbo.[int alias]}), and it then has to be resolved by its code, not by the words of its name.
     */
    static String identityBaseType(String typeName, int dataType) {
        Matcher matcher = IDENTITY_TYPE_NAME.matcher(typeName);
        if (!matcher.matches()) {
            return typeName;
        }
        String baseType = matcher.group(1).toLowerCase(Locale.ROOT);
        boolean codeMatches;
        switch (baseType) {
            case "tinyint":
                codeMatches = dataType == Types.TINYINT;
                break;
            case "smallint":
                codeMatches = dataType == Types.SMALLINT;
                break;
            case "int":
                codeMatches = dataType == Types.INTEGER;
                break;
            case "bigint":
                codeMatches = dataType == Types.BIGINT;
                break;
            default:
                codeMatches = dataType == Types.DECIMAL || dataType == Types.NUMERIC;
                break;
        }
        return codeMatches ? baseType : typeName;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String rawType = fieldInfo.getDataTypeName().orElse("unknown").toLowerCase();
        // An IDENTITY column is reported as "int identity" or "decimal(18,0) identity": only the base type
        // is matched below. Any other name is matched as it is: system type names are single words, and a
        // user-defined alias type may be named with spaces or parentheses ("int alias") and must not be
        // mistaken for the system type its name starts with.
        String ssType = identityBaseType(rawType, fieldInfo.getDataType());
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
            case "xml":
            case "sql_variant":
            case "geometry":
            case "geography":
            case "hierarchyid":
            case "json":
            case "vector":
                // SQL Server system types that Doris does not support. They are listed explicitly
                // so that they never reach the JDBC type code fallback below.
                return ConnectorType.of("UNSUPPORTED");
            default:
                return jdbcTypeCodeToConnectorType(fieldInfo);
        }
    }

    /**
     * Fallback for type names that are not SQL Server system types.
     * <p>
     * User-defined alias types ({@code CREATE TYPE dbo.my_type FROM varchar(50)}) are reported by
     * {@code DatabaseMetaData.getColumns()} with {@code TYPE_NAME} set to the alias name, so they can not be
     * matched by name. {@code DATA_TYPE}, {@code COLUMN_SIZE} and {@code DECIMAL_DIGITS} still describe the
     * base type, so the standard {@link Types} code is used to resolve the Doris type. The mapping mirrors
     * the name based one above.
     * <p>
     * Binary codes are deliberately not mapped: mssql-jdbc also reports CLR user-defined types
     * (geometry, geography, hierarchyid, ...) as {@link Types#VARBINARY}, so they can not be told apart from
     * an alias over a binary type by the type code alone. Vendor specific codes stay unsupported as well.
     */
    private ConnectorType jdbcTypeCodeToConnectorType(JdbcFieldInfo fieldInfo) {
        switch (fieldInfo.getDataType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return ConnectorType.of("BOOLEAN");
            // SQL Server tinyint is unsigned (0 to 255), so it needs SMALLINT
            case Types.TINYINT:
            case Types.SMALLINT:
                return ConnectorType.of("SMALLINT");
            case Types.INTEGER:
                return ConnectorType.of("INT");
            case Types.BIGINT:
                return ConnectorType.of("BIGINT");
            case Types.REAL:
                return ConnectorType.of("FLOAT");
            case Types.FLOAT:
            case Types.DOUBLE:
                return ConnectorType.of("DOUBLE");
            case Types.DECIMAL:
            case Types.NUMERIC: {
                // money and smallmoney are reported as DECIMAL(19,4) and DECIMAL(10,4)
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
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.TIME:
                return ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    @Override
    protected String getSchemaPatternForDatabaseNameList() {
        // "%" is a JDBC schemaPattern wildcard that matches all schemas. mssql-jdbc 13.4 filters
        // built-in schemas when catalog is non-empty and schemaPattern is null.
        return "%";
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
