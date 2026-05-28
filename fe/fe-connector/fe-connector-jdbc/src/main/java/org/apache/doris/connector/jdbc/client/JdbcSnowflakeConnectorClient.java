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

import java.sql.Types;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Snowflake-specific JDBC connector client.
 */
public class JdbcSnowflakeConnectorClient extends JdbcConnectorClient {

    public JdbcSnowflakeConnectorClient(
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
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("information_schema");
        return set;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String typeName = fieldInfo.getDataTypeName().orElse("").toUpperCase(Locale.ROOT);
        switch (typeName) {
            case "BOOLEAN":
                return ConnectorType.of("BOOLEAN");
            case "BYTEINT":
            case "TINYINT":
                return ConnectorType.of("TINYINT");
            case "SMALLINT":
                return ConnectorType.of("SMALLINT");
            case "INTEGER":
            case "INT":
                return ConnectorType.of("INT");
            case "BIGINT":
                return ConnectorType.of("BIGINT");
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
            case "FIXED":
                return createDecimalOrString(fieldInfo.requiredColumnSize(), fieldInfo.requiredDecimalDigits());
            case "REAL":
            case "FLOAT":
            case "FLOAT4":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
                return ConnectorType.of("DOUBLE");
            case "DATE":
                return ConnectorType.of("DATEV2");
            case "TIME":
                return ConnectorType.of("STRING");
            case "TIMESTAMP":
            case "DATETIME":
            case "TIMESTAMP_NTZ":
                return ConnectorType.of("DATETIMEV2", timestampScale(fieldInfo), -1);
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
                if (enableMappingTimestampTz) {
                    return ConnectorType.of("TIMESTAMPTZ", timestampScale(fieldInfo), -1);
                }
                return ConnectorType.of("DATETIMEV2", timestampScale(fieldInfo), -1);
            case "CHAR":
            case "CHARACTER":
            case "VARCHAR":
            case "TEXT":
            case "STRING":
                return ConnectorType.of("STRING");
            case "BINARY":
            case "VARBINARY":
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            case "VARIANT":
            case "OBJECT":
            case "ARRAY":
            case "GEOGRAPHY":
            case "GEOMETRY":
            case "VECTOR":
                return ConnectorType.of("STRING");
            default:
                return fallbackByJdbcType(fieldInfo);
        }
    }

    private ConnectorType fallbackByJdbcType(JdbcFieldInfo fieldInfo) {
        switch (fieldInfo.getDataType()) {
            case Types.BOOLEAN:
            case Types.BIT:
                return ConnectorType.of("BOOLEAN");
            case Types.TINYINT:
                return ConnectorType.of("TINYINT");
            case Types.SMALLINT:
                return ConnectorType.of("SMALLINT");
            case Types.INTEGER:
                return ConnectorType.of("INT");
            case Types.BIGINT:
                return ConnectorType.of("BIGINT");
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return ConnectorType.of("DOUBLE");
            case Types.NUMERIC:
            case Types.DECIMAL:
                return createDecimalOrString(fieldInfo.requiredColumnSize(), fieldInfo.requiredDecimalDigits());
            case Types.DATE:
                return ConnectorType.of("DATEV2");
            case Types.TIMESTAMP:
                return ConnectorType.of("DATETIMEV2", timestampScale(fieldInfo), -1);
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return enableMappingTimestampTz
                        ? ConnectorType.of("TIMESTAMPTZ", timestampScale(fieldInfo), -1)
                        : ConnectorType.of("DATETIMEV2", timestampScale(fieldInfo), -1);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.TIME:
            case Types.OTHER:
                return ConnectorType.of("STRING");
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private int timestampScale(JdbcFieldInfo fieldInfo) {
        int scale = fieldInfo.getDecimalDigits().orElse(0);
        if (scale < 0) {
            return 0;
        }
        return Math.min(scale, JDBC_DATETIME_SCALE);
    }
}
