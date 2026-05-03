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

import java.util.Map;

/**
 * Trino/Presto-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcTrinoClient}.
 */
public class JdbcTrinoConnectorClient extends JdbcConnectorClient {

    public JdbcTrinoConnectorClient(
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
        String trinoType = fieldInfo.getDataTypeName().orElse("unknown").toLowerCase();

        if (trinoType.startsWith("decimal(")) {
            return parseDecimal(trinoType);
        }
        if (trinoType.startsWith("char(")) {
            return parseChar(trinoType);
        }
        if (trinoType.startsWith("timestamp(")) {
            return parseTimestamp(trinoType);
        }
        if (trinoType.startsWith("array(")) {
            return parseArray(trinoType, fieldInfo);
        }

        switch (trinoType) {
            case "boolean":
                return ConnectorType.of("BOOLEAN");
            case "tinyint":
                return ConnectorType.of("TINYINT");
            case "smallint":
                return ConnectorType.of("SMALLINT");
            case "integer":
                return ConnectorType.of("INT");
            case "bigint":
                return ConnectorType.of("BIGINT");
            case "real":
                return ConnectorType.of("FLOAT");
            case "double":
                return ConnectorType.of("DOUBLE");
            case "date":
                return ConnectorType.of("DATEV2");
            case "json":
                return ConnectorType.of("STRING");
            default:
                if (trinoType.startsWith("varchar") || trinoType.startsWith("time")) {
                    return ConnectorType.of("STRING");
                }
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private ConnectorType parseDecimal(String type) {
        // "decimal(precision,scale)"
        String inner = type.substring(8, type.length() - 1);
        String[] parts = inner.split(",");
        int p = Integer.parseInt(parts[0].trim());
        int s = Integer.parseInt(parts[1].trim());
        return createDecimalOrString(p, s);
    }

    private ConnectorType parseChar(String type) {
        String inner = type.substring(5, type.length() - 1);
        int size = Integer.parseInt(inner.trim());
        return ConnectorType.of("CHAR", size, -1);
    }

    private ConnectorType parseTimestamp(String type) {
        String inner = type.substring(10, type.length() - 1);
        int scale = Integer.parseInt(inner.trim());
        scale = Math.min(scale, JDBC_DATETIME_SCALE);
        return ConnectorType.of("DATETIMEV2", scale, -1);
    }

    private ConnectorType parseArray(String type, JdbcFieldInfo fieldInfo) {
        String inner = type.substring(6, type.length() - 1);
        JdbcFieldInfo copy = new JdbcFieldInfo(fieldInfo);
        copy.setDataTypeName(java.util.Optional.of(inner));
        ConnectorType elementType = jdbcTypeToConnectorType(copy);
        return ConnectorType.arrayOf(elementType);
    }
}
