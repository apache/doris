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
 * SAP HANA-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcSapHanaClient}.
 */
public class JdbcSapHanaConnectorClient extends JdbcConnectorClient {

    public JdbcSapHanaConnectorClient(
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
        return new String[] {"TABLE", "VIEW", "OLAP VIEW", "JOIN VIEW",
                "HIERARCHY VIEW", "CALC VIEW"};
    }

    @Override
    protected String getTestQuery() {
        return "SELECT 1 FROM DUMMY";
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String hanaType = fieldInfo.getDataTypeName().orElse("unknown").toUpperCase();
        switch (hanaType) {
            case "TINYINT":
                return ConnectorType.of("TINYINT");
            case "SMALLINT":
                return ConnectorType.of("SMALLINT");
            case "INTEGER":
                return ConnectorType.of("INT");
            case "BIGINT":
                return ConnectorType.of("BIGINT");
            case "SMALLDECIMAL":
            case "DECIMAL": {
                int precision = fieldInfo.requiredColumnSize();
                int scale = fieldInfo.requiredDecimalDigits();
                if (scale == 0 && precision == 0) {
                    return ConnectorType.of("DOUBLE");
                }
                return createDecimalOrString(precision, scale);
            }
            case "REAL":
                return ConnectorType.of("FLOAT");
            case "DOUBLE":
                return ConnectorType.of("DOUBLE");
            case "TIMESTAMP": {
                int scale = fieldInfo.getDecimalDigits().orElse(0);
                scale = Math.min(scale, JDBC_DATETIME_SCALE);
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "SECONDDATE":
                return ConnectorType.of("DATETIMEV2", 0, -1);
            case "DATE":
                return ConnectorType.of("DATEV2");
            case "BOOLEAN":
                return ConnectorType.of("BOOLEAN");
            case "CHAR":
            case "NCHAR":
                return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
            case "VARCHAR":
            case "NVARCHAR":
            case "ALPHANUM":
            case "SHORTTEXT":
            case "CLOB":
            case "NCLOB":
            case "TEXT":
            case "BINTEXT":
            case "TIME":
                return ConnectorType.of("STRING");
            case "BINARY":
            case "VARBINARY":
                return ConnectorType.of("STRING");
            case "BLOB":
            case "ST_GEOMETRY":
            case "ST_POINT":
                return ConnectorType.of("UNSUPPORTED");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }
}
