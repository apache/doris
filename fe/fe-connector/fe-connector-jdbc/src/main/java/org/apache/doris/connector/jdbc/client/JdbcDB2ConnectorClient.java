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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * IBM DB2-specific JDBC connector client.
 * Adapted from fe-core's {@code JdbcDB2Client}.
 */
public class JdbcDB2ConnectorClient extends JdbcConnectorClient {

    public JdbcDB2ConnectorClient(
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
        return "select 1 from sysibm.sysdummy1";
    }

    @Override
    public List<String> getDatabaseNameList() {
        Connection conn = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<>();
        try {
            conn = getConnection();
            if (onlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String current = conn.getSchema();
                names.add(current);
            } else {
                rs = conn.getMetaData().getSchemas();
                while (rs.next()) {
                    names.add(rs.getString("TABLE_SCHEM"));
                }
            }
        } catch (SQLException e) {
            throw new DorisConnectorException("Failed to get database name list from DB2", e);
        } finally {
            closeResources(rs, conn);
        }
        return filterDatabaseNames(names);
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        Set<String> set = new HashSet<>();
        set.add("nullid");
        set.add("sqlj");
        set.add("syscat");
        set.add("sysfun");
        set.add("sysibm");
        set.add("sysibmadm");
        set.add("sysibminternal");
        set.add("sysibmts");
        set.add("sysproc");
        set.add("syspublic");
        set.add("sysstat");
        set.add("systools");
        return set;
    }

    @Override
    public ConnectorType jdbcTypeToConnectorType(JdbcFieldInfo fieldInfo) {
        String db2Type = fieldInfo.getDataTypeName().orElse("unknown").toUpperCase();
        switch (db2Type) {
            case "BOOLEAN":
                return ConnectorType.of("BOOLEAN");
            case "SMALLINT":
                return ConnectorType.of("SMALLINT");
            case "INTEGER":
                return ConnectorType.of("INT");
            case "BIGINT":
                return ConnectorType.of("BIGINT");
            case "REAL":
                return ConnectorType.of("FLOAT");
            case "DOUBLE":
            case "DECFLOAT":
                return ConnectorType.of("DOUBLE");
            case "DECIMAL": {
                int precision = fieldInfo.requiredColumnSize();
                int scale = fieldInfo.requiredDecimalDigits();
                return createDecimalOrString(precision, scale);
            }
            case "DATE":
                return ConnectorType.of("DATEV2");
            case "TIMESTAMP": {
                int scale = fieldInfo.getDecimalDigits().orElse(0);
                scale = Math.min(scale, JDBC_DATETIME_SCALE);
                return ConnectorType.of("DATETIMEV2", scale, -1);
            }
            case "CHAR":
                return ConnectorType.of("CHAR", fieldInfo.requiredColumnSize(), -1);
            case "VARCHAR":
            case "LONG VARCHAR":
            case "CLOB":
            case "XML":
            case "TIME":
            case "GRAPHIC":
            case "VARGRAPHIC":
            case "LONG VARGRAPHIC":
            case "DBCLOB":
                return ConnectorType.of("STRING");
            case "BLOB":
            case "BINARY":
            case "VARBINARY":
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fieldInfo.requiredColumnSize(), -1)
                        : ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }
}
