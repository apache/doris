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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class JdbcDB2Client extends JdbcClient {

    protected JdbcDB2Client(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    public String getTestQuery() {
        return "select 1 from sysibm.sysdummy1";
    }

    @Override
    public List<String> getDatabaseNameList() {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<String> remoteDatabaseNames = Lists.newArrayList();
        try {
            if (isOnlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String currentDatabase = conn.getSchema().trim();
                remoteDatabaseNames.add(currentDatabase);
            } else {
                rs = conn.getMetaData().getSchemas(conn.getCatalog(), null);
                while (rs.next()) {
                    remoteDatabaseNames.add(rs.getString("TABLE_SCHEM").trim());
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
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String db2Type = fieldSchema.getDataTypeName().orElse("unknown");
        switch (db2Type) {
            case "SMALLINT":
                return Type.SMALLINT;
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "DECFLOAT":
            case "DECIMAL": {
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return createDecimalOrStringType(precision, scale);
            }
            case "DOUBLE":
                return Type.DOUBLE;
            case "REAL":
                return Type.FLOAT;
            case "CHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.getColumnSize().orElse(0));
                return charType;
            case "VARCHAR":
            case "LONG VARCHAR":
                ScalarType varcharType = ScalarType.createType(PrimitiveType.VARCHAR);
                varcharType.setLength(fieldSchema.getColumnSize().orElse(0));
                return varcharType;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "TIME":
            case "CLOB":
            case "VARGRAPHIC":
            case "LONG VARGRAPHIC":
            case "XML":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
