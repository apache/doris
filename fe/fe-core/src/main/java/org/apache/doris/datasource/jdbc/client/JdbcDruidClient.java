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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Consumer;


public class JdbcDruidClient extends JdbcClient {
    protected JdbcDruidClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String getCatalogName(Connection conn) throws SQLException {
        return conn.getCatalog();
    }

    @Override
    protected String getDatabaseQuery() {
        return "select SCHEMA_NAME from INFORMATION_SCHEMA.SCHEMATA where \"SCHEMA_NAME\" != 'view' and \"SCHEMA_NAME\" != 'lookup'  ";
    }

    @Override
    protected void processTable(String dbName, String tableName, String[] tableTypes,
                                Consumer<ResultSet> resultSetConsumer) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            rs = databaseMetaData.getTables(null, dbName, null, null);
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String druidDataType = fieldSchema.getDataTypeName();
        switch (druidDataType) {
            case "BIGINT":
                return Type.BIGINT;
            case "DOUBLE":
            case "FLOAT":
                return Type.DOUBLE;
            case "TIMESTAMP":
                int scale = fieldSchema.getDecimalDigits();
                if (scale == -1 || scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            case "VARCHAR":
                if (fieldSchema.columnSize == -1){
                    return ScalarType.createStringType();
                }
                return ScalarType.createVarchar(fieldSchema.columnSize);
            default:
                break;
        }
        // complex<Json>
        if (druidDataType.contains("COMPLEX")) {
            String complexDataType = druidDataType.substring(8, druidDataType.length() - 1);
            if (complexDataType.equals("json")) {
                return ScalarType.createJsonbType();
            }
            return Type.UNSUPPORTED;
        }


        return Type.UNSUPPORTED;
    }
}
