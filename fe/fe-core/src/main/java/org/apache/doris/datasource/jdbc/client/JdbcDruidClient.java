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

import java.sql.Connection;
import java.sql.SQLException;


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
        return "select SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA  WHERE \"SCHEMA_NAME\" = 'druid'";
    }

    @Override
    protected String[] getTableTypes() {
        return new String[]{"TABLE", " SYSTEM_TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String druidDataType = fieldSchema.getDataTypeName();
        switch (druidDataType) {
            case "BIGINT":
                return Type.BIGINT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "FLOAT":
                return Type.FLOAT;
            case "TIMESTAMP": {
                int scale = fieldSchema.getDecimalDigits();
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(6);
            }
            case "VARCHAR":
                return ScalarType.createStringType();
            default:
                break;
        }
        // complex<Json>
        if (druidDataType.contains("COMPLEX")) {
            String complexDataType = druidDataType.substring(8, druidDataType.length() - 1);
            switch (complexDataType) {
                case "json":
                    return ScalarType.createJsonbType();
                default:
                    return Type.UNSUPPORTED;
            }
        }


        return Type.UNSUPPORTED;
    }
}
