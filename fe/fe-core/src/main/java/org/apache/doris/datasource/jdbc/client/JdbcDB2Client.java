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

public class JdbcDB2Client extends JdbcClient {

    protected JdbcDB2Client(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String getDatabaseQuery() {
        return "SELECT schemaname FROM syscat.schemata WHERE DEFINER = CURRENT USER;";
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String db2Type = fieldSchema.getDataTypeName();
        switch (db2Type) {
            case "SMALLINT":
                return Type.SMALLINT;
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "DECFLOAT":
            case "DECIMAL": {
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "DOUBLE":
                return Type.DOUBLE;
            case "REAL":
                return Type.FLOAT;
            case "CHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.columnSize);
                return charType;
            case "VARCHAR":
            case "LONG VARCHAR":
                ScalarType varcharType = ScalarType.createType(PrimitiveType.VARCHAR);
                varcharType.setLength(fieldSchema.columnSize);
                return varcharType;
            case "DATE":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits();
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "TIME":
            case "CLOB":
            case "VARGRAPHIC":
            case "LONG VARGRAPHIC":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
