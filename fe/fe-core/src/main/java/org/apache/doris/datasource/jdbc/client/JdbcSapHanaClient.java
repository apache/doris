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

public class JdbcSapHanaClient extends JdbcClient {
    protected JdbcSapHanaClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "OLAP VIEW", "JOIN VIEW", "HIERARCHY VIEW", "CALC VIEW"};
    }

    @Override
    public String getTestQuery() {
        return "SELECT 1 FROM DUMMY";
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String hanaType = fieldSchema.getDataTypeName().orElse("unknown");
        switch (hanaType) {
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "SMALLDECIMAL":
            case "DECIMAL": {
                if (!fieldSchema.getDecimalDigits().isPresent()) {
                    return Type.DOUBLE;
                } else  {
                    int precision = fieldSchema.getColumnSize().orElse(0);
                    int scale = fieldSchema.getDecimalDigits().orElse(0);
                    return createDecimalOrStringType(precision, scale);
                }
            }
            case "REAL":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "TIMESTAMP": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "SECONDDATE":
                // SECONDDATE with second precision
                return ScalarType.createDatetimeV2Type(0);
            case "DATE":
                return ScalarType.createDateV2Type();
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "CHAR":
            case "NCHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.getColumnSize().orElse(0));
                return charType;
            case "TIME":
            case "VARCHAR":
            case "NVARCHAR":
            case "ALPHANUM":
            case "SHORTTEXT":
            case "CLOB":
            case "NCLOB":
            case "TEXT":
            case "BINTEXT":
            case "BINARY":
            case "VARBINARY":
                return ScalarType.createStringType();
            case "BLOB":
            case "ST_GEOMETRY":
            case "ST_POINT":
            default:
                return Type.UNSUPPORTED;
        }
    }
}
