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

public class JdbcPostgreSQLClient extends JdbcClient {

    protected JdbcPostgreSQLClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "PARTITIONED TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String pgType = fieldSchema.getDataTypeName().orElse("unknown");
        switch (pgType) {
            case "int2":
            case "smallserial":
                return Type.SMALLINT;
            case "int4":
            case "serial":
                return Type.INT;
            case "int8":
            case "bigserial":
                return Type.BIGINT;
            case "numeric": {
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return createDecimalOrStringType(precision, scale);
            }
            case "float4":
                return Type.FLOAT;
            case "float8":
                return Type.DOUBLE;
            case "bpchar":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.getColumnSize().orElse(0));
                return charType;
            case "timestamp":
            case "timestamptz": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "date":
                return ScalarType.createDateV2Type();
            case "bool":
                return Type.BOOLEAN;
            case "bit":
                if (fieldSchema.getColumnSize().orElse(0) == 1) {
                    return Type.BOOLEAN;
                } else {
                    return ScalarType.createStringType();
                }
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
            case "varchar":
            case "text":
            case "time":
            case "timetz":
            case "interval":
            case "cidr":
            case "inet":
            case "macaddr":
            case "varbit":
            case "uuid":
            case "bytea":
            case "json":
            case "jsonb":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
