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

public class JdbcClickHouseClient extends JdbcClient {

    protected JdbcClickHouseClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String getDatabaseQuery() {
        return "SHOW DATABASES";
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "SYSTEM TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {

        String ckType = fieldSchema.getDataTypeName();

        if (ckType.startsWith("LowCardinality")) {
            ckType = ckType.substring(15, ckType.length() - 1);
            if (ckType.startsWith("Nullable")) {
                ckType = ckType.substring(9, ckType.length() - 1);
            }
        } else if (ckType.startsWith("Nullable")) {
            ckType = ckType.substring(9, ckType.length() - 1);
        }

        if (ckType.startsWith("Decimal")) {
            String[] accuracy = ckType.substring(8, ckType.length() - 1).split(", ");
            int precision = Integer.parseInt(accuracy[0]);
            int scale = Integer.parseInt(accuracy[1]);
            return createDecimalOrStringType(precision, scale);
        }

        if ("String".contains(ckType) || ckType.startsWith("Enum")
                || ckType.startsWith("IPv") || "UUID".contains(ckType)
                || ckType.startsWith("FixedString")) {
            return ScalarType.createStringType();
        }

        if (ckType.startsWith("DateTime")) {
            // DateTime with second precision
            if (ckType.startsWith("DateTime(") || ckType.equals("DateTime")) {
                return ScalarType.createDatetimeV2Type(0);
            } else {
                int indexStart = ckType.indexOf('(');
                int indexEnd = ckType.indexOf(')');
                if (indexStart != -1 && indexEnd != -1) {
                    String scaleStr = ckType.substring(indexStart + 1, indexEnd);
                    int scale = Integer.parseInt(scaleStr);
                    if (scale > JDBC_DATETIME_SCALE) {
                        scale = JDBC_DATETIME_SCALE;
                    }
                    // return with the actual scale
                    return ScalarType.createDatetimeV2Type(scale);
                } else {
                    // default precision if not specified
                    return ScalarType.createDatetimeV2Type(JDBC_DATETIME_SCALE);
                }
            }
        }

        if (ckType.startsWith("Array")) {
            String cktype = ckType.substring(6, ckType.length() - 1);
            fieldSchema.setDataTypeName(cktype);
            Type type = jdbcTypeToDoris(fieldSchema);
            return ArrayType.create(type, true);
        }

        switch (ckType) {
            case "Bool":
                return Type.BOOLEAN;
            case "Int8":
                return Type.TINYINT;
            case "Int16":
            case "UInt8":
                return Type.SMALLINT;
            case "Int32":
            case "UInt16":
                return Type.INT;
            case "Int64":
            case "UInt32":
                return Type.BIGINT;
            case "Int128":
            case "UInt64":
                return Type.LARGEINT;
            case "Int256":
            case "UInt128":
            case "UInt256":
                return ScalarType.createStringType();
            case "Float32":
                return Type.FLOAT;
            case "Float64":
                return Type.DOUBLE;
            case "Date":
            case "Date32":
                return ScalarType.createDateV2Type();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
