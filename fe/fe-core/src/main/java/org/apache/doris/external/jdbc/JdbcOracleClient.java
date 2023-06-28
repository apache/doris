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

package org.apache.doris.external.jdbc;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

public class JdbcOracleClient extends JdbcClient {

    protected JdbcOracleClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String getDatabaseQuery() {
        return "SELECT DISTINCT OWNER FROM all_tables";
    }

    @Override
    protected String modifyTableNameIfNecessary(String tableName) {
        return tableName.replace("/", "%");
    }

    @Override
    protected boolean isTableModified(String modifiedTableName, String actualTableName) {
        return !modifiedTableName.equals(actualTableName);
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String oracleType = fieldSchema.getDataTypeName();
        if (oracleType.startsWith("INTERVAL")) {
            oracleType = oracleType.substring(0, 8);
        } else if (oracleType.startsWith("TIMESTAMP")) {
            if (oracleType.contains("TIME ZONE") || oracleType.contains("LOCAL TIME ZONE")) {
                return Type.UNSUPPORTED;
            }
            // oracle can support nanosecond, will lose precision
            int scale = fieldSchema.getDecimalDigits();
            if (scale > 6) {
                scale = 6;
            }
            return ScalarType.createDatetimeV2Type(scale);
        }
        switch (oracleType) {
            /**
             * The data type NUMBER(p,s) of oracle has some different of doris decimal type in semantics.
             * For Oracle Number(p,s) type:
             * 1. if s<0 , it means this is an Interger.
             *    This NUMBER(p,s) has (p+|s| ) significant digit, and rounding will be performed at s position.
             *    eg: if we insert 1234567 into NUMBER(5,-2) type, then the oracle will store 1234500.
             *    In this case, Doris will use INT type (TINYINT/SMALLINT/INT/.../LARGEINT).
             * 2. if s>=0 && s<p , it just like doris Decimal(p,s) behavior.
             * 3. if s>=0 && s>p, it means this is a decimal(like 0.xxxxx).
             *    p represents how many digits can be left to the left after the decimal point,
             *    the figure after the decimal point s will be rounded.
             *    eg: we can not insert 0.0123456 into NUMBER(5,7) type,
             *    because there must be two zeros on the right side of the decimal point,
             *    we can insert 0.0012345 into NUMBER(5,7) type.
             *    In this case, Doris will use DECIMAL(s,s)
             * 4. if we don't specify p and s for NUMBER(p,s), just NUMBER, the p and s of NUMBER are uncertain.
             *    In this case, doris can not determine p and s, so doris can not determine data type.
             */
            case "NUMBER":
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                if (scale <= 0) {
                    precision -= scale;
                    if (precision < 3) {
                        return Type.TINYINT;
                    } else if (precision < 5) {
                        return Type.SMALLINT;
                    } else if (precision < 10) {
                        return Type.INT;
                    } else if (precision < 19) {
                        return Type.BIGINT;
                    } else if (precision < 39) {
                        // LARGEINT supports up to 38 numbers.
                        return Type.LARGEINT;
                    } else {
                        return ScalarType.createStringType();
                    }
                }
                // scale > 0
                if (precision < scale) {
                    precision = scale;
                }
                return createDecimalOrStringType(precision, scale);
            case "FLOAT":
                return Type.DOUBLE;
            case "DATE":
                // can save date and time with second precision
                return ScalarType.createDatetimeV2Type(0);
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "INTERVAL":
                return ScalarType.createStringType();
            case "BLOB":
            case "CLOB":
            case "NCLOB":
            case "BFILE":
            case "BINARY_FLOAT":
            case "BINARY_DOUBLE":
            default:
                return Type.UNSUPPORTED;
        }
    }
}
