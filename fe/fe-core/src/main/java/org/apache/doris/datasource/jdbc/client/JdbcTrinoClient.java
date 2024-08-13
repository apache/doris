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
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import java.util.Optional;

public class JdbcTrinoClient extends JdbcClient {
    protected JdbcTrinoClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String trinoType = fieldSchema.getDataTypeName().orElse("unknown");
        switch (trinoType) {
            case "integer":
                return Type.INT;
            case "bigint":
                return Type.BIGINT;
            case "smallint":
                return Type.SMALLINT;
            case "tinyint":
                return Type.TINYINT;
            case "double":
                return Type.DOUBLE;
            case "real":
                return Type.FLOAT;
            case "boolean":
                return Type.BOOLEAN;
            case "date":
                return ScalarType.createDateV2Type();
            case "json":
                return ScalarType.createStringType();
            default:
                break;
        }

        if (trinoType.startsWith("decimal")) {
            String[] split = trinoType.split("\\(");
            String[] precisionAndScale = split[1].split(",");
            int precision = Integer.parseInt(precisionAndScale[0]);
            int scale = Integer.parseInt(precisionAndScale[1].substring(0, precisionAndScale[1].length() - 1));
            return createDecimalOrStringType(precision, scale);
        }

        if (trinoType.startsWith("char")) {
            ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
            charType.setLength(fieldSchema.getColumnSize().orElse(0));
            return charType;
        }

        if (trinoType.startsWith("timestamp")) {
            int scale = fieldSchema.getDecimalDigits().orElse(0);
            if (scale > 6) {
                scale = 6;
            }
            return ScalarType.createDatetimeV2Type(scale);
        }

        if (trinoType.startsWith("array")) {
            String trinoArrType = trinoType.substring(6, trinoType.length() - 1);
            fieldSchema.setDataTypeName(Optional.of(trinoArrType));
            Type type = jdbcTypeToDoris(fieldSchema);
            return ArrayType.create(type, true);
        }

        if (trinoType.startsWith("varchar") || trinoType.startsWith("time")) {
            return ScalarType.createStringType();
        }

        return Type.UNSUPPORTED;
    }
}
