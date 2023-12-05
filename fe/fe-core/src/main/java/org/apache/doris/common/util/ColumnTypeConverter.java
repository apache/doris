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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Type;

import java.util.HashMap;
import java.util.Map;

public class ColumnTypeConverter {
    private static final Map<String, Type> typeMap = new HashMap<>();

    static {
        typeMap.put("TINYINT", Type.TINYINT);
        typeMap.put("SMALLINT", Type.SMALLINT);
        typeMap.put("INT", Type.INT);
        typeMap.put("BIGINT", Type.BIGINT);
        typeMap.put("LARGEINT", Type.LARGEINT);
        typeMap.put("UNSIGNED_TINYINT", Type.UNSUPPORTED);
        typeMap.put("UNSIGNED_SMALLINT", Type.UNSUPPORTED);
        typeMap.put("UNSIGNED_INT", Type.UNSUPPORTED);
        typeMap.put("UNSIGNED_BIGINT", Type.UNSUPPORTED);
        typeMap.put("FLOAT", Type.FLOAT);
        typeMap.put("DISCRETE_DOUBLE", Type.DOUBLE);
        typeMap.put("DOUBLE", Type.DOUBLE);
        typeMap.put("CHAR", Type.CHAR);
        typeMap.put("DATE", Type.DATE);
        typeMap.put("DATEV2", Type.DATEV2);
        typeMap.put("DATETIMEV2", Type.DATETIMEV2);
        typeMap.put("DATETIME", Type.DATETIME);
        typeMap.put("DECIMAL32", Type.DECIMAL32);
        typeMap.put("DECIMAL64", Type.DECIMAL64);
        typeMap.put("DECIMAL128I", Type.DECIMAL128);
        typeMap.put("DECIMAL", Type.DECIMALV2);
        typeMap.put("VARCHAR", Type.VARCHAR);
        typeMap.put("STRING", Type.STRING);
        typeMap.put("JSONB", Type.JSONB);
        typeMap.put("VARIANT", Type.VARIANT);
        typeMap.put("BOOLEAN", Type.BOOLEAN);
        typeMap.put("HLL", Type.HLL);
        typeMap.put("STRUCT", Type.STRUCT);
        typeMap.put("LIST", Type.UNSUPPORTED);
        typeMap.put("MAP", Type.MAP);
        typeMap.put("OBJECT", Type.UNSUPPORTED);
        typeMap.put("ARRAY", Type.ARRAY);
        typeMap.put("QUANTILE_STATE", Type.QUANTILE_STATE);
        typeMap.put("AGG_STATE", Type.AGG_STATE);
    }

    public static Type getTypeFromTypeName(String typeName) {
        return typeMap.getOrDefault(typeName, Type.UNSUPPORTED);
    }
}

