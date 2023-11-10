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

package org.apache.doris.catalog;

import org.apache.doris.common.Config;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public enum PrimitiveType {
    INVALID_TYPE("INVALID_TYPE", -1, TPrimitiveType.INVALID_TYPE, false),
    UNSUPPORTED("UNSUPPORTED_TYPE", -1, TPrimitiveType.UNSUPPORTED, false),
    // NULL_TYPE - used only in LiteralPredicate and NullLiteral to make NULLs compatible
    // with all other types.
    NULL_TYPE("NULL_TYPE", 1, TPrimitiveType.NULL_TYPE, false),
    BOOLEAN("BOOLEAN", 1, TPrimitiveType.BOOLEAN, true),
    TINYINT("TINYINT", 1, TPrimitiveType.TINYINT, true),
    SMALLINT("SMALLINT", 2, TPrimitiveType.SMALLINT, true),
    INT("INT", 4, TPrimitiveType.INT, true),
    BIGINT("BIGINT", 8, TPrimitiveType.BIGINT, true),
    LARGEINT("LARGEINT", 16, TPrimitiveType.LARGEINT, true),
    FLOAT("FLOAT", 4, TPrimitiveType.FLOAT, true),
    DOUBLE("DOUBLE", 8, TPrimitiveType.DOUBLE, true),
    DATE("DATE", 16, TPrimitiveType.DATE, true),
    DATETIME("DATETIME", 16, TPrimitiveType.DATETIME, true),
    IPV4("IPV4", 4, TPrimitiveType.IPV4, true),
    IPV6("IPV6", 16, TPrimitiveType.IPV6, true),
    // Fixed length char array.
    CHAR("CHAR", 16, TPrimitiveType.CHAR, true),
    // 8-byte pointer and 4-byte length indicator (12 bytes total).
    // Aligning to 8 bytes so 16 total.
    VARCHAR("VARCHAR", 16, TPrimitiveType.VARCHAR, true),
    JSONB("JSON", 16, TPrimitiveType.JSONB, true),

    DECIMALV2("DECIMALV2", 16, TPrimitiveType.DECIMALV2, true),
    DECIMAL32("DECIMAL32", 4, TPrimitiveType.DECIMAL32, true),
    DECIMAL64("DECIMAL64", 8, TPrimitiveType.DECIMAL64, true),
    DECIMAL128("DECIMAL128", 16, TPrimitiveType.DECIMAL128I, true),
    DECIMAL256("DECIMAL256", 32, TPrimitiveType.DECIMAL256, false),
    TIME("TIME", 8, TPrimitiveType.TIME, false),
    // these following types are stored as object binary in BE.
    HLL("HLL", 16, TPrimitiveType.HLL, true),
    BITMAP("BITMAP", 16, TPrimitiveType.OBJECT, true),
    QUANTILE_STATE("QUANTILE_STATE", 16, TPrimitiveType.QUANTILE_STATE, true),
    AGG_STATE("AGG_STATE", 16, TPrimitiveType.AGG_STATE, true),
    DATEV2("DATEV2", 4, TPrimitiveType.DATEV2, true),
    DATETIMEV2("DATETIMEV2", 8, TPrimitiveType.DATETIMEV2, true),
    TIMEV2("TIMEV2", 8, TPrimitiveType.TIMEV2, false),
    LAMBDA_FUNCTION("LAMBDA_FUNCTION", 16, TPrimitiveType.LAMBDA_FUNCTION, false),

    // sizeof(CollectionValue)
    ARRAY("ARRAY", 32, TPrimitiveType.ARRAY, true),
    MAP("MAP", 24, TPrimitiveType.MAP, true),
    // sizeof(StructValue)
    // 8-byte pointer and 4-byte size and 1 bytes has_null (13 bytes total)
    // Aligning to 16 bytes total.
    STRUCT("STRUCT", 16, TPrimitiveType.STRUCT, false),
    STRING("STRING", 16, TPrimitiveType.STRING, true),
    VARIANT("VARIANT", 24, TPrimitiveType.VARIANT, false),
    TEMPLATE("TEMPLATE", -1, TPrimitiveType.INVALID_TYPE, false),
    // Unsupported scalar types.
    BINARY("BINARY", -1, TPrimitiveType.BINARY, false),
    ALL("ALL", -1, TPrimitiveType.INVALID_TYPE, false);


    private static final int DATE_INDEX_LEN = 3;
    private static final int DATEV2_INDEX_LEN = 4;
    private static final int DATETIME_INDEX_LEN = 8;
    private static final int VARCHAR_INDEX_LEN = 20;
    private static final int STRING_INDEX_LEN = 20;
    private static final int DECIMAL_INDEX_LEN = 12;

    public static final ImmutableSet<PrimitiveType> typeWithPrecision;

    static {
        ImmutableSet.Builder<PrimitiveType> builder = ImmutableSet.builder();
        builder.add(DECIMAL32);
        builder.add(DECIMAL64);
        builder.add(DECIMAL128);
        builder.add(DECIMAL256);
        builder.add(DATETIMEV2);
        typeWithPrecision = builder.build();
    }

    private static final ImmutableSetMultimap<PrimitiveType, PrimitiveType> implicitCastMap;

    public static ImmutableSetMultimap<PrimitiveType, PrimitiveType> getImplicitCastMap() {
        return implicitCastMap;
    }

    static {
        ImmutableSetMultimap.Builder<PrimitiveType, PrimitiveType> builder = ImmutableSetMultimap.builder();
        // Nulltype
        builder.put(NULL_TYPE, BOOLEAN);
        builder.put(NULL_TYPE, TINYINT);
        builder.put(NULL_TYPE, SMALLINT);
        builder.put(NULL_TYPE, INT);
        builder.put(NULL_TYPE, BIGINT);
        builder.put(NULL_TYPE, LARGEINT);
        builder.put(NULL_TYPE, FLOAT);
        builder.put(NULL_TYPE, DOUBLE);
        builder.put(NULL_TYPE, DATE);
        builder.put(NULL_TYPE, DATETIME);
        builder.put(NULL_TYPE, DATEV2);
        builder.put(NULL_TYPE, DATETIMEV2);
        builder.put(NULL_TYPE, IPV4);
        builder.put(NULL_TYPE, IPV6);
        builder.put(NULL_TYPE, DECIMALV2);
        builder.put(NULL_TYPE, DECIMAL32);
        builder.put(NULL_TYPE, DECIMAL64);
        builder.put(NULL_TYPE, DECIMAL128);
        builder.put(NULL_TYPE, DECIMAL256);
        builder.put(NULL_TYPE, CHAR);
        builder.put(NULL_TYPE, VARCHAR);
        builder.put(NULL_TYPE, STRING);
        builder.put(NULL_TYPE, JSONB);
        builder.put(NULL_TYPE, VARIANT);
        builder.put(NULL_TYPE, BITMAP); //TODO(weixiang):why null type can cast to bitmap?
        builder.put(NULL_TYPE, TIME);
        builder.put(NULL_TYPE, TIMEV2);
        // Boolean
        builder.put(BOOLEAN, BOOLEAN);
        builder.put(BOOLEAN, TINYINT);
        builder.put(BOOLEAN, SMALLINT);
        builder.put(BOOLEAN, INT);
        builder.put(BOOLEAN, BIGINT);
        builder.put(BOOLEAN, LARGEINT);
        builder.put(BOOLEAN, FLOAT);
        builder.put(BOOLEAN, DOUBLE);
        builder.put(BOOLEAN, DATE);
        builder.put(BOOLEAN, DATETIME);
        builder.put(BOOLEAN, DATEV2);
        builder.put(BOOLEAN, DATETIMEV2);
        builder.put(BOOLEAN, IPV4);
        builder.put(BOOLEAN, IPV6);
        builder.put(BOOLEAN, DECIMALV2);
        builder.put(BOOLEAN, DECIMAL32);
        builder.put(BOOLEAN, DECIMAL64);
        builder.put(BOOLEAN, DECIMAL128);
        builder.put(BOOLEAN, DECIMAL256);
        builder.put(BOOLEAN, VARCHAR);
        builder.put(BOOLEAN, STRING);
        // Tinyint
        builder.put(TINYINT, BOOLEAN);
        builder.put(TINYINT, TINYINT);
        builder.put(TINYINT, SMALLINT);
        builder.put(TINYINT, INT);
        builder.put(TINYINT, BIGINT);
        builder.put(TINYINT, LARGEINT);
        builder.put(TINYINT, FLOAT);
        builder.put(TINYINT, DOUBLE);
        builder.put(TINYINT, DATE);
        builder.put(TINYINT, DATETIME);
        builder.put(TINYINT, DATEV2);
        builder.put(TINYINT, DATETIMEV2);
        builder.put(TINYINT, IPV4);
        builder.put(TINYINT, IPV6);
        builder.put(TINYINT, DECIMALV2);
        builder.put(TINYINT, DECIMAL32);
        builder.put(TINYINT, DECIMAL64);
        builder.put(TINYINT, DECIMAL128);
        builder.put(TINYINT, DECIMAL256);
        builder.put(TINYINT, VARCHAR);
        builder.put(TINYINT, STRING);
        builder.put(TINYINT, TIME);
        builder.put(TINYINT, TIMEV2);
        // Smallint
        builder.put(SMALLINT, BOOLEAN);
        builder.put(SMALLINT, TINYINT);
        builder.put(SMALLINT, SMALLINT);
        builder.put(SMALLINT, INT);
        builder.put(SMALLINT, BIGINT);
        builder.put(SMALLINT, LARGEINT);
        builder.put(SMALLINT, FLOAT);
        builder.put(SMALLINT, DOUBLE);
        builder.put(SMALLINT, DATE);
        builder.put(SMALLINT, DATETIME);
        builder.put(SMALLINT, DATEV2);
        builder.put(SMALLINT, DATETIMEV2);
        builder.put(SMALLINT, IPV4);
        builder.put(SMALLINT, IPV6);
        builder.put(SMALLINT, DECIMALV2);
        builder.put(SMALLINT, DECIMAL32);
        builder.put(SMALLINT, DECIMAL64);
        builder.put(SMALLINT, DECIMAL128);
        builder.put(SMALLINT, DECIMAL256);
        builder.put(SMALLINT, VARCHAR);
        builder.put(SMALLINT, STRING);
        builder.put(SMALLINT, TIME);
        builder.put(SMALLINT, TIMEV2);
        // Int
        builder.put(INT, BOOLEAN);
        builder.put(INT, TINYINT);
        builder.put(INT, SMALLINT);
        builder.put(INT, INT);
        builder.put(INT, BIGINT);
        builder.put(INT, LARGEINT);
        builder.put(INT, FLOAT);
        builder.put(INT, DOUBLE);
        builder.put(INT, DATE);
        builder.put(INT, DATETIME);
        builder.put(INT, DATEV2);
        builder.put(INT, DATETIMEV2);
        builder.put(INT, IPV4);
        builder.put(INT, IPV6);
        builder.put(INT, DECIMALV2);
        builder.put(INT, DECIMAL32);
        builder.put(INT, DECIMAL64);
        builder.put(INT, DECIMAL128);
        builder.put(INT, DECIMAL256);
        builder.put(INT, VARCHAR);
        builder.put(INT, STRING);
        builder.put(INT, TIME);
        builder.put(INT, TIMEV2);
        // Bigint
        builder.put(BIGINT, BOOLEAN);
        builder.put(BIGINT, TINYINT);
        builder.put(BIGINT, SMALLINT);
        builder.put(BIGINT, INT);
        builder.put(BIGINT, BIGINT);
        builder.put(BIGINT, LARGEINT);
        builder.put(BIGINT, FLOAT);
        builder.put(BIGINT, DOUBLE);
        builder.put(BIGINT, DATE);
        builder.put(BIGINT, DATETIME);
        builder.put(BIGINT, DATEV2);
        builder.put(BIGINT, DATETIMEV2);
        builder.put(BIGINT, IPV4);
        builder.put(BIGINT, IPV6);
        builder.put(BIGINT, DECIMALV2);
        builder.put(BIGINT, DECIMAL32);
        builder.put(BIGINT, DECIMAL64);
        builder.put(BIGINT, DECIMAL128);
        builder.put(BIGINT, DECIMAL256);
        builder.put(BIGINT, VARCHAR);
        builder.put(BIGINT, STRING);
        builder.put(BIGINT, TIME);
        builder.put(BIGINT, TIMEV2);
        // Largeint
        builder.put(LARGEINT, BOOLEAN);
        builder.put(LARGEINT, TINYINT);
        builder.put(LARGEINT, SMALLINT);
        builder.put(LARGEINT, INT);
        builder.put(LARGEINT, BIGINT);
        builder.put(LARGEINT, LARGEINT);
        builder.put(LARGEINT, FLOAT);
        builder.put(LARGEINT, DOUBLE);
        builder.put(LARGEINT, DATE);
        builder.put(LARGEINT, DATETIME);
        builder.put(LARGEINT, DATEV2);
        builder.put(LARGEINT, DATETIMEV2);
        builder.put(LARGEINT, IPV4);
        builder.put(LARGEINT, IPV6);
        builder.put(LARGEINT, DECIMALV2);
        builder.put(LARGEINT, DECIMAL32);
        builder.put(LARGEINT, DECIMAL64);
        builder.put(LARGEINT, DECIMAL128);
        builder.put(LARGEINT, DECIMAL256);
        builder.put(LARGEINT, VARCHAR);
        builder.put(LARGEINT, STRING);
        builder.put(LARGEINT, TIME);
        builder.put(LARGEINT, TIMEV2);
        // Float
        builder.put(FLOAT, BOOLEAN);
        builder.put(FLOAT, TINYINT);
        builder.put(FLOAT, SMALLINT);
        builder.put(FLOAT, INT);
        builder.put(FLOAT, BIGINT);
        builder.put(FLOAT, LARGEINT);
        builder.put(FLOAT, FLOAT);
        builder.put(FLOAT, DOUBLE);
        builder.put(FLOAT, DATE);
        builder.put(FLOAT, DATETIME);
        builder.put(FLOAT, DATEV2);
        builder.put(FLOAT, DATETIMEV2);
        builder.put(FLOAT, IPV4);
        builder.put(FLOAT, IPV6);
        builder.put(FLOAT, DECIMALV2);
        builder.put(FLOAT, DECIMAL32);
        builder.put(FLOAT, DECIMAL64);
        builder.put(FLOAT, DECIMAL128);
        builder.put(FLOAT, DECIMAL256);
        builder.put(FLOAT, VARCHAR);
        builder.put(FLOAT, STRING);
        builder.put(FLOAT, TIME);
        builder.put(FLOAT, TIMEV2);
        // Double
        builder.put(DOUBLE, BOOLEAN);
        builder.put(DOUBLE, TINYINT);
        builder.put(DOUBLE, SMALLINT);
        builder.put(DOUBLE, INT);
        builder.put(DOUBLE, BIGINT);
        builder.put(DOUBLE, LARGEINT);
        builder.put(DOUBLE, FLOAT);
        builder.put(DOUBLE, DOUBLE);
        builder.put(DOUBLE, DATE);
        builder.put(DOUBLE, DATETIME);
        builder.put(DOUBLE, DATEV2);
        builder.put(DOUBLE, DATETIMEV2);
        builder.put(DOUBLE, IPV4);
        builder.put(DOUBLE, IPV6);
        builder.put(DOUBLE, DECIMALV2);
        builder.put(DOUBLE, DECIMAL32);
        builder.put(DOUBLE, DECIMAL64);
        builder.put(DOUBLE, DECIMAL128);
        builder.put(DOUBLE, DECIMAL256);
        builder.put(DOUBLE, VARCHAR);
        builder.put(DOUBLE, STRING);
        builder.put(DOUBLE, TIME);
        builder.put(DOUBLE, TIMEV2);
        // Date
        builder.put(DATE, BOOLEAN);
        builder.put(DATE, TINYINT);
        builder.put(DATE, SMALLINT);
        builder.put(DATE, INT);
        builder.put(DATE, BIGINT);
        builder.put(DATE, LARGEINT);
        builder.put(DATE, FLOAT);
        builder.put(DATE, DOUBLE);
        builder.put(DATE, DATE);
        builder.put(DATE, DATETIME);
        builder.put(DATE, DATEV2);
        builder.put(DATE, DATETIMEV2);
        builder.put(DATE, DECIMALV2);
        builder.put(DATE, DECIMAL32);
        builder.put(DATE, DECIMAL64);
        builder.put(DATE, DECIMAL128);
        builder.put(DATE, DECIMAL256);
        builder.put(DATE, VARCHAR);
        builder.put(DATE, STRING);
        // Datetime
        builder.put(DATETIME, BOOLEAN);
        builder.put(DATETIME, TINYINT);
        builder.put(DATETIME, SMALLINT);
        builder.put(DATETIME, INT);
        builder.put(DATETIME, BIGINT);
        builder.put(DATETIME, LARGEINT);
        builder.put(DATETIME, FLOAT);
        builder.put(DATETIME, DOUBLE);
        builder.put(DATETIME, DATE);
        builder.put(DATETIME, DATETIME);
        builder.put(DATETIME, DATEV2);
        builder.put(DATETIME, DATETIMEV2);
        builder.put(DATETIME, DECIMALV2);
        builder.put(DATETIME, DECIMAL32);
        builder.put(DATETIME, DECIMAL64);
        builder.put(DATETIME, DECIMAL128);
        builder.put(DATETIME, DECIMAL256);
        builder.put(DATETIME, VARCHAR);
        builder.put(DATETIME, STRING);
        // DateV2
        builder.put(DATEV2, BOOLEAN);
        builder.put(DATEV2, TINYINT);
        builder.put(DATEV2, SMALLINT);
        builder.put(DATEV2, INT);
        builder.put(DATEV2, BIGINT);
        builder.put(DATEV2, LARGEINT);
        builder.put(DATEV2, FLOAT);
        builder.put(DATEV2, DOUBLE);
        builder.put(DATEV2, DATE);
        builder.put(DATEV2, DATETIME);
        builder.put(DATEV2, DATEV2);
        builder.put(DATEV2, DATETIMEV2);
        builder.put(DATEV2, DECIMALV2);
        builder.put(DATEV2, DECIMAL32);
        builder.put(DATEV2, DECIMAL64);
        builder.put(DATEV2, DECIMAL128);
        builder.put(DATEV2, DECIMAL256);
        builder.put(DATEV2, VARCHAR);
        builder.put(DATEV2, STRING);
        // DatetimeV2
        builder.put(DATETIMEV2, BOOLEAN);
        builder.put(DATETIMEV2, TINYINT);
        builder.put(DATETIMEV2, SMALLINT);
        builder.put(DATETIMEV2, INT);
        builder.put(DATETIMEV2, BIGINT);
        builder.put(DATETIMEV2, LARGEINT);
        builder.put(DATETIMEV2, FLOAT);
        builder.put(DATETIMEV2, DOUBLE);
        builder.put(DATETIMEV2, DATE);
        builder.put(DATETIMEV2, DATETIME);
        builder.put(DATETIMEV2, DATEV2);
        builder.put(DATETIMEV2, DATETIMEV2);
        builder.put(DATETIMEV2, DECIMALV2);
        builder.put(DATETIMEV2, DECIMAL32);
        builder.put(DATETIMEV2, DECIMAL64);
        builder.put(DATETIMEV2, DECIMAL128);
        builder.put(DATETIMEV2, DECIMAL256);
        builder.put(DATETIMEV2, VARCHAR);
        builder.put(DATETIMEV2, STRING);
        // Char
        builder.put(CHAR, BOOLEAN);
        builder.put(CHAR, TINYINT);
        builder.put(CHAR, SMALLINT);
        builder.put(CHAR, CHAR);
        builder.put(CHAR, INT);
        builder.put(CHAR, BIGINT);
        builder.put(CHAR, LARGEINT);
        builder.put(CHAR, FLOAT);
        builder.put(CHAR, DOUBLE);
        builder.put(CHAR, DATE);
        builder.put(CHAR, DATETIME);
        builder.put(CHAR, DATEV2);
        builder.put(CHAR, DATETIMEV2);
        builder.put(CHAR, DECIMALV2);
        builder.put(CHAR, DECIMAL32);
        builder.put(CHAR, DECIMAL64);
        builder.put(CHAR, DECIMAL128);
        builder.put(CHAR, DECIMAL256);
        builder.put(CHAR, VARCHAR);
        builder.put(CHAR, STRING);
        builder.put(CHAR, TIME);
        builder.put(CHAR, TIMEV2);
        // Varchar
        builder.put(VARCHAR, BOOLEAN);
        builder.put(VARCHAR, TINYINT);
        builder.put(VARCHAR, SMALLINT);
        builder.put(VARCHAR, INT);
        builder.put(VARCHAR, BIGINT);
        builder.put(VARCHAR, LARGEINT);
        builder.put(VARCHAR, FLOAT);
        builder.put(VARCHAR, DOUBLE);
        builder.put(VARCHAR, DATE);
        builder.put(VARCHAR, DATETIME);
        builder.put(VARCHAR, DATEV2);
        builder.put(VARCHAR, DATETIMEV2);
        builder.put(VARCHAR, IPV4);
        builder.put(VARCHAR, IPV6);
        builder.put(VARCHAR, DECIMALV2);
        builder.put(VARCHAR, DECIMAL32);
        builder.put(VARCHAR, DECIMAL64);
        builder.put(VARCHAR, DECIMAL128);
        builder.put(VARCHAR, DECIMAL256);
        builder.put(VARCHAR, VARCHAR);
        builder.put(VARCHAR, JSONB);
        builder.put(VARCHAR, VARIANT);
        builder.put(VARCHAR, STRING);
        builder.put(VARCHAR, TIME);
        builder.put(VARCHAR, TIMEV2);

        // String
        builder.put(STRING, BOOLEAN);
        builder.put(STRING, TINYINT);
        builder.put(STRING, SMALLINT);
        builder.put(STRING, INT);
        builder.put(STRING, BIGINT);
        builder.put(STRING, LARGEINT);
        builder.put(STRING, FLOAT);
        builder.put(STRING, DOUBLE);
        builder.put(STRING, DATE);
        builder.put(STRING, DATETIME);
        builder.put(STRING, DATEV2);
        builder.put(STRING, DATETIMEV2);
        builder.put(STRING, IPV4);
        builder.put(STRING, IPV6);
        builder.put(STRING, DECIMALV2);
        builder.put(STRING, DECIMAL32);
        builder.put(STRING, DECIMAL64);
        builder.put(STRING, DECIMAL128);
        builder.put(STRING, DECIMAL256);
        builder.put(STRING, VARCHAR);
        builder.put(STRING, JSONB);
        builder.put(STRING, VARIANT);
        builder.put(STRING, STRING);
        builder.put(STRING, TIME);
        builder.put(STRING, TIMEV2);

        // DecimalV2
        builder.put(DECIMALV2, BOOLEAN);
        builder.put(DECIMALV2, TINYINT);
        builder.put(DECIMALV2, SMALLINT);
        builder.put(DECIMALV2, INT);
        builder.put(DECIMALV2, BIGINT);
        builder.put(DECIMALV2, LARGEINT);
        builder.put(DECIMALV2, FLOAT);
        builder.put(DECIMALV2, DOUBLE);
        builder.put(DECIMALV2, DECIMALV2);
        builder.put(DECIMALV2, DECIMAL32);
        builder.put(DECIMALV2, DECIMAL64);
        builder.put(DECIMALV2, DECIMAL128);
        builder.put(DECIMALV2, DECIMAL256);
        builder.put(DECIMALV2, VARCHAR);
        builder.put(DECIMALV2, STRING);

        builder.put(DECIMAL32, BOOLEAN);
        builder.put(DECIMAL32, TINYINT);
        builder.put(DECIMAL32, SMALLINT);
        builder.put(DECIMAL32, INT);
        builder.put(DECIMAL32, BIGINT);
        builder.put(DECIMAL32, LARGEINT);
        builder.put(DECIMAL32, FLOAT);
        builder.put(DECIMAL32, DOUBLE);
        builder.put(DECIMAL32, DECIMALV2);
        builder.put(DECIMAL32, DECIMAL32);
        builder.put(DECIMAL32, DECIMAL64);
        builder.put(DECIMAL32, DECIMAL128);
        builder.put(DECIMAL32, DECIMAL256);
        builder.put(DECIMAL32, VARCHAR);
        builder.put(DECIMAL32, STRING);

        builder.put(DECIMAL64, BOOLEAN);
        builder.put(DECIMAL64, TINYINT);
        builder.put(DECIMAL64, SMALLINT);
        builder.put(DECIMAL64, INT);
        builder.put(DECIMAL64, BIGINT);
        builder.put(DECIMAL64, LARGEINT);
        builder.put(DECIMAL64, FLOAT);
        builder.put(DECIMAL64, DOUBLE);
        builder.put(DECIMAL64, DECIMALV2);
        builder.put(DECIMAL64, DECIMAL32);
        builder.put(DECIMAL64, DECIMAL64);
        builder.put(DECIMAL64, DECIMAL128);
        builder.put(DECIMAL64, DECIMAL256);
        builder.put(DECIMAL64, VARCHAR);
        builder.put(DECIMAL64, STRING);

        builder.put(DECIMAL128, BOOLEAN);
        builder.put(DECIMAL128, TINYINT);
        builder.put(DECIMAL128, SMALLINT);
        builder.put(DECIMAL128, INT);
        builder.put(DECIMAL128, BIGINT);
        builder.put(DECIMAL128, LARGEINT);
        builder.put(DECIMAL128, FLOAT);
        builder.put(DECIMAL128, DOUBLE);
        builder.put(DECIMAL128, DECIMALV2);
        builder.put(DECIMAL128, DECIMAL32);
        builder.put(DECIMAL128, DECIMAL64);
        builder.put(DECIMAL128, DECIMAL128);
        builder.put(DECIMAL128, DECIMAL256);
        builder.put(DECIMAL128, VARCHAR);
        builder.put(DECIMAL128, STRING);

        // decimal256
        builder.put(DECIMAL256, BOOLEAN);
        builder.put(DECIMAL256, TINYINT);
        builder.put(DECIMAL256, SMALLINT);
        builder.put(DECIMAL256, INT);
        builder.put(DECIMAL256, BIGINT);
        builder.put(DECIMAL256, LARGEINT);
        builder.put(DECIMAL256, FLOAT);
        builder.put(DECIMAL256, DOUBLE);
        builder.put(DECIMAL256, DECIMALV2);
        builder.put(DECIMAL256, DECIMAL32);
        builder.put(DECIMAL256, DECIMAL64);
        builder.put(DECIMAL256, DECIMAL128);
        builder.put(DECIMAL256, DECIMAL256);
        builder.put(DECIMAL256, VARCHAR);
        builder.put(DECIMAL256, STRING);

        // JSONB
        builder.put(JSONB, BOOLEAN);
        builder.put(JSONB, TINYINT);
        builder.put(JSONB, SMALLINT);
        builder.put(JSONB, INT);
        builder.put(JSONB, BIGINT);
        builder.put(JSONB, LARGEINT);
        builder.put(JSONB, FLOAT);
        builder.put(JSONB, DOUBLE);
        builder.put(JSONB, DECIMALV2);
        builder.put(JSONB, DECIMAL32);
        builder.put(JSONB, DECIMAL64);
        builder.put(JSONB, DECIMAL128);
        // TODO: support and test decimal256?
        // builder.put(JSONB, DECIMAL256);
        builder.put(JSONB, VARCHAR);
        builder.put(JSONB, STRING);
        builder.put(JSONB, VARIANT);

        // VARIANT
        builder.put(VARIANT, VARCHAR);
        builder.put(VARIANT, STRING);
        builder.put(VARIANT, JSONB);

        // HLL
        builder.put(HLL, HLL);

        // BITMAP
        builder.put(BITMAP, BITMAP);

        // QUANTILE_STATE
        builder.put(QUANTILE_STATE, QUANTILE_STATE);

        builder.put(AGG_STATE, AGG_STATE);
        builder.put(AGG_STATE, VARCHAR);

        // TIME
        builder.put(TIME, TIME);
        builder.put(TIME, TIMEV2);
        builder.put(TIME, DOUBLE);

        //TIMEV2
        builder.put(TIMEV2, TIME);
        builder.put(TIMEV2, TIMEV2);
        builder.put(TIMEV2, DOUBLE);

        implicitCastMap = builder.build();
    }

    private static final ArrayList<PrimitiveType> integerTypes;
    private static final ArrayList<PrimitiveType> numericTypes;
    private static final ArrayList<PrimitiveType> supportedTypes;

    static {
        integerTypes = Lists.newArrayList();
        integerTypes.add(TINYINT);
        integerTypes.add(SMALLINT);
        integerTypes.add(INT);
        integerTypes.add(BIGINT);
        integerTypes.add(LARGEINT);

        numericTypes = Lists.newArrayList();
        numericTypes.add(TINYINT);
        numericTypes.add(SMALLINT);
        numericTypes.add(INT);
        numericTypes.add(BIGINT);
        numericTypes.add(LARGEINT);
        numericTypes.add(FLOAT);
        numericTypes.add(DOUBLE);
        numericTypes.add(DECIMALV2);
        numericTypes.add(DECIMAL32);
        numericTypes.add(DECIMAL64);
        numericTypes.add(DECIMAL128);
        numericTypes.add(DECIMAL256);

        supportedTypes = Lists.newArrayList();
        supportedTypes.add(NULL_TYPE);
        supportedTypes.add(BOOLEAN);
        supportedTypes.add(TINYINT);
        supportedTypes.add(SMALLINT);
        supportedTypes.add(INT);
        supportedTypes.add(BIGINT);
        supportedTypes.add(LARGEINT);
        supportedTypes.add(FLOAT);
        supportedTypes.add(DOUBLE);
        supportedTypes.add(VARCHAR);
        supportedTypes.add(JSONB);
        supportedTypes.add(VARIANT);
        supportedTypes.add(STRING);
        supportedTypes.add(HLL);
        supportedTypes.add(CHAR);
        supportedTypes.add(DATE);
        supportedTypes.add(DATETIME);
        supportedTypes.add(TIME);
        supportedTypes.add(DATEV2);
        supportedTypes.add(DATETIMEV2);
        supportedTypes.add(TIMEV2);
        supportedTypes.add(IPV4);
        supportedTypes.add(IPV6);
        supportedTypes.add(DECIMALV2);
        supportedTypes.add(DECIMAL32);
        supportedTypes.add(DECIMAL64);
        supportedTypes.add(DECIMAL128);
        supportedTypes.add(DECIMAL256);
        supportedTypes.add(BITMAP);
        supportedTypes.add(ARRAY);
        supportedTypes.add(MAP);
        supportedTypes.add(QUANTILE_STATE);
        supportedTypes.add(AGG_STATE);
    }

    public static ArrayList<PrimitiveType> getIntegerTypes() {
        return integerTypes;
    }

    public static ArrayList<PrimitiveType> getNumericTypes() {
        return numericTypes;
    }

    public static ArrayList<PrimitiveType> getSupportedTypes() {
        return supportedTypes;
    }

    // Check whether 'type' can cast to 'target'
    public static boolean isImplicitCast(PrimitiveType type, PrimitiveType target) {
        return implicitCastMap.get(type).contains(target);
    }

    private final String description;
    private final int slotSize;  // size of tuple slot for this type
    private final TPrimitiveType thriftType;
    private final boolean availableInDdl;
    private boolean isTimeType = false;

    PrimitiveType(String description, int slotSize, TPrimitiveType thriftType, boolean availableInDdl) {
        this.description = description;
        this.slotSize = slotSize;
        this.thriftType = thriftType;
        this.availableInDdl = availableInDdl;
    }

    public void setTimeType() {
        isTimeType = true;
    }

    public boolean isTimeType() {
        return isTimeType;
    }

    public static PrimitiveType fromThrift(TPrimitiveType tPrimitiveType) {
        switch (tPrimitiveType) {
            case NULL_TYPE:
                return NULL_TYPE;
            case BOOLEAN:
                return BOOLEAN;
            case TINYINT:
                return TINYINT;
            case SMALLINT:
                return SMALLINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case LARGEINT:
                return LARGEINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case DATE:
                return DATE;
            case DATETIME:
                return DATETIME;
            case DATEV2:
                return DATEV2;
            case DATETIMEV2:
                return DATETIMEV2;
            case IPV4:
                return IPV4;
            case IPV6:
                return IPV6;
            case BINARY:
                return BINARY;
            case DECIMALV2:
                return DECIMALV2;
            case DECIMAL32:
                return DECIMAL32;
            case DECIMAL64:
                return DECIMAL64;
            case DECIMAL128I:
                return DECIMAL128;
            case DECIMAL256:
                return DECIMAL256;
            case TIME:
                return TIME;
            case TIMEV2:
                return TIMEV2;
            case VARCHAR:
                return VARCHAR;
            case JSONB:
                return JSONB;
            case STRING:
                return STRING;
            case CHAR:
                return CHAR;
            case HLL:
                return HLL;
            case OBJECT:
                return BITMAP;
            case QUANTILE_STATE:
                return QUANTILE_STATE;
            case AGG_STATE:
                return AGG_STATE;
            case ARRAY:
                return ARRAY;
            case MAP:
                return MAP;
            case STRUCT:
                return STRUCT;
            case ALL:
                return ALL;
            case VARIANT:
                return VARIANT;
            default:
                return INVALID_TYPE;
        }
    }

    public static List<TPrimitiveType> toThrift(PrimitiveType[] types) {
        List<TPrimitiveType> result = Lists.newArrayList();
        for (PrimitiveType t : types) {
            result.add(t.toThrift());
        }
        return result;
    }

    public static int getMaxSlotSize() {
        return ARRAY.slotSize;
    }

    @Override
    public String toString() {
        return description;
    }

    public TPrimitiveType toThrift() {
        return thriftType;
    }

    public int getSlotSize() {
        return slotSize;
    }

    public boolean isAvailableInDdl() {
        return availableInDdl;
    }

    public boolean isFixedPointType() {
        return this == TINYINT
                || this == SMALLINT
                || this == INT
                || this == BIGINT
                || this == LARGEINT;
    }

    public boolean isFloatingPointType() {
        return this == FLOAT || this == DOUBLE;
    }


    public boolean isDecimalV2Type() {
        return this == DECIMALV2;
    }

    public boolean isDecimalV3Type() {
        return this == DECIMAL32 || this == DECIMAL64 || this == DECIMAL128 || this == DECIMAL256;
    }

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2Type() || isDecimalV3Type();
    }

    public boolean isValid() {
        return this != INVALID_TYPE;
    }

    public boolean isNull() {
        return this == NULL_TYPE;
    }

    public boolean isDateType() {
        return (this == DATE || this == DATETIME || this == DATEV2 || this == DATETIMEV2);
    }

    public boolean isDateV2Type() {
        return (this == DATEV2 || this == DATETIMEV2);
    }

    public boolean isArrayType() {
        return this == ARRAY;
    }

    public boolean isMapType() {
        return this == MAP;
    }

    public boolean isStructType() {
        return this == STRUCT;
    }

    public boolean isComplexType() {
        return this == ARRAY || this == MAP || this == STRUCT;
    }

    public boolean isHllType() {
        return this == HLL;
    }

    public boolean isBitmapType() {
        return this == BITMAP;
    }

    public boolean isVariantType() {
        return this == VARIANT;
    }

    public boolean isStringType() {
        return (this == VARCHAR || this == CHAR || this == STRING || this == AGG_STATE);
    }

    public boolean isJsonbType() {
        return this == JSONB;
    }

    public boolean isCharFamily() {
        return (this == VARCHAR || this == CHAR || this == STRING);
    }

    public boolean isIntegerType() {
        return (this == TINYINT || this == SMALLINT
                || this == INT || this == BIGINT);
    }

    public boolean isIPType() {
        return (this == IPV4 || this == IPV6);
    }

    public boolean isIPv4Type() {
        return (this == IPV4);
    }

    public boolean isIPv6Type() {
        return (this == IPV6);
    }

    // TODO(zhaochun): Add Mysql Type to it's private field
    public MysqlColType toMysqlType() {
        switch (this) {
            // MySQL use Tinyint(1) to represent boolean
            case BOOLEAN:
            case TINYINT:
                return MysqlColType.MYSQL_TYPE_TINY;
            case SMALLINT:
                return MysqlColType.MYSQL_TYPE_SHORT;
            case INT:
                return MysqlColType.MYSQL_TYPE_LONG;
            case BIGINT:
                return MysqlColType.MYSQL_TYPE_LONGLONG;
            case LARGEINT:
                if (Config.use_mysql_bigint_for_largeint) {
                    return MysqlColType.MYSQL_TYPE_LONGLONG;
                } else {
                    return MysqlColType.MYSQL_TYPE_STRING;
                }
            case FLOAT:
                return MysqlColType.MYSQL_TYPE_FLOAT;
            case DOUBLE:
                return MysqlColType.MYSQL_TYPE_DOUBLE;
            case TIME:
            case TIMEV2:
                return MysqlColType.MYSQL_TYPE_TIME;
            case DATE:
            case DATEV2:
                return MysqlColType.MYSQL_TYPE_DATE;
            case DATETIME:
            case DATETIMEV2: {
                if (isTimeType) {
                    return MysqlColType.MYSQL_TYPE_TIME;
                } else {
                    return MysqlColType.MYSQL_TYPE_DATETIME;
                }
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                return MysqlColType.MYSQL_TYPE_NEWDECIMAL;
            case STRING:
                return MysqlColType.MYSQL_TYPE_BLOB;
            case JSONB:
            case VARIANT:
                return MysqlColType.MYSQL_TYPE_JSON;
            case MAP:
                return MysqlColType.MYSQL_TYPE_MAP;
            default:
                return MysqlColType.MYSQL_TYPE_STRING;
        }
    }

    public int getOlapColumnIndexSize() {
        switch (this) {
            case DATE:
                return DATE_INDEX_LEN;
            case DATEV2:
                return DATEV2_INDEX_LEN;
            case DATETIME:
            case DATETIMEV2:
                return DATETIME_INDEX_LEN;
            case VARCHAR:
                return VARCHAR_INDEX_LEN;
            case CHAR:
                // char index size is length
                return -1;
            case STRING:
                return STRING_INDEX_LEN;
            case DECIMALV2:
                return DECIMAL_INDEX_LEN;
            case DECIMAL32:
                return 4;
            case DECIMAL64:
                return 8;
            case DECIMAL128:
                return 16;
            case DECIMAL256:
                return 32;
            default:
                return this.getSlotSize();
        }
    }
}

