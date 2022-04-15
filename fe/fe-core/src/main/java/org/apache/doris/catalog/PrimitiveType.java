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

import org.apache.doris.mysql.MysqlColType;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

public enum PrimitiveType {
    INVALID_TYPE("INVALID_TYPE", -1, TPrimitiveType.INVALID_TYPE),
    // NULL_TYPE - used only in LiteralPredicate and NullLiteral to make NULLs compatible
    // with all other types.
    NULL_TYPE("NULL_TYPE", 1, TPrimitiveType.NULL_TYPE),
    BOOLEAN("BOOLEAN", 1, TPrimitiveType.BOOLEAN),
    TINYINT("TINYINT", 1, TPrimitiveType.TINYINT),
    SMALLINT("SMALLINT", 2, TPrimitiveType.SMALLINT),
    INT("INT", 4, TPrimitiveType.INT),
    BIGINT("BIGINT", 8, TPrimitiveType.BIGINT),
    LARGEINT("LARGEINT", 16, TPrimitiveType.LARGEINT),
    FLOAT("FLOAT", 4, TPrimitiveType.FLOAT),
    DOUBLE("DOUBLE", 8, TPrimitiveType.DOUBLE),
    DATE("DATE", 16, TPrimitiveType.DATE),
    DATETIME("DATETIME", 16, TPrimitiveType.DATETIME),
    // Fixed length char array.
    CHAR("CHAR", 16, TPrimitiveType.CHAR),
    // 8-byte pointer and 4-byte length indicator (12 bytes total).
    // Aligning to 8 bytes so 16 total.
    VARCHAR("VARCHAR", 16, TPrimitiveType.VARCHAR),

    DECIMALV2("DECIMALV2", 16, TPrimitiveType.DECIMALV2),
    
    HLL("HLL", 16, TPrimitiveType.HLL),
    TIME("TIME", 8, TPrimitiveType.TIME),
    // we use OBJECT type represent BITMAP type in Backend
    BITMAP("BITMAP", 16, TPrimitiveType.OBJECT),
    ARRAY("ARRAY", 24, TPrimitiveType.ARRAY),
    MAP("MAP", 24, TPrimitiveType.MAP),
    STRUCT("MAP", 24, TPrimitiveType.STRUCT),
    STRING("STRING", 16, TPrimitiveType.STRING),
    // Unsupported scalar types.
    BINARY("BINARY", -1, TPrimitiveType.BINARY),
    ALL("ALL", -1, TPrimitiveType.INVALID_TYPE);


    private static final int DATE_INDEX_LEN = 3;
    private static final int DATETIME_INDEX_LEN = 8;
    private static final int VARCHAR_INDEX_LEN = 20;
    private static final int STRING_INDEX_LEN = 20;
    private static final int DECIMAL_INDEX_LEN = 12;

    private static ImmutableSetMultimap<PrimitiveType, PrimitiveType> implicitCastMap;
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
        builder.put(NULL_TYPE, DECIMALV2);
        builder.put(NULL_TYPE, CHAR);
        builder.put(NULL_TYPE, VARCHAR);
        builder.put(NULL_TYPE, STRING);
        builder.put(NULL_TYPE, BITMAP);
        builder.put(NULL_TYPE, TIME);
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
        builder.put(BOOLEAN, DECIMALV2);
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
        builder.put(TINYINT, DECIMALV2);
        builder.put(TINYINT, VARCHAR);
        builder.put(TINYINT, STRING);
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
        builder.put(SMALLINT, DECIMALV2);
        builder.put(SMALLINT, VARCHAR);
        builder.put(SMALLINT, STRING);
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
        builder.put(INT, DECIMALV2);
        builder.put(INT, VARCHAR);
        builder.put(INT, STRING);
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
        builder.put(BIGINT, DECIMALV2);
        builder.put(BIGINT, VARCHAR);
        builder.put(BIGINT, STRING);
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
        builder.put(LARGEINT, DECIMALV2);
        builder.put(LARGEINT, VARCHAR);
        builder.put(LARGEINT, STRING);
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
        builder.put(FLOAT, DECIMALV2);
        builder.put(FLOAT, VARCHAR);
        builder.put(FLOAT, STRING);
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
        builder.put(DOUBLE, DECIMALV2);
        builder.put(DOUBLE, VARCHAR);
        builder.put(DOUBLE, STRING);
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
        builder.put(DATE, DECIMALV2);
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
        builder.put(DATETIME, DECIMALV2);
        builder.put(DATETIME, VARCHAR);
        builder.put(DATETIME, STRING);
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
        builder.put(CHAR, DECIMALV2);
        builder.put(CHAR, VARCHAR);
        builder.put(CHAR, STRING);
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
        builder.put(VARCHAR, DECIMALV2);
        builder.put(VARCHAR, VARCHAR);
        builder.put(VARCHAR, STRING);

        // Varchar
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
        builder.put(STRING, DECIMALV2);
        builder.put(STRING, VARCHAR);
        builder.put(STRING, STRING);

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
        builder.put(DECIMALV2, VARCHAR);
        builder.put(DECIMALV2, STRING);

        // HLL
        builder.put(HLL, HLL);

        // BITMAP
        builder.put(BITMAP, BITMAP);

        //TIME
        builder.put(TIME, TIME);
        builder.put(TIME, DOUBLE);

        implicitCastMap = builder.build();
    }

    private static ArrayList<PrimitiveType> integerTypes;
    private static ArrayList<PrimitiveType> numericTypes;
    private static ArrayList<PrimitiveType> supportedTypes;

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
        supportedTypes.add(STRING);
        supportedTypes.add(HLL);
        supportedTypes.add(CHAR);
        supportedTypes.add(DATE);
        supportedTypes.add(DATETIME);
        supportedTypes.add(TIME);
        supportedTypes.add(DECIMALV2);
        supportedTypes.add(BITMAP);
        supportedTypes.add(ARRAY);
        supportedTypes.add(MAP);
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

    /**
     * Matrix that records "smallest" assignment-compatible type of two types
     * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
     * incompatible). A value of any of the two types could be assigned to a slot
     * of the assignment-compatible type without loss of precision.
     * <p/>
     * We chose not to follow MySQL's type casting behavior as described here:
     * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
     * for the following reasons:
     * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
     * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
     * special cases when dealing with dates and timestamps
     */
    private static PrimitiveType[][] compatibilityMatrix;

    static {
        compatibilityMatrix = new PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];

        // NULL_TYPE is compatible with any type and results in the non-null type.
        compatibilityMatrix[NULL_TYPE.ordinal()][NULL_TYPE.ordinal()] = NULL_TYPE;
        compatibilityMatrix[NULL_TYPE.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
        compatibilityMatrix[NULL_TYPE.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[NULL_TYPE.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[NULL_TYPE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[NULL_TYPE.ordinal()][DATE.ordinal()] = DATE;
        compatibilityMatrix[NULL_TYPE.ordinal()][DATETIME.ordinal()] = DATETIME;
        compatibilityMatrix[NULL_TYPE.ordinal()][CHAR.ordinal()] = CHAR;
        compatibilityMatrix[NULL_TYPE.ordinal()][VARCHAR.ordinal()] = VARCHAR;
        compatibilityMatrix[NULL_TYPE.ordinal()][STRING.ordinal()] = STRING;
        compatibilityMatrix[NULL_TYPE.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[NULL_TYPE.ordinal()][TIME.ordinal()] = TIME;
        compatibilityMatrix[NULL_TYPE.ordinal()][BITMAP.ordinal()] = BITMAP;

        compatibilityMatrix[BOOLEAN.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
        compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[BOOLEAN.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[BOOLEAN.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[TINYINT.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[TINYINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[TINYINT.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[SMALLINT.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[SMALLINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[SMALLINT.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[INT.ordinal()][INT.ordinal()] = INT;
        compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[INT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[INT.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[BIGINT.ordinal()][BIGINT.ordinal()] = BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[BIGINT.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[LARGEINT.ordinal()][LARGEINT.ordinal()] = LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][FLOAT.ordinal()] = DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[LARGEINT.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[FLOAT.ordinal()][FLOAT.ordinal()] = FLOAT;
        compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[FLOAT.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[DOUBLE.ordinal()][DOUBLE.ordinal()] = DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DOUBLE.ordinal()][TIME.ordinal()] = TIME;

        compatibilityMatrix[DATE.ordinal()][DATE.ordinal()] = DATE;
        compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = DATETIME;
        compatibilityMatrix[DATE.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[DATETIME.ordinal()][DATETIME.ordinal()] = DATETIME;
        compatibilityMatrix[DATETIME.ordinal()][CHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][VARCHAR.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[CHAR.ordinal()][CHAR.ordinal()] = CHAR;
        compatibilityMatrix[CHAR.ordinal()][VARCHAR.ordinal()] = VARCHAR;
        compatibilityMatrix[CHAR.ordinal()][STRING.ordinal()] = STRING;
        compatibilityMatrix[CHAR.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[VARCHAR.ordinal()][VARCHAR.ordinal()] = VARCHAR;
        compatibilityMatrix[VARCHAR.ordinal()][STRING.ordinal()] = STRING;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[STRING.ordinal()][STRING.ordinal()] = STRING;
        compatibilityMatrix[STRING.ordinal()][DECIMALV2.ordinal()] = INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[DECIMALV2.ordinal()][DECIMALV2.ordinal()] = DECIMALV2;
        compatibilityMatrix[DECIMALV2.ordinal()][TIME.ordinal()] = INVALID_TYPE;
        
        compatibilityMatrix[HLL.ordinal()][HLL.ordinal()] = HLL;
        compatibilityMatrix[HLL.ordinal()][TIME.ordinal()] = INVALID_TYPE;

        compatibilityMatrix[BITMAP.ordinal()][BITMAP.ordinal()] = BITMAP;

        compatibilityMatrix[TIME.ordinal()][TIME.ordinal()] = TIME;
    }

    static {
        // NULL_TYPE is compatible with any type and results in the non-null type.
        compatibilityMatrix[NULL_TYPE.ordinal()][NULL_TYPE.ordinal()] = NULL_TYPE;
        compatibilityMatrix[NULL_TYPE.ordinal()][BOOLEAN.ordinal()] = BOOLEAN;
        compatibilityMatrix[NULL_TYPE.ordinal()][TINYINT.ordinal()] = TINYINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][SMALLINT.ordinal()] = SMALLINT;
        compatibilityMatrix[NULL_TYPE.ordinal()][INT.ordinal()] = INT;
    }

    private final String description;
    private final int slotSize;  // size of tuple slot for this type
    private final TPrimitiveType thriftType;
    private boolean isTimeType = false;

    private PrimitiveType(String description, int slotSize, TPrimitiveType thriftType) {
        this.description = description;
        this.slotSize = slotSize;
        this.thriftType = thriftType;
    }

    public void setTimeType() {
        isTimeType = true;
    }

    /**
     * @return
     */
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
            case BINARY:
                return BINARY;
            case DECIMALV2:
                return DECIMALV2;
            case TIME:
                return TIME;
            case VARCHAR:
                return VARCHAR;
            case STRING:
                return STRING;
            case CHAR:
                return CHAR;
            case HLL:
                return HLL;
            case OBJECT:
                return BITMAP;
            case ARRAY:
                return ARRAY;
            case MAP:
                return MAP;
            case STRUCT:
                return STRUCT;
            case ALL:
                return ALL;
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

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t
     * without loss of precision. Returns INVALID_TYPE if there is no such type
     * or if any of t1 and t2 is INVALID_TYPE.
     */
    public static PrimitiveType getAssignmentCompatibleType(PrimitiveType t1, PrimitiveType t2) {
        if (!t1.isValid() || !t2.isValid()) {
            return INVALID_TYPE;
        }

        PrimitiveType smallerType = (t1.ordinal() < t2.ordinal() ? t1 : t2);
        PrimitiveType largerType = (t1.ordinal() > t2.ordinal() ? t1 : t2);
        PrimitiveType result = compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        Preconditions.checkNotNull(result);
        return result;
    }

    /**
     * Returns if it is compatible to implicitly cast from t1 to t2 (casting from
     * t1 to t2 results in no loss of precision.
     */
    public static boolean isImplicitlyCastable(PrimitiveType t1, PrimitiveType t2) {
        return getAssignmentCompatibleType(t1, t2) == t2;
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

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2Type();
    }

    public boolean isValid() {
        return this != INVALID_TYPE;
    }

    public boolean isNull() {
        return this == NULL_TYPE;
    }

    public boolean isDateType() {
        return (this == DATE || this == DATETIME);
    }

    public boolean isArrayType(){
        return this == ARRAY;
    }

    public boolean isStringType() {
        return (this == VARCHAR || this == CHAR || this == HLL || this == STRING);
    }

    public boolean isCharFamily() {
        return (this == VARCHAR || this == CHAR || this == STRING);
    }

    public boolean isIntegerType() {
        return (this == TINYINT || this == SMALLINT
                || this == INT || this == BIGINT);
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
            case FLOAT:
                return MysqlColType.MYSQL_TYPE_FLOAT;
            case DOUBLE:
                return MysqlColType.MYSQL_TYPE_DOUBLE;
            case TIME:
                return MysqlColType.MYSQL_TYPE_TIME;
            case DATE:
                return MysqlColType.MYSQL_TYPE_DATE;
            case DATETIME: {
                if (isTimeType) {
                    return MysqlColType.MYSQL_TYPE_TIME;
                }  else {
                    return MysqlColType.MYSQL_TYPE_DATETIME;
                }
            }
            case DECIMALV2:
                return MysqlColType.MYSQL_TYPE_NEWDECIMAL;
            case STRING:
                return MysqlColType.MYSQL_TYPE_BLOB;
            default:
                return MysqlColType.MYSQL_TYPE_STRING;
        }
    }

    public int getOlapColumnIndexSize() {
        switch (this) {
            case DATE:
                return DATE_INDEX_LEN;
            case DATETIME:
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
            default:
                return this.getSlotSize();
        }
    }
}
