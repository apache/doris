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
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

public enum PrimitiveType {
    // DO NOT CHANGE desc and to string of these types, they are used in persist
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
    VARBINARY("VARBINARY", 16, TPrimitiveType.VARBINARY, true),

    DECIMALV2("DECIMALV2", 16, TPrimitiveType.DECIMALV2, true),
    DECIMAL32("DECIMAL32", 4, TPrimitiveType.DECIMAL32, true),
    DECIMAL64("DECIMAL64", 8, TPrimitiveType.DECIMAL64, true),
    DECIMAL128("DECIMAL128", 16, TPrimitiveType.DECIMAL128I, true),
    DECIMAL256("DECIMAL256", 32, TPrimitiveType.DECIMAL256, false),
    // these following types are stored as object binary in BE.
    HLL("HLL", 16, TPrimitiveType.HLL, true),
    BITMAP("BITMAP", 16, TPrimitiveType.BITMAP, true),
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
    BINARY("BINARY", -1, TPrimitiveType.BINARY, false);


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

    private static final ArrayList<PrimitiveType> integerTypes;
    private static final ArrayList<PrimitiveType> supportedTypes;

    static {
        integerTypes = Lists.newArrayList();
        integerTypes.add(TINYINT);
        integerTypes.add(SMALLINT);
        integerTypes.add(INT);
        integerTypes.add(BIGINT);
        integerTypes.add(LARGEINT);

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

    public static ArrayList<PrimitiveType> getSupportedTypes() {
        return supportedTypes;
    }

    @SerializedName("d")
    private final String description;
    @SerializedName("s")
    private final int slotSize;  // size of tuple slot for this type
    @SerializedName("t")
    private final TPrimitiveType thriftType;
    @SerializedName("a")
    private final boolean availableInDdl;
    @SerializedName("it")
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
            case BITMAP:
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
            case VARIANT:
                return VARIANT;
            case VARBINARY:
                return VARBINARY;
            case ALL:
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

    public boolean isQuantileStateType() {
        return this == QUANTILE_STATE;
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

    public boolean isVarbinaryType() {
        return (this == VARBINARY);
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
            case VARBINARY:
                return MysqlColType.MYSQL_TYPE_VARSTRING;
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
