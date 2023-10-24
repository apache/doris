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
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Describes a scalar type. For most types this class just wraps a PrimitiveType enum,
 * but for types like CHAR and DECIMAL, this class contain additional information.
 *
 * Scalar types have a few ways they can be compared to other scalar types. They can be:
 *   1. completely identical,
 *   2. implicitly castable (convertible without loss of precision)
 *   3. subtype. For example, in the case of decimal, a type can be decimal(*, *)
 *   indicating that any decimal type is a subtype of the decimal type.
 */
public class ScalarType extends Type {
    // We use a fixed-length decimal type to represent a date time.
    public static final int DATETIME_PRECISION = 18;

    // SQL allows the engine to pick the default precision. We pick the largest
    // precision that is supported by the smallest decimal type in the BE (4 bytes).
    public static final int DEFAULT_PRECISION = 9;
    public static final int DEFAULT_SCALE = 0; // SQL standard

    // Longest supported VARCHAR and CHAR, chosen to match Hive.
    public static final int MAX_VARCHAR_LENGTH = 65533;

    public static final int MAX_CHAR_LENGTH = 255;

    // HLL DEFAULT LENGTH  2^14(registers) + 1(type)
    public static final int MAX_HLL_LENGTH = 16385;

    // Longest CHAR that we in line in the tuple.
    // Keep consistent with backend ColumnType::CHAR_INLINE_LENGTH
    public static final int CHAR_INLINE_LENGTH = 128;

    // Max length of String types, in be storage layer store string length
    // using int32, the max length is 2GB, the first 4 bytes store the length
    // so the max available length is 2GB - 4
    public static final int MAX_STRING_LENGTH = 0x7fffffff - 4;

    public static final int MAX_JSONB_LENGTH = 0x7fffffff - 4;

    // Hive, mysql, sql server standard.
    public static final int MAX_PRECISION = 38;
    public static final int MAX_DECIMALV2_PRECISION = 27;
    public static final int MAX_DECIMALV2_SCALE = 9;
    public static final int MAX_DECIMAL32_PRECISION = 9;
    public static final int MAX_DECIMAL64_PRECISION = 18;
    public static final int MAX_DECIMAL128_PRECISION = 38;
    public static final int DEFAULT_MIN_AVG_DECIMAL128_SCALE = 4;
    public static final int MAX_DATETIMEV2_SCALE = 6;

    private long byteSize = -1;


    /**
     * Set byte size of expression
     */
    public void setByteSize(long byteSize) {
        this.byteSize = byteSize;
    }

    public long getByteSize() {
        return byteSize;
    }

    private static final Logger LOG = LogManager.getLogger(ScalarType.class);
    @SerializedName(value = "type")
    private final PrimitiveType type;

    // Only used for type CHAR.
    @SerializedName(value = "len")
    private int len = -1;

    // Only used if type is DECIMAL. -1 (for both) is used to represent a
    // decimal with any precision and scale.
    // It is invalid to have one by -1 and not the other.
    // TODO: we could use that to store DECIMAL(8,*), indicating a decimal
    // with 8 digits of precision and any valid ([0-8]) scale.
    @SerializedName(value = "precision")
    private int precision;
    @SerializedName(value = "scale")
    private int scale;

    // Only used for alias function decimal
    @SerializedName(value = "precisionStr")
    private String precisionStr;
    // Only used for alias function decimal
    @SerializedName(value = "scaleStr")
    private String scaleStr;
    // Only used for alias function char/varchar
    @SerializedName(value = "lenStr")
    private String lenStr;

    public ScalarType(PrimitiveType type) {
        this.type = type;
    }

    public static ScalarType createType(PrimitiveType type, int len, int precision, int scale) {
        switch (type) {
            case CHAR:
                return createCharType(len);
            case VARCHAR:
                return createVarcharType(len);
            case STRING:
                return createStringType();
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return createDecimalV3Type(precision, scale);
            case DECIMALV2:
                return createDecimalType(precision, scale);
            case DATETIMEV2:
                return createDatetimeV2Type(scale);
            case TIMEV2:
                return createTimeV2Type(scale);
            default:
                return createType(type);
        }
    }

    public static ScalarType createType(PrimitiveType type) {
        switch (type) {
            case INVALID_TYPE:
                return INVALID;
            case NULL_TYPE:
                return NULL;
            case BOOLEAN:
                return BOOLEAN;
            case SMALLINT:
                return SMALLINT;
            case TINYINT:
                return TINYINT;
            case INT:
                return INT;
            case BIGINT:
                return BIGINT;
            case FLOAT:
                return FLOAT;
            case DOUBLE:
                return DOUBLE;
            case CHAR:
                return CHAR;
            case VARCHAR:
                return createVarcharType();
            case BINARY:
                return createStringType();
            case JSONB:
                return createJsonbType();
            case VARIANT:
                return createVariantType();
            case STRING:
                return createStringType();
            case HLL:
                return createHllType();
            case BITMAP:
                return BITMAP;
            case QUANTILE_STATE:
                return QUANTILE_STATE;
            case AGG_STATE:
                return AGG_STATE;
            case LAMBDA_FUNCTION:
                return LAMBDA_FUNCTION;
            case DATE:
                return DATE;
            case DATETIME:
                return DATETIME;
            case DATEV2:
                return DATEV2;
            case DATETIMEV2:
                return DEFAULT_DATETIMEV2;
            case TIMEV2:
                return TIMEV2;
            case TIME:
                return TIME;
            case DECIMAL32:
                return DEFAULT_DECIMAL32;
            case DECIMAL64:
                return DEFAULT_DECIMAL64;
            case DECIMAL128:
                return DEFAULT_DECIMAL128;
            case DECIMALV2:
                return DEFAULT_DECIMALV2;
            case LARGEINT:
                return LARGEINT;
            case ALL:
                return ALL;
            default:
                LOG.warn("type={}", type);
                Preconditions.checkState(false);
                return NULL;
        }
    }

    public static ScalarType createType(String type) {
        switch (type) {
            case "INVALID_TYPE":
                return INVALID;
            case "NULL_TYPE":
                return NULL;
            case "BOOLEAN":
                return BOOLEAN;
            case "SMALLINT":
                return SMALLINT;
            case "TINYINT":
                return TINYINT;
            case "INT":
                return INT;
            case "BIGINT":
                return BIGINT;
            case "FLOAT":
                return FLOAT;
            case "DOUBLE":
                return DOUBLE;
            case "CHAR":
                return CHAR;
            case "VARCHAR":
                return createVarcharType();
            case "JSON":
                return createJsonbType();
            case "VARIANT":
                return createVariantType();
            case "STRING":
            case "TEXT":
                return createStringType();
            case "HLL":
                return createHllType();
            case "BITMAP":
                return BITMAP;
            case "QUANTILE_STATE":
                return QUANTILE_STATE;
            case "AGG_STATE":
                return AGG_STATE;
            case "LAMBDA_FUNCTION":
                return LAMBDA_FUNCTION;
            case "DATE":
                return DATE;
            case "DATETIME":
                return DATETIME;
            case "DATEV2":
                return DATEV2;
            case "DATETIMEV2":
                return DATETIMEV2;
            case "TIME":
                return TIME;
            case "DECIMAL":
            case "DECIMALV2":
                return createDecimalType();
            case "DECIMALV3":
                return createDecimalV3Type();
            case "LARGEINT":
                return LARGEINT;
            default:
                LOG.warn("type={}", type);
                Preconditions.checkState(false);
                return NULL;
        }
    }

    public static ScalarType createCharType(int len) {
        ScalarType type = new ScalarType(PrimitiveType.CHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createCharType(String lenStr) {
        ScalarType type = new ScalarType(PrimitiveType.CHAR);
        type.lenStr = lenStr;
        return type;
    }

    public static ScalarType createChar(int len) {
        ScalarType type = new ScalarType(PrimitiveType.CHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createDecimalType() {
        if (Config.enable_decimal_conversion) {
            return DEFAULT_DECIMALV3;
        } else {
            return DEFAULT_DECIMALV2;
        }
    }

    public static ScalarType createDecimalType(int precision) {
        return createDecimalType(precision, DEFAULT_SCALE);
    }

    public static ScalarType createDecimalType(int precision, int scale) {
        ScalarType type = new ScalarType(getSuitableDecimalType(precision, true));
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static ScalarType createDecimalType(PrimitiveType primitiveType, int precision, int scale) {
        ScalarType type = new ScalarType(primitiveType);
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static ScalarType createDecimalType(String precisionStr) {
        int precision = Integer.parseInt(precisionStr);
        ScalarType type = new ScalarType(getSuitableDecimalType(precision, true));
        type.precisionStr = precisionStr;
        type.scaleStr = null;
        return type;
    }

    public static ScalarType createDecimalType(String precisionStr, String scaleStr) {
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precisionStr = precisionStr;
        type.scaleStr = scaleStr;
        return type;
    }

    public static ScalarType createDecimalV3Type() {
        return DEFAULT_DECIMALV3;
    }

    public static ScalarType createDecimalV3Type(int precision) {
        return createDecimalV3Type(precision, DEFAULT_SCALE);
    }

    public static ScalarType createDecimalV3Type(int precision, int scale) {
        ScalarType type = new ScalarType(getSuitableDecimalType(precision, false));
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static ScalarType createDecimalV3Type(String precisionStr) {
        int precision = Integer.parseInt(precisionStr);
        ScalarType type = new ScalarType(getSuitableDecimalType(precision, false));
        type.precisionStr = precisionStr;
        type.scaleStr = null;
        return type;
    }

    public static ScalarType createDecimalV3Type(String precisionStr, String scaleStr) {
        ScalarType type = new ScalarType(getSuitableDecimalType(Integer.parseInt(precisionStr), false));
        type.precisionStr = precisionStr;
        type.scaleStr = scaleStr;
        return type;
    }

    public static PrimitiveType getSuitableDecimalType(int precision, boolean decimalV2) {
        if (decimalV2 && !Config.enable_decimal_conversion) {
            return PrimitiveType.DECIMALV2;
        }
        if (precision <= MAX_DECIMAL32_PRECISION) {
            return PrimitiveType.DECIMAL32;
        } else if (precision <= MAX_DECIMAL64_PRECISION) {
            return PrimitiveType.DECIMAL64;
        } else {
            return PrimitiveType.DECIMAL128;
        }
    }

    public static ScalarType createDecimalTypeInternal(int precision, int scale, boolean decimalV2) {
        ScalarType type = new ScalarType(getSuitableDecimalType(precision, decimalV2));
        type.precision = Math.min(precision, MAX_PRECISION);
        type.scale = Math.min(type.precision, scale);
        return type;
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createDatetimeV2Type(int scale) {
        ScalarType type = new ScalarType(PrimitiveType.DATETIMEV2);
        type.precision = DATETIME_PRECISION;
        type.scale = scale;
        return type;
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createTimeV2Type(int scale) {
        ScalarType type = new ScalarType(PrimitiveType.TIMEV2);
        type.precision = DATETIME_PRECISION;
        type.scale = scale;
        return type;
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createDatetimeType() {
        if (!Config.enable_date_conversion) {
            return new ScalarType(PrimitiveType.DATETIME);
        }
        ScalarType type = new ScalarType(PrimitiveType.DATETIMEV2);
        type.precision = DATETIME_PRECISION;
        type.scale = 0;
        return type;
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createDateType() {
        if (Config.enable_date_conversion) {
            return new ScalarType(PrimitiveType.DATEV2);
        } else {
            return new ScalarType(PrimitiveType.DATE);
        }
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createTimeType() {
        if (!Config.enable_date_conversion) {
            return new ScalarType(PrimitiveType.TIME);
        }
        ScalarType type = new ScalarType(PrimitiveType.TIMEV2);
        type.precision = DATETIME_PRECISION;
        type.scale = 0;
        return type;
    }

    public static ScalarType createDateV2Type() {
        return new ScalarType(PrimitiveType.DATEV2);
    }

    public static Type getDefaultDateType(Type type) {
        switch (type.getPrimitiveType()) {
            case DATE:
                if (Config.enable_date_conversion) {
                    return Type.DATEV2;
                } else {
                    return Type.DATE;
                }
            case DATETIME:
                if (Config.enable_date_conversion) {
                    return Type.DATETIMEV2;
                } else {
                    return Type.DATETIME;
                }
            case DATEV2:
            case DATETIMEV2:
            default:
                return type;
        }
    }

    /**
     * create a wider decimal type.
     */
    public static ScalarType createWiderDecimalV3Type(int precision, int scale) {
        ScalarType type = new ScalarType(getSuitableDecimalType(precision, false));
        if (precision <= MAX_DECIMAL32_PRECISION) {
            type.precision = MAX_DECIMAL32_PRECISION;
        } else if (precision <= MAX_DECIMAL64_PRECISION) {
            type.precision = MAX_DECIMAL64_PRECISION;
        } else {
            type.precision = MAX_DECIMAL128_PRECISION;
        }
        type.scale = scale;
        return type;
    }

    public static ScalarType createVarcharType(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createVarcharType(String lenStr) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.lenStr = lenStr;
        return type;
    }

    public static ScalarType createStringType() {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.STRING);
        type.len = MAX_STRING_LENGTH;
        return type;
    }

    public static ScalarType createJsonbType() {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.JSONB);
        type.len = MAX_JSONB_LENGTH;
        return type;
    }

    public static ScalarType createVariantType() {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARIANT);
        type.len = MAX_STRING_LENGTH;
        return type;
    }

    public static ScalarType createVarchar(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createVarcharType() {
        // Because ScalarType is not an immutable class, it will call setLength() sometimes.
        // So currently don't use DEFAULT_VARCHAR, will improve it in the future.
        return new ScalarType(PrimitiveType.VARCHAR);
    }

    public static ScalarType createHllType() {
        ScalarType type = new ScalarType(PrimitiveType.HLL);
        type.len = MAX_HLL_LENGTH;
        return type;
    }

    @Override
    public String toString() {
        if (type == PrimitiveType.CHAR) {
            if (isWildcardChar()) {
                return "CHARACTER";
            }
            return "CHAR(" + len + ")";
        } else  if (type == PrimitiveType.DECIMALV2) {
            if (isWildcardDecimal()) {
                return "DECIMAL(*, *)";
            }
            return "DECIMAL(" + precision + ", " + scale + ")";
        } else  if (type.isDecimalV3Type()) {
            if (isWildcardDecimal()) {
                return "DECIMALV3(*, *)";
            }
            return "DECIMALV3(" + precision + ", " + scale + ")";
        } else  if (type == PrimitiveType.DATETIMEV2) {
            return "DATETIMEV2(" + scale + ")";
        } else  if (type == PrimitiveType.TIMEV2) {
            return "TIMEV2(" + scale + ")";
        } else if (type == PrimitiveType.VARCHAR) {
            if (isWildcardVarchar()) {
                return "VARCHAR(*)";
            }
            return "VARCHAR(" + len + ")";
        } else if (type == PrimitiveType.STRING) {
            return "TEXT";
        } else if (type == PrimitiveType.JSONB) {
            return "JSON";
        } else if (type == PrimitiveType.VARIANT) {
            return "VARIANT";
        }
        return type.toString();
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case CHAR:
                if (isWildcardVarchar()) {
                    stringBuilder.append("character");
                } else if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("char").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("char").append("(`").append(lenStr).append("`)");
                }
                break;
            case VARCHAR:
                if (isWildcardVarchar()) {
                    stringBuilder.append("varchar(*)");
                } else if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("varchar").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("varchar").append("(`").append(lenStr).append("`)");
                }
                break;
            case DECIMALV2:
                if (Strings.isNullOrEmpty(precisionStr)) {
                    stringBuilder.append("decimal").append("(").append(precision)
                            .append(", ").append(scale).append(")");
                } else if (!Strings.isNullOrEmpty(precisionStr) && !Strings.isNullOrEmpty(scaleStr)) {
                    stringBuilder.append("decimal").append("(`").append(precisionStr)
                            .append("`, `").append(scaleStr).append("`)");
                } else {
                    stringBuilder.append("decimal").append("(`").append(precisionStr).append("`)");
                }
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                String typeName = "decimalv3";
                if (Strings.isNullOrEmpty(precisionStr)) {
                    stringBuilder.append(typeName).append("(").append(precision)
                        .append(", ").append(scale).append(")");
                } else if (!Strings.isNullOrEmpty(precisionStr) && !Strings.isNullOrEmpty(scaleStr)) {
                    stringBuilder.append(typeName).append("(`").append(precisionStr)
                        .append("`, `").append(scaleStr).append("`)");
                } else {
                    stringBuilder.append(typeName).append("(`").append(precisionStr).append("`)");
                }
                break;
            case DATETIMEV2:
                stringBuilder.append("datetimev2").append("(").append(scale).append(")");
                break;
            case TIME:
                stringBuilder.append("time");
                break;
            case TIMEV2:
                stringBuilder.append("time").append("(").append(scale).append(")");
                break;
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint(4)";
            case SMALLINT:
                return "smallint(6)";
            case INT:
                return "int(11)";
            case BIGINT:
                return "bigint(20)";
            case LARGEINT:
                return "largeint(40)";
            case FLOAT:
            case DOUBLE:
            case DATE:
            case DATETIME:
            case DATEV2:
            case HLL:
            case BITMAP:
            case VARIANT:
            case QUANTILE_STATE:
            case LAMBDA_FUNCTION:
            case ARRAY:
            case NULL_TYPE:
                stringBuilder.append(type.toString().toLowerCase());
                break;
            case STRING:
                stringBuilder.append("text");
                break;
            case JSONB:
                stringBuilder.append("json");
                break;
            case AGG_STATE:
                stringBuilder.append("agg_state(unknown)");
                break;
            default:
                stringBuilder.append("unknown type: " + type.toString());
                break;
        }
        return stringBuilder.toString();
    }

    @Override
    protected String prettyPrint(int lpad) {
        return Strings.repeat(" ", lpad) + toSql();
    }

    @Override
    public void toThrift(TTypeDesc container) {
        TTypeNode node = new TTypeNode();
        container.types.add(node);
        node.setType(TTypeNodeType.SCALAR);
        TScalarType scalarType = new TScalarType();
        scalarType.setType(type.toThrift());
        container.setByteSize(byteSize);

        switch (type) {
            case VARCHAR:
            case CHAR:
            case HLL:
            case STRING:
            case JSONB:
            case VARIANT: {
                scalarType.setLen(getLength());
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DATETIMEV2: {
                Preconditions.checkArgument(precision >= scale,
                        String.format("given precision %d is out of scale bound %d", precision, scale));
                scalarType.setScale(scale);
                scalarType.setPrecision(precision);
                break;
            }
            case TIMEV2: {
                Preconditions.checkArgument(precision >= scale,
                        String.format("given precision %d is out of scale bound %d", precision, scale));
                scalarType.setScale(scale);
                scalarType.setPrecision(precision);
                break;
            }
            default:
                break;
        }
        node.setScalarType(scalarType);
    }

    public int decimalPrecision() {
        Preconditions.checkState(type == PrimitiveType.DECIMALV2 || type == PrimitiveType.DATETIMEV2
                || type == PrimitiveType.TIMEV2 || type == PrimitiveType.DECIMAL32
                || type == PrimitiveType.DECIMAL64 || type == PrimitiveType.DECIMAL128);
        return precision;
    }

    public int decimalScale() {
        Preconditions.checkState(type == PrimitiveType.DECIMALV2 || type == PrimitiveType.DATETIMEV2
                || type == PrimitiveType.TIMEV2 || type == PrimitiveType.DECIMAL32
                || type == PrimitiveType.DECIMAL64 || type == PrimitiveType.DECIMAL128);
        return scale;
    }

    @Override
    public PrimitiveType getPrimitiveType() {
        return type;
    }

    public int ordinal() {
        return type.ordinal();
    }

    @Override
    public int getLength() {
        if (len == -1) {
            if (type == PrimitiveType.CHAR) {
                return MAX_CHAR_LENGTH;
            } else if (type == PrimitiveType.STRING) {
                return MAX_STRING_LENGTH;
            } else {
                return MAX_VARCHAR_LENGTH;
            }
        }
        return len;
    }

    @Override
    public int getRawLength() {
        return len;
    }

    public void setLength(int len) {
        this.len = len;
    }

    public void setMaxLength() {
        this.len = -1;
    }

    public boolean isLengthSet() {
        return getPrimitiveType() == PrimitiveType.HLL || len > 0 || !Strings.isNullOrEmpty(lenStr);
    }

    // add scalar infix to override with getPrecision
    public int getScalarScale() {
        return scale;
    }

    public int getScalarPrecision() {
        return precision;
    }

    public String getScalarPrecisionStr() {
        return precisionStr;
    }

    public String getScalarScaleStr() {
        return scaleStr;
    }

    public String getLenStr() {
        return lenStr;
    }

    @Override
    public boolean isWildcardDecimal() {
        return (type.isDecimalV2Type() || type.isDecimalV3Type())
                && precision == -1 && scale == -1;
    }

    @Override
    public boolean isWildcardVarchar() {
        return (type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) && (len == -1 || len == MAX_VARCHAR_LENGTH);
    }

    @Override
    public boolean isWildcardChar() {
        return type == PrimitiveType.CHAR && (len == -1 || len == MAX_CHAR_LENGTH);
    }

    @Override
    public boolean isFixedLengthType() {
        return type == PrimitiveType.BOOLEAN || type == PrimitiveType.TINYINT
                || type == PrimitiveType.SMALLINT || type == PrimitiveType.INT
                || type == PrimitiveType.BIGINT || type == PrimitiveType.FLOAT
                || type == PrimitiveType.DOUBLE || type == PrimitiveType.DATE
                || type == PrimitiveType.DATETIME || type == PrimitiveType.DECIMALV2 || type.isDecimalV3Type()
                || type == PrimitiveType.CHAR || type == PrimitiveType.DATEV2 || type == PrimitiveType.DATETIMEV2
                || type == PrimitiveType.TIMEV2;
    }

    @Override
    public boolean isSupported() {
        switch (type) {
            case BINARY:
            case UNSUPPORTED:
                return false;
            default:
                return true;
        }
    }

    @Override
    public boolean supportsTablePartitioning() {
        if (!isSupported() || isComplexType()) {
            return false;
        }
        return true;
    }

    @Override
    public int getSlotSize() {
        return type.getSlotSize();
    }

    /**
     * Returns true if this object is of type t.
     * Handles wildcard types. That is, if t is the wildcard type variant
     * of 'this', returns true.
     */
    @Override
    public boolean matchesType(Type t) {
        if (equals(t)) {
            return true;
        }
        if (t.isAnyType()) {
            return t.matchesType(this);
        }
        if (!t.isScalarType()) {
            return false;
        }
        ScalarType scalarType = (ScalarType) t;
        if (type == PrimitiveType.VARCHAR && scalarType.isWildcardVarchar()) {
            return true;
        }
        if (type == PrimitiveType.CHAR && scalarType.isWildcardChar()) {
            return true;
        }
        if (type.isStringType() && scalarType.isStringType()) {
            return true;
        }
        if (isDecimalV2() && scalarType.isWildcardDecimal() && scalarType.isDecimalV2()) {
            Preconditions.checkState(!isWildcardDecimal());
            return true;
        }
        if (isDecimalV3() && scalarType.isWildcardDecimal() && scalarType.isDecimalV3()) {
            Preconditions.checkState(!isWildcardDecimal());
            return true;
        }
        if (isDecimalV2() && scalarType.isDecimalV2()) {
            return true;
        }
        if (isDecimalV3() && scalarType.isDecimalV3()) {
            return precision == scalarType.precision && scale == scalarType.scale;
        }
        if (isDatetimeV2() && scalarType.isDatetimeV2()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScalarType)) {
            return false;
        }
        ScalarType other = (ScalarType) o;
        if ((this.isDatetimeV2() && other.isDatetimeV2())) {
            return this.decimalScale() == other.decimalScale();
        }
        if (this.isTimeV2() && other.isTimeV2()) {
            return this.decimalScale() == other.decimalScale();
        }
        if (type.isDecimalV3Type() && other.isDecimalV3()) {
            return precision == other.precision && scale == other.scale;
        }
        if (type != other.type) {
            return false;
        }
        if (type == PrimitiveType.CHAR) {
            return len == other.len;
        }
        if (type == PrimitiveType.VARCHAR) {
            return len == other.len;
        }
        if (type.isDecimalV2Type() || type == PrimitiveType.DATETIMEV2 || type == PrimitiveType.TIMEV2) {
            return precision == other.precision && scale == other.scale;
        }
        return true;
    }

    public Type getMaxResolutionType() {
        if (isIntegerType()) {
            return ScalarType.BIGINT;
            // Timestamps get summed as DOUBLE for AVG.
        } else if (isFloatingPointType()) {
            return ScalarType.DOUBLE;
        } else if (isNull()) {
            return ScalarType.NULL;
        } else if (isDecimalV2()) {
            return createDecimalTypeInternal(MAX_PRECISION, scale, true);
        } else if (getPrimitiveType() == PrimitiveType.DECIMAL32) {
            return createDecimalTypeInternal(MAX_DECIMAL32_PRECISION, scale, false);
        } else if (getPrimitiveType() == PrimitiveType.DECIMAL64) {
            return createDecimalTypeInternal(MAX_DECIMAL64_PRECISION, scale, false);
        } else if (getPrimitiveType() == PrimitiveType.DECIMAL128) {
            return createDecimalTypeInternal(MAX_DECIMAL128_PRECISION, scale, false);
        } else if (isLargeIntType()) {
            return ScalarType.LARGEINT;
        } else if (isDatetimeV2()) {
            return createDatetimeV2Type(6);
        } else if (isTimeV2()) {
            return createTimeV2Type(6);
        } else {
            return ScalarType.INVALID;
        }
    }

    public ScalarType getNextResolutionType() {
        Preconditions.checkState(isNumericType() || isNull());
        if (type == PrimitiveType.DOUBLE || type == PrimitiveType.BIGINT || isNull()) {
            return this;
        } else if (type == PrimitiveType.DECIMALV2) {
            return createDecimalTypeInternal(MAX_PRECISION, scale, true);
        } else if (type == PrimitiveType.DECIMAL32) {
            return createDecimalTypeInternal(MAX_DECIMAL64_PRECISION, scale, false);
        } else if (type == PrimitiveType.DECIMAL64) {
            return createDecimalTypeInternal(MAX_DECIMAL128_PRECISION, scale, false);
        } else if (type == PrimitiveType.DECIMAL128) {
            return createDecimalTypeInternal(MAX_DECIMAL128_PRECISION, scale, false);
        } else if (type == PrimitiveType.DATETIMEV2) {
            return createDatetimeV2Type(6);
        } else if (type == PrimitiveType.TIMEV2) {
            return createTimeV2Type(6);
        }
        return createType(PrimitiveType.values()[type.ordinal() + 1]);
    }

    /**
     * Returns the smallest decimal type that can safely store this type. Returns
     * INVALID if this type cannot be stored as a decimal.
     */
    public ScalarType getMinResolutionDecimal() {
        switch (type) {
            case NULL_TYPE:
                return Type.NULL;
            case DECIMALV2:
                return this;
            case TINYINT:
                return createDecimalType(3);
            case SMALLINT:
                return createDecimalType(5);
            case INT:
                return createDecimalType(10);
            case BIGINT:
                return createDecimalType(19);
            case FLOAT:
                return createDecimalTypeInternal(MAX_PRECISION, 9, false);
            case DOUBLE:
                return createDecimalTypeInternal(MAX_PRECISION, 17, false);
            default:
                return ScalarType.INVALID;
        }
    }

    /**
     * Returns true if this decimal type is a supertype of the other decimal type.
     * e.g. (10,3) is a supertype of (3,3) but (5,4) is not a supertype of (3,0).
     * To be a super type of another decimal, the number of digits before and after
     * the decimal point must be greater or equal.
     */
    public boolean isSupertypeOf(ScalarType o) {
        Preconditions.checkState(isDecimalV2() || isDecimalV3());
        Preconditions.checkState(o.isDecimalV2() || o.isDecimalV3());
        if (isWildcardDecimal()) {
            return true;
        }
        if (o.isWildcardDecimal()) {
            return false;
        }
        return scale >= o.scale && precision - scale >= o.precision - o.scale;
    }

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t.
     * If strict, only return types when there will be no loss of precision.
     * Returns INVALID_TYPE if there is no such type or if any of t1 and t2
     * is INVALID_TYPE.
     */
    public static ScalarType getAssignmentCompatibleType(
            ScalarType t1, ScalarType t2, boolean strict) {
        if (!t1.isValid() || !t2.isValid()) {
            return INVALID;
        }
        if (t1.equals(t2)) {
            return t1;
        }
        if (t1.isNull()) {
            return t2;
        }
        if (t2.isNull()) {
            return t1;
        }

        boolean t1IsHLL = t1.type == PrimitiveType.HLL;
        boolean t2IsHLL = t2.type == PrimitiveType.HLL;
        if (t1IsHLL || t2IsHLL) {
            if (t1IsHLL && t2IsHLL) {
                return createHllType();
            }
            return INVALID;
        }

        boolean t1IsBitMap = t1.type == PrimitiveType.BITMAP;
        boolean t2IsBitMap = t2.type == PrimitiveType.BITMAP;
        if (t1IsBitMap || t2IsBitMap) {
            if (t1IsBitMap && t2IsBitMap) {
                return BITMAP;
            }
            return INVALID;
        }

        // for cast all type
        if (t1.type == PrimitiveType.ALL || t2.type == PrimitiveType.ALL) {
            return Type.ALL;
        }

        if (t1.isStringType() || t2.isStringType()) {
            if (t1.type == PrimitiveType.STRING || t2.type == PrimitiveType.STRING) {
                return createStringType();
            }
            return createVarcharType(Math.max(t1.len, t2.len));
        }

        if (((t1.isDecimalV3() || t1.isDecimalV2()) && (t2.isDateV2() || t2.isDate()))
                || ((t2.isDecimalV3() || t2.isDecimalV2()) && (t1.isDateV2() || t1.isDate()))) {
            return Type.DOUBLE;
        }

        if (t1.isDecimalV2() && t2.isDecimalV2()) {
            return getAssignmentCompatibleDecimalV2Type(t1, t2);
        }

        if ((t1.isDecimalV3() && t2.isDecimalV2()) || (t2.isDecimalV3() && t1.isDecimalV2())) {
            int scale = Math.max(t1.scale, t2.scale);
            int integerPart = Math.max(t1.precision - t1.scale, t2.precision - t2.scale);
            return ScalarType.createDecimalV3Type(integerPart + scale, scale);
        }

        if (t1.isDecimalV2() || t2.isDecimalV2()) {
            if (t1.isFloatingPointType() || t2.isFloatingPointType()) {
                return Type.DOUBLE;
            }
            return t1.isDecimalV2() ? t1 : t2;
        }

        if (t1.isDecimalV3() || t2.isDecimalV3()) {
            if (t1.isFloatingPointType() || t2.isFloatingPointType()) {
                return t1.isFloatingPointType() ? t1 : t2;
            } else if (t1.isBoolean() || t2.isBoolean()) {
                return t1.isDecimalV3() ? t1 : t2;
            }
        }

        if ((t1.isDecimalV3() && t2.isFixedPointType())
                || (t2.isDecimalV3() && t1.isFixedPointType())) {
            int precision;
            int scale;
            ScalarType intType;
            if (t1.isDecimalV3()) {
                precision = t1.precision;
                scale = t1.scale;
                intType = t2;
            } else {
                precision = t2.precision;
                scale = t2.scale;
                intType = t1;
            }
            int integerPart = precision - scale;
            if (intType.isScalarType(PrimitiveType.TINYINT)
                    || intType.isScalarType(PrimitiveType.SMALLINT)) {
                integerPart = Math.max(integerPart, new BigDecimal(Short.MAX_VALUE).precision());
            } else if (intType.isScalarType(PrimitiveType.INT)) {
                integerPart = Math.max(integerPart, new BigDecimal(Integer.MAX_VALUE).precision());
            } else {
                integerPart = ScalarType.MAX_DECIMAL128_PRECISION - scale;
            }
            if (scale + integerPart <= ScalarType.MAX_DECIMAL128_PRECISION) {
                return ScalarType.createDecimalV3Type(scale + integerPart, scale);
            } else {
                return Type.DOUBLE;
            }
        }

        if (t1.isDecimalV3() && t2.isDecimalV3()) {
            ScalarType finalType = ScalarType.createDecimalV3Type(Math.max(t1.decimalPrecision() - t1.decimalScale(),
                    t2.decimalPrecision() - t2.decimalScale()) + Math.max(t1.decimalScale(),
                    t2.decimalScale()), Math.max(t1.decimalScale(), t2.decimalScale()));
            if (finalType.getPrecision() > MAX_PRECISION) {
                finalType = ScalarType.createDecimalV3Type(MAX_PRECISION, finalType.getScalarScale());
            }
            return finalType;
        }

        PrimitiveType smallerType =
                (t1.type.ordinal() < t2.type.ordinal() ? t1.type : t2.type);
        PrimitiveType largerType =
                (t1.type.ordinal() > t2.type.ordinal() ? t1.type : t2.type);
        PrimitiveType result = null;
        if (t1.isDatetimeV2() && t2.isDatetimeV2()) {
            return t1.scale > t2.scale ? t1 : t2;
        }
        if ((t1.isDatetimeV2() || t1.isDateV2()) && (t2.isDatetimeV2() || t2.isDateV2())) {
            return t1.isDatetimeV2() ? t1 : t2;
        }
        if (strict) {
            result = strictCompatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        }
        if (result == null) {
            result = compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        }
        Preconditions.checkNotNull(result);
        if (result == PrimitiveType.DECIMALV2) {
            return Type.MAX_DECIMALV2_TYPE;
        }
        return createType(result);
    }

    public static ScalarType getAssignmentCompatibleDecimalV2Type(ScalarType t1, ScalarType t2) {
        int targetScale = Math.max(t1.decimalScale(), t2.decimalScale());
        int targetPrecision = Math.max(t1.decimalPrecision() - t1.decimalScale(), t2.decimalPrecision()
                - t2.decimalScale()) + targetScale;
        return ScalarType.createDecimalType(PrimitiveType.DECIMALV2,
                targetPrecision, targetScale);
    }

    public static ScalarType getAssignmentCompatibleDecimalV3Type(ScalarType t1, ScalarType t2) {
        int targetScale = Math.max(t1.decimalScale(), t2.decimalScale());
        int targetPrecision = Math.max(t1.decimalPrecision() - t1.decimalScale(), t2.decimalPrecision()
                - t2.decimalScale()) + targetScale;
        return ScalarType.createDecimalV3Type(targetPrecision, targetScale);
    }

    /**
     * Returns true t1 can be implicitly cast to t2, false otherwise.
     * If strict is true, only consider casts that result in no loss of precision.
     */
    public static boolean isImplicitlyCastable(
            ScalarType t1, ScalarType t2, boolean strict) {
        return getAssignmentCompatibleType(t1, t2, strict).matchesType(t2);
    }

    public static boolean canCastTo(ScalarType type, ScalarType targetType) {
        return PrimitiveType.isImplicitCast(type.getPrimitiveType(), targetType.getPrimitiveType());
    }

    /**
     * Decimal default precision is 9 and scale is 0, this method return whether this is
     * default decimal v3 or v2
     */
    public boolean isDefaultDecimal() {
        return (isDecimalV3() || isDecimalV2())
                && DEFAULT_PRECISION == this.precision
                && DEFAULT_SCALE == this.scale;
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = type.toThrift();
        if (type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) {
            thrift.setLen(len);
        }
        if (type == PrimitiveType.DECIMALV2 || type.isDecimalV3Type()
                || type == PrimitiveType.DATETIMEV2 || type == PrimitiveType.TIMEV2) {
            thrift.setPrecision(precision);
            thrift.setScale(scale);
        }
        return thrift;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + Objects.hashCode(type);
        result = 31 * result + precision;
        result = 31 * result + scale;
        return result;
    }
}
