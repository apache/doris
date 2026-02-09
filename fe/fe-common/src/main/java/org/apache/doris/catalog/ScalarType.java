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

    public static final int MAX_VARBINARY_LENGTH = 0x7fffffff; // 2GB/int32 max length

    // Hive, mysql, sql server standard.
    public static final int MAX_DECIMALV2_PRECISION = 27;
    public static final int MAX_DECIMALV2_SCALE = 9;
    public static final int MAX_DECIMAL32_PRECISION = 9;
    public static final int MAX_DECIMAL64_PRECISION = 18;
    public static final int MAX_DECIMAL128_PRECISION = 38;
    public static final int MAX_DECIMAL256_PRECISION = 76;
    public static final int DEFAULT_MIN_AVG_DECIMAL128_SCALE = 4;
    public static final int MAX_DATETIMEV2_SCALE = 6;
    public static final int MAX_PRECISION = MAX_DECIMAL256_PRECISION;

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
            case DECIMAL256:
                return createDecimalV3Type(precision, scale);
            case DECIMALV2:
                return createDecimalType(precision, scale);
            case DATETIMEV2:
                return createDatetimeV2Type(scale);
            case TIMEV2:
                return createTimeV2Type(scale);
            case TIMESTAMPTZ:
                return createTimeStampTzType(scale);
            case VARBINARY:
                return createVarbinaryType(len);
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
            case TIMESTAMPTZ:
                return DEFAULT_TIMESTAMP_TZ;
            case DECIMAL32:
                return DEFAULT_DECIMAL32;
            case DECIMAL64:
                return DEFAULT_DECIMAL64;
            case DECIMAL128:
                return DEFAULT_DECIMAL128;
            case DECIMAL256:
                return DEFAULT_DECIMAL256;
            case DECIMALV2:
                return DEFAULT_DECIMALV2;
            case LARGEINT:
                return LARGEINT;
            case IPV4:
                return IPV4;
            case IPV6:
                return IPV6;
            case VARBINARY:
                return VARBINARY;
            default:
                LOG.warn("type={}", type);
                Preconditions.checkState(false, "type.name()=" + type.name());
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
            case "TIMEV2":
                return TIMEV2;
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

    public static ScalarType createDecimalV2Type() {
        Preconditions.checkState(!Config.disable_decimalv2, "DecimalV2 is disable in fe.conf!");
        return DEFAULT_DECIMALV2;
    }

    public static ScalarType createDecimalV2Type(int precision) {
        Preconditions.checkState(!Config.disable_decimalv2, "DecimalV2 is disable in fe.conf!");
        return createDecimalV2Type(precision, DEFAULT_SCALE);
    }

    public static ScalarType createDecimalV2Type(int precision, int scale) {
        Preconditions.checkState(!Config.disable_decimalv2, "DecimalV2 is disable in fe.conf!");
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static ScalarType createDecimalV2Type(String precisionStr) {
        Preconditions.checkState(!Config.disable_decimalv2, "DecimalV2 is disable in fe.conf!");
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precisionStr = precisionStr;
        type.scaleStr = null;
        return type;
    }

    public static ScalarType createDecimalV2Type(String precisionStr, String scaleStr) {
        Preconditions.checkState(!Config.disable_decimalv2, "DecimalV2 is disable in fe.conf!");
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
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
        } else if (precision <= MAX_DECIMAL128_PRECISION) {
            return PrimitiveType.DECIMAL128;
        } else {
            return PrimitiveType.DECIMAL256;
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
    public static ScalarType createTimeStampTzType(int scale) {
        ScalarType type = new ScalarType(PrimitiveType.TIMESTAMPTZ);
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
    public static ScalarType createDatetimeV1Type() {
        Preconditions.checkState(!Config.disable_datev1, "Datev1 is disable in fe.conf!");
        return new ScalarType(PrimitiveType.DATETIME);
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createDateV1Type() {
        Preconditions.checkState(!Config.disable_datev1, "Datev1 is disable in fe.conf!");
        return new ScalarType(PrimitiveType.DATE);
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static ScalarType createTimeType() {
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

    public static ScalarType createVarbinaryType(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARBINARY);
        type.len = len;
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
        // Not return ScalarType return VariantType instead for compatibility reason
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        return new VariantType();
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
                return "character(" + MAX_CHAR_LENGTH + ")";
            }
            return "char(" + len + ")";
        } else  if (type == PrimitiveType.DECIMALV2) {
            if (isWildcardDecimal()) {
                return "decimal(*,*)";
            }
            return "decimal(" + precision + "," + scale + ")";
        } else  if (type.isDecimalV3Type()) {
            if (isWildcardDecimal()) {
                return "decimalv3(*,*)";
            }
            return "decimalv3(" + precision + "," + scale + ")";
        } else  if (type == PrimitiveType.DATETIMEV2) {
            return "datetimev2(" + scale + ")";
        } else if (type == PrimitiveType.TIMEV2) {
            return "timev2(" + scale + ")";
        } else if (type == PrimitiveType.TIMESTAMPTZ) {
            return "timestamptz(" + scale + ")";
        } else if (type == PrimitiveType.VARCHAR) {
            if (isWildcardVarchar()) {
                return "varchar(" + MAX_VARCHAR_LENGTH + ")";
            }
            return "varchar(" + len + ")";
        } else if (type == PrimitiveType.STRING) {
            return "text";
        } else if (type == PrimitiveType.JSONB) {
            return "json";
        }
        return type.toString().toLowerCase();
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case CHAR:
                if (isWildcardChar()) {
                    stringBuilder.append("character").append("(").append(MAX_CHAR_LENGTH).append(")");
                } else if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("char").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("char").append("(`").append(lenStr).append("`)");
                }
                break;
            case VARCHAR:
                if (isWildcardVarchar()) {
                    return "varchar(" + MAX_VARCHAR_LENGTH + ")";
                } else if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("varchar").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("varchar").append("(`").append(lenStr).append("`)");
                }
                break;
            case DECIMALV2:
                if (Strings.isNullOrEmpty(precisionStr)) {
                    stringBuilder.append("decimalv2").append("(").append(precision)
                            .append(",").append(scale).append(")");
                } else if (!Strings.isNullOrEmpty(precisionStr) && !Strings.isNullOrEmpty(scaleStr)) {
                    stringBuilder.append("decimalv2").append("(`").append(precisionStr)
                            .append("`,`").append(scaleStr).append("`)");
                } else {
                    stringBuilder.append("decimalv2").append("(`").append(precisionStr).append("`)");
                }
                break;
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                String typeName = "decimalv3";
                if (Strings.isNullOrEmpty(precisionStr)) {
                    stringBuilder.append(typeName).append("(").append(precision)
                        .append(",").append(scale).append(")");
                } else if (!Strings.isNullOrEmpty(precisionStr) && !Strings.isNullOrEmpty(scaleStr)) {
                    stringBuilder.append(typeName).append("(`").append(precisionStr)
                        .append("`,`").append(scaleStr).append("`)");
                } else {
                    stringBuilder.append(typeName).append("(`").append(precisionStr).append("`)");
                }
                break;
            case DATETIMEV2:
                stringBuilder.append("datetimev2").append("(").append(scale).append(")");
                break;
            case TIMEV2:
                stringBuilder.append("time").append("(").append(scale).append(")");
                break;
            case TIMESTAMPTZ:
                stringBuilder.append("timestamptz").append("(").append(scale).append(")");
                break;
            case BOOLEAN:
                return "boolean";
            case TINYINT:
                return "tinyint";
            case SMALLINT:
                return "smallint";
            case INT:
                return "int";
            case BIGINT:
                return "bigint";
            case LARGEINT:
                return "largeint";
            case IPV4:
                return "ipv4";
            case IPV6:
                return "ipv6";
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
            case NULL_TYPE:
                stringBuilder.append(type.toString().toLowerCase());
                break;
            case STRING:
                stringBuilder.append("text");
                break;
            case JSONB:
                stringBuilder.append("json");
                break;
            case VARBINARY:
                if (isWildcardVarbinary()) {
                    return "varbinary(" + MAX_VARCHAR_LENGTH + ")";
                } else if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("varbinary").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("varbinary").append("(`").append(lenStr).append("`)");
                }
                break;
            default:
                stringBuilder.append("unknown type: ").append(type);
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
            case VARBINARY:
            case JSONB: {
                scalarType.setLen(getLength());
                break;
            }
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
            case DATETIMEV2:
            case TIMESTAMPTZ:
            case TIMEV2: {
                if (precision < scale) {
                    throw new IllegalArgumentException(
                            String.format("given precision %d is out of scale bound %d", precision, scale));
                }
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
                || type == PrimitiveType.TIMESTAMPTZ || type == PrimitiveType.TIMEV2
                || type == PrimitiveType.DECIMAL32 || type == PrimitiveType.DECIMAL64
                || type == PrimitiveType.DECIMAL128 || type == PrimitiveType.DECIMAL256);
        return precision;
    }

    public int decimalScale() {
        Preconditions.checkState(type == PrimitiveType.DECIMALV2
                || type == PrimitiveType.DATETIMEV2 || type == PrimitiveType.TIMESTAMPTZ
                || type == PrimitiveType.TIMEV2 || type == PrimitiveType.DECIMAL32
                || type == PrimitiveType.DECIMAL64 || type == PrimitiveType.DECIMAL128
                || type == PrimitiveType.DECIMAL256);
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
    public boolean isWildcardTimeV2() {
        return type == PrimitiveType.TIMEV2 && scale == -1;
    }

    @Override
    public boolean isWildcardDatetimeV2() {
        return type == PrimitiveType.DATETIMEV2 && scale == -1;
    }

    @Override
    public boolean isWildcardTimeStampTz() {
        return type == PrimitiveType.TIMESTAMPTZ && scale == -1;
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
    public boolean isWildcardVarbinary() {
        return type == PrimitiveType.VARBINARY && (len == -1 || len == MAX_CHAR_LENGTH);
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
        if (isDatetimeV2() && scalarType.isWildcardDatetimeV2()) {
            Preconditions.checkState(!isWildcardDatetimeV2());
            return true;
        }
        if (isTimeStampTz() && scalarType.isWildcardTimeStampTz()) {
            Preconditions.checkState(!isWildcardTimeStampTz());
            return true;
        }
        if (isTimeV2() && scalarType.isWildcardTimeV2()) {
            Preconditions.checkState(!isWildcardTimeV2());
            return true;
        }
        if (isVariantType() && scalarType.isVariantType()) {
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
        if (type != other.type) {
            return false;
        }
        if (type == PrimitiveType.CHAR) {
            return len == other.len;
        }
        if (type == PrimitiveType.VARCHAR) {
            return len == other.len;
        }
        if (type.isDecimalV3Type() || type.isDecimalV2Type()) {
            return precision == other.precision && scale == other.scale;
        }
        if (type == PrimitiveType.DATETIMEV2 || type == PrimitiveType.TIMESTAMPTZ || type == PrimitiveType.TIMEV2) {
            return scale == other.scale;
        }
        return true;
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = type.toThrift();
        if (type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) {
            thrift.setLen(len);
        }
        if (type == PrimitiveType.DECIMALV2 || type.isDecimalV3Type() || type == PrimitiveType.TIMESTAMPTZ
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

    public int getVariantMaxSubcolumnsCount() {
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        if (this instanceof VariantType) {
            return ((VariantType) this).getVariantMaxSubcolumnsCount();
        }
        return 0; // The old variant type had a default value of 0.
    }

    public boolean getVariantEnableTypedPathsToSparse() {
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        if (this instanceof VariantType) {
            return ((VariantType) this).getEnableTypedPathsToSparse();
        }
        return false; // The old variant type had a default value of false.
    }

    public int getVariantMaxSparseColumnStatisticsSize() {
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        if (this instanceof VariantType) {
            return ((VariantType) this).getVariantMaxSparseColumnStatisticsSize();
        }
        return 0; // The old variant type had a default value of 0.
    }

    public int getVariantSparseHashShardCount() {
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        if (this instanceof VariantType) {
            return ((VariantType) this).getVariantSparseHashShardCount();
        }
        return 0; // The old variant type had a default value of 0.
    }

    public boolean getVariantEnableDocMode() {
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        if (this instanceof VariantType) {
            return ((VariantType) this).getEnableVariantDocMode();
        }
        return false; // The old variant type had a default value of false.
    }

    public long getvariantDocMaterializationMinRows() {
        // In the past, variant metadata used the ScalarType type.
        // Now, we use VariantType, which inherits from ScalarType, as the new metadata storage.
        if (this instanceof VariantType) {
            return ((VariantType) this).getvariantDocMaterializationMinRows();
        }
        return 0L; // The old variant type had a default value of 0.
    }

    public int getVariantDocShardCount() {
        if (this instanceof VariantType) {
            return ((VariantType) this).getVariantDocShardCount();
        }
        // Backward-compatible default bucket count used by BE doc snapshot writer/reader.
        return 128;
    }
}
