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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

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
    private static final Logger LOG = LogManager.getLogger(ScalarType.class);

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

    // Hive, mysql, sql server standard.
    public static final int MAX_PRECISION = 38;

    @SerializedName(value = "type")
    private final PrimitiveType type;

    // Only used for type CHAR.
    @SerializedName(value = "len")
    private int len = -1;
    private boolean isAssignedStrLenInColDefinition = false;

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

    protected ScalarType(PrimitiveType type) {
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
            case DECIMALV2:
                return createDecimalV2Type(precision, scale);
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
            case STRING:
                return createStringType();
            case HLL:
                return createHllType();
            case BITMAP:
                return BITMAP;
            case QUANTILE_STATE:
                return QUANTILE_STATE;
            case DATE:
                return DATE;
            case DATETIME:
                return DATETIME;
            case TIME:
                return TIME;
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
            case "STRING":
            case "TEXT":
                return createStringType();
            case "HLL":
                return createHllType();
            case "BITMAP":
                return BITMAP;
            case "QUANTILE_STATE":
                return QUANTILE_STATE;
            case "DATE":
                return DATE;
            case "DATETIME":
                return DATETIME;
            case "TIME":
                return TIME;
            case "DECIMAL":
            case "DECIMALV2":
                return (ScalarType) createDecimalV2Type();
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

    public static ScalarType createDecimalV2Type() {
        return DEFAULT_DECIMALV2;
    }

    public static ScalarType createDecimalV2Type(int precision) {
        return createDecimalV2Type(precision, DEFAULT_SCALE);
    }

    public static ScalarType createDecimalV2Type(int precision, int scale) {
        // Preconditions.checkState(precision >= 0); // Enforced by parser
        // Preconditions.checkState(scale >= 0); // Enforced by parser.
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precision = precision;
        type.scale = scale;
        return type;
    }

    public static ScalarType createDecimalV2Type(String precisionStr) {
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precisionStr = precisionStr;
        type.scaleStr = null;
        return type;
    }

    public static ScalarType createDecimalV2Type(String precisionStr, String scaleStr) {
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precisionStr = precisionStr;
        type.scaleStr = scaleStr;
        return type;
    }

    public static ScalarType createDecimalV2TypeInternal(int precision, int scale) {
        ScalarType type = new ScalarType(PrimitiveType.DECIMALV2);
        type.precision = Math.min(precision, MAX_PRECISION);
        type.scale = Math.min(type.precision, scale);
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

    public static ScalarType createVarchar(int len) {
        // length checked in analysis
        ScalarType type = new ScalarType(PrimitiveType.VARCHAR);
        type.len = len;
        return type;
    }

    public static ScalarType createVarcharType() {
        return DEFAULT_VARCHAR;
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
                return "CHAR(*)";
            }
            return "CHAR(" + len + ")";
        } else  if (type == PrimitiveType.DECIMALV2) {
            if (isWildcardDecimal()) {
                return "DECIMAL(*,*)";
            }
            return "DECIMAL(" + precision + "," + scale + ")";
        } else if (type == PrimitiveType.VARCHAR) {
            if (isWildcardVarchar()) {
                return "VARCHAR(*)";
            }
            return "VARCHAR(" + len + ")";
        } else if (type == PrimitiveType.STRING) {
            return "TEXT";
        }
        return type.toString();
    }

    @Override
    public String toSql(int depth) {
        StringBuilder stringBuilder = new StringBuilder();
        switch (type) {
            case CHAR:
                if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("char").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("char").append("(`").append(lenStr).append("`)");
                }
                break;
            case VARCHAR:
                if (Strings.isNullOrEmpty(lenStr)) {
                    stringBuilder.append("varchar").append("(").append(len).append(")");
                } else {
                    stringBuilder.append("varchar").append("(`").append(lenStr).append("`)");
                }
                break;
            case DECIMALV2:
                if (Strings.isNullOrEmpty(precisionStr)) {
                    stringBuilder.append("decimal").append("(").append(precision).append(", ").append(scale).append(")");
                } else if (!Strings.isNullOrEmpty(precisionStr) && !Strings.isNullOrEmpty(scaleStr)) {
                    stringBuilder.append("decimal").append("(`").append(precisionStr).append("`, `").append(scaleStr).append("`)");
                } else {
                    stringBuilder.append("decimal").append("(`").append(precisionStr).append("`)");
                }
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
            case HLL:
            case BITMAP:
            case QUANTILE_STATE:
                stringBuilder.append(type.toString().toLowerCase());
                break;
            case STRING:
                stringBuilder.append("text");
                break;
            case ARRAY:
                stringBuilder.append(type.toString().toLowerCase());
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

        switch(type) {
            case VARCHAR:
            case CHAR:
            case HLL:
            case STRING: {
                scalarType.setLen(len);
                break;
            }
            case DECIMALV2: {
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
        Preconditions.checkState(type == PrimitiveType.DECIMALV2);
        return precision;
    }

    public int decimalScale() {
        Preconditions.checkState(type == PrimitiveType.DECIMALV2);
        return scale;
    }

    @Override
    public PrimitiveType getPrimitiveType() { return type; }
    public int ordinal() { return type.ordinal(); }

    @Override
    public int getLength() { return len; }
    public void setLength(int len) {this.len = len; }
    public boolean isAssignedStrLenInColDefinition() { return isAssignedStrLenInColDefinition; }
    public void setAssignedStrLenInColDefinition() { this.isAssignedStrLenInColDefinition = true; }

    // add scalar infix to override with getPrecision
    public int getScalarScale() { return scale; }
    public int getScalarPrecision() { return precision; }

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
        return (type == PrimitiveType.DECIMALV2)
                && precision == -1 && scale == -1;
    }

    @Override
    public boolean isWildcardVarchar() {
        return (type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) && len == -1;
    }

    @Override
    public boolean isWildcardChar() {
        return type == PrimitiveType.CHAR && len == -1;
    }

    @Override
    public boolean isFixedLengthType() {
        return type == PrimitiveType.BOOLEAN || type == PrimitiveType.TINYINT
                || type == PrimitiveType.SMALLINT || type == PrimitiveType.INT
                || type == PrimitiveType.BIGINT || type == PrimitiveType.FLOAT
                || type == PrimitiveType.DOUBLE || type == PrimitiveType.DATE
                || type == PrimitiveType.DATETIME || type == PrimitiveType.DECIMALV2
                || type == PrimitiveType.CHAR;
    }

    @Override
    public boolean isSupported() {
        switch (type) {
            case BINARY:
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
        if (!t.isScalarType()) {
            return false;
        }
        ScalarType scalarType = (ScalarType) t;
        if (type == PrimitiveType.VARCHAR && scalarType.isWildcardVarchar()) {
            Preconditions.checkState(!isWildcardVarchar());
            return true;
        }
        if (type == PrimitiveType.CHAR && scalarType.isWildcardChar()) {
            Preconditions.checkState(!isWildcardChar());
            return true;
        }
        if (type == PrimitiveType.CHAR && scalarType.isStringType()) {
            return true;
        }
        if (type == PrimitiveType.VARCHAR && scalarType.isStringType()) {
            return true;
        }
        if (isDecimalV2() && scalarType.isWildcardDecimal()) {
            Preconditions.checkState(!isWildcardDecimal());
            return true;
        }
        if (isDecimalV2() && scalarType.isDecimalV2()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ScalarType)) {
            return false;
        }
        ScalarType other = (ScalarType)o;
        if (type != other.type) {
            return false;
        }
        if (type == PrimitiveType.CHAR) {
            return len == other.len;
        }
        if (type == PrimitiveType.VARCHAR) {
            return len == other.len;
        }
        if ( type == PrimitiveType.DECIMALV2) {
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
            return createDecimalV2TypeInternal(MAX_PRECISION, scale);
        } else if (isLargeIntType()) {
        return ScalarType.LARGEINT;
        } else {
            return ScalarType.INVALID;
        }
    }

    public ScalarType getNextResolutionType() {
        Preconditions.checkState(isNumericType() || isNull());
        if (type == PrimitiveType.DOUBLE || type == PrimitiveType.BIGINT || isNull()) {
            return this;
        } else if (type == PrimitiveType.DECIMALV2) {
            return createDecimalV2TypeInternal(MAX_PRECISION, scale);
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
                return createDecimalV2Type(3);
            case SMALLINT:
                return createDecimalV2Type(5);
            case INT:
                return createDecimalV2Type(10);
            case BIGINT:
                return createDecimalV2Type(19);
            case FLOAT:
                return createDecimalV2TypeInternal(MAX_PRECISION, 9);
            case DOUBLE:
                return createDecimalV2TypeInternal(MAX_PRECISION, 17);
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
        Preconditions.checkState(isDecimalV2());
        Preconditions.checkState(o.isDecimalV2());
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

        if (t1.isDecimalV2() && t2.isDate()
                || t1.isDate() && t2.isDecimalV2()) {
            return INVALID;
        }

        if (t1.isDecimalV2() || t2.isDecimalV2()) {
            return DECIMALV2;
        }

        PrimitiveType smallerType =
                (t1.type.ordinal() < t2.type.ordinal() ? t1.type : t2.type);
        PrimitiveType largerType =
                (t1.type.ordinal() > t2.type.ordinal() ? t1.type : t2.type);
        PrimitiveType result = null;
        if (strict) {
            result = strictCompatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        }
        if (result == null) {
            result = compatibilityMatrix[smallerType.ordinal()][largerType.ordinal()];
        }
        Preconditions.checkNotNull(result);
        return createType(result);
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

    @Override
    public int getStorageLayoutBytes() {
        switch (type) {
            case BOOLEAN:
            case TINYINT:
                return 1;
            case SMALLINT:
                return 2;
            case INT:
            case FLOAT:
                return 4;
            case BIGINT:
            case TIME:
            case DATETIME:
                return 8;
            case LARGEINT:
            case DECIMALV2:
                return 16;
            case DOUBLE:
                return 12;
            case DATE:
                return 3;
            case CHAR:
            case VARCHAR:
                return len;
            case HLL:
                return 16385;
            case BITMAP:
                return 1024; // this is a estimated value
            case QUANTILE_STATE:
                return 1024; // TODO(weixiang): no used in FE, figure out whether can delete this funcion?
            case STRING:
                return 1024;
            default:
                return 0;
        }
    }

    @Override
    public TColumnType toColumnTypeThrift() {
        TColumnType thrift = new TColumnType();
        thrift.type = type.toThrift();
        if (type == PrimitiveType.CHAR || type == PrimitiveType.VARCHAR || type == PrimitiveType.HLL) {
            thrift.setLen(len);
        }
        if (type == PrimitiveType.DECIMALV2) {
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

    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ScalarType read(DataInput input) throws IOException {
        String json = Text.readString(input);
        return GsonUtils.GSON.fromJson(json, ScalarType.class);
    }
}
