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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TScalarType;
import org.apache.doris.thrift.TStructField;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TTypeNodeType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class describing an Impala data type (scalar/complex type).
 * Mostly contains static type instances and helper methods for convenience, as well
 * as abstract methods that subclasses must implement.
 */
public abstract class Type {
    // Currently only support Array type with max 9 depths.
    public static int MAX_NESTING_DEPTH = 9;

    // Static constant types for scalar types that don't require additional information.
    public static final ScalarType INVALID = new ScalarType(PrimitiveType.INVALID_TYPE);
    public static final ScalarType NULL = new ScalarType(PrimitiveType.NULL_TYPE);
    public static final ScalarType BOOLEAN = new ScalarType(PrimitiveType.BOOLEAN);
    public static final ScalarType TINYINT = new ScalarType(PrimitiveType.TINYINT);
    public static final ScalarType SMALLINT = new ScalarType(PrimitiveType.SMALLINT);
    public static final ScalarType INT = new ScalarType(PrimitiveType.INT);
    public static final ScalarType BIGINT = new ScalarType(PrimitiveType.BIGINT);
    public static final ScalarType LARGEINT = new ScalarType(PrimitiveType.LARGEINT);
    public static final ScalarType FLOAT = new ScalarType(PrimitiveType.FLOAT);
    public static final ScalarType DOUBLE = new ScalarType(PrimitiveType.DOUBLE);
    public static final ScalarType DATE = new ScalarType(PrimitiveType.DATE);
    public static final ScalarType DATETIME = new ScalarType(PrimitiveType.DATETIME);
    public static final ScalarType DATEV2 = new ScalarType(PrimitiveType.DATEV2);
    public static final ScalarType TIMEV2 = new ScalarType(PrimitiveType.TIMEV2);
    public static final ScalarType TIME = new ScalarType(PrimitiveType.TIME);
    public static final ScalarType STRING = new ScalarType(PrimitiveType.STRING);
    public static final ScalarType DEFAULT_DECIMALV2 = ScalarType.createDecimalType(PrimitiveType.DECIMALV2,
                    ScalarType.DEFAULT_PRECISION, ScalarType.DEFAULT_SCALE);

    public static final ScalarType MAX_DECIMALV2_TYPE = ScalarType.createDecimalType(PrimitiveType.DECIMALV2,
            ScalarType.MAX_DECIMALV2_PRECISION, ScalarType.MAX_DECIMALV2_SCALE);

    public static final ScalarType DEFAULT_DECIMAL32 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL32, ScalarType.MAX_DECIMAL32_PRECISION,
                    ScalarType.DEFAULT_SCALE);

    public static final ScalarType DEFAULT_DECIMAL64 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL64, ScalarType.MAX_DECIMAL64_PRECISION,
                    ScalarType.DEFAULT_SCALE);

    public static final ScalarType DEFAULT_DECIMAL128 =
            ScalarType.createDecimalType(PrimitiveType.DECIMAL128, ScalarType.MAX_DECIMAL128_PRECISION,
                    ScalarType.DEFAULT_SCALE);
    public static final ScalarType DEFAULT_DECIMALV3 = DEFAULT_DECIMAL32;
    public static final ScalarType DEFAULT_DATETIMEV2 = ScalarType.createDatetimeV2Type(0);
    public static final ScalarType DATETIMEV2 = DEFAULT_DATETIMEV2;
    public static final ScalarType DEFAULT_TIMEV2 = ScalarType.createTimeV2Type(0);
    public static final ScalarType DECIMALV2 = DEFAULT_DECIMALV2;
    public static final ScalarType DECIMAL32 = DEFAULT_DECIMAL32;
    public static final ScalarType DECIMAL64 = DEFAULT_DECIMAL64;
    public static final ScalarType DECIMAL128 = DEFAULT_DECIMAL128;
    public static final ScalarType JSONB = new ScalarType(PrimitiveType.JSONB);
    // (ScalarType) ScalarType.createDecimalTypeInternal(-1, -1);
    public static final ScalarType DEFAULT_VARCHAR = ScalarType.createVarcharType(-1);
    public static final ScalarType VARCHAR = ScalarType.createVarcharType(-1);
    public static final ScalarType HLL = ScalarType.createHllType();
    public static final ScalarType CHAR = ScalarType.createCharType(-1);
    public static final ScalarType BITMAP = new ScalarType(PrimitiveType.BITMAP);
    public static final ScalarType QUANTILE_STATE = new ScalarType(PrimitiveType.QUANTILE_STATE);
    // Only used for alias function, to represent any type in function args
    public static final ScalarType ALL = new ScalarType(PrimitiveType.ALL);
    public static final MapType MAP = new MapType();
    public static final ArrayType ARRAY = ArrayType.create();
    public static final StructType STRUCT = new StructType();

    private static final Logger LOG = LogManager.getLogger(Type.class);
    private static final ArrayList<ScalarType> integerTypes;
    private static final ArrayList<ScalarType> numericTypes;
    private static final ArrayList<ScalarType> supportedTypes;
    private static final ArrayList<Type> arraySubTypes;
    private static final ArrayList<ScalarType> trivialTypes;

    static {
        integerTypes = Lists.newArrayList();
        integerTypes.add(TINYINT);
        integerTypes.add(SMALLINT);
        integerTypes.add(INT);
        integerTypes.add(BIGINT);
        integerTypes.add(LARGEINT);

        numericTypes = Lists.newArrayList();
        numericTypes.addAll(integerTypes);
        numericTypes.add(FLOAT);
        numericTypes.add(DOUBLE);
        numericTypes.add(MAX_DECIMALV2_TYPE);
        numericTypes.add(DECIMAL32);
        numericTypes.add(DECIMAL64);
        numericTypes.add(DECIMAL128);

        trivialTypes = Lists.newArrayList();
        trivialTypes.addAll(numericTypes);
        trivialTypes.add(BOOLEAN);
        trivialTypes.add(VARCHAR);
        trivialTypes.add(STRING);
        trivialTypes.add(CHAR);
        trivialTypes.add(DATE);
        trivialTypes.add(DATETIME);
        trivialTypes.add(DATEV2);
        trivialTypes.add(DATETIMEV2);
        trivialTypes.add(TIME);
        trivialTypes.add(TIMEV2);
        trivialTypes.add(JSONB);
        trivialTypes.add(DECIMAL32);
        trivialTypes.add(DECIMAL64);
        trivialTypes.add(DECIMAL128);

        supportedTypes = Lists.newArrayList();
        supportedTypes.addAll(trivialTypes);
        supportedTypes.add(NULL);
        supportedTypes.add(HLL);
        supportedTypes.add(BITMAP);
        supportedTypes.add(QUANTILE_STATE);

        arraySubTypes = Lists.newArrayList();
        arraySubTypes.add(BOOLEAN);
        arraySubTypes.addAll(integerTypes);
        arraySubTypes.add(FLOAT);
        arraySubTypes.add(DOUBLE);
        arraySubTypes.add(DECIMALV2);
        arraySubTypes.add(DATE);
        arraySubTypes.add(DATETIME);
        arraySubTypes.add(DATEV2);
        arraySubTypes.add(DATETIMEV2);
        arraySubTypes.add(CHAR);
        arraySubTypes.add(VARCHAR);
        arraySubTypes.add(STRING);
    }

    public static ArrayList<ScalarType> getIntegerTypes() {
        return integerTypes;
    }

    public static ArrayList<ScalarType> getNumericTypes() {
        return numericTypes;
    }

    public static ArrayList<ScalarType> getTrivialTypes() {
        return trivialTypes;
    }

    public static ArrayList<ScalarType> getSupportedTypes() {
        return supportedTypes;
    }

    public static ArrayList<Type> getArraySubTypes() {
        return arraySubTypes;
    }

    /**
     * The output of this is stored directly in the hive metastore as the column type.
     * The string must match exactly.
     */
    public final String toSql() {
        return toSql(0);
    }

    /**
     * Recursive helper for toSql() to be implemented by subclasses. Keeps track of the
     * nesting depth and terminates the recursion if MAX_NESTING_DEPTH is reached.
     */
    protected abstract String toSql(int depth);

    /**
     * Same as toSql() but adds newlines and spaces for better readability of nested types.
     */
    public String prettyPrint() {
        return prettyPrint(0);
    }

    /**
     * Pretty prints this type with lpad number of leading spaces. Used to implement
     * prettyPrint() with space-indented nested types.
     */
    protected abstract String prettyPrint(int lpad);

    public boolean isInvalid() {
        return isScalarType(PrimitiveType.INVALID_TYPE);
    }

    public boolean isValid() {
        return !isInvalid();
    }

    public boolean isNull() {
        return isScalarType(PrimitiveType.NULL_TYPE);
    }

    public boolean isBoolean() {
        return isScalarType(PrimitiveType.BOOLEAN);
    }

    public boolean isDecimalV2() {
        return isScalarType(PrimitiveType.DECIMALV2);
    }

    public boolean isDecimalV3() {
        return isScalarType(PrimitiveType.DECIMAL32) || isScalarType(PrimitiveType.DECIMAL64)
                || isScalarType(PrimitiveType.DECIMAL128);
    }

    public boolean isDatetimeV2() {
        return isScalarType(PrimitiveType.DATETIMEV2);
    }

    public boolean isTimeV2() {
        return isScalarType(PrimitiveType.TIMEV2);
    }

    public boolean isWildcardDecimal() {
        return false;
    }

    public boolean isWildcardVarchar() {
        return false;
    }

    public boolean isWildcardChar() {
        return false;
    }

    public boolean isStringType() {
        return isScalarType(PrimitiveType.VARCHAR)
                || isScalarType(PrimitiveType.CHAR)
                || isScalarType(PrimitiveType.STRING);
    }

    public boolean isVarchar() {
        return isScalarType(PrimitiveType.VARCHAR);
    }

    public boolean isJsonbType() {
        return isScalarType(PrimitiveType.JSONB);
    }

    // only metric types have the following constraint:
    // 1. don't support as key column
    // 2. don't support filter
    // 3. don't support group by
    // 4. don't support index
    public boolean isOnlyMetricType() {
        return isObjectStored() || isArrayType();
    }

    public static final String OnlyMetricTypeErrorMsg =
            "Doris hll, bitmap and array column must use with specific function, and don't support filter or group by."
                    + "please run 'help hll' or 'help bitmap' or 'help array' in your mysql client.";

    public boolean isHllType() {
        return isScalarType(PrimitiveType.HLL);
    }

    public boolean isBitmapType() {
        return isScalarType(PrimitiveType.BITMAP);
    }

    public boolean isQuantileStateType() {
        return isScalarType(PrimitiveType.QUANTILE_STATE);
    }

    public boolean isObjectStored() {
        return isHllType() || isBitmapType() || isQuantileStateType();
    }

    public boolean isScalarType() {
        return this instanceof ScalarType;
    }

    public boolean isScalarType(PrimitiveType t) {
        return isScalarType() && ((ScalarType) this).getPrimitiveType() == t;
    }

    public boolean isFixedPointType() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT)
                || isScalarType(PrimitiveType.LARGEINT);
    }

    public boolean isFloatingPointType() {
        return isScalarType(PrimitiveType.FLOAT) || isScalarType(PrimitiveType.DOUBLE);
    }

    public boolean isIntegerType() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT) || isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isInteger32Type() {
        return isScalarType(PrimitiveType.TINYINT) || isScalarType(PrimitiveType.SMALLINT)
                || isScalarType(PrimitiveType.INT);
    }

    public boolean isBigIntType() {
        return isScalarType(PrimitiveType.BIGINT);
    }

    public boolean isLargeIntType() {
        return isScalarType(PrimitiveType.LARGEINT);
    }

    // TODO: Handle complex types properly. Some instances may be fixed length.
    public boolean isFixedLengthType() {
        return false;
    }

    public boolean isNumericType() {
        return isFixedPointType() || isFloatingPointType() || isDecimalV2() || isDecimalV3();
    }

    public boolean isNativeType() {
        return isFixedPointType() || isFloatingPointType() || isBoolean();
    }

    public boolean isDateType() {
        return isScalarType(PrimitiveType.DATE) || isScalarType(PrimitiveType.DATETIME)
            || isScalarType(PrimitiveType.DATEV2) || isScalarType(PrimitiveType.DATETIMEV2);
    }

    public boolean isDatetime() {
        return isScalarType(PrimitiveType.DATETIME);
    }

    public boolean isTime() {
        return isScalarType(PrimitiveType.TIME);
    }

    public boolean isComplexType() {
        return isStructType() || isCollectionType();
    }

    public boolean isCollectionType() {
        return isMapType() || isArrayType() || isMultiRowType();
    }

    public boolean isMapType() {
        return this instanceof MapType;
    }

    public boolean isArrayType() {
        return this instanceof ArrayType;
    }

    public boolean isMultiRowType() {
        return this instanceof MultiRowType;
    }

    public boolean isStructType() {
        return this instanceof StructType;
    }

    public boolean isDate() {
        return isScalarType(PrimitiveType.DATE);
    }

    public boolean isDateV2() {
        return isScalarType(PrimitiveType.DATEV2);
    }

    /**
     * Returns true if Impala supports this type in the metdata. It does not mean we
     * can manipulate data of this type. For tables that contain columns with these
     * types, we can safely skip over them.
     */
    public boolean isSupported() {
        return true;
    }

    public int getLength() {
        return -1;
    }

    /**
     * Indicates whether we support partitioning tables on columns of this type.
     */
    public boolean supportsTablePartitioning() {
        return false;
    }

    public PrimitiveType getPrimitiveType() {
        return PrimitiveType.INVALID_TYPE;
    }

    /**
     * Returns the size in bytes of the fixed-length portion that a slot of this type
     * occupies in a tuple.
     */
    public int getSlotSize() {
        // 8-byte pointer and 4-byte length indicator (12 bytes total).
        // Per struct alignment rules, there is an extra 4 bytes of padding to align to 8
        // bytes so 16 bytes total.
        if (isCollectionType()) {
            return 16;
        }
        throw new IllegalStateException("getSlotSize() not implemented for type " + toSql());
    }

    public TTypeDesc toThrift() {
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        toThrift(container);
        return container;
    }

    public TColumnType toColumnTypeThrift() {
        return null;
    }

    /**
     * Subclasses should override this method to add themselves to the thrift container.
     */
    public abstract void toThrift(TTypeDesc container);

    /**
     * Returns true if this type is equal to t, or if t is a wildcard variant of this
     * type. Subclasses should override this as appropriate. The default implementation
     * here is to avoid special-casing logic in callers for concrete types.
     */
    public boolean matchesType(Type t) {
        return false;
    }

    /**
     * Returns true if t1 can be implicitly cast to t2 according to Impala's casting rules.
     * Implicit casts are always allowed when no loss of precision would result (i.e. every
     * value of t1 can be represented exactly by a value of t2). Implicit casts are allowed
     * in certain other cases such as casting numeric types to floating point types and
     * converting strings to timestamps.
     * If strict is true, only consider casts that result in no loss of precision.
     * TODO: Support casting of non-scalar types.
     */
    public static boolean isImplicitlyCastable(Type t1, Type t2, boolean strict) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return ScalarType.isImplicitlyCastable((ScalarType) t1, (ScalarType) t2, strict);
        }
        if (t1.isComplexType() || t2.isComplexType()) {
            if (t1.isArrayType() && t2.isArrayType()) {
                return t1.matchesType(t2);
            } else if (t1.isMapType() && t2.isMapType()) {
                return true;
            } else if (t1.isStructType() && t2.isStructType()) {
                return true;
            }
            return false;
        }
        return false;
    }

    public static boolean canCastTo(Type sourceType, Type targetType) {
        if (sourceType.isScalarType() && targetType.isScalarType()) {
            return ScalarType.canCastTo((ScalarType) sourceType, (ScalarType) targetType);
        } else if (sourceType.isArrayType() && targetType.isArrayType()) {
            return ArrayType.canCastTo((ArrayType) sourceType, (ArrayType) targetType);
        } else if (targetType.isArrayType() && !((ArrayType) targetType).getItemType().isScalarType()
                && !sourceType.isNull()) {
            // TODO: current not support cast any non-array type(except for null) to nested array type.
            return false;
        }
        return sourceType.isNull() || sourceType.getPrimitiveType().isCharFamily();
    }

    /**
     * Return type t such that values from both t1 and t2 can be assigned to t without an
     * explicit cast. If strict, does not consider conversions that would result in loss
     * of precision (e.g. converting decimal to float). Returns INVALID_TYPE if there is
     * no such type or if any of t1 and t2 is INVALID_TYPE.
     * TODO: Support non-scalar types.
     */
    public static Type getAssignmentCompatibleType(Type t1, Type t2, boolean strict) {
        if (t1.isScalarType() && t2.isScalarType()) {
            return ScalarType.getAssignmentCompatibleType((ScalarType) t1, (ScalarType) t2, strict);
        }

        if (t1.isArrayType() && t2.isArrayType()) {
            ArrayType arrayType1 = (ArrayType) t1;
            ArrayType arrayType2 = (ArrayType) t2;
            Type itemCompatibleType = Type.getAssignmentCompatibleType(arrayType1.getItemType(),
                    arrayType2.getItemType(), strict);

            if (itemCompatibleType.isInvalid()) {
                return itemCompatibleType;
            }

            return new ArrayType(itemCompatibleType, arrayType1.getContainsNull() || arrayType2.getContainsNull());
        } else if (t1.isArrayType() && t2.isNull()) {
            return t1;
        } else if (t1.isNull() && t2.isArrayType()) {
            return t2;
        }

        return ScalarType.INVALID;
    }

    public static Type getNextNumType(Type t) {
        switch (t.getPrimitiveType()) {
            case BOOLEAN:
                return TINYINT;
            case TINYINT:
                return SMALLINT;
            case SMALLINT:
                return INT;
            case INT:
                return BIGINT;
            case BIGINT:
                return BIGINT;
            case LARGEINT:
                return LARGEINT;
            case FLOAT:
                return DOUBLE;
            case DOUBLE:
                return DOUBLE;
            case DECIMALV2:
                return MAX_DECIMALV2_TYPE;
            case DECIMAL32:
                return DECIMAL32;
            case DECIMAL64:
                return DECIMAL64;
            case DECIMAL128:
                return DECIMAL128;
            default:
                return INVALID;
        }
    }

    /**
     * Returns true if expr is StringLiteral and can parse to valid type, false
     * otherwise.
     * This function only support LargeInt and BigInt now.
     */
    public static boolean canParseTo(Expr expr, PrimitiveType type) {
        if (expr instanceof StringLiteral) {
            if (type == PrimitiveType.BIGINT) {
                return canParseToBigInt((StringLiteral) expr);
            } else if (type == PrimitiveType.LARGEINT) {
                return canParseToLargeInt((StringLiteral) expr);
            }
        }
        return false;
    }

    /**
     * Returns true if expr can parse to valid BigInt, false otherwise.
     */
    private static boolean canParseToBigInt(StringLiteral expr) {
        String value = ((StringLiteral) expr).getValue();
        return Longs.tryParse(value) != null;
    }

    /**
     * Returns true if expr can parse to valid LargeInt, false otherwise.
     */
    private static boolean canParseToLargeInt(Expr expr) {
        try {
            new LargeIntLiteral(((StringLiteral) expr).getValue());
        } catch (AnalysisException e) {
            return false;
        }
        return true;
    }

    /**
     * Returns true if this type exceeds the MAX_NESTING_DEPTH, false otherwise.
     */
    public boolean exceedsMaxNestingDepth() {
        return exceedsMaxNestingDepth(0);
    }

    /**
     * Helper for exceedsMaxNestingDepth(). Recursively computes the max nesting depth,
     * terminating early if MAX_NESTING_DEPTH is reached. Returns true if this type
     * exceeds the MAX_NESTING_DEPTH, false otherwise.
     *
     * Examples of types and their nesting depth:
     * INT --> 1
     * STRUCT<f1:INT> --> 2
     * STRUCT<f1:STRUCT<f2:INT>> --> 3
     * ARRAY<INT> --> 2
     * ARRAY<STRUCT<f1:INT>> --> 3
     * MAP<STRING,INT> --> 2
     * MAP<STRING,STRUCT<f1:INT>> --> 3
     */
    private boolean exceedsMaxNestingDepth(int d) {
        if (d > MAX_NESTING_DEPTH) {
            return true;
        }
        if (isStructType()) {
            StructType structType = (StructType) this;
            for (StructField f : structType.getFields()) {
                if (f.getType().exceedsMaxNestingDepth(d + 1)) {
                    return true;
                }
            }
        } else if (isArrayType()) {
            ArrayType arrayType = (ArrayType) this;
            Type itemType = arrayType.getItemType();
            return itemType.exceedsMaxNestingDepth(d + 1);
        } else if (isMultiRowType()) {
            MultiRowType multiRowType = (MultiRowType) this;
            return multiRowType.getItemType().exceedsMaxNestingDepth(d + 1);
        } else if (isMapType()) {
            MapType mapType = (MapType) this;
            return mapType.getValueType().exceedsMaxNestingDepth(d + 1);
        } else {
            Preconditions.checkState(isScalarType());
        }
        return false;
    }

    // TODO(dhc): fix this
    public static Type fromPrimitiveType(PrimitiveType type) {
        switch (type) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case TINYINT:
                return Type.TINYINT;
            case SMALLINT:
                return Type.SMALLINT;
            case INT:
                return Type.INT;
            case BIGINT:
                return Type.BIGINT;
            case LARGEINT:
                return Type.LARGEINT;
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case DATE:
                return Type.DATE;
            case DATETIME:
                return Type.DATETIME;
            case TIME:
                return Type.TIME;
            case DATEV2:
                return Type.DATEV2;
            case DATETIMEV2:
                return Type.DATETIMEV2;
            case TIMEV2:
                return Type.TIMEV2;
            case DECIMALV2:
                return Type.DECIMALV2;
            case DECIMAL32:
                return Type.DECIMAL32;
            case DECIMAL64:
                return Type.DECIMAL64;
            case DECIMAL128:
                return Type.DECIMAL128;
            case CHAR:
                return Type.CHAR;
            case VARCHAR:
                return Type.VARCHAR;
            case JSONB:
                return Type.JSONB;
            case STRING:
                return Type.STRING;
            case HLL:
                return Type.HLL;
            case ARRAY:
                return ArrayType.create();
            case MAP:
                return new MapType();
            case STRUCT:
                return new StructType();
            case BITMAP:
                return Type.BITMAP;
            case QUANTILE_STATE:
                return Type.QUANTILE_STATE;
            default:
                return null;
        }
    }

    public static List<TTypeDesc> toThrift(Type[] types) {
        return toThrift(Lists.newArrayList(types));
    }

    public static List<TTypeDesc> toThrift(ArrayList<Type> types) {
        ArrayList<TTypeDesc> result = Lists.newArrayList();
        for (Type t : types) {
            result.add(t.toThrift());
        }
        return result;
    }

    public static List<TTypeDesc> toThrift(ArrayList<Type> types, ArrayList<Type> realTypes) {
        ArrayList<TTypeDesc> result = Lists.newArrayList();
        for (int i = 0; i < types.size(); i++) {
            if (PrimitiveType.typeWithPrecision.contains(realTypes.get(i).getPrimitiveType())) {
                result.add(realTypes.get(i).toThrift());
            } else {
                result.add(types.get(i).toThrift());
            }
        }
        return result;
    }

    public static Type fromThrift(TTypeDesc thrift) {
        Preconditions.checkState(thrift.types.size() > 0);
        Pair<Type, Integer> t = fromThrift(thrift, 0);
        Preconditions.checkState(t.second.equals(thrift.getTypesSize()));
        return t.first;
    }

    /**
     * Constructs a ColumnType rooted at the TTypeNode at nodeIdx in TColumnType.
     * Returned pair: The resulting ColumnType and the next nodeIdx that is not a child
     * type of the result.
     */
    protected static Pair<Type, Integer> fromThrift(TTypeDesc col, int nodeIdx) {
        TTypeNode node = col.getTypes().get(nodeIdx);
        Type type = null;
        int tmpNodeIdx = nodeIdx;
        switch (node.getType()) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case SCALAR: {
                Preconditions.checkState(node.isSetScalarType());
                TScalarType scalarType = node.getScalarType();
                if (scalarType.getType() == TPrimitiveType.CHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createCharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.VARCHAR) {
                    Preconditions.checkState(scalarType.isSetLen());
                    type = ScalarType.createVarcharType(scalarType.getLen());
                } else if (scalarType.getType() == TPrimitiveType.HLL) {
                    type = ScalarType.createHllType();
                } else if (scalarType.getType() == TPrimitiveType.DECIMALV2
                        || scalarType.getType() == TPrimitiveType.DECIMAL32
                        || scalarType.getType() == TPrimitiveType.DECIMAL64
                        || scalarType.getType() == TPrimitiveType.DECIMAL128I) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetPrecision());
                    type = ScalarType.createDecimalType(scalarType.getPrecision(),
                            scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.DATETIMEV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createDatetimeV2Type(scalarType.getScale());
                } else if (scalarType.getType() == TPrimitiveType.TIMEV2) {
                    Preconditions.checkState(scalarType.isSetPrecision()
                            && scalarType.isSetScale());
                    type = ScalarType.createTimeV2Type(scalarType.getScale());
                } else {
                    type = ScalarType.createType(
                            PrimitiveType.fromThrift(scalarType.getType()));
                }
                ++tmpNodeIdx;
                break;
            }
            case ARRAY: {
                Preconditions.checkState(tmpNodeIdx + 1 < col.getTypesSize());
                Pair<Type, Integer> childType = fromThrift(col, tmpNodeIdx + 1);
                type = new ArrayType(childType.first);
                tmpNodeIdx = childType.second;
                break;
            }
            case MAP: {
                Preconditions.checkState(tmpNodeIdx + 2 < col.getTypesSize());
                Pair<Type, Integer> keyType = fromThrift(col, tmpNodeIdx + 1);
                Pair<Type, Integer> valueType = fromThrift(col, keyType.second);
                type = new MapType(keyType.first, valueType.first);
                tmpNodeIdx = valueType.second;
                break;
            }
            case STRUCT: {
                Preconditions.checkState(tmpNodeIdx + node.getStructFieldsSize() < col.getTypesSize());
                ArrayList<StructField> structFields = Lists.newArrayList();
                ++tmpNodeIdx;
                for (int i = 0; i < node.getStructFieldsSize(); ++i) {
                    TStructField thriftField = node.getStructFields().get(i);
                    String name = thriftField.getName();
                    String comment = null;
                    if (thriftField.isSetComment()) {
                        comment = thriftField.getComment();
                    }
                    Pair<Type, Integer> res = fromThrift(col, tmpNodeIdx);
                    tmpNodeIdx = res.second.intValue();
                    structFields.add(new StructField(name, res.first, comment));
                }
                type = new StructType(structFields);
                break;
            }
        }
        return Pair.of(type, tmpNodeIdx);
    }

    /**
     * Utility function to get the primitive type of a thrift type that is known
     * to be scalar.
     */
    public TPrimitiveType getTPrimitiveType(TTypeDesc ttype) {
        Preconditions.checkState(ttype.getTypesSize() == 1);
        Preconditions.checkState(ttype.types.get(0).getType() == TTypeNodeType.SCALAR);
        return ttype.types.get(0).scalar_type.getType();
    }

    /**
     * JDBC data type description
     * Returns the column size for this type.
     * For numeric data this is the maximum precision.
     * For character data this is the length in characters.
     * For datetime types this is the length in characters of the String representation
     * (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data this is the length in bytes.
     * Null is returned for data types where the column size is not applicable.
     */
    public Integer getColumnSize() {
        if (!isScalarType()) {
            return null;
        }
        if (isNumericType()) {
            return getPrecision();
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
            case STRING:
            case HLL:
                return t.getLength();
            default:
                return null;
        }
    }

    /**
     * For schema change, convert data type to string,
     * get the size of string representation
     */
    public int getColumnStringRepSize() throws DdlException {
        if (isScalarType(PrimitiveType.FLOAT)) {
            return 24; // see be/src/gutil/strings/numbers.h kFloatToBufferSize
        }
        if (isScalarType(PrimitiveType.DOUBLE)) {
            return 32; // see be/src/gutil/strings/numbers.h kDoubleToBufferSize
        }
        if (isNumericType()) {
            int size = getPrecision() + 1; // +1 for minus symbol
            if (isScalarType(PrimitiveType.DECIMALV2) || isDecimalV3()) {
                size += 1; // +1 for decimal point
            }
            return size;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case CHAR:
            case VARCHAR:
                return t.getLength();
            case STRING:
                return 2147483647; // defined by be/src/olap/olap_define.h, OLAP_STRING_MAX_LENGTH
            default:
                throw new DdlException("Can not change " + t.getPrimitiveType() + " to char/varchar/string");
        }
    }

    /**
     * JDBC data type description
     * For numeric types, returns the maximum precision for this type.
     * For non-numeric types, returns null.
     */
    public Integer getPrecision() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case TINYINT:
                return 3;
            case SMALLINT:
                return 5;
            case INT:
                return 10;
            case BIGINT:
                return 19;
            case LARGEINT:
                return 39;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DATETIMEV2:
            case TIMEV2:
                return t.decimalPrecision();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * Returns the number of fractional digits for this type, or null if not applicable.
     * For timestamp/time types, returns the number of digits in the fractional seconds
     * component.
     */
    public Integer getDecimalDigits() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return 0;
            case FLOAT:
                return 7;
            case DOUBLE:
                return 15;
            case DATETIMEV2:
            case TIMEV2:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return t.decimalScale();
            default:
                return null;
        }
    }

    /**
     * JDBC data type description
     * For numeric data types, either 10 or 2. If it is 10, the values in COLUMN_SIZE
     * and DECIMAL_DIGITS give the number of decimal digits allowed for the column.
     * For example, a DECIMAL(12,5) column would return a NUM_PREC_RADIX of 10,
     * a COLUMN_SIZE of 12, and a DECIMAL_DIGITS of 5; a FLOAT column could return
     * a NUM_PREC_RADIX of 10, a COLUMN_SIZE of 15, and a DECIMAL_DIGITS of NULL.
     * If it is 2, the values in COLUMN_SIZE and DECIMAL_DIGITS give the number of bits
     * allowed in the column. For example, a FLOAT column could return a RADIX of 2,
     * a COLUMN_SIZE of 53, and a DECIMAL_DIGITS of NULL. NULL is returned for data
     * types where NUM_PREC_RADIX is not applicable.
     */
    public Integer getNumPrecRadix() {
        if (!isScalarType()) {
            return null;
        }
        ScalarType t = (ScalarType) this;
        switch (t.getPrimitiveType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return 10;
            default:
                // everything else (including boolean and string) is null
                return null;
        }
    }

    /**
     * Matrix that records "smallest" assignment-compatible type of two types
     * (INVALID_TYPE if no such type exists, ie, if the input types are fundamentally
     * incompatible). A value of any of the two types could be assigned to a slot
     * of the assignment-compatible type. For strict compatibility, this can be done
     * without any loss of precision. For non-strict compatibility, there may be loss of
     * precision, e.g. if converting from BIGINT to FLOAT.
     *
     * We chose not to follow MySQL's type casting behavior as described here:
     * http://dev.mysql.com/doc/refman/5.0/en/type-conversion.html
     * for the following reasons:
     * conservative casting in arithmetic exprs: TINYINT + TINYINT -> BIGINT
     * comparison of many types as double: INT < FLOAT -> comparison as DOUBLE
     * special cases when dealing with dates and timestamps.
     */
    protected static PrimitiveType[][] compatibilityMatrix;

    /**
     * If we are checking in strict mode, any non-null entry in this matrix overrides
     * compatibilityMatrix. If the entry is null, the entry in compatibility matrix
     * is valid.
     */
    protected static PrimitiveType[][] strictCompatibilityMatrix;

    static {
        compatibilityMatrix = new
                PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];
        strictCompatibilityMatrix = new
                PrimitiveType[PrimitiveType.values().length][PrimitiveType.values().length];

        for (int i = 0; i < PrimitiveType.values().length; ++i) {
            // Each type is compatible with itself.
            compatibilityMatrix[i][i] = PrimitiveType.values()[i];
        }

        // BOOLEAN
        compatibilityMatrix[BOOLEAN.ordinal()][TINYINT.ordinal()] = PrimitiveType.TINYINT;
        compatibilityMatrix[BOOLEAN.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        compatibilityMatrix[BOOLEAN.ordinal()][INT.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[BOOLEAN.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BOOLEAN.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[BOOLEAN.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        compatibilityMatrix[BOOLEAN.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BOOLEAN.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BOOLEAN.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;

        // TINYINT
        compatibilityMatrix[TINYINT.ordinal()][SMALLINT.ordinal()] = PrimitiveType.SMALLINT;
        compatibilityMatrix[TINYINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[TINYINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[TINYINT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        // 8 bit integer fits in mantissa of both float and double.
        compatibilityMatrix[TINYINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        compatibilityMatrix[TINYINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[TINYINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[TINYINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[TINYINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TINYINT.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // SMALLINT
        compatibilityMatrix[SMALLINT.ordinal()][INT.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[SMALLINT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[SMALLINT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        // 16 bit integer fits in mantissa of both float and double.
        compatibilityMatrix[SMALLINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        compatibilityMatrix[SMALLINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[SMALLINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[SMALLINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[SMALLINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[SMALLINT.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // INT
        compatibilityMatrix[INT.ordinal()][BIGINT.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[INT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        // 32 bit integer fits only mantissa of double.
        // TODO: arguably we should promote INT + FLOAT to DOUBLE to avoid loss of precision,
        // but we depend on it remaining FLOAT for some use cases, e.g.
        // "insert into tbl (float_col) select int_col + float_col from ..."
        compatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        strictCompatibilityMatrix[INT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][DATE.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[INT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DATEV2.ordinal()] = PrimitiveType.INT;
        compatibilityMatrix[INT.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[INT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[INT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[INT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[INT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[INT.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // BIGINT
        // 64 bit integer does not fit in mantissa of double or float.
        // TODO: arguably we should always promote BIGINT + FLOAT to double here to keep as
        // much precision as possible, but we depend on this implicit cast for some use
        // cases, similarly to INT + FLOAT.
        compatibilityMatrix[BIGINT.ordinal()][LARGEINT.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.FLOAT;
        strictCompatibilityMatrix[BIGINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        // TODO: we're breaking the definition of strict compatibility for BIGINT + DOUBLE,
        // but this forces function overloading to consider the DOUBLE overload first.
        compatibilityMatrix[BIGINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][DATE.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][DATEV2.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.BIGINT;
        compatibilityMatrix[BIGINT.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[BIGINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[BIGINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[BIGINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BIGINT.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // LARGEINT
        compatibilityMatrix[LARGEINT.ordinal()][FLOAT.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][DATE.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][DATETIME.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][DATEV2.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.LARGEINT;
        compatibilityMatrix[LARGEINT.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[LARGEINT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[LARGEINT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[LARGEINT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[LARGEINT.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // FLOAT
        compatibilityMatrix[FLOAT.ordinal()][DOUBLE.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DATETIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[FLOAT.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[FLOAT.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[FLOAT.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[FLOAT.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DOUBLE
        compatibilityMatrix[DOUBLE.ordinal()][DATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DATETIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DOUBLE.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DOUBLE.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][TIME.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DOUBLE.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DOUBLE;
        compatibilityMatrix[DOUBLE.ordinal()][TIMEV2.ordinal()] = PrimitiveType.DOUBLE;

        // DATE
        compatibilityMatrix[DATE.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIME;
        compatibilityMatrix[DATE.ordinal()][DATEV2.ordinal()] = PrimitiveType.DATEV2;
        compatibilityMatrix[DATE.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATE.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DATE.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DATE.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATE.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DATE.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATE.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATEV2
        compatibilityMatrix[DATEV2.ordinal()][DATE.ordinal()] = PrimitiveType.DATEV2;
        compatibilityMatrix[DATEV2.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATEV2.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATEV2.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DATEV2.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DATEV2.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATEV2.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DATEV2.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATEV2.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATETIME
        compatibilityMatrix[DATETIME.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATETIME.ordinal()][DATEV2.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATETIME.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATETIME.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DATETIME.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIME.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DATETIMEV2
        compatibilityMatrix[DATETIMEV2.ordinal()][CHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][DATETIME.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATETIMEV2.ordinal()][DATEV2.ordinal()] = PrimitiveType.DATETIMEV2;
        compatibilityMatrix[DATETIMEV2.ordinal()][VARCHAR.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DATETIMEV2.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATETIMEV2.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DATETIMEV2.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DATETIMEV2.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DATETIMEV2.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // We can convert some but not all string values to timestamps.
        // CHAR
        compatibilityMatrix[CHAR.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;
        compatibilityMatrix[CHAR.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[CHAR.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
        compatibilityMatrix[CHAR.ordinal()][JSONB.ordinal()] = PrimitiveType.CHAR;
        compatibilityMatrix[CHAR.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        // VARCHAR
        compatibilityMatrix[VARCHAR.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[VARCHAR.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
        compatibilityMatrix[VARCHAR.ordinal()][JSONB.ordinal()] = PrimitiveType.VARCHAR;
        compatibilityMatrix[VARCHAR.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;

        //String
        compatibilityMatrix[STRING.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[STRING.ordinal()][JSONB.ordinal()] = PrimitiveType.STRING;

        //JSONB
        compatibilityMatrix[JSONB.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[JSONB.ordinal()][STRING.ordinal()] = PrimitiveType.STRING;
        compatibilityMatrix[JSONB.ordinal()][CHAR.ordinal()] = PrimitiveType.CHAR;
        compatibilityMatrix[JSONB.ordinal()][VARCHAR.ordinal()] = PrimitiveType.VARCHAR;

        // DECIMALV2
        compatibilityMatrix[DECIMALV2.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][DATEV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DECIMALV2.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DECIMALV2;
        compatibilityMatrix[DECIMALV2.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMALV2.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;

        // DECIMAL32
        compatibilityMatrix[DECIMAL32.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][DATEV2.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DECIMAL32.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DECIMAL32;
        compatibilityMatrix[DECIMAL32.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL32.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;

        // DECIMAL64
        compatibilityMatrix[DECIMAL64.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][DATEV2.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL64.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL64.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL64;
        compatibilityMatrix[DECIMAL64.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.DECIMAL128;

        // DECIMAL128
        compatibilityMatrix[DECIMAL128.ordinal()][HLL.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][DATEV2.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMAL128.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMAL128.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMALV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.DECIMAL128;
        compatibilityMatrix[DECIMAL128.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.DECIMAL128;


        // HLL
        compatibilityMatrix[HLL.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][BITMAP.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[HLL.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;


        // BITMAP
        compatibilityMatrix[BITMAP.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][QUANTILE_STATE.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[BITMAP.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;

        //QUANTILE_STATE
        compatibilityMatrix[QUANTILE_STATE.ordinal()][JSONB.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[QUANTILE_STATE.ordinal()][STRING.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[QUANTILE_STATE.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[QUANTILE_STATE.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[QUANTILE_STATE.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[QUANTILE_STATE.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[QUANTILE_STATE.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;


        // TIME why here not???
        compatibilityMatrix[TIME.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIME.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIME.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIME.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIME.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIME.ordinal()][DATEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIME.ordinal()][DATETIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;

        compatibilityMatrix[TIMEV2.ordinal()][TIMEV2.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIMEV2.ordinal()][TIME.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIMEV2.ordinal()][DECIMAL32.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIMEV2.ordinal()][DECIMAL64.ordinal()] = PrimitiveType.INVALID_TYPE;
        compatibilityMatrix[TIMEV2.ordinal()][DECIMAL128.ordinal()] = PrimitiveType.INVALID_TYPE;

        // Check all of the necessary entries that should be filled.
        // ignore binary and all
        for (int i = 0; i < PrimitiveType.values().length - 2; ++i) {
            for (int j = i; j < PrimitiveType.values().length - 2; ++j) {
                PrimitiveType t1 = PrimitiveType.values()[i];
                PrimitiveType t2 = PrimitiveType.values()[j];
                // DECIMAL, NULL, and INVALID_TYPE  are handled separately.
                if (t1 == PrimitiveType.INVALID_TYPE || t2 == PrimitiveType.INVALID_TYPE
                        || t1 == PrimitiveType.NULL_TYPE || t2 == PrimitiveType.NULL_TYPE
                        || t1 == PrimitiveType.ARRAY || t2 == PrimitiveType.ARRAY
                        || t1 == PrimitiveType.DECIMALV2 || t2 == PrimitiveType.DECIMALV2
                        || t1 == PrimitiveType.TIME || t2 == PrimitiveType.TIME
                        || t1 == PrimitiveType.TIMEV2 || t2 == PrimitiveType.TIMEV2
                        || t1 == PrimitiveType.MAP || t2 == PrimitiveType.MAP
                        || t1 == PrimitiveType.STRUCT || t2 == PrimitiveType.STRUCT) {
                    continue;
                }
                Preconditions.checkNotNull(compatibilityMatrix[i][j]);
            }
        }
    }

    public Type getResultType() {
        switch (this.getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return BIGINT;
            case LARGEINT:
                return LARGEINT;
            case FLOAT:
            case DOUBLE:
                return DOUBLE;
            case DATE:
            case DATEV2:
            case DATETIME:
            case TIME:
            case CHAR:
            case VARCHAR:
            case HLL:
            case BITMAP:
            case QUANTILE_STATE:
                return VARCHAR;
            case DATETIMEV2:
                return DEFAULT_DATETIMEV2;
            case TIMEV2:
                return DEFAULT_TIMEV2;
            case DECIMALV2:
                return DECIMALV2;
            case DECIMAL32:
                return DECIMAL32;
            case DECIMAL64:
                return DECIMAL64;
            case DECIMAL128:
                return DECIMAL128;
            case STRING:
                return STRING;
            case JSONB:
                return JSONB;
            default:
                return INVALID;

        }
    }

    public static Type getCmpType(Type t1, Type t2) {
        if (t1.getPrimitiveType() == PrimitiveType.NULL_TYPE) {
            return t2;
        }
        if (t2.getPrimitiveType() == PrimitiveType.NULL_TYPE) {
            return t1;
        }

        PrimitiveType t1ResultType = t1.getResultType().getPrimitiveType();
        PrimitiveType t2ResultType = t2.getResultType().getPrimitiveType();
        if (canCompareDate(t1.getPrimitiveType(), t2.getPrimitiveType())) {
            return getDateComparisonResultType((ScalarType) t1, (ScalarType) t2);
        }

        // Following logical is compatible with MySQL.
        if (t1ResultType == PrimitiveType.VARCHAR && t2ResultType == PrimitiveType.VARCHAR) {
            return Type.VARCHAR;
        }
        if ((t1ResultType == PrimitiveType.STRING && t2ResultType == PrimitiveType.STRING)
                || (t1ResultType == PrimitiveType.STRING && t2ResultType == PrimitiveType.VARCHAR)
                || (t1ResultType == PrimitiveType.VARCHAR && t2ResultType == PrimitiveType.STRING)) {
            return Type.STRING;
        }
        // TODO(wzy): support NUMERIC/CHAR cast to JSONB
        if (t1ResultType == PrimitiveType.JSONB && t2ResultType == PrimitiveType.JSONB) {
            return Type.JSONB;
        }
        if ((t1ResultType == PrimitiveType.JSONB && t2ResultType == PrimitiveType.VARCHAR)
                || (t1ResultType == PrimitiveType.VARCHAR && t2ResultType == PrimitiveType.JSONB)) {
            return Type.VARCHAR;
        }
        if ((t1ResultType == PrimitiveType.JSONB && t2ResultType == PrimitiveType.STRING)
                || (t1ResultType == PrimitiveType.STRING && t2ResultType == PrimitiveType.JSONB)) {
            return Type.STRING;
        }

        // int family type and char family type should cast to char family type
        if ((t1ResultType.isFixedPointType() && t2ResultType.isCharFamily())
                || (t2ResultType.isFixedPointType() && t1ResultType.isCharFamily())) {
            return t1.isStringType() ?  t1 : t2;
        }

        if (t1ResultType == PrimitiveType.BIGINT && t2ResultType == PrimitiveType.BIGINT) {
            return getAssignmentCompatibleType(t1, t2, false);
        }
        if (t1ResultType.isDecimalV3Type() && t2ResultType.isDecimalV3Type()) {
            int resultPrecision = Math.max(t1.getPrecision(), t2.getPrecision());
            PrimitiveType resultDecimalType;
            if (resultPrecision <= ScalarType.MAX_DECIMAL32_PRECISION) {
                resultDecimalType = PrimitiveType.DECIMAL32;
            } else if (resultPrecision <= ScalarType.MAX_DECIMAL64_PRECISION) {
                resultDecimalType = PrimitiveType.DECIMAL64;
            } else {
                resultDecimalType = PrimitiveType.DECIMAL128;
            }
            return ScalarType.createDecimalType(resultDecimalType, resultPrecision,
                    Math.max(((ScalarType) t1).getScalarScale(), ((ScalarType) t2).getScalarScale()));
        }
        if (t1ResultType.isDecimalV3Type() || t2ResultType.isDecimalV3Type()) {
            return getAssignmentCompatibleType(t1, t2, false);
        }
        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.DECIMALV2)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.DECIMALV2)) {
            return Type.DECIMALV2;
        }
        if ((t1ResultType == PrimitiveType.BIGINT
                || t1ResultType == PrimitiveType.LARGEINT)
                && (t2ResultType == PrimitiveType.BIGINT
                || t2ResultType == PrimitiveType.LARGEINT)) {
            return Type.LARGEINT;
        }
        return Type.DOUBLE;
    }

    private static boolean canCompareDate(PrimitiveType t1, PrimitiveType t2) {
        if (t1.isDateType()) {
            if (t2.isDateType() || t2.isStringType() || t2.isIntegerType()) {
                return true;
            }
            return false;
        } else if (t2.isDateType()) {
            if (t1.isStringType() || t1.isIntegerType()) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    private static Type getDateComparisonResultType(ScalarType t1, ScalarType t2) {
        if (t1.isDate() && t2.isDate()) {
            return ScalarType.getDefaultDateType(Type.DATE);
        } else if ((t1.isDateV2() && t2.isDate()) || t1.isDate() && t2.isDateV2()) {
            return Type.DATEV2;
        } else if (t1.isDateV2() && t2.isDateV2()) {
            return Type.DATEV2;
        } else if (t1.isDatetime() && t2.isDatetime()) {
            return ScalarType.getDefaultDateType(Type.DATETIME);
        } else if (t1.isDatetime() && t2.isDatetimeV2()) {
            return t2;
        } else if (t1.isDatetimeV2() && t2.isDatetime()) {
            return t1;
        } else if (t1.isDatetimeV2() && t2.isDatetimeV2()) {
            return t1.decimalScale() > t2.decimalScale() ? t1 : t2;
        } else if (t1.isDatetimeV2()) {
            return t1;
        } else if (t2.isDatetimeV2()) {
            return t2;
        } else if (t2.isDateV2() || t1.isDateV2()) {
            return Type.DATETIMEV2;
        } else {
            return ScalarType.getDefaultDateType(Type.DATETIME);
        }
    }

    public Type getMaxResolutionType() {
        Preconditions.checkState(true, "must implemented");
        return null;
    }

    public Type getNumResultType() {
        switch (getPrimitiveType()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case DATE:
            case DATEV2:
            case DATETIME:
                return Type.BIGINT;
            case LARGEINT:
                return Type.LARGEINT;
            case FLOAT:
            case DOUBLE:
            case TIME:
            case CHAR:
            case VARCHAR:
            case STRING:
            case HLL:
                return Type.DOUBLE;
            case DATETIMEV2:
                return Type.DEFAULT_DATETIMEV2;
            case TIMEV2:
                return Type.DEFAULT_TIMEV2;
            case DECIMALV2:
                return Type.DECIMALV2;
            case DECIMAL32:
                return Type.DECIMAL32;
            case DECIMAL64:
                return Type.DECIMAL64;
            case DECIMAL128:
                return Type.DECIMAL128;
            default:
                return Type.INVALID;

        }
    }

    public int getStorageLayoutBytes() {
        return 0;
    }

    public int getIndexSize() {
        if (this.getPrimitiveType() == PrimitiveType.CHAR) {
            return ((ScalarType) this).getLength();
        } else {
            return this.getPrimitiveType().getOlapColumnIndexSize();
        }
    }

    // Whether `type1` matches the exact type of `type2`.
    public static boolean matchExactType(Type type1, Type type2) {
        if (type1.matchesType(type2)) {
            if (PrimitiveType.typeWithPrecision.contains(type2.getPrimitiveType())) {
                // For types which has precision and scale, we also need to check quality between precisions and scales
                if ((((ScalarType) type2).decimalPrecision()
                        == ((ScalarType) type1).decimalPrecision()) && (((ScalarType) type2).decimalScale()
                        == ((ScalarType) type1).decimalScale())) {
                    return true;
                }
            } else if (type2.isArrayType()) {
                // For types array, we also need to check contains null for case like
                // cast(array<not_null(int)> as array<int>)
                if (((ArrayType) type2).getContainsNull() == ((ArrayType) type1).getContainsNull()) {
                    return true;
                }
            } else {
                return true;
            }
        }
        return false;
    }
}

