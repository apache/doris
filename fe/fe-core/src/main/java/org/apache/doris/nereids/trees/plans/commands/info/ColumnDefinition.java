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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * column definition
 * TODO: complex types will not work, we will support them later.
 */
public class ColumnDefinition {
    private final String name;
    private DataType type;
    private boolean isKey;
    private AggregateType aggType;
    private boolean isNullable;
    private Optional<DefaultValue> defaultValue;
    private Optional<DefaultValue> onUpdateDefaultValue = Optional.empty();
    private final String comment;
    private final boolean isVisible;
    private boolean aggTypeImplicit = false;
    private boolean isAutoInc = false;
    private int clusterKeyId = -1;

    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType, boolean isNullable,
            Optional<DefaultValue> defaultValue, String comment) {
        this(name, type, isKey, aggType, isNullable, defaultValue, comment, true);
    }

    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType,
            boolean isNullable, boolean isAutoInc, Optional<DefaultValue> defaultValue,
            Optional<DefaultValue> onUpdateDefaultValue, String comment) {
        this(name, type, isKey, aggType, isNullable, isAutoInc, defaultValue, onUpdateDefaultValue,
                comment, true);
    }

    /**
     * constructor
     */
    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType, boolean isNullable,
            Optional<DefaultValue> defaultValue, String comment, boolean isVisible) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.comment = comment;
        this.isVisible = isVisible;
    }

    /**
     * constructor
     */
    private ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType,
            boolean isNullable, boolean isAutoInc, Optional<DefaultValue> defaultValue,
            Optional<DefaultValue> onUpdateDefaultValue, String comment, boolean isVisible) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNullable = isNullable;
        this.isAutoInc = isAutoInc;
        this.defaultValue = defaultValue;
        this.onUpdateDefaultValue = onUpdateDefaultValue;
        this.comment = comment;
        this.isVisible = isVisible;
    }

    public ColumnDefinition(String name, DataType type, boolean isNullable) {
        this(name, type, false, null, isNullable, Optional.empty(), "");
    }

    public ColumnDefinition(String name, DataType type, boolean isNullable, String comment) {
        this(name, type, false, null, isNullable, Optional.empty(), comment);
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public AggregateType getAggType() {
        return aggType;
    }

    public void setAggType(AggregateType aggType) {
        this.aggType = aggType;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public boolean isKey() {
        return isKey;
    }

    public void setIsKey(boolean isKey) {
        this.isKey = isKey;
    }

    public void setClusterKeyId(int clusterKeyId) {
        this.clusterKeyId = clusterKeyId;
    }

    public boolean isAutoInc() {
        return isAutoInc;
    }

    private DataType updateCharacterTypeLength(DataType dataType) {
        if (dataType instanceof ArrayType) {
            return ArrayType.of(updateCharacterTypeLength(((ArrayType) dataType).getItemType()));
        } else if (dataType instanceof MapType) {
            DataType keyType = updateCharacterTypeLength(((MapType) dataType).getKeyType());
            DataType valueType = updateCharacterTypeLength(((MapType) dataType).getValueType());
            return MapType.of(keyType, valueType);
        } else if (dataType instanceof StructType) {
            List<StructField> structFields = ((StructType) dataType).getFields().stream()
                    .map(sf -> sf.withDataType(updateCharacterTypeLength(sf.getDataType())))
                    .collect(ImmutableList.toImmutableList());
            return new StructType(structFields);
        } else {
            if (dataType.isStringLikeType()) {
                if (dataType instanceof CharType && ((CharType) dataType).getLen() == -1) {
                    return new CharType(1);
                } else if (dataType instanceof VarcharType && ((VarcharType) dataType).getLen() == -1) {
                    return new VarcharType(VarcharType.MAX_VARCHAR_LENGTH);
                }
            }
            return dataType;
        }
    }

    /**
     * validate column definition and analyze
     */
    public void validate(boolean isOlap, Set<String> keysSet, boolean isEnableMergeOnWrite, KeysType keysType) {
        validateDataType(type.toCatalogDataType());
        type = updateCharacterTypeLength(type);
        if (type.isArrayType()) {
            int depth = 0;
            DataType curType = type;
            while (curType.isArrayType()) {
                curType = ((ArrayType) curType).getItemType();
                depth++;
            }
            if (depth > 9) {
                throw new AnalysisException("Type exceeds the maximum nesting depth of 9");
            }
        }
        if (type.isHllType() || type.isQuantileStateType() || type.isBitmapType()) {
            if (aggType != null) {
                isNullable = false;
            }
        }

        // check keys type
        if (keysSet.contains(name)) {
            isKey = true;
            if (aggType != null) {
                throw new AnalysisException(
                        String.format("Key column %s can not set aggregation type", name));
            }
            if (isOlap) {
                if (type.isFloatLikeType()) {
                    throw new AnalysisException(
                            "Float or double can not used as a key, use decimal instead.");
                } else if (type.isStringType()) {
                    throw new AnalysisException(
                            "String Type should not be used in key column[" + name + "]");
                } else if (type.isArrayType()) {
                    throw new AnalysisException("Array can only be used in the non-key column of"
                            + " the duplicate table at present.");
                }
            }
            if (type.isBitmapType() || type.isHllType() || type.isQuantileStateType()) {
                throw new AnalysisException("Key column can not set complex type:" + name);
            } else if (type.isJsonType()) {
                throw new AnalysisException(
                        "JsonType type should not be used in key column[" + getName() + "].");
            } else if (type.isMapType()) {
                throw new AnalysisException("Map can only be used in the non-key column of"
                        + " the duplicate table at present.");
            } else if (type.isStructType()) {
                throw new AnalysisException("Struct can only be used in the non-key column of"
                        + " the duplicate table at present.");
            }
        } else if (aggType == null && isOlap) {
            Preconditions.checkState(keysType != null, "keysType is null");
            if (keysType.equals(KeysType.DUP_KEYS)) {
                aggType = AggregateType.NONE;
            } else if (keysType.equals(KeysType.UNIQUE_KEYS) && isEnableMergeOnWrite) {
                aggType = AggregateType.NONE;
            } else if (!keysType.equals(KeysType.AGG_KEYS)) {
                aggType = AggregateType.REPLACE;
            } else {
                throw new AnalysisException("should set aggregation type to non-key column when in aggregate key");
            }
        }

        if (isOlap) {
            if (!isKey && keysType.equals(KeysType.UNIQUE_KEYS)) {
                aggTypeImplicit = true;
            }

            // If aggregate type is REPLACE_IF_NOT_NULL, we set it nullable.
            // If default value is not set, we set it NULL
            if (aggType == AggregateType.REPLACE_IF_NOT_NULL) {
                isNullable = true;
                if (!defaultValue.isPresent()) {
                    defaultValue = Optional.of(DefaultValue.NULL_DEFAULT_VALUE);
                }
            }
        }

        // check default value
        if (type.isHllType()) {
            if (defaultValue.isPresent()) {
                throw new AnalysisException("Hll type column can not set default value");
            }
            defaultValue = Optional.of(DefaultValue.HLL_EMPTY_DEFAULT_VALUE);
        } else if (type.isBitmapType()) {
            if (defaultValue.isPresent() && defaultValue.get() != DefaultValue.NULL_DEFAULT_VALUE) {
                throw new AnalysisException("Bitmap type column can not set default value");
            }
            defaultValue = Optional.of(DefaultValue.BITMAP_EMPTY_DEFAULT_VALUE);
        } else if (type.isArrayType() && defaultValue.isPresent() && isOlap
                && defaultValue.get() != DefaultValue.NULL_DEFAULT_VALUE && !defaultValue.get()
                        .getValue().equals(DefaultValue.ARRAY_EMPTY_DEFAULT_VALUE.getValue())) {
            throw new AnalysisException("Array type column default value only support null or "
                    + DefaultValue.ARRAY_EMPTY_DEFAULT_VALUE);
        } else if (type.isMapType()) {
            if (defaultValue.isPresent() && defaultValue.get() != DefaultValue.NULL_DEFAULT_VALUE) {
                throw new AnalysisException("Map type column default value just support null");
            }
        } else if (type.isStructType()) {
            if (defaultValue.isPresent() && defaultValue.get() != DefaultValue.NULL_DEFAULT_VALUE) {
                throw new AnalysisException("Struct type column default value just support null");
            }
        }

        if (!isNullable && defaultValue.isPresent()
                && defaultValue.get() == DefaultValue.NULL_DEFAULT_VALUE) {
            throw new AnalysisException(
                    "Can not set null default value to non nullable column: " + name);
        }

        if (defaultValue.isPresent()
                && defaultValue.get().getValue() != null
                && type.toCatalogDataType().isScalarType()) {
            try {
                ColumnDef.validateDefaultValue(type.toCatalogDataType(),
                        defaultValue.get().getValue(), defaultValue.get().getDefaultValueExprDef());
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage(), e);
            }
        }
        if (onUpdateDefaultValue.isPresent()
                && onUpdateDefaultValue.get().getValue() != null
                && type.toCatalogDataType().isScalarType()) {
            try {
                ColumnDef.validateDefaultValue(type.toCatalogDataType(),
                        onUpdateDefaultValue.get().getValue(), onUpdateDefaultValue.get().getDefaultValueExprDef());
            } catch (Exception e) {
                throw new AnalysisException("meet error when validating the on update value of column["
                        + name + "], reason: " + e.getMessage());
            }
            if (onUpdateDefaultValue.get().isCurrentTimeStamp()) {
                if (!defaultValue.isPresent() || !defaultValue.get().isCurrentTimeStamp()) {
                    throw new AnalysisException("You must set the default value of the column["
                            + name + "] to CURRENT_TIMESTAMP when using 'ON UPDATE CURRENT_TIMESTAMP'.");
                }
            } else if (onUpdateDefaultValue.get().isCurrentTimeStampWithPrecision()) {
                if (!defaultValue.isPresent() || !defaultValue.get().isCurrentTimeStampWithPrecision()) {
                    throw new AnalysisException("You must set the default value of the column["
                            + name + "] to CURRENT_TIMESTAMP when using 'ON UPDATE CURRENT_TIMESTAMP'.");
                }
                long precision1 = onUpdateDefaultValue.get().getCurrentTimeStampPrecision();
                long precision2 = defaultValue.get().getCurrentTimeStampPrecision();
                if (precision1 != precision2) {
                    throw new AnalysisException("The precision of the default value of column["
                            + name + "] should be the same with the precision in 'ON UPDATE CURRENT_TIMESTAMP'.");
                }
            }
        }

        // from old planner CreateTableStmt's analyze method, after call columnDef.analyze(engineName.equals("olap"));
        if (isOlap && type.isComplexType()) {
            if (aggType != null && aggType != AggregateType.NONE
                    && aggType != AggregateType.REPLACE) {
                throw new AnalysisException(type.toCatalogDataType().getPrimitiveType()
                        + " column can't support aggregation " + aggType);
            }
            if (isKey) {
                throw new AnalysisException(type.toCatalogDataType().getPrimitiveType()
                        + " can only be used in the non-key column of the duplicate table at present.");
            }
        }

        if (type.isTimeLikeType()) {
            throw new AnalysisException("Time type is not supported for olap table");
        }
    }

    /**
     * check if is nested complex type.
     */
    private boolean isNestedComplexType(DataType dataType) {
        if (!dataType.isComplexType()) {
            return false;
        }
        if (dataType instanceof ArrayType) {
            if (((ArrayType) dataType).getItemType() instanceof ArrayType) {
                return isNestedComplexType(((ArrayType) dataType).getItemType());
            } else {
                return ((ArrayType) dataType).getItemType().isComplexType();
            }
        }
        if (dataType instanceof MapType) {
            return ((MapType) dataType).getKeyType().isComplexType()
                    || ((MapType) dataType).getValueType().isComplexType();
        }
        if (dataType instanceof StructType) {
            return ((StructType) dataType).getFields().stream().anyMatch(f -> f.getDataType().isComplexType());
        }
        return false;
    }

    // from TypeDef.java analyze()
    private void validateDataType(Type catalogType) {
        if (catalogType.exceedsMaxNestingDepth()) {
            throw new AnalysisException(
                    String.format("Type exceeds the maximum nesting depth of %s:\n%s",
                            Type.MAX_NESTING_DEPTH, catalogType.toSql()));
        }
        if (!catalogType.isSupported()) {
            throw new AnalysisException("Unsupported data type: " + catalogType.toSql());
        }

        if (catalogType.isScalarType()) {
            validateScalarType((ScalarType) catalogType);
        } else if (catalogType.isComplexType()) {
            // now we not support array / map / struct nesting complex type
            if (catalogType.isArrayType()) {
                Type itemType = ((org.apache.doris.catalog.ArrayType) catalogType).getItemType();
                if (itemType instanceof ScalarType) {
                    validateNestedType(catalogType, (ScalarType) itemType);
                } else if (Config.disable_nested_complex_type
                        && !(itemType instanceof org.apache.doris.catalog.ArrayType)) {
                    // now we can array nesting array
                    throw new AnalysisException(
                            "Unsupported data type: ARRAY<" + itemType.toSql() + ">");
                }
            }
            if (catalogType.isMapType()) {
                org.apache.doris.catalog.MapType mt =
                        (org.apache.doris.catalog.MapType) catalogType;
                if (Config.disable_nested_complex_type && (!(mt.getKeyType() instanceof ScalarType)
                        || !(mt.getValueType() instanceof ScalarType))) {
                    throw new AnalysisException("Unsupported data type: MAP<"
                            + mt.getKeyType().toSql() + "," + mt.getValueType().toSql() + ">");
                }
                if (mt.getKeyType() instanceof ScalarType) {
                    validateNestedType(catalogType, (ScalarType) mt.getKeyType());
                }
                if (mt.getValueType() instanceof ScalarType) {
                    validateNestedType(catalogType, (ScalarType) mt.getValueType());
                }
            }
            if (catalogType.isStructType()) {
                ArrayList<org.apache.doris.catalog.StructField> fields =
                        ((org.apache.doris.catalog.StructType) catalogType).getFields();
                Set<String> fieldNames = new HashSet<>();
                for (org.apache.doris.catalog.StructField field : fields) {
                    Type fieldType = field.getType();
                    if (fieldType instanceof ScalarType) {
                        validateNestedType(catalogType, (ScalarType) fieldType);
                        if (!fieldNames.add(field.getName())) {
                            throw new AnalysisException("Duplicate field name " + field.getName()
                                    + " in struct " + catalogType.toSql());
                        }
                    } else if (Config.disable_nested_complex_type) {
                        throw new AnalysisException(
                                "Unsupported field type: " + fieldType.toSql() + " for STRUCT");
                    }
                }
            }
        }
    }

    private void validateScalarType(ScalarType scalarType) {
        PrimitiveType type = scalarType.getPrimitiveType();
        // When string type length is not assigned, it needs to be assigned to 1.
        if (scalarType.getPrimitiveType().isStringType() && !scalarType.isLengthSet()) {
            if (scalarType.getPrimitiveType() == PrimitiveType.VARCHAR) {
                // always set varchar length MAX_VARCHAR_LENGTH
                scalarType.setLength(ScalarType.MAX_VARCHAR_LENGTH);
            } else if (scalarType.getPrimitiveType() == PrimitiveType.STRING) {
                // always set text length MAX_STRING_LENGTH
                scalarType.setLength(ScalarType.MAX_STRING_LENGTH);
            } else {
                scalarType.setLength(1);
            }
        }
        switch (type) {
            case CHAR:
            case VARCHAR: {
                String name;
                int maxLen;
                if (type == PrimitiveType.VARCHAR) {
                    name = "VARCHAR";
                    maxLen = ScalarType.MAX_VARCHAR_LENGTH;
                } else {
                    name = "CHAR";
                    maxLen = ScalarType.MAX_CHAR_LENGTH;
                    return;
                }
                int len = scalarType.getLength();
                // len is decided by child, when it is -1.

                if (len <= 0) {
                    throw new AnalysisException(name + " size must be > 0: " + len);
                }
                if (scalarType.getLength() > maxLen) {
                    throw new AnalysisException(name + " size must be <= " + maxLen + ": " + len);
                }
                break;
            }
            case DECIMALV2: {
                int precision = scalarType.decimalPrecision();
                int scale = scalarType.decimalScale();
                // precision: [1, 27]
                if (precision < 1 || precision > ScalarType.MAX_DECIMALV2_PRECISION) {
                    throw new AnalysisException("Precision of decimal must between 1 and 27."
                            + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > ScalarType.MAX_DECIMALV2_SCALE) {
                    throw new AnalysisException("Scale of decimal must between 0 and 9."
                            + " Scale was set to: " + scale + ".");
                }
                if (precision - scale > ScalarType.MAX_DECIMALV2_PRECISION
                        - ScalarType.MAX_DECIMALV2_SCALE) {
                    throw new AnalysisException("Invalid decimal type with precision = " + precision
                            + ", scale = " + scale);
                }
                // scale < precision
                if (scale > precision) {
                    throw new AnalysisException("Scale of decimal must be smaller than precision."
                            + " Scale is " + scale + " and precision is " + precision);
                }
                break;
            }
            case DECIMAL32: {
                int decimal32Precision = scalarType.decimalPrecision();
                int decimal32Scale = scalarType.decimalScale();
                if (decimal32Precision < 1
                        || decimal32Precision > ScalarType.MAX_DECIMAL32_PRECISION) {
                    throw new AnalysisException("Precision of decimal must between 1 and 9."
                            + " Precision was set to: " + decimal32Precision + ".");
                }
                // scale >= 0
                if (decimal32Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0."
                            + " Scale was set to: " + decimal32Scale + ".");
                }
                // scale < precision
                if (decimal32Scale > decimal32Precision) {
                    throw new AnalysisException(
                            "Scale of decimal must be smaller than precision." + " Scale is "
                                    + decimal32Scale + " and precision is " + decimal32Precision);
                }
                break;
            }
            case DECIMAL64: {
                int decimal64Precision = scalarType.decimalPrecision();
                int decimal64Scale = scalarType.decimalScale();
                if (decimal64Precision < 1
                        || decimal64Precision > ScalarType.MAX_DECIMAL64_PRECISION) {
                    throw new AnalysisException("Precision of decimal64 must between 1 and 18."
                            + " Precision was set to: " + decimal64Precision + ".");
                }
                // scale >= 0
                if (decimal64Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0."
                            + " Scale was set to: " + decimal64Scale + ".");
                }
                // scale < precision
                if (decimal64Scale > decimal64Precision) {
                    throw new AnalysisException(
                            "Scale of decimal must be smaller than precision." + " Scale is "
                                    + decimal64Scale + " and precision is " + decimal64Precision);
                }
                break;
            }
            case DECIMAL128: {
                int decimal128Precision = scalarType.decimalPrecision();
                int decimal128Scale = scalarType.decimalScale();
                if (decimal128Precision < 1
                        || decimal128Precision > ScalarType.MAX_DECIMAL128_PRECISION) {
                    throw new AnalysisException("Precision of decimal128 must between 1 and 38."
                            + " Precision was set to: " + decimal128Precision + ".");
                }
                // scale >= 0
                if (decimal128Scale < 0) {
                    throw new AnalysisException("Scale of decimal must not be less than 0."
                            + " Scale was set to: " + decimal128Scale + ".");
                }
                // scale < precision
                if (decimal128Scale > decimal128Precision) {
                    throw new AnalysisException(
                            "Scale of decimal must be smaller than precision." + " Scale is "
                                    + decimal128Scale + " and precision is " + decimal128Precision);
                }
                break;
            }
            case DECIMAL256: {
                if (SessionVariable.getEnableDecimal256()) {
                    int precision = scalarType.decimalPrecision();
                    int scale = scalarType.decimalScale();
                    if (precision < 1 || precision > ScalarType.MAX_DECIMAL256_PRECISION) {
                        throw new AnalysisException("Precision of decimal256 must between 1 and 76."
                                + " Precision was set to: " + precision + ".");
                    }
                    // scale >= 0
                    if (scale < 0) {
                        throw new AnalysisException("Scale of decimal must not be less than 0."
                                + " Scale was set to: " + scale + ".");
                    }
                    // scale < precision
                    if (scale > precision) {
                        throw new AnalysisException(
                                "Scale of decimal must be smaller than precision." + " Scale is "
                                        + scale + " and precision is " + precision);
                    }
                    break;
                } else {
                    int precision = scalarType.decimalPrecision();
                    throw new AnalysisException("Column of type Decimal256 with precision "
                            + precision + " in not supported.");
                }
            }
            case TIMEV2:
            case DATETIMEV2: {
                int precision = scalarType.decimalPrecision();
                int scale = scalarType.decimalScale();
                // precision: [1, 27]
                if (precision != ScalarType.DATETIME_PRECISION) {
                    throw new AnalysisException(
                            "Precision of Datetime/Time must be " + ScalarType.DATETIME_PRECISION
                                    + "." + " Precision was set to: " + precision + ".");
                }
                // scale: [0, 9]
                if (scale < 0 || scale > 6) {
                    throw new AnalysisException("Scale of Datetime/Time must between 0 and 6."
                            + " Scale was set to: " + scale + ".");
                }
                break;
            }
            case INVALID_TYPE:
                throw new AnalysisException("Invalid type.");
            default:
                break;
        }
    }

    private void validateNestedType(Type parent, Type child) throws AnalysisException {
        if (child.isNull()) {
            throw new AnalysisException("Unsupported data type: " + child.toSql());
        }
        // check whether the sub-type is supported
        if (!parent.supportSubType(child)) {
            throw new AnalysisException(
                    parent.getPrimitiveType() + " unsupported sub-type: " + child.toSql());
        }
        validateDataType(child);
    }

    /**
     * translate to catalog create table stmt
     */
    public Column translateToCatalogStyle() {
        Column column = new Column(name, type.toCatalogDataType(), isKey, aggType, isNullable,
                isAutoInc, defaultValue.map(DefaultValue::getRawValue).orElse(null), comment, isVisible,
                defaultValue.map(DefaultValue::getDefaultValueExprDef).orElse(null), Column.COLUMN_UNIQUE_ID_INIT_VALUE,
                defaultValue.map(DefaultValue::getValue).orElse(null), onUpdateDefaultValue.isPresent(),
                onUpdateDefaultValue.map(DefaultValue::getDefaultValueExprDef).orElse(null), clusterKeyId);
        column.setAggregationTypeImplicit(aggTypeImplicit);
        return column;
    }

    // hidden column
    public static ColumnDefinition newDeleteSignColumnDefinition() {
        return new ColumnDefinition(Column.DELETE_SIGN, TinyIntType.INSTANCE, false, null, false,
                Optional.of(new DefaultValue(DefaultValue.ZERO_NUMBER)), "doris delete flag hidden column", false);
    }

    public static ColumnDefinition newDeleteSignColumnDefinition(AggregateType aggregateType) {
        return new ColumnDefinition(Column.DELETE_SIGN, TinyIntType.INSTANCE, false, aggregateType, false,
                Optional.of(new DefaultValue(DefaultValue.ZERO_NUMBER)), "doris delete flag hidden column", false);
    }

    public static ColumnDefinition newSequenceColumnDefinition(DataType type) {
        return new ColumnDefinition(Column.SEQUENCE_COL, type, false, null, true,
                Optional.empty(), "sequence column hidden column", false);
    }

    public static ColumnDefinition newSequenceColumnDefinition(DataType type, AggregateType aggregateType) {
        return new ColumnDefinition(Column.SEQUENCE_COL, type, false, aggregateType, true,
                Optional.empty(), "sequence column hidden column", false);
    }

    public static ColumnDefinition newRowStoreColumnDefinition(AggregateType aggregateType) {
        return new ColumnDefinition(Column.ROW_STORE_COL, StringType.INSTANCE, false, aggregateType, false,
                Optional.of(new DefaultValue("")), "doris row store hidden column", false);
    }

    public static ColumnDefinition newVersionColumnDefinition(AggregateType aggregateType) {
        return new ColumnDefinition(Column.VERSION_COL, BigIntType.INSTANCE, false, aggregateType, false,
                Optional.of(new DefaultValue(DefaultValue.ZERO_NUMBER)), "doris version hidden column", false);
    }

}
