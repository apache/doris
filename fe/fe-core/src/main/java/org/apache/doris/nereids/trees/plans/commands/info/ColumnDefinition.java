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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
        if (Config.disable_nested_complex_type && isNestedComplexType(type)) {
            throw new AnalysisException("Unsupported data type: " + type.toSql());
        }
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
        if (type.isHllType() || type.isQuantileStateType()) {
            if (aggType == null) {
                throw new AnalysisException("column: " + name + " must be used in AGG_KEYS.");
            }
            isNullable = false;
        }
        if (type.isBitmapType()) {
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

        if (type.isObjectType()) {
            if (!type.isBitmapType()) {
                if (keysType != KeysType.AGG_KEYS) {
                    throw new AnalysisException("column:" + name + " must be used in AGG_KEYS.");
                }
            }
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
