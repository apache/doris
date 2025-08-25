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
import org.apache.doris.analysis.ColumnNullableType;
import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

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
    private long autoIncInitValue = -1;
    private int clusterKeyId = -1;
    private Optional<GeneratedColumnDesc> generatedColumnDesc = Optional.empty();
    private Set<String> generatedColumnsThatReferToThis = new HashSet<>();
    // if add hidden column, must set enableAddHiddenColumn true
    private boolean enableAddHiddenColumn = false;

    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType, boolean isNullable,
            Optional<DefaultValue> defaultValue, String comment) {
        this(name, type, isKey, aggType, isNullable, defaultValue, comment, true);
    }

    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType,
            ColumnNullableType nullableType, long autoIncInitValue, Optional<DefaultValue> defaultValue,
            Optional<DefaultValue> onUpdateDefaultValue, String comment,
            Optional<GeneratedColumnDesc> generatedColumnDesc) {
        this(name, type, isKey, aggType, nullableType, autoIncInitValue, defaultValue, onUpdateDefaultValue,
                comment, true, generatedColumnDesc);
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
            boolean isNullable, long autoIncInitValue, Optional<DefaultValue> defaultValue,
            Optional<DefaultValue> onUpdateDefaultValue, String comment, boolean isVisible) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNullable = isNullable;
        this.autoIncInitValue = autoIncInitValue;
        this.defaultValue = defaultValue;
        this.onUpdateDefaultValue = onUpdateDefaultValue;
        this.comment = comment;
        this.isVisible = isVisible;
    }

    /**
     * constructor
     */
    public ColumnDefinition(String name, DataType type, boolean isKey, AggregateType aggType,
            ColumnNullableType nullableType, long autoIncInitValue, Optional<DefaultValue> defaultValue,
            Optional<DefaultValue> onUpdateDefaultValue, String comment, boolean isVisible,
            Optional<GeneratedColumnDesc> generatedColumnDesc) {
        this.name = name;
        this.type = type;
        this.isKey = isKey;
        this.aggType = aggType;
        this.isNullable = nullableType.getNullable(type.toCatalogDataType().getPrimitiveType());
        this.autoIncInitValue = autoIncInitValue;
        this.defaultValue = defaultValue;
        this.onUpdateDefaultValue = onUpdateDefaultValue;
        this.comment = comment;
        this.isVisible = isVisible;
        this.generatedColumnDesc = generatedColumnDesc;
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

    public boolean hasDefaultValue() {
        return defaultValue.isPresent();
    }

    public boolean isVisible() {
        return isVisible;
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        sb.append(type.toSql()).append(" ");

        if (aggType != null && aggType != AggregateType.NONE) {
            sb.append(aggType.name()).append(" ");
        }

        if (!isNullable) {
            sb.append("NOT NULL ");
        } else {
            // should append NULL to make result can be executed right.
            sb.append("NULL ");
        }

        if (autoIncInitValue != -1) {
            sb.append("AUTO_INCREMENT ");
            sb.append("(");
            sb.append(autoIncInitValue);
            sb.append(")");
        }

        if (defaultValue.isPresent()) {
            String value = defaultValue.get().getValue();
            if (value != null) {
                DefaultValueExprDef exprDef = defaultValue.get().getDefaultValueExprDef();
                if (!type.isBitmapType() && !type.isHllType()) {
                    if (exprDef != null) {
                        sb.append("DEFAULT ").append(value).append(" ");
                    } else {
                        sb.append("DEFAULT ").append("\"").append(SqlUtils.escapeQuota(value)).append("\"")
                                .append(" ");
                    }
                } else if (type.isBitmapType()) {
                    sb.append("DEFAULT ").append(exprDef.getExprName()).append(" ");
                }
            } else {
                sb.append("DEFAULT ").append("NULL").append(" ");
            }
        }
        sb.append("COMMENT \"").append(SqlUtils.escapeQuota(comment)).append("\"");

        return sb.toString();
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
            if (dataType.isStringLikeType() && !((CharacterType) dataType).isLengthSet()) {
                if (dataType instanceof CharType) {
                    return new CharType(1);
                } else if (dataType instanceof VarcharType) {
                    return new VarcharType(VarcharType.MAX_VARCHAR_LENGTH);
                }
            }
            return dataType;
        }
    }

    private void checkKeyColumnType(boolean isOlap) {
        if (isOlap) {
            if (type.isFloatLikeType()) {
                throw new AnalysisException("Float or double can not used as a key, use decimal instead.");
            } else if (type.isStringType()) {
                throw new AnalysisException("String Type should not be used in key column[" + name + "]");
            } else if (type.isArrayType()) {
                throw new AnalysisException("Array can only be used in the non-key column of"
                        + " the duplicate table at present.");
            } else if (type.isBitmapType() || type.isHllType() || type.isQuantileStateType()) {
                throw new AnalysisException("Key column can not set complex type:" + name);
            } else if (type.isJsonType()) {
                throw new AnalysisException("JsonType type should not be used in key column[" + getName() + "].");
            } else if (type.isVariantType()) {
                throw new AnalysisException("Variant type should not be used in key column[" + getName() + "].");
            } else if (type.isMapType()) {
                throw new AnalysisException("Map can only be used in the non-key column of"
                        + " the duplicate table at present.");
            } else if (type.isStructType()) {
                throw new AnalysisException("Struct can only be used in the non-key column of"
                        + " the duplicate table at present.");
            }
        }
    }

    /**
     * validate column definition and analyze
     */
    public void validate(boolean isOlap, Set<String> keysSet, Set<String> clusterKeySet, boolean isEnableMergeOnWrite,
            KeysType keysType) {
        try {
            // if enableAddHiddenColumn is true, can add hidden column.
            // So does not check if the column name starts with __DORIS_
            if (enableAddHiddenColumn) {
                FeNameFormat.checkColumnNameBypassHiddenColumn(name);
            } else {
                FeNameFormat.checkColumnName(name);
            }

            FeNameFormat.checkColumnCommentLength(comment);
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        type.validateDataType();
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
            if (isKey) {
                throw new AnalysisException("Key column can not set complex type:" + name);
            }
            if (keysType.equals(KeysType.AGG_KEYS)) {
                if (aggType == null) {
                    throw new AnalysisException("complex type have to use aggregate function: " + name);
                }
            }
            if (isNullable) {
                throw new AnalysisException("complex type column must be not nullable, column:" + name);
            }
        }

        if (keysSet.contains(name)) {
            isKey = true;
        }

        // check keys type
        if (isKey || clusterKeySet.contains(name)) {
            checkKeyColumnType(isOlap);
        }

        if (aggType != null) {
            if (isKey) {
                throw new AnalysisException(
                        String.format("Key column %s can not set aggregation type", name));
            }
            // check if aggregate type is valid
            if (aggType != AggregateType.GENERIC
                    && !aggType.checkCompatibility(type.toCatalogDataType().getPrimitiveType())) {
                throw new AnalysisException(String.format("Aggregate type %s is not compatible with primitive type %s",
                        aggType, type.toSql()));
            }
            if (aggType == AggregateType.GENERIC) {
                if (!SessionVariable.enableAggState()) {
                    throw new AnalysisException("agg state not enable, need set enable_agg_state=true");
                }
            }
        } else if (aggType == null && isOlap && !isKey) {
            Preconditions.checkState(keysType != null, "keysType is null");
            if (keysType.equals(KeysType.DUP_KEYS)) {
                aggType = AggregateType.NONE;
            } else if (keysType.equals(KeysType.UNIQUE_KEYS) && isEnableMergeOnWrite) {
                aggType = AggregateType.NONE;
            } else if (!keysType.equals(KeysType.AGG_KEYS)) {
                aggType = AggregateType.REPLACE;
            } else {
                throw new AnalysisException("should set aggregation type to non-key column in aggregate key table");
            }
        }

        if (isOlap) {
            if (!isKey) {
                if (keysType.equals(KeysType.UNIQUE_KEYS)) {
                    if (isEnableMergeOnWrite) {
                        aggTypeImplicit = false;
                    } else {
                        aggTypeImplicit = true;
                    }
                } else if (keysType.equals(KeysType.DUP_KEYS)) {
                    aggTypeImplicit = true;
                }
            }

            // If aggregate type is REPLACE_IF_NOT_NULL, we set it nullable.
            // If default value is not set, we set it NULL
            if (aggType == AggregateType.REPLACE_IF_NOT_NULL) {
                if (!isNullable) {
                    throw new AnalysisException(
                            "REPLACE_IF_NOT_NULL column must be nullable, maybe should use REPLACE, column:" + name);
                }
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
            if (defaultValue.isPresent() && isOlap && defaultValue.get() != DefaultValue.NULL_DEFAULT_VALUE
                    && !defaultValue.get().getValue().equals(DefaultValue.BITMAP_EMPTY_DEFAULT_VALUE.getValue())) {
                throw new AnalysisException("Bitmap type column default value only support "
                        + DefaultValue.BITMAP_EMPTY_DEFAULT_VALUE);
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
        } else if (type.isJsonType() || type.isVariantType()) {
            if (defaultValue.isPresent() && defaultValue.get() != DefaultValue.NULL_DEFAULT_VALUE) {
                throw new AnalysisException("Json or Variant type column default value just support null");
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
            if (isKey) {
                throw new AnalysisException(type.toCatalogDataType().getPrimitiveType()
                        + " can only be used in the non-key column at present.");
            }
            if (type.isAggStateType()) {
                if (aggType == null) {
                    throw new AnalysisException(type.toCatalogDataType().getPrimitiveType()
                            + " column must have aggregation type");
                } else {
                    if (aggType != AggregateType.GENERIC
                            && aggType != AggregateType.NONE
                            && aggType != AggregateType.REPLACE
                            && aggType != AggregateType.REPLACE_IF_NOT_NULL) {
                        throw new AnalysisException(type.toCatalogDataType().getPrimitiveType()
                                + " column can't support aggregation " + aggType);
                    }
                }
            } else {
                if (aggType != null && aggType != AggregateType.NONE && aggType != AggregateType.REPLACE
                        && aggType != AggregateType.REPLACE_IF_NOT_NULL) {
                    throw new AnalysisException(type.toCatalogDataType().getPrimitiveType()
                            + " column can't support aggregation " + aggType);
                }
            }
        }

        if (type.isTimeType()) {
            throw new AnalysisException("Time type is not supported for olap table");
        }
        validateGeneratedColumnInfo();
    }

    /**
     * translate to catalog create table stmt
     */
    public Column translateToCatalogStyle() {
        Column column = new Column(name, type.toCatalogDataType(), isKey, aggType, isNullable,
                autoIncInitValue, defaultValue.map(DefaultValue::getValue).orElse(null), comment, isVisible,
                defaultValue.map(DefaultValue::getDefaultValueExprDef).orElse(null), Column.COLUMN_UNIQUE_ID_INIT_VALUE,
                defaultValue.map(DefaultValue::getValue).orElse(null), onUpdateDefaultValue.isPresent(),
                onUpdateDefaultValue.map(DefaultValue::getDefaultValueExprDef).orElse(null), clusterKeyId,
                generatedColumnDesc.map(GeneratedColumnDesc::translateToInfo).orElse(null),
                generatedColumnsThatReferToThis);
        column.setAggregationTypeImplicit(aggTypeImplicit);
        return column;
    }

    /**
     * translate to catalog column for schema change
     */
    public Column translateToCatalogStyleForSchemaChange() {
        Column column = new Column(name, type.toCatalogDataType(), isKey, aggType, isNullable,
                autoIncInitValue, defaultValue.map(DefaultValue::getValue).orElse(null), comment, isVisible,
                defaultValue.map(DefaultValue::getDefaultValueExprDef).orElse(null), Column.COLUMN_UNIQUE_ID_INIT_VALUE,
                defaultValue.map(DefaultValue::getRawValue).orElse(null), onUpdateDefaultValue.isPresent(),
                onUpdateDefaultValue.map(DefaultValue::getDefaultValueExprDef).orElse(null), clusterKeyId,
                generatedColumnDesc.map(GeneratedColumnDesc::translateToInfo).orElse(null),
                generatedColumnsThatReferToThis);
        column.setAggregationTypeImplicit(aggTypeImplicit);
        return column;
    }

    /**
     * add hidden column
     */
    public static ColumnDefinition newDeleteSignColumnDefinition() {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.DELETE_SIGN, TinyIntType.INSTANCE, false, null,
                false, Optional.of(new DefaultValue(DefaultValue.ZERO_NUMBER)),
                "doris delete flag hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    /**
     * add hidden column
     */
    public static ColumnDefinition newDeleteSignColumnDefinition(AggregateType aggregateType) {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.DELETE_SIGN, TinyIntType.INSTANCE, false,
                        aggregateType, false, Optional.of(new DefaultValue(DefaultValue.ZERO_NUMBER)),
                "doris delete flag hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    /**
     * add hidden column
     */
    public static ColumnDefinition newSequenceColumnDefinition(DataType type) {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.SEQUENCE_COL, type, false, null,
                true, Optional.empty(),
                "sequence column hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    /**
     * add hidden column
     */
    public static ColumnDefinition newSequenceColumnDefinition(DataType type, AggregateType aggregateType) {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.SEQUENCE_COL, type, false, aggregateType,
                true, Optional.empty(),
                "sequence column hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    /**
     * add hidden column
     */
    public static ColumnDefinition newRowStoreColumnDefinition(AggregateType aggregateType) {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.ROW_STORE_COL, StringType.INSTANCE, false,
                        aggregateType, false, Optional.of(new DefaultValue("")),
                "doris row store hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    /**
     * add hidden column
     */
    public static ColumnDefinition newVersionColumnDefinition(AggregateType aggregateType) {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.VERSION_COL, BigIntType.INSTANCE, false,
                    aggregateType, false, Optional.of(new DefaultValue(DefaultValue.ZERO_NUMBER)),
                "doris version hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    /**
     * used in CreateTableInfo.validate(), specify the default value as DefaultValue.NULL_DEFAULT_VALUE
     * becasue ColumnDefinition.validate() will check that bitmap type column don't set default value
     * and then set the default value of that column to bitmap_empty()
     */
    public static ColumnDefinition newSkipBitmapColumnDef(AggregateType aggregateType) {
        ColumnDefinition columnDefinition = new ColumnDefinition(Column.SKIP_BITMAP_COL, BitmapType.INSTANCE, false,
                aggregateType, false, Optional.of(DefaultValue.BITMAP_EMPTY_DEFAULT_VALUE),
                "doris skip bitmap hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);

        return columnDefinition;
    }

    public Optional<GeneratedColumnDesc> getGeneratedColumnDesc() {
        return generatedColumnDesc;
    }

    public long getAutoIncInitValue() {
        return autoIncInitValue;
    }

    public void addGeneratedColumnsThatReferToThis(List<String> list) {
        generatedColumnsThatReferToThis.addAll(list);
    }

    public void setEnableAddHiddenColumn(boolean enableAddHiddenColumn) {
        this.enableAddHiddenColumn = enableAddHiddenColumn;
    }

    private void validateGeneratedColumnInfo() {
        // for generated column
        if (generatedColumnDesc.isPresent()) {
            if (autoIncInitValue != -1) {
                throw new AnalysisException("Generated columns cannot be auto_increment.");
            }
            if (defaultValue.isPresent() && !defaultValue.get().equals(DefaultValue.NULL_DEFAULT_VALUE)) {
                throw new AnalysisException("Generated columns cannot have default value.");
            }
            if (onUpdateDefaultValue.isPresent()) {
                throw new AnalysisException("Generated columns cannot have on update default value.");
            }
        }
    }
}
