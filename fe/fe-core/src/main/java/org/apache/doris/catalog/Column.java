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

import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class represents the column-related metadata.
 */
public class Column implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Column.class);
    public static final String HIDDEN_COLUMN_PREFIX = "__DORIS_";
    // all shadow indexes should have this prefix in name
    public static final String SHADOW_NAME_PREFIX = "__doris_shadow_";
    // NOTE: you should name hidden column start with '__DORIS_' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    public static final String DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    public static final String WHERE_SIGN = "__DORIS_WHERE_SIGN__";
    public static final String SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";
    public static final String ROWID_COL = "__DORIS_ROWID_COL__";
    public static final String GLOBAL_ROWID_COL = "__DORIS_GLOBAL_ROWID_COL__";
    public static final String ROW_STORE_COL = "__DORIS_ROW_STORE_COL__";
    public static final String VERSION_COL = "__DORIS_VERSION_COL__";
    public static final String SKIP_BITMAP_COL = "__DORIS_SKIP_BITMAP_COL__";
    public static final String ICEBERG_ROWID_COL = "__DORIS_ICEBERG_ROWID_COL__";
    // table stream columns
    public static final String STREAM_CHANGE_TYPE_COL = "__DORIS_STREAM_CHANGE_TYPE_COL__";
    public static final String STREAM_SEQ_COL = "__DORIS_STREAM_SEQUENCE_COL__";
    // NOTE: you should name hidden column start with '__DORIS_' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    private static final String COLUMN_ARRAY_CHILDREN = "item";
    private static final String COLUMN_AGG_ARGUMENT_CHILDREN = "argument";
    public static final int COLUMN_UNIQUE_ID_INIT_VALUE = -1;
    private static final String COLUMN_MAP_KEY = "key";
    private static final String COLUMN_MAP_VALUE = "value";

    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "type")
    private Type type;
    // column is key: aggregate type is null
    // column is not key and has no aggregate type: aggregate type is none
    // column is not key and has aggregate type: aggregate type is name of aggregate function.
    @SerializedName(value = "aggregationType")
    private AggregateType aggregationType;

    // if isAggregationTypeImplicit is true, the actual aggregation type will not be shown in show create table
    // the key type of table is duplicate or unique: the isAggregationTypeImplicit of value columns are true
    // other cases: the isAggregationTypeImplicit is false
    @SerializedName(value = "isAggregationTypeImplicit")
    private boolean isAggregationTypeImplicit;
    @SerializedName(value = "isKey")
    private boolean isKey;
    @SerializedName(value = "isAllowNull")
    private boolean isAllowNull;
    @SerializedName(value = "isAutoInc")
    private boolean isAutoInc;

    @SerializedName(value = "autoIncInitValue")
    private long autoIncInitValue;
    @SerializedName(value = "defaultValue")
    private String defaultValue;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "children")
    private List<Column> children;
    /**
     * This is similar as `defaultValue`. Differences are:
     * 1. `realDefaultValue` indicates the **default underlying literal**.
     * 2. Instead, `defaultValue` indicates the **original expression** which is specified by users.
     *
     * For example, if user create a table with (columnA, DATETIME, DEFAULT CURRENT_TIMESTAMP)
     * `realDefaultValue` here is current date time while `defaultValue` is `CURRENT_TIMESTAMP`.
     */
    @SerializedName(value = "realDefaultValue")
    private String realDefaultValue;
    // Define expr may exist in two forms, one is analyzed, and the other is not analyzed.
    // Currently, analyzed define expr is only used when creating materialized views,
    // so the define expr in RollupJob must be analyzed.
    // In other cases, such as define expr in `MaterializedIndexMeta`, it may not be analyzed after being replayed.
    private Expr defineExpr; // use to define column in materialize view
    private String defineName = null;
    @SerializedName(value = "visible")
    private boolean visible;
    @SerializedName(value = "defaultValueExprDef")
    private DefaultValueExprDef defaultValueExprDef; // used for default value

    @SerializedName(value = "uniqueId")
    private int uniqueId;

    @SerializedName(value = "clusterKeyId")
    private int clusterKeyId = -1;

    private boolean isCompoundKey = false;

    @SerializedName(value = "hasOnUpdateDefaultValue")
    private boolean hasOnUpdateDefaultValue = false;

    @SerializedName(value = "onUpdateDefaultValueExprDef")
    private DefaultValueExprDef onUpdateDefaultValueExprDef;

    @SerializedName(value = "gci")
    private GeneratedColumnInfo generatedColumnInfo;

    @SerializedName(value = "gctt")
    private Set<String> generatedColumnsThatReferToThis;

    // used for variant sub-field pattern type
    @SerializedName(value = "fpt")
    private PatternType fieldPatternType;

    // used for saving some extra information, such as timezone info of datetime column
    // Maybe deprecated if we implement real timestamp with timezone type.
    @SerializedName(value = "ei")
    private String extraInfo;

    @SerializedName(value = "sv")
    private Map<String, String> sessionVariables;

    public Column() {
        this.name = "";
        this.type = Type.NULL;
        this.isAggregationTypeImplicit = false;
        this.isKey = false;
        this.visible = true;
        this.defineExpr = null;
        this.children = null;
        this.uniqueId = -1;
        this.sessionVariables = null;
    }

    public Column(String name, PrimitiveType dataType) {
        this(name, ScalarType.createType(dataType), false, null, false, null, "");
    }

    public Column(String name, PrimitiveType dataType, boolean isAllowNull) {
        this(name, ScalarType.createType(dataType), isAllowNull);
    }

    public Column(String name, PrimitiveType dataType, int len, int precision, int scale, boolean isAllowNull) {
        this(name, ScalarType.createType(dataType, len, precision, scale), isAllowNull);
    }

    public Column(String name, Type type, boolean isAllowNull) {
        this(name, type, false, null, isAllowNull, null, "");
    }

    public Column(String name, Type type, boolean isAllowNull, String comment) {
        this(name, type, false, null, isAllowNull, null, comment);
    }

    public Column(String name, Type type) {
        this(name, type, false, null, false, null, "");
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, String defaultValue,
            String comment) {
        this(name, type, isKey, aggregateType, false, defaultValue, comment);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            String defaultValue, String comment) {
        this(name, type, isKey, aggregateType, isAllowNull, -1, defaultValue, comment, true, null,
                COLUMN_UNIQUE_ID_INIT_VALUE, defaultValue, false, null, null,
                Sets.newHashSet(), null);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            String comment, boolean visible, int colUniqueId) {
        this(name, type, isKey, aggregateType, isAllowNull, -1, null, comment, visible, null, colUniqueId, null,
                false, null, null,  Sets.newHashSet(), null);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
                  String defaultValue, String comment, boolean visible, int colUniqueId) {
        this(name, type, isKey, aggregateType, isAllowNull, -1, defaultValue, comment, visible, null, colUniqueId, null,
                false, null, null, Sets.newHashSet(), null);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            String defaultValue, String comment, boolean visible, DefaultValueExprDef defaultValueExprDef,
            int colUniqueId, String realDefaultValue) {
        this(name, type, isKey, aggregateType, isAllowNull, -1, defaultValue, comment, visible, defaultValueExprDef,
                colUniqueId, realDefaultValue, false, null, null,  Sets.newHashSet(), null);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            long autoIncInitValue, String defaultValue, String comment, boolean visible,
            DefaultValueExprDef defaultValueExprDef, int colUniqueId, String realDefaultValue) {
        this(name, type, isKey, aggregateType, isAllowNull, autoIncInitValue, defaultValue, comment, visible,
                defaultValueExprDef, colUniqueId, realDefaultValue, false, null, null, Sets.newHashSet(), null);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            long autoIncInitValue, String defaultValue, String comment, boolean visible,
            DefaultValueExprDef defaultValueExprDef, int colUniqueId, String realDefaultValue,
            boolean hasOnUpdateDefaultValue, DefaultValueExprDef onUpdateDefaultValueExprDef,
            GeneratedColumnInfo generatedColumnInfo, Set<String> generatedColumnsThatReferToThis,
            Map<String, String> sessionVariables) {
        this.name = name;
        if (this.name == null) {
            this.name = "";
        }

        this.type = type;
        if (this.type == null) {
            this.type = Type.NULL;
        }

        this.aggregationType = aggregateType;
        this.isAggregationTypeImplicit = false;
        this.isKey = isKey;
        this.isAllowNull = isAllowNull;
        this.isAutoInc = autoIncInitValue != -1;
        this.autoIncInitValue = autoIncInitValue;
        this.defaultValue = defaultValue;
        this.realDefaultValue = realDefaultValue;
        this.defaultValueExprDef = defaultValueExprDef;
        this.comment = StringUtils.isBlank(comment) ? null : comment;
        this.visible = visible;
        this.children = null;
        createChildrenColumn(this.type, this);
        this.uniqueId = colUniqueId;
        this.hasOnUpdateDefaultValue = hasOnUpdateDefaultValue;
        this.onUpdateDefaultValueExprDef = onUpdateDefaultValueExprDef;
        this.generatedColumnInfo = generatedColumnInfo;
        if (CollectionUtils.isNotEmpty(generatedColumnsThatReferToThis)) {
            this.generatedColumnsThatReferToThis = new HashSet<>(generatedColumnsThatReferToThis);
        }
        this.sessionVariables = sessionVariables;

        if (this.type.isAggStateType()) {
            AggStateType aggState = (AggStateType) (this.type);
            for (int i = 0; i < aggState.getSubTypes().size(); i++) {
                Column c = new Column(COLUMN_AGG_ARGUMENT_CHILDREN, aggState.getSubTypes().get(i));
                c.setIsAllowNull(aggState.getSubTypeNullables().get(i));
                addChildrenColumn(c);
            }
            this.isAllowNull = false;
            this.aggregationType = AggregateType.GENERIC;
        }
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType,
            boolean isAllowNull, long autoIncInitValue, String defaultValue, String comment,
            boolean visible, DefaultValueExprDef defaultValueExprDef, int colUniqueId,
            String realDefaultValue, boolean hasOnUpdateDefaultValue,
            DefaultValueExprDef onUpdateDefaultValueExprDef, int clusterKeyId,
            GeneratedColumnInfo generatedColumnInfo, Set<String> generatedColumnsThatReferToThis,
            Map<String, String> sessionVariables) {
        this(name, type, isKey, aggregateType, isAllowNull, autoIncInitValue, defaultValue, comment,
                visible, defaultValueExprDef, colUniqueId, realDefaultValue,
                hasOnUpdateDefaultValue, onUpdateDefaultValueExprDef, generatedColumnInfo,
                generatedColumnsThatReferToThis, sessionVariables);
        this.clusterKeyId = clusterKeyId;
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            long autoIncInitValue, String defaultValue, String comment, boolean visible,
            DefaultValueExprDef defaultValueExprDef, int colUniqueId, String realDefaultValue, int clusterKeyId,
            GeneratedColumnInfo generatedColumnInfo, Set<String> generatedColumnsThatReferToThis) {
        this(name, type, isKey, aggregateType, isAllowNull, autoIncInitValue, defaultValue, comment, visible,
                defaultValueExprDef, colUniqueId, realDefaultValue, false, null, generatedColumnInfo,
                generatedColumnsThatReferToThis, null);
        this.clusterKeyId = clusterKeyId;
    }

    public Column(Column column) {
        this.name = column.getName();
        this.type = column.type;
        this.aggregationType = column.getAggregationType();
        this.isAggregationTypeImplicit = column.isAggregationTypeImplicit();
        this.isKey = column.isKey();
        this.isCompoundKey = column.isCompoundKey();
        this.isAllowNull = column.isAllowNull();
        this.isAutoInc = column.isAutoInc();
        this.defaultValue = column.getDefaultValue();
        this.realDefaultValue = column.realDefaultValue;
        this.defaultValueExprDef = column.defaultValueExprDef;
        this.comment = column.getComment();
        this.visible = column.visible;
        this.children = column.getChildren();
        this.uniqueId = column.getUniqueId();
        this.defineExpr = column.getDefineExpr();
        this.defineName = column.getRealDefineName();
        this.hasOnUpdateDefaultValue = column.hasOnUpdateDefaultValue;
        this.onUpdateDefaultValueExprDef = column.onUpdateDefaultValueExprDef;
        this.clusterKeyId = column.getClusterKeyId();
        this.generatedColumnInfo = column.generatedColumnInfo;
        this.sessionVariables = column.sessionVariables;
    }

    public void createChildrenColumn(Type type, Column column) {
        if (type.isArrayType()) {
            Column c = new Column(COLUMN_ARRAY_CHILDREN, ((ArrayType) type).getItemType());
            c.setIsAllowNull(((ArrayType) type).getContainsNull());
            column.addChildrenColumn(c);
        } else if (type.isMapType()) {
            Column k = new Column(COLUMN_MAP_KEY, ((MapType) type).getKeyType());
            Column v = new Column(COLUMN_MAP_VALUE, ((MapType) type).getValueType());
            k.setIsAllowNull(((MapType) type).getIsKeyContainsNull());
            v.setIsAllowNull(((MapType) type).getIsValueContainsNull());
            column.addChildrenColumn(k);
            column.addChildrenColumn(v);
        } else if (type.isStructType()) {
            ArrayList<StructField> fields = ((StructType) type).getFields();
            for (StructField field : fields) {
                Column c = new Column(field.getName(), field.getType());
                c.setIsAllowNull(field.getContainsNull());
                column.addChildrenColumn(c);
            }
        } else if (type.isVariantType() && type instanceof VariantType) {
            // variant may contain predefined structured fields
            ArrayList<VariantField> fields = ((VariantType) type).getPredefinedFields();
            for (VariantField field : fields) {
                // set column name as pattern
                Column c = new Column(field.getPattern(), field.getType());
                c.setIsAllowNull(true);
                c.setFieldPatternType(field.getPatternType());
                column.addChildrenColumn(c);
            }
        }
    }

    public List<Column> getChildren() {
        return children;
    }

    private void addChildrenColumn(Column column) {
        if (this.children == null) {
            this.children = Lists.newArrayListWithExpectedSize(2);
        }
        this.children.add(column);
    }

    public String getDefineName() {
        if (defineName != null) {
            return defineName;
        }
        return name;
    }

    // In order for the copy constructor to get the real defineName value.
    // getDefineName() cannot meet this requirement
    public String getRealDefineName() {
        return defineName;
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return this.name;
    }

    public String getNonShadowName() {
        return removeNamePrefix(name);
    }

    public String getDisplayName() {
        return name;
    }

    public void setIsKey(boolean isKey) {
        // column is key, aggregationType is always null, isAggregationTypeImplicit is always false.
        if (isKey) {
            setAggregationType(null, false);
        }
        this.isKey = isKey;
    }

    public boolean isKey() {
        return this.isKey;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setIsVisible(boolean isVisible) {
        this.visible = isVisible;
    }

    public boolean isDeleteSignColumn() {
        // aggregationType is NONE for unique table with merge on write.
        return !visible && (aggregationType == AggregateType.REPLACE
                || aggregationType == AggregateType.NONE) && nameEquals(DELETE_SIGN, true);
    }

    public boolean isSequenceColumn() {
        // aggregationType is NONE for unique table with merge on write.
        return !visible && (aggregationType == AggregateType.REPLACE
                || aggregationType == AggregateType.NONE) && nameEquals(SEQUENCE_COL, true);
    }

    public boolean isRowStoreColumn() {
        return !visible && (aggregationType == AggregateType.REPLACE
                || aggregationType == AggregateType.NONE || aggregationType == null)
                && nameEquals(ROW_STORE_COL, true);
    }

    public boolean isVersionColumn() {
        // aggregationType is NONE for unique table with merge on write.
        return !visible && (aggregationType == AggregateType.REPLACE
                || aggregationType == AggregateType.NONE) && nameEquals(VERSION_COL, true);
    }

    public boolean isSkipBitmapColumn() {
        return !visible && (aggregationType == AggregateType.REPLACE
                || aggregationType == AggregateType.NONE || aggregationType == null)
                && nameEquals(SKIP_BITMAP_COL, true);
    }

    // now we only support BloomFilter on (same behavior with BE):
    // smallint/int/bigint/largeint
    // string/varchar/char/variant
    // date/datetime/datev2/datetimev2
    // decimal/decimal32/decimal64/decimal128I/decimal256
    // ipv4/ipv6
    public boolean isSupportBloomFilter() {
        PrimitiveType pType = getDataType();
        return (pType ==  PrimitiveType.SMALLINT || pType == PrimitiveType.INT
                || pType == PrimitiveType.BIGINT || pType == PrimitiveType.LARGEINT)
                || pType.isCharFamily() || pType.isDateLikeType() || pType.isVariantType()
                || pType.isDecimalV2Type() || pType.isDecimalV3Type() || pType.isIPType();
    }

    public PrimitiveType getDataType() {
        return type.getPrimitiveType();
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getOriginType() {
        return type;
    }

    public int getStrLen() {
        return type.getLength();
    }

    public int getPrecision() {
        return type instanceof ScalarType ? ((ScalarType) type).getScalarPrecision() : -1;
    }

    public int getScale() {
        return type instanceof ScalarType ? ((ScalarType) type).getScalarScale() : -1;
    }

    public AggregateType getAggregationType() {
        return this.aggregationType;
    }

    public String getAggregationString() {
        return getAggregationType().name();
    }

    public boolean isAggregated() {
        return aggregationType != null && aggregationType != AggregateType.NONE;
    }

    public boolean isAggregationTypeImplicit() {
        return this.isAggregationTypeImplicit;
    }

    public void setAggregationType(AggregateType aggregationType, boolean isAggregationTypeImplicit) {
        this.aggregationType = aggregationType;
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public void setAggregationTypeImplicit(boolean isAggregationTypeImplicit) {
        this.isAggregationTypeImplicit = isAggregationTypeImplicit;
    }

    public boolean isAllowNull() {
        return isAllowNull;
    }

    public boolean isAutoInc() {
        return isAutoInc;
    }

    public void setIsAutoInc(boolean isAutoInc) {
        this.isAutoInc = isAutoInc;
    }

    public void setIsAllowNull(boolean isAllowNull) {
        this.isAllowNull = isAllowNull;
    }

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public String getRealDefaultValue() {
        return realDefaultValue;
    }

    public String getDefaultValueSql() {
        if (defaultValue == null) {
            return null;
        }
        if (defaultValueExprDef != null) {
            return defaultValueExprDef.getSql();
        }
        if (this.type.isNumericType()) {
            return defaultValue;
        } else {
            return "'" + defaultValue.replace("'", "''") + "'";
        }
    }

    public void setComment(String comment) {
        this.comment = StringUtils.isBlank(comment) ? null : comment;
    }

    public String getComment() {
        return getComment(false);
    }

    public String getComment(boolean escapeQuota) {
        String comment = this.comment == null ? "" : this.comment;
        if (!escapeQuota) {
            return comment;
        }
        return SqlUtils.escapeQuota(comment);
    }

    public int getOlapColumnIndexSize() {
        PrimitiveType type = this.getDataType();
        if (type == PrimitiveType.CHAR) {
            return getStrLen();
        } else {
            return type.getOlapColumnIndexSize();
        }
    }

    public boolean hasOnUpdateDefaultValue() {
        return hasOnUpdateDefaultValue;
    }

    public String getOnUpdateDefaultValueSql() {
        return onUpdateDefaultValueExprDef.getSql();
    }

    // CLOUD_CODE_BEGIN

    // CLOUD_CODE_END

    public void checkSchemaChangeAllowed(Column other) throws DdlException {
        if (Strings.isNullOrEmpty(other.name)) {
            throw new DdlException("Dest column name is empty");
        }

        if (!ColumnType.isSchemaChangeAllowed(type, other.type)) {
            throw new DdlException("Can not change " + getDataType() + " to " + other.getDataType());
        }

        if (type.isNumericType() && other.type.isStringType()) {
            try {
                Integer lSize = type.getColumnStringRepSize();
                Integer rSize = other.type.getColumnStringRepSize();
                if (rSize < lSize) {
                    throw new DdlException(
                            "Can not change from wider type " + type.toSql() + " to narrower type "
                                    + other.type.toSql());
                }
            } catch (TypeException e) {
                throw new DdlException(e.getMessage());
            }
        }

        if (!Objects.equals(this.aggregationType, other.aggregationType)) {
            throw new DdlException("Can not change aggregation type");
        }

        if (this.isAllowNull && !other.isAllowNull) {
            throw new DdlException("Can not change from nullable to non-nullable");
        }

        if (this.getDefaultValue() == null) {
            if (other.getDefaultValue() != null) {
                throw new DdlException("Can not change default value");
            }
        } else {
            if (!this.getDefaultValue().equals(other.getDefaultValue())) {
                throw new DdlException("Can not change default value");
            }
        }

        if (type.isStringType() && other.type.isStringType()) {
            ColumnType.checkForTypeLengthChange(type, other.type);
        }

        // Nested types only support changing the order and increasing the length of the nested char type
        // Char-type only support length growing
        ColumnType.checkSupportSchemaChangeForComplexType(type, other.type, false);

        // now we support convert decimal to varchar type
        if ((getDataType() == PrimitiveType.DECIMALV2 || getDataType().isDecimalV3Type())
                && (other.getDataType() == PrimitiveType.VARCHAR || other.getDataType() == PrimitiveType.STRING)) {
            return;
        }
        // TODO check cluster key

        if (generatedColumnInfo != null || other.getGeneratedColumnInfo() != null) {
            throw new DdlException("Not supporting alter table modify generated columns.");
        }

        if (type.isVariantType() && other.type.isVariantType()) {
            if (this.getVariantMaxSubcolumnsCount() != other.getVariantMaxSubcolumnsCount()) {
                throw new DdlException("Can not change variant max subcolumns count");
            }
            if (this.getVariantEnableTypedPathsToSparse() != other.getVariantEnableTypedPathsToSparse()) {
                throw new DdlException("Can not change variant enable typed paths to sparse");
            }
            if (this.getVariantMaxSparseColumnStatisticsSize() != other.getVariantMaxSparseColumnStatisticsSize()) {
                throw new DdlException("Can not change variant max sparse column statistics size");
            }
            if (this.getVariantSparseHashShardCount() != other.getVariantSparseHashShardCount()) {
                throw new DdlException("Can not change variant sparse bucket num");
            }
            if (this.getVariantEnableDocMode() != other.getVariantEnableDocMode()) {
                throw new DdlException("Can not change variant enable doc snapshot mode");
            }
            if (this.getVariantDocShardCount() != other.getVariantDocShardCount()) {
                throw new DdlException("Can not change variant doc snapshot shard count");
            }
            if (this.getVariantEnableNestedGroup() != other.getVariantEnableNestedGroup()) {
                throw new DdlException("Can not change variant enable nested group");
            }
            if (CollectionUtils.isNotEmpty(this.getChildren()) || CollectionUtils.isNotEmpty(other.getChildren())) {
                throw new DdlException("Can not change variant schema templates");
            }
        }
    }

    public boolean nameEquals(String otherColName, boolean ignorePrefix) {
        if (CaseSensibility.COLUMN.getCaseSensibility()) {
            if (!ignorePrefix) {
                return name.equals(otherColName);
            } else {
                return removeNamePrefix(name).equals(removeNamePrefix(otherColName));
            }
        } else {
            if (!ignorePrefix) {
                return name.equalsIgnoreCase(otherColName);
            } else {
                return removeNamePrefix(name).equalsIgnoreCase(removeNamePrefix(otherColName));
            }
        }
    }

    public static String removeNamePrefix(String colName) {
        if (colName.startsWith(SHADOW_NAME_PREFIX)) {
            return colName.substring(SHADOW_NAME_PREFIX.length());
        }
        return colName;
    }

    public static String getShadowName(String colName) {
        if (isShadowColumn(colName)) {
            return colName;
        }
        return SHADOW_NAME_PREFIX + colName;
    }

    public static boolean isShadowColumn(String colName) {
        return colName.startsWith(SHADOW_NAME_PREFIX);
    }

    public Expr getDefineExpr() {
        return defineExpr;
    }

    public void setDefineExpr(Expr expr) {
        defineExpr = expr;
    }

    public DefaultValueExprDef getDefaultValueExprDef() {
        return defaultValueExprDef;
    }

    public List<SlotRef> getRefColumns() {
        if (defineExpr == null) {
            return null;
        } else {
            List<SlotRef> slots = new ArrayList<>();
            defineExpr.collect(SlotRef.class, slots);
            return slots;
        }
    }

    public boolean isClusterKey() {
        return clusterKeyId != -1;
    }

    public int getClusterKeyId() {
        return clusterKeyId;
    }

    public String toSql() {
        return toSql(false, false);
    }

    public String toSql(boolean isUniqueTable) {
        return toSql(isUniqueTable, false);
    }

    public String toSql(boolean isUniqueTable, boolean isCompatible) {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        String typeStr = type.toSql();

        // show change datetimeV2/dateV2 to datetime/date
        if (isCompatible) {
            sb.append(type.hideVersionForVersionColumn(true));
        } else {
            sb.append(typeStr);
        }
        if (aggregationType != null && aggregationType != AggregateType.NONE && !isUniqueTable
                && !isAggregationTypeImplicit) {
            sb.append(" ").append(aggregationType.toSql());
        }
        if (generatedColumnInfo != null) {
            sb.append(" AS (").append(generatedColumnInfo.getExprSql()).append(")");
        }
        if (isAllowNull) {
            sb.append(" NULL");
        } else {
            sb.append(" NOT NULL");
        }
        if (isAutoInc) {
            sb.append(" AUTO_INCREMENT(").append(autoIncInitValue).append(")");
        }
        if (defaultValue != null && getDataType() != PrimitiveType.HLL && getDataType() != PrimitiveType.BITMAP) {
            if (defaultValueExprDef != null) {
                sb.append(" DEFAULT ").append(defaultValue).append("");
            } else {
                sb.append(" DEFAULT \"").append(defaultValue).append("\"");
            }
        }
        if ((getDataType() == PrimitiveType.BITMAP) && defaultValue != null) {
            if (defaultValueExprDef != null) {
                sb.append(" DEFAULT ").append(defaultValueExprDef.getExprName()).append("");
            }
        }
        if (hasOnUpdateDefaultValue) {
            sb.append(" ON UPDATE ").append(defaultValue).append("");
        }
        if (StringUtils.isNotBlank(comment)) {
            sb.append(" COMMENT \"").append(getComment(true)).append("\"");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, getDataType(), getStrLen(), getPrecision(), getScale(), aggregationType,
                isAggregationTypeImplicit, isKey, isAllowNull, isAutoInc, defaultValue, getComment(), children, visible,
                realDefaultValue, clusterKeyId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof Column)) {
            return false;
        }

        Column other = (Column) obj;

        return name.equalsIgnoreCase(other.name)
                && Objects.equals(getDefaultValue(), other.getDefaultValue())
                && Objects.equals(aggregationType, other.aggregationType)
                && isAggregationTypeImplicit == other.isAggregationTypeImplicit
                && isKey == other.isKey
                && isAllowNull == other.isAllowNull
                && isAutoInc == other.isAutoInc
                && Objects.equals(type, other.type)
                && Objects.equals(getComment(), other.getComment())
                && visible == other.visible
                && Objects.equals(children, other.children)
                && Objects.equals(realDefaultValue, other.realDefaultValue)
                && clusterKeyId == other.clusterKeyId;
    }

    // distribution column compare only care about attrs which affect data,
    // do not care about attrs, such as comment
    public boolean equalsForDistribution(Column other) {
        if (other == this) {
            return true;
        }

        boolean ok = name.equalsIgnoreCase(other.name)
                && Objects.equals(getDefaultValue(), other.getDefaultValue())
                && Objects.equals(aggregationType, other.aggregationType)
                && isAggregationTypeImplicit == other.isAggregationTypeImplicit
                && isKey == other.isKey
                && isAllowNull == other.isAllowNull
                && getDataType().equals(other.getDataType())
                && getStrLen() == other.getStrLen()
                && getPrecision() == other.getPrecision()
                && getScale() == other.getScale()
                && visible == other.visible
                && Objects.equals(children, other.children)
                && Objects.equals(realDefaultValue, other.realDefaultValue)
                && clusterKeyId == other.clusterKeyId;

        if (!ok && LOG.isDebugEnabled()) {
            LOG.debug("this column: name {} default value {} aggregationType {} isAggregationTypeImplicit {} "
                            + "isKey {}, isAllowNull {}, datatype {}, strlen {}, precision {}, scale {}, visible {} "
                            + "children {}, realDefaultValue {}, clusterKeyId {}",
                    name, getDefaultValue(), aggregationType, isAggregationTypeImplicit, isKey, isAllowNull,
                    getDataType(), getStrLen(), getPrecision(), getScale(), visible, children, realDefaultValue,
                    clusterKeyId);
            LOG.debug("other column: name {} default value {} aggregationType {} isAggregationTypeImplicit {} "
                            + "isKey {}, isAllowNull {}, datatype {}, strlen {}, precision {}, scale {}, visible {}, "
                            + "children {}, realDefaultValue {}, clusterKeyId {}",
                    other.name, other.getDefaultValue(), other.aggregationType, other.isAggregationTypeImplicit,
                    other.isKey, other.isAllowNull, other.getDataType(), other.getStrLen(), other.getPrecision(),
                    other.getScale(), other.visible, other.children, other.realDefaultValue, other.clusterKeyId);
        }
        return ok;
    }

    // Gen a signature string of this column, contains:
    // name, type, is key, nullable, aggr type, default
    public String getSignatureString(Map<PrimitiveType, String> typeStringMap) {
        PrimitiveType dataType = getDataType();
        StringBuilder sb = new StringBuilder(name);
        switch (dataType) {
            case CHAR:
            case VARCHAR:
                sb.append(String.format(typeStringMap.get(dataType), getStrLen()));
                break;
            case JSONB:
                sb.append(type.toString());
                break;
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
            case DECIMAL256:
                sb.append(String.format(typeStringMap.get(dataType), getPrecision(), getScale()));
                break;
            case ARRAY:
            case MAP:
            case STRUCT:
                sb.append(type.toString());
                break;
            default:
                sb.append(typeStringMap.get(dataType));
                break;
        }
        sb.append(isKey);
        sb.append(isAllowNull);
        sb.append(aggregationType);
        sb.append(clusterKeyId);
        sb.append(defaultValue == null ? "" : defaultValue);
        return sb.toString();
    }

    public void setUniqueId(int colUniqueId) {
        this.uniqueId = colUniqueId;
    }

    public void setWithTZExtraInfo() {
        this.extraInfo = Strings.isNullOrEmpty(extraInfo) ? "WITH_TIMEZONE" : extraInfo + ", WITH_TIMEZONE";
    }

    public int getUniqueId() {
        return this.uniqueId;
    }

    public long getAutoIncInitValue() {
        return this.autoIncInitValue;
    }

    public boolean isCompoundKey() {
        return isCompoundKey;
    }

    public void setCompoundKey(boolean compoundKey) {
        isCompoundKey = compoundKey;
    }

    public boolean hasDefaultValue() {
        return defaultValue != null || realDefaultValue != null || defaultValueExprDef != null;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // This just for bugfix. Because when user upgrade from 0.x to 1.1.x,
        // the length of String type become 1. The reason is not very clear and maybe fixed by #14275.
        // Here we try to rectify the error string length, by setting all String' length to MAX_STRING_LENGTH
        // when replaying edit log.
        if (type.isScalarType() && type.getPrimitiveType() == PrimitiveType.STRING
                && type.getLength() != ScalarType.MAX_STRING_LENGTH) {
            ((ScalarType) type).setLength(ScalarType.MAX_STRING_LENGTH);
        }
        if (CollectionUtils.isEmpty(generatedColumnsThatReferToThis)) {
            generatedColumnsThatReferToThis = null;
        }
        if (StringUtils.isBlank(comment)) {
            comment = null;
        }
        if (CollectionUtils.isEmpty(children)) {
            children = null;
        }
    }

    public boolean isMaterializedViewColumn() {
        return defineExpr != null;
    }

    // this function is try to get base column name for distribution column, partition column, key column
    // or delete condition column in load job. So these columns are all simple SlotRef not other complex expr like a +_b
    // under the assumption, we just try to get base column when the defineExpr is a simple SlotRef with a Column in tbl
    public String tryGetBaseColumnName() {
        String colName = name;
        if (defineExpr != null && defineExpr instanceof SlotRef) {
            Column baseCol = ((SlotRef) defineExpr).getColumn();
            if (baseCol != null) {
                colName = baseCol.getName();
            }
        }
        return colName;
    }

    public boolean isGeneratedColumn() {
        return generatedColumnInfo != null;
    }

    public GeneratedColumnInfo getGeneratedColumnInfo() {
        return generatedColumnInfo;
    }

    public Set<String> getGeneratedColumnsThatReferToThis() {
        return generatedColumnsThatReferToThis;
    }

    public void setDefaultValueInfo(Column refColumn) {
        this.defaultValue = refColumn.defaultValue;
        this.defaultValueExprDef = refColumn.defaultValueExprDef;
        this.realDefaultValue = refColumn.realDefaultValue;
    }

    public int getVariantMaxSubcolumnsCount() {
        return type.isVariantType() ? ((ScalarType) type).getVariantMaxSubcolumnsCount() : -1;
    }

    public boolean getVariantEnableTypedPathsToSparse() {
        return type.isVariantType() ? ((ScalarType) type).getVariantEnableTypedPathsToSparse() : false;
    }

    public int getVariantMaxSparseColumnStatisticsSize() {
        return type.isVariantType() ? ((ScalarType) type).getVariantMaxSparseColumnStatisticsSize() : -1;
    }

    public int getVariantSparseHashShardCount() {
        return type.isVariantType() ? ((ScalarType) type).getVariantSparseHashShardCount() : -1;
    }

    public boolean getVariantEnableDocMode() {
        return type.isVariantType() ? ((ScalarType) type).getVariantEnableDocMode() : false;
    }

    public long getvariantDocMaterializationMinRows() {
        return type.isVariantType() ? ((ScalarType) type).getvariantDocMaterializationMinRows() : 0L;
    }

    public int getVariantDocShardCount() {
        return type.isVariantType() ? ((ScalarType) type).getVariantDocShardCount() : 128;
    }

    public boolean getVariantEnableNestedGroup() {
        return type.isVariantType() ? ((ScalarType) type).getVariantEnableNestedGroup() : false;
    }

    public void setFieldPatternType(PatternType type) {
        fieldPatternType = type;
    }

    public PatternType getFieldPatternType() {
        return fieldPatternType;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }

    public void setSessionVariables(Map<String, String> sessionVariables) {
        this.sessionVariables = sessionVariables;
    }
}
