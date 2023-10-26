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

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TColumnType;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class represents the column-related metadata.
 */
public class Column implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Column.class);
    public static final String DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    public static final String WHERE_SIGN = "__DORIS_WHERE_SIGN__";
    public static final String SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";
    public static final String ROWID_COL = "__DORIS_ROWID_COL__";
    public static final String ROW_STORE_COL = "__DORIS_ROW_STORE_COL__";
    public static final String DYNAMIC_COLUMN_NAME = "__DORIS_DYNAMIC_COL__";
    public static final String VERSION_COL = "__DORIS_VERSION_COL__";
    private static final String COLUMN_ARRAY_CHILDREN = "item";
    private static final String COLUMN_STRUCT_CHILDREN = "field";
    private static final String COLUMN_AGG_ARGUMENT_CHILDREN = "argument";
    public static final int COLUMN_UNIQUE_ID_INIT_VALUE = -1;
    private static final String COLUMN_MAP_KEY = "key";
    private static final String COLUMN_MAP_VALUE = "value";

    public static final Column UNSUPPORTED_COLUMN = new Column("unknown", Type.UNSUPPORTED, true, null, true, false,
            null, "invalid", true, null, -1, null);

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
    @SerializedName(value = "defaultValue")
    private String defaultValue;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "stats")
    private ColumnStats stats;     // cardinality and selectivity etc.
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

    @SerializedName(value = "genericAggregationName")
    private String genericAggregationName;

    private boolean isCompoundKey = false;

    public Column() {
        this.name = "";
        this.type = Type.NULL;
        this.isAggregationTypeImplicit = false;
        this.isKey = false;
        this.stats = new ColumnStats();
        this.visible = true;
        this.defineExpr = null;
        this.children = new ArrayList<>();
        this.uniqueId = -1;
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

    public Column(String name, Type type) {
        this(name, type, false, null, false, null, "");
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, String defaultValue,
            String comment) {
        this(name, type, isKey, aggregateType, false, defaultValue, comment);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            String defaultValue, String comment) {
        this(name, type, isKey, aggregateType, isAllowNull, false, defaultValue, comment, true, null,
                COLUMN_UNIQUE_ID_INIT_VALUE, defaultValue);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            String comment, boolean visible, int colUniqueId) {
        this(name, type, isKey, aggregateType, isAllowNull, false, null, comment, visible, null, colUniqueId, null);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            String defaultValue, String comment, boolean visible, DefaultValueExprDef defaultValueExprDef,
            int colUniqueId, String realDefaultValue) {
        this(name, type, isKey, aggregateType, isAllowNull, false, defaultValue, comment, visible, defaultValueExprDef,
                colUniqueId, realDefaultValue);
    }

    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
            boolean isAutoInc, String defaultValue, String comment, boolean visible,
            DefaultValueExprDef defaultValueExprDef, int colUniqueId, String realDefaultValue) {
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
        this.isAutoInc = isAutoInc;
        this.defaultValue = defaultValue;
        this.realDefaultValue = realDefaultValue;
        this.defaultValueExprDef = defaultValueExprDef;
        this.comment = comment;
        this.stats = new ColumnStats();
        this.visible = visible;
        this.children = new ArrayList<>();
        createChildrenColumn(this.type, this);
        this.uniqueId = colUniqueId;

        if (type.isAggStateType()) {
            AggStateType aggState = (AggStateType) type;
            for (int i = 0; i < aggState.getSubTypes().size(); i++) {
                Column c = new Column(COLUMN_AGG_ARGUMENT_CHILDREN, aggState.getSubTypes().get(i));
                c.setIsAllowNull(aggState.getSubTypeNullables().get(i));
                addChildrenColumn(c);
            }
            this.genericAggregationName = aggState.getFunctionName();
            this.aggregationType = AggregateType.GENERIC_AGGREGATION;
        }
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
        this.stats = column.getStats();
        this.visible = column.visible;
        this.children = column.getChildren();
        this.uniqueId = column.getUniqueId();
        this.defineExpr = column.getDefineExpr();
        this.defineName = column.getDefineName();
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
        }
    }

    public List<Column> getChildren() {
        return children;
    }

    private void addChildrenColumn(Column column) {
        this.children.add(column);
    }

    public void setDefineName(String defineName) {
        this.defineName = defineName;
    }

    public String getDefineName() {
        if (defineName != null) {
            return defineName;
        }
        return name;
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

    public String getNameWithoutMvPrefix() {
        return CreateMaterializedViewStmt.mvColumnBreaker(name);
    }

    public static String getNameWithoutMvPrefix(String originalName) {
        return CreateMaterializedViewStmt.mvColumnBreaker(originalName);
    }

    public String getDisplayName() {
        if (defineExpr == null) {
            return getNameWithoutMvPrefix();
        } else {
            return MaterializedIndexMeta.normalizeName(defineExpr.toSql());
        }
    }

    public String getNameWithoutPrefix(String prefix) {
        if (isNameWithPrefix(prefix)) {
            return name.substring(prefix.length());
        }
        return name;
    }

    public boolean isNameWithPrefix(String prefix) {
        return this.name.startsWith(prefix);
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
        if (getAggregationType() == AggregateType.GENERIC_AGGREGATION) {
            return getGenericAggregationString();
        } else {
            return getAggregationType().name();
        }
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

    public void setIsAllowNull(boolean isAllowNull) {
        this.isAllowNull = isAllowNull;
    }

    public String getDefaultValue() {
        return this.defaultValue;
    }

    public Expr getDefaultValueExpr() throws AnalysisException {
        StringLiteral defaultValueLiteral = new StringLiteral(defaultValue);
        if (getDataType() == PrimitiveType.VARCHAR) {
            return defaultValueLiteral;
        }
        if (defaultValueExprDef != null) {
            return defaultValueExprDef.getExpr(type);
        }
        Expr result = defaultValueLiteral.castTo(getType());
        result.checkValueValid();
        return result;
    }


    public void setStats(ColumnStats stats) {
        this.stats = stats;
    }

    public ColumnStats getStats() {
        return this.stats;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getComment() {
        return getComment(false);
    }

    public String getComment(boolean escapeQuota) {
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

    public TColumn toThrift() {
        TColumn tColumn = new TColumn();
        tColumn.setColumnName(removeNamePrefix(this.name));

        TColumnType tColumnType = new TColumnType();
        tColumnType.setType(this.getDataType().toThrift());
        tColumnType.setLen(this.getStrLen());
        tColumnType.setPrecision(this.getPrecision());
        tColumnType.setScale(this.getScale());

        tColumnType.setIndexLen(this.getOlapColumnIndexSize());

        tColumn.setColumnType(tColumnType);
        if (null != this.aggregationType) {
            tColumn.setAggregationType(this.aggregationType.toThrift());
        }

        tColumn.setIsKey(this.isKey);
        tColumn.setIsAllowNull(this.isAllowNull);
        tColumn.setIsAutoIncrement(this.isAutoInc);
        // keep compatibility
        tColumn.setDefaultValue(this.realDefaultValue == null ? this.defaultValue : this.realDefaultValue);
        tColumn.setVisible(visible);
        toChildrenThrift(this, tColumn);

        tColumn.setColUniqueId(uniqueId);

        if (type.isAggStateType()) {
            AggStateType aggState = (AggStateType) type;
            tColumn.setAggregation(aggState.getFunctionName());
            tColumn.setResultIsNullable(aggState.getResultIsNullable());
            for (Column column : children) {
                tColumn.addToChildrenColumn(column.toThrift());
            }
        }
        // ATTN:
        // Currently, this `toThrift()` method is only used from CreateReplicaTask.
        // And CreateReplicaTask does not need `defineExpr` field.
        // The `defineExpr` is only used when creating `TAlterMaterializedViewParam`, which is in `AlterReplicaTask`.
        // And when creating `TAlterMaterializedViewParam`, the `defineExpr` is certainly analyzed.
        // If we need to use `defineExpr` and call defineExpr.treeToThrift(),
        // make sure it is analyzed, or NPE will thrown.
        return tColumn;
    }

    private void setChildrenTColumn(Column children, TColumn tColumn) {
        TColumn childrenTColumn = new TColumn();
        childrenTColumn.setColumnName(children.name);

        TColumnType childrenTColumnType = new TColumnType();
        childrenTColumnType.setType(children.getDataType().toThrift());
        childrenTColumnType.setLen(children.getStrLen());
        childrenTColumnType.setPrecision(children.getPrecision());
        childrenTColumnType.setScale(children.getScale());
        childrenTColumnType.setIndexLen(children.getOlapColumnIndexSize());

        childrenTColumn.setColumnType(childrenTColumnType);
        childrenTColumn.setIsAllowNull(children.isAllowNull());
        // TODO: If we don't set the aggregate type for children, the type will be
        //  considered as TAggregationType::SUM after deserializing in BE.
        //  For now, we make children inherit the aggregate type from their parent.
        if (tColumn.getAggregationType() != null) {
            childrenTColumn.setAggregationType(tColumn.getAggregationType());
        }

        tColumn.children_column.add(childrenTColumn);
        toChildrenThrift(children, childrenTColumn);
    }

    private void toChildrenThrift(Column column, TColumn tColumn) {
        if (column.type.isArrayType()) {
            Column children = column.getChildren().get(0);
            tColumn.setChildrenColumn(new ArrayList<>());
            setChildrenTColumn(children, tColumn);
        } else if (column.type.isMapType()) {
            Column k = column.getChildren().get(0);
            Column v = column.getChildren().get(1);
            tColumn.setChildrenColumn(new ArrayList<>());
            setChildrenTColumn(k, tColumn);
            setChildrenTColumn(v, tColumn);
        } else if (column.type.isStructType()) {
            List<Column> childrenColumns = column.getChildren();
            tColumn.setChildrenColumn(new ArrayList<>());
            for (Column children : childrenColumns) {
                setChildrenTColumn(children, tColumn);
            }
        }
    }


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

        if (this.aggregationType != other.aggregationType) {
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

        if ((getDataType() == PrimitiveType.VARCHAR && other.getDataType() == PrimitiveType.VARCHAR) || (
                getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.VARCHAR) || (
                getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.CHAR)) {
            if (getStrLen() > other.getStrLen()) {
                throw new DdlException("Cannot shorten string length");
            }
        }

        // now we support convert decimal to varchar type
        if ((getDataType() == PrimitiveType.DECIMALV2 || getDataType().isDecimalV3Type())
                && (other.getDataType() == PrimitiveType.VARCHAR || other.getDataType() == PrimitiveType.STRING)) {
            return;
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
        if (colName.startsWith(SchemaChangeHandler.SHADOW_NAME_PREFIX)) {
            return colName.substring(SchemaChangeHandler.SHADOW_NAME_PREFIX.length());
        }
        return colName;
    }

    public static String getShadowName(String colName) {
        if (isShadowColumn(colName)) {
            return colName;
        }
        return SchemaChangeHandler.SHADOW_NAME_PREFIX + colName;
    }

    public static boolean isShadowColumn(String colName) {
        return colName.startsWith(SchemaChangeHandler.SHADOW_NAME_PREFIX);
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

    public String toSql() {
        return toSql(false, false);
    }

    public String toSql(boolean isUniqueTable) {
        return toSql(isUniqueTable, false);
    }

    public String getGenericAggregationString() {
        StringBuilder sb = new StringBuilder();
        sb.append(genericAggregationName).append("(");
        for (int i = 0; i < children.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(children.get(i).getType().toSql());
            if (children.get(i).isAllowNull()) {
                sb.append(" NULL");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    public String toSql(boolean isUniqueTable, boolean isCompatible) {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        String typeStr = type.toSql();

        // show change datetimeV2/dateV2 to datetime/date
        if (isCompatible) {
            if (type.isDatetimeV2()) {
                sb.append("datetime");
                if (((ScalarType) type).getScalarScale() > 0) {
                    sb.append("(").append(((ScalarType) type).getScalarScale()).append(")");
                }
            } else if (type.isDateV2()) {
                sb.append("date");
            } else if (type.isDecimalV3()) {
                sb.append("DECIMAL");
                ScalarType sType = (ScalarType) type;
                int scale = sType.getScalarScale();
                int precision = sType.getScalarPrecision();
                // not default
                if (scale > 0 && precision != 9) {
                    sb.append("(").append(precision).append(", ").append(scale)
                            .append(")");
                }
            } else {
                sb.append(typeStr);
            }
        } else {
            sb.append(typeStr);
        }
        if (aggregationType == AggregateType.GENERIC_AGGREGATION) {
            sb.append(" ").append(getGenericAggregationString());
        } else if (aggregationType != null && aggregationType != AggregateType.NONE && !isUniqueTable
                && !isAggregationTypeImplicit) {
            sb.append(" ").append(aggregationType.name());
        }
        if (isAllowNull) {
            sb.append(" NULL");
        } else {
            sb.append(" NOT NULL");
        }
        if (isAutoInc) {
            sb.append(" AUTO_INCREMENT");
        }
        if (defaultValue != null && getDataType() != PrimitiveType.HLL && getDataType() != PrimitiveType.BITMAP) {
            if (defaultValueExprDef != null) {
                sb.append(" DEFAULT ").append(defaultValue).append("");
            } else {
                sb.append(" DEFAULT \"").append(defaultValue).append("\"");
            }
        }
        if (StringUtils.isNotBlank(comment)) {
            sb.append(" COMMENT '").append(getComment(true)).append("'");
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
                isAggregationTypeImplicit, isKey, isAllowNull, isAutoInc, defaultValue, comment, children, visible,
                realDefaultValue);
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
                && getDataType().equals(other.getDataType())
                && getStrLen() == other.getStrLen()
                && getPrecision() == other.getPrecision()
                && getScale() == other.getScale()
                && Objects.equals(comment, other.comment)
                && visible == other.visible
                && Objects.equals(children, other.children)
                && Objects.equals(realDefaultValue, other.realDefaultValue);
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
                && Objects.equals(realDefaultValue, other.realDefaultValue);

        if (!ok) {
            LOG.info("this column: name {} default value {} aggregationType {} isAggregationTypeImplicit {} "
                     + "isKey {}, isAllowNull {}, datatype {}, strlen {}, precision {}, scale {}, visible {} "
                     + "children {} realDefaultValue {}",
                     name, getDefaultValue(), aggregationType, isAggregationTypeImplicit, isKey, isAllowNull,
                     getDataType(), getStrLen(), getPrecision(), getScale(), visible, children, realDefaultValue);
            LOG.info("other column: name {} default value {} aggregationType {} isAggregationTypeImplicit {} "
                     + "isKey {}, isAllowNull {}, datatype {}, strlen {}, precision {}, scale {}, visible {} "
                     + "children {} realDefaultValue {}",
                     other.name, other.getDefaultValue(), other.aggregationType, other.isAggregationTypeImplicit,
                     other.isKey, other.isAllowNull, other.getDataType(), other.getStrLen(), other.getPrecision(),
                     other.getScale(), other.visible, other.children, other.realDefaultValue);
        }
        return ok;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        name = Text.readString(in);
        type = ColumnType.read(in);
        boolean notNull = in.readBoolean();
        if (notNull) {
            aggregationType = AggregateType.valueOf(Text.readString(in));
            isAggregationTypeImplicit = in.readBoolean();
        }
        isKey = in.readBoolean();
        isAllowNull = in.readBoolean();
        notNull = in.readBoolean();
        if (notNull) {
            defaultValue = Text.readString(in);
            realDefaultValue = defaultValue;
        }
        stats = ColumnStats.read(in);

        comment = Text.readString(in);
    }

    public static Column read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Column.class);
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
        sb.append(defaultValue == null ? "" : defaultValue);
        return sb.toString();
    }

    public void setUniqueId(int colUniqueId) {
        this.uniqueId = colUniqueId;
    }

    public int getUniqueId() {
        return this.uniqueId;
    }

    public void setIndexFlag(TColumn tColumn, OlapTable olapTable) {
        List<Index> indexes = olapTable.getIndexes();
        for (Index index : indexes) {
            if (index.getIndexType() == IndexDef.IndexType.BITMAP) {
                List<String> columns = index.getColumns();
                if (tColumn.getColumnName().equals(columns.get(0))) {
                    tColumn.setHasBitmapIndex(true);
                }
            }
        }
        Set<String> bfColumns = olapTable.getCopiedBfColumns();
        if (bfColumns != null && bfColumns.contains(tColumn.getColumnName())) {
            tColumn.setIsBloomFilterColumn(true);
        }
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
    }

    public boolean isMaterializedViewColumn() {
        return getName().startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)
                || getName().startsWith(CreateMaterializedViewStmt.MATERIALIZED_VIEW_AGGREGATE_NAME_PREFIX);
    }
}
