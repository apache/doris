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
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TColumnType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents the column-related metadata.
 */
public class Column implements Writable {
    private static final Logger LOG = LogManager.getLogger(Column.class);
    public static final String DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    public static final String SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";
    private static final String COLUMN_ARRAY_CHILDREN = "item";

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
    @SerializedName(value = "defaultValue")
    private String defaultValue;
    @SerializedName(value = "comment")
    private String comment;
    @SerializedName(value = "stats")
    private ColumnStats stats;     // cardinality and selectivity etc.
    @SerializedName(value = "children")
    private List<Column> children;
    // Define expr may exist in two forms, one is analyzed, and the other is not analyzed.
    // Currently, analyzed define expr is only used when creating materialized views, so the define expr in RollupJob must be analyzed.
    // In other cases, such as define expr in `MaterializedIndexMeta`, it may not be analyzed after being replayed.
    private Expr defineExpr; // use to define column in materialize view
    @SerializedName(value = "visible")
    private boolean visible;

    public Column() {
        name = "";
        type = Type.NULL;
        isAggregationTypeImplicit = false;
        isKey = false;
        stats = new ColumnStats();
        visible = true;
        children = new ArrayList<>(Type.MAX_NESTING_DEPTH);
    }

    public Column(String name, PrimitiveType dataType) {
        this(name, ScalarType.createType(dataType), false, null, false, null, "");
    }

    public Column(String name, PrimitiveType dataType, boolean isAllowNull) {
        this(name, ScalarType.createType(dataType), false, null, isAllowNull, null, "");
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
        this(name, type, isKey, aggregateType, isAllowNull, defaultValue, comment, true);
    }
    public Column(String name, Type type, boolean isKey, AggregateType aggregateType, boolean isAllowNull,
                  String defaultValue, String comment, boolean visible) {
        this.name = name;
        if (name == null) {
            this.name = "";
        }

        this.type = type;
        if (type == null) {
            this.type = Type.NULL;
        }

        aggregationType = aggregateType;
        isAggregationTypeImplicit = false;
        this.isKey = isKey;
        this.isAllowNull = isAllowNull;
        this.defaultValue = defaultValue;
        this.comment = comment;
        stats = new ColumnStats();
        this.visible = visible;
        children = new ArrayList<>(Type.MAX_NESTING_DEPTH);
        createChildrenColumn(this.type, this);
    }

    public Column(Column column) {
        name = column.getName();
        type = column.type;
        aggregationType = column.getAggregationType();
        isAggregationTypeImplicit = column.isAggregationTypeImplicit();
        isKey = column.isKey();
        isAllowNull = column.isAllowNull();
        defaultValue = column.getDefaultValue();
        comment = column.getComment();
        stats = column.getStats();
        visible = column.visible;
        children = column.getChildren();
    }

    public void createChildrenColumn(Type type, Column column) {
        if (type.isArrayType()) {
            Column c = new Column(COLUMN_ARRAY_CHILDREN, ((ArrayType) type).getItemType());
            // TODO We always set the item type in array nullable.
            //  We may provide an alternative to configure this property of
            //  the item type in array in future.
            c.setIsAllowNull(true);
            column.addChildrenColumn(c);
        }
    }

    public List<Column> getChildren() {
        return children;
    }

    private void addChildrenColumn(Column column) {
        children.add(column);
    }

    public void setName(String newName) {
        name = newName;
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        if (defineExpr == null) {
            return name;
        } else {
            return defineExpr.toSql();
        }
    }

    public String getNameWithoutPrefix(String prefix) {
        if (isNameWithPrefix(prefix)) {
            return name.substring(prefix.length());
        }
        return name;
    }

    public boolean isNameWithPrefix(String prefix) {
        return name.startsWith(prefix);
    }

    public void setIsKey(boolean isKey) {
        // column is key, aggregationType is always null, isAggregationTypeImplicit is always false.
        if (isKey) {
            setAggregationType(null, false);
        }
        this.isKey = isKey;
    }

    public boolean isKey() {
        return isKey;
    }

    public boolean isVisible() {
        return visible;
    }

    public void setIsVisible(boolean isVisible) {
        visible = isVisible;
    }

    public boolean isDeleteSignColumn() {
        return !visible && aggregationType == AggregateType.REPLACE && nameEquals(DELETE_SIGN, true);
    }

    public boolean isSequenceColumn() {
        return !visible && aggregationType == AggregateType.REPLACE && nameEquals(SEQUENCE_COL, true);
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
        return aggregationType;
    }

    public boolean isAggregated() {
        return aggregationType != null && aggregationType != AggregateType.NONE;
    }

    public boolean isAggregationTypeImplicit() {
        return isAggregationTypeImplicit;
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

    public void setIsAllowNull(boolean isAllowNull) {
        this.isAllowNull = isAllowNull;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Expr getDefaultValueExpr() throws AnalysisException {
        StringLiteral defaultValueLiteral = new StringLiteral(defaultValue);
        if (getDataType() == PrimitiveType.VARCHAR) {
            return defaultValueLiteral;
        }
        Expr result = defaultValueLiteral.castTo(getType());
        result.checkValueValid();
        return result;
    }

    public void setStats(ColumnStats stats) {
        this.stats = stats;
    }

    public ColumnStats getStats() {
        return stats;
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
        PrimitiveType type = getDataType();
        if (type == PrimitiveType.CHAR) {
            return getStrLen();
        } else {
            return type.getOlapColumnIndexSize();
        }
    }

    public TColumn toThrift() {
        TColumn tColumn = new TColumn();
        tColumn.setColumnName(name);

        TColumnType tColumnType = new TColumnType();
        tColumnType.setType(getDataType().toThrift());
        tColumnType.setLen(getStrLen());
        tColumnType.setPrecision(getPrecision());
        tColumnType.setScale(getScale());

        tColumnType.setIndexLen(getOlapColumnIndexSize());

        tColumn.setColumnType(tColumnType);
        if (null != aggregationType) {
            tColumn.setAggregationType(aggregationType.toThrift());
        }
        tColumn.setIsKey(isKey);
        tColumn.setIsAllowNull(isAllowNull);
        tColumn.setDefaultValue(defaultValue);
        tColumn.setVisible(visible);
        toChildrenThrift(this, tColumn);

        // ATTN:
        // Currently, this `toThrift()` method is only used from CreateReplicaTask.
        // And CreateReplicaTask does not need `defineExpr` field.
        // The `defineExpr` is only used when creating `TAlterMaterializedViewParam`, which is in `AlterReplicaTask`.
        // And when creating `TAlterMaterializedViewParam`, the `defineExpr` is certainly analyzed.
        // If we need to use `defineExpr` and call defineExpr.treeToThrift(), make sure it is analyzed, or NPE will thrown.
        return tColumn;
    }

    private void toChildrenThrift(Column column, TColumn tColumn) {
        if (column.type.isArrayType()) {
            Column children = column.getChildren().get(0);

            TColumn childrenTColumn = new TColumn();
            childrenTColumn.setColumnName(children.name);

            TColumnType childrenTColumnType = new TColumnType();
            childrenTColumnType.setType(children.getDataType().toThrift());
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

            tColumn.setChildrenColumn(new ArrayList<>());
            tColumn.children_column.add(childrenTColumn);

            toChildrenThrift(children, childrenTColumn);
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
            Integer lSize = type.getColumnStringRepSize();
            Integer rSize = other.type.getColumnStringRepSize();
            if (rSize < lSize) {
                throw new DdlException("Can not change from wider type " + type.toSql()
                        + " to narrower type " + other.type.toSql());
            }
        }

        if (aggregationType != other.aggregationType) {
            throw new DdlException("Can not change aggregation type");
        }

        if (isAllowNull && !other.isAllowNull) {
            throw new DdlException("Can not change from nullable to non-nullable");
        }

        if (getDefaultValue() == null) {
            if (other.getDefaultValue() != null) {
                throw new DdlException("Can not change default value");
            }
        } else {
            if (!getDefaultValue().equals(other.getDefaultValue())) {
                throw new DdlException("Can not change default value");
            }
        }

        if ((getDataType() == PrimitiveType.VARCHAR && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.CHAR)) {
            if (getStrLen() > other.getStrLen()) {
                throw new DdlException("Cannot shorten string length");
            }
        }

        // now we support convert decimal to varchar type
        if (getDataType() == PrimitiveType.DECIMALV2 && (other.getDataType() == PrimitiveType.VARCHAR
                || other.getDataType() == PrimitiveType.STRING)) {
            return;
        }

        if (getPrecision() != other.getPrecision()) {
            throw new DdlException("Cannot change precision");
        }

        if (getScale() != other.getScale()) {
            throw new DdlException("Cannot change scale");
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
        if (colName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
            return colName.substring(SchemaChangeHandler.SHADOW_NAME_PRFIX.length());
        }
        return colName;
    }

    public static String getShadowName(String colName) {
        if (isShadowColumn(colName)) {
            return colName;
        }
        return SchemaChangeHandler.SHADOW_NAME_PRFIX + colName;
    }

    public static boolean isShadowColumn(String colName) {
        return colName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX);
    }

    public Expr getDefineExpr() {
        return defineExpr;
    }

    public void setDefineExpr(Expr expr) {
        defineExpr = expr;
    }

    public SlotRef getRefColumn() {
        List<Expr> slots = new ArrayList<>();
        if (defineExpr == null) {
            return null;
        } else {
            defineExpr.collect(SlotRef.class, slots);
            Preconditions.checkArgument(slots.size() == 1);
            return (SlotRef) slots.get(0);
        }
    }

    public String toSql() {
        return toSql(false);
    }

    public String toSql(boolean isUniqueTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        String typeStr = type.toSql();
        sb.append(typeStr).append(" ");
        if (aggregationType != null && aggregationType != AggregateType.NONE
                && !isUniqueTable &&  !isAggregationTypeImplicit) {
            sb.append(aggregationType.name()).append(" ");
        }
        if (isAllowNull) {
            sb.append("NULL ");
        } else {
            sb.append("NOT NULL ");
        }
        if (defaultValue != null && getDataType() != PrimitiveType.HLL && getDataType() != PrimitiveType.BITMAP) {
            sb.append("DEFAULT \"").append(defaultValue).append("\" ");
        }
        sb.append("COMMENT \"").append(getComment(true)).append("\"");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, getDataType(), aggregationType, isAggregationTypeImplicit,
                isKey, isAllowNull, getDefaultValue(), getStrLen(), getPrecision(), getScale(),
                comment, visible, children);
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

        if (!name.equalsIgnoreCase(other.getName())) {
            return false;
        }
        if (getDataType() != other.getDataType()) {
            return false;
        }
        if (aggregationType != other.getAggregationType()) {
            return false;
        }
        if (isAggregationTypeImplicit != other.isAggregationTypeImplicit()) {
            return false;
        }
        if (isKey != other.isKey()) {
            return false;
        }
        if (isAllowNull != other.isAllowNull) {
            return false;
        }
        if (getDefaultValue() == null) {
            if (other.getDefaultValue() != null) {
                return false;
            }
        } else {
            if (!getDefaultValue().equals(other.getDefaultValue())) {
                return false;
            }
        }

        if (getStrLen() != other.getStrLen()) {
            return false;
        }
        if (getPrecision() != other.getPrecision()) {
            return false;
        }
        if (getScale() != other.getScale()) {
            return false;
        }

        if (!comment.equals(other.getComment())) {
            return false;
        }
        if (!visible == other.visible) {
            return false;
        }

        if (children.size() != other.children.size()) {
            return false;
        }

        for (int i = 0; i < children.size(); i++) {
            if (!children.get(i).equals(other.getChildren().get(i))) {
                return false;
            }
        }

        return true;
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
                sb.append(String.format(typeStringMap.get(dataType), getStrLen()));
                break;
            case VARCHAR:
                sb.append(String.format(typeStringMap.get(dataType), getStrLen()));
                break;
            case DECIMALV2:
                sb.append(String.format(typeStringMap.get(dataType), getPrecision(), getScale()));
                break;
            case ARRAY:
                sb.append(type.toString());
                break;
            case MAP:
                sb.append(type.toString());
                break;
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
}
