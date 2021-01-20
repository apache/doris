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
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TColumnType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class represents the column-related metadata.
 */
public class Column implements Writable {
    private static final Logger LOG = LogManager.getLogger(Column.class);
    public static final String DELETE_SIGN = "__DORIS_DELETE_SIGN__";
    public static final String SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";
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
    // Define expr may exist in two forms, one is analyzed, and the other is not analyzed.
    // Currently, analyzed define expr is only used when creating materialized views, so the define expr in RollupJob must be analyzed.
    // In other cases, such as define expr in `MaterializedIndexMeta`, it may not be analyzed after being relayed.
    private Expr defineExpr; // use to define column in materialize view
    @SerializedName(value = "visible")
    private boolean visible;

    public Column() {
        this.name = "";
        this.type = Type.NULL;
        this.isAggregationTypeImplicit = false;
        this.isKey = false;
        this.stats = new ColumnStats();
        this.visible = true;
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
        this.defaultValue = defaultValue;
        this.comment = comment;

        this.stats = new ColumnStats();
        this.visible = visible;
    }

    public Column(Column column) {
        this.name = column.getName();
        this.type = column.type;
        this.aggregationType = column.getAggregationType();
        this.isAggregationTypeImplicit = column.isAggregationTypeImplicit();
        this.isKey = column.isKey();
        this.isAllowNull = column.isAllowNull();
        this.defaultValue = column.getDefaultValue();
        this.comment = column.getComment();
        this.stats = column.getStats();
        this.visible = column.visible;
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return this.name;
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
        return this.name.startsWith(prefix);
    }

    public void setIsKey(boolean isKey) {
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
        return !visible && aggregationType == AggregateType.REPLACE && nameEquals(DELETE_SIGN, true);
    }

    public boolean isSequenceColumn() {
        return !visible && aggregationType == AggregateType.REPLACE && nameEquals(SEQUENCE_COL, true);
    }

    public PrimitiveType getDataType() { return type.getPrimitiveType(); }

    public Type getType() { return ScalarType.createType(type.getPrimitiveType()); }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getOriginType() { return type; }

    public int getStrLen() { return ((ScalarType) type).getLength(); }
    public int getPrecision() { return ((ScalarType) type).getScalarPrecision(); }
    public int getScale() { return ((ScalarType) type).getScalarScale(); }

    public AggregateType getAggregationType() {
        return this.aggregationType;
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

    public void setIsAllowNull(boolean isAllowNull) {
        this.isAllowNull = isAllowNull;
    }

    public String getDefaultValue() {
        return this.defaultValue;
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
        return comment;
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
        tColumn.setColumnName(this.name);

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
        tColumn.setDefaultValue(this.defaultValue);
        tColumn.setVisible(visible);
        // The define expr does not need to be serialized here for now.
        // At present, only serialized(analyzed) define expr is directly used when creating a materialized view.
        // It will not be used here, but through another structure `TAlterMaterializedViewParam`.
        return tColumn;
    }

    public void checkSchemaChangeAllowed(Column other) throws DdlException {
        if (Strings.isNullOrEmpty(other.name)) {
            throw new DdlException("Dest column name is empty");
        }

        if (!ColumnType.isSchemaChangeAllowed(type, other.type)) {
            throw new DdlException("Can not change " + getDataType() + " to " + other.getDataType());
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

        if ((getDataType() == PrimitiveType.VARCHAR && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.CHAR && other.getDataType() == PrimitiveType.CHAR)) {
            if (getStrLen() > other.getStrLen()) {
                throw new DdlException("Cannot shorten string length");
            }
        }

        // now we support convert decimal to varchar type
        if ((getDataType() == PrimitiveType.DECIMAL && other.getDataType() == PrimitiveType.VARCHAR)
                || (getDataType() == PrimitiveType.DECIMALV2 && other.getDataType() == PrimitiveType.VARCHAR)) {
            return;
        }

        if (this.getPrecision() != other.getPrecision()) {
            throw new DdlException("Cannot change precision");
        }

        if (this.getScale() != other.getScale()) {
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
        sb.append("COMMENT \"").append(comment).append("\"");

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
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

        if (!this.name.equalsIgnoreCase(other.getName())) {
            return false;
        }
        if (this.getDataType() != other.getDataType()) {
            return false;
        }
        if (this.aggregationType != other.getAggregationType()) {
            return false;
        }
        if (this.isAggregationTypeImplicit != other.isAggregationTypeImplicit()) {
            return false;
        }
        if (this.isKey != other.isKey()) {
            return false;
        }
        if (this.isAllowNull != other.isAllowNull) {
            return false;
        }
        if (this.getDefaultValue() == null) {
            if (other.getDefaultValue() != null) {
                return false;
            }
        } else {
            if (!this.getDefaultValue().equals(other.getDefaultValue())) {
                return false;
            }
        }

        if (this.getStrLen() != other.getStrLen()) {
            return false;
        }
        if (this.getPrecision() != other.getPrecision()) {
            return false;
        }
        if (this.getScale() != other.getScale()) {
            return false;
        }

        if (!comment.equals(other.getComment())) {
            return false;
        }
        if (!visible == other.visible) {
            return false;
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

            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
                isAggregationTypeImplicit = in.readBoolean();
            } else {
                isAggregationTypeImplicit = false;
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_30) {
            isKey = in.readBoolean();
        } else {
            isKey = (aggregationType == null);
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_22) {
            isAllowNull = in.readBoolean();
        } else {
            isAllowNull = false;
        }

        notNull = in.readBoolean();
        if (notNull) {
            defaultValue = Text.readString(in);
        }
        stats = ColumnStats.read(in);

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_10) {
            comment = Text.readString(in);
        } else {
            comment = "";
        }
    }

    public static Column read(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_86) {
            Column column = new Column();
            column.readFields(in);
            return column;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, Column.class);
        }
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
            case DECIMAL:
            case DECIMALV2:
                sb.append(String.format(typeStringMap.get(dataType), getPrecision(), getScale()));
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
