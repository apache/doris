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

import java.util.ArrayList;
import java.util.List;

import org.apache.doris.alter.SchemaChangeHandler;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TColumn;
import org.apache.doris.thrift.TColumnType;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class represents the column-related metadata.
 */
public class Column implements Writable {
    private static final Logger LOG = LogManager.getLogger(Column.class);

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
    @SerializedName(value = "name")
    private List<Column> children;

    public Column() {
        this.name = "";
        this.type = Type.NULL;
        this.isAggregationTypeImplicit = false;
        this.isKey = false;
        this.stats = new ColumnStats();
        this.children = new ArrayList<>(Type.MAX_NESTING_DEPTH);
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

        this.children = new ArrayList<>(Type.MAX_NESTING_DEPTH);

        createChildrenColumn(this.type, this);
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
        this.children = column.getChildren();
    }

    public void createChildrenColumn(Type type, Column column) {
        if (type.isArrayType()) {
            Column c = new Column(COLUMN_ARRAY_CHILDREN, ((ArrayType) type).getItemType());
            column.addChildrenColumn(c);
        }
    }

    public List<Column> getChildren() {
        return children;
    }

    private void addChildrenColumn(Column column) {
        this.children.add(column);
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return this.name;
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

    public PrimitiveType getDataType() { return type.getPrimitiveType(); }

    public Type getType() { return ScalarType.createType(type.getPrimitiveType()); }

    public Type getOriginType() { return type; }

    public int getStrLen() { return ((ScalarType) type).getLength(); }
    public int getPrecision() { return ((ScalarType) type).getScalarPrecision(); }
    public int getScale() { return ((ScalarType) type).getScalarScale(); }

    public AggregateType getAggregationType() {
        return this.aggregationType;
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
        tColumn.setColumn_name(this.name);

        TColumnType tColumnType = new TColumnType();
        tColumnType.setType(this.getDataType().toThrift());
        tColumnType.setLen(this.getStrLen());
        tColumnType.setPrecision(this.getPrecision());
        tColumnType.setScale(this.getScale());

        tColumnType.setIndex_len(this.getOlapColumnIndexSize());

        tColumn.setColumn_type(tColumnType);
        if (null != this.aggregationType) {
            tColumn.setAggregation_type(this.aggregationType.toThrift());
        }
        tColumn.setIs_key(this.isKey);
        tColumn.setIs_allow_null(this.isAllowNull);
        tColumn.setDefault_value(this.defaultValue);

        toChildrenThrift(this, tColumn);

        return tColumn;
    }

    private void toChildrenThrift(Column column, TColumn tColumn) {
        if (column.type.isArrayType()) {
            Column children = column.getChildren().get(0);

            TColumn childrenTColumn = new TColumn();
            childrenTColumn.setColumn_name(children.name);

            TColumnType childrenTColumnType = new TColumnType();
            childrenTColumnType.setType(children.getDataType().toThrift());
            childrenTColumnType.setType(children.getDataType().toThrift());
            childrenTColumnType.setLen(children.getStrLen());
            childrenTColumnType.setPrecision(children.getPrecision());
            childrenTColumnType.setScale(children.getScale());

            childrenTColumnType.setIndex_len(children.getOlapColumnIndexSize());
            childrenTColumn.setColumn_type(childrenTColumnType);


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

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("`").append(name).append("` ");
        String typeStr = type.toSql();
        sb.append(typeStr).append(" ");
        if (aggregationType != null && aggregationType != AggregateType.NONE && !isAggregationTypeImplicit) {
            sb.append(aggregationType.name()).append(" ");
        }
        if (isAllowNull) {
            sb.append("NULL ");
        } else {
            sb.append("NOT NULL ");
        }
        if (defaultValue != null && getDataType() != PrimitiveType.HLL) {
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
        Text.writeString(out, name);
        ColumnType.write(out, type);
        if (null == aggregationType) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, aggregationType.name());
            out.writeBoolean(isAggregationTypeImplicit);
        }

        out.writeBoolean(isKey);
        out.writeBoolean(isAllowNull);

        if (defaultValue == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, defaultValue);
        }
        stats.write(out);

        Text.writeString(out, comment);

        out.writeInt(children.size());

        for (Column child : children) {
            child.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
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

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_72) {
            int childrenSize = in.readInt();

            for (int i = 0; i < childrenSize; i++) {
                children.add(Column.read(in));
            }

            if (type.isArrayType()) {
                ((ArrayType)type).setItemType(children.get(0).type);
            }
        }
    }

    public static Column read(DataInput in) throws IOException {
        Column column = new Column();
        column.readFields(in);
        return column;
    }
}
