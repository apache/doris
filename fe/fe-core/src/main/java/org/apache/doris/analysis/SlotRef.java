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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SlotRef.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.info.TableNameInfo;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class SlotRef extends Expr {
    @SerializedName("tn")
    private TableNameInfo tableNameInfo;
    @SerializedName("col")
    private String col;
    // Used in toSql
    private String label;
    private List<String> subColPath;

    // results of analysis
    protected SlotDescriptor desc;

    // Only used write
    private SlotRef() {
        super();
    }

    public SlotRef(TableNameInfo tableNameInfo, String col) {
        super();
        this.tableNameInfo = tableNameInfo;
        this.col = col;
        this.label = "`" + col + "`";
        this.nullable = false;
    }

    // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
    // a table's column.
    public SlotRef(SlotDescriptor desc) {
        super();
        this.tableNameInfo = null;
        this.col = desc.getColumn() != null ? desc.getColumn().getName() : null;
        this.desc = desc;
        this.type = desc.getType();
        // TODO(zc): label is meaningful
        this.label = desc.getLabel();
        if (this.type.equals(Type.CHAR)) {
            this.type = Type.VARCHAR;
        }
        this.subColPath = desc.getSubColLables();
        this.nullable = desc.getIsNullable();
    }

    // nereids use this constructor to build aggFnParam
    public SlotRef(Type type, boolean nullable) {
        super();
        // tuple id and slot id is meaningless here, nereids just use type and nullable
        // to build the TAggregateExpr.param_types
        TupleDescriptor tupleDescriptor = new TupleDescriptor(new TupleId(-1));
        desc = new SlotDescriptor(new SlotId(-1), tupleDescriptor);
        tupleDescriptor.addSlot(desc);
        desc.setIsNullable(nullable);
        this.nullable = nullable;
        desc.setType(type);
        this.type = type;
    }

    protected SlotRef(SlotRef other) {
        super(other);
        tableNameInfo = other.tableNameInfo;
        col = other.col;
        label = other.label;
        desc = other.desc;
        subColPath = other.subColPath;
    }

    @Override
    public Expr clone() {
        return new SlotRef(this);
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }

    public String getCol() {
        return col;
    }

    public String getLabel() {
        return label;
    }

    public List<String> getSubColPath() {
        return subColPath;
    }

    public SlotDescriptor getDesc() {
        return desc;
    }

    public SlotId getSlotId() {
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }

    public Column getColumn() {
        if (desc == null) {
            return null;
        } else {
            return desc.getColumn();
        }
    }

    // NOTE: this is used to set tblName to null,
    // so we can only get column name when calling toSql
    public void setTableNameInfoToNull() {
        this.tableNameInfo = null;
    }

    public void setDesc(SlotDescriptor desc) {
        this.desc = desc;
    }

    @Override
    public String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("slotDesc", desc != null ? desc.debugString() : "null");
        helper.add("col", col);
        helper.add("type", type.toSql());
        helper.add("label", label);
        helper.add("tblName", tableNameInfo != null ? tableNameInfo.toSql() : "null");
        helper.add("subColPath", subColPath);
        return helper.toString();
    }

    @Override
    public <R, C> R accept(ExprVisitor<R, C> visitor, C context) {
        return visitor.visitSlotRef(this, context);
    }

    @Override
    public int hashCode() {
        if (desc != null) {
            return desc.getId().hashCode();
        }
        if (subColPath == null || subColPath.isEmpty()) {
            return Objects.hashCode((tableNameInfo == null ? "" : tableNameInfo.toSql() + "." + label).toLowerCase());
        }
        int result = Objects.hashCode((tableNameInfo == null ? "" : tableNameInfo.toSql() + "." + label).toLowerCase());
        for (String sublabel : subColPath) {
            result = 31 * result + Objects.hashCode(sublabel);
        }
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        SlotRef other = (SlotRef) obj;
        // check slot ids first; if they're both set we only need to compare those
        // (regardless of how the ref was constructed)
        if (desc != null && other.desc != null) {
            return desc.getId().equals(other.desc.getId());
        }
        return notCheckDescIdEquals(obj);
    }

    @Override
    public boolean notCheckDescIdEquals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        SlotRef other = (SlotRef) obj;
        if ((tableNameInfo == null) != (other.tableNameInfo == null)) {
            return false;
        }
        if (tableNameInfo != null && !tableNameInfo.equals(other.tableNameInfo)) {
            return false;
        }
        if ((col == null) != (other.col == null)) {
            return false;
        }
        if (col != null && !col.equalsIgnoreCase(other.col)) {
            return false;
        }
        if ((subColPath == null) != (other.subColPath == null)) {
            return false;
        }
        if (subColPath != null
                && subColPath.equals(other.subColPath)) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        if (desc != null) {
            List<Expr> exprs = desc.getSourceExprs();
            return CollectionUtils.isNotEmpty(exprs) && exprs.stream().allMatch(Expr::isConstant);
        }
        return false;
    }

    @Override
    public boolean isBound(SlotId slotId) {
        return desc.getId().equals(slotId);
    }

    @Override
    public void getTableIdToColumnNames(Map<Long, Set<String>> tableIdToColumnNames) {
        if (desc == null) {
            return;
        }

        if (col == null) {
            for (Expr expr : desc.getSourceExprs()) {
                expr.getTableIdToColumnNames(tableIdToColumnNames);
            }
        } else {
            if (desc.getParent().getTable() == null) {
                // Maybe this column comes from inline view.
                return;
            }
            Long tableId = desc.getParent().getTable().getId();
            Set<String> columnNames = tableIdToColumnNames.get(tableId);
            if (columnNames == null) {
                columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                tableIdToColumnNames.put(tableId, columnNames);
            }
            columnNames.add(desc.getColumn().getName());
        }
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getColumnName() {
        if (subColPath != null && !subColPath.isEmpty()) {
            return col + "." + String.join(".", subColPath);
        }
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (tableNameInfo != null) {
            builder.append(tableNameInfo).append(".");
        }
        if (label != null) {
            builder.append(label);
        }
        return builder.toString();
    }
}
