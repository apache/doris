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
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.OdbcTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.ToSqlContext;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSlotRef;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
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
        analysisDone();
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

    public SlotDescriptor getDesc() {
        return desc;
    }

    public SlotId getSlotId() {
        Preconditions.checkState(isAnalyzed);
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
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();
        String subColumnPaths = "";
        if (subColPath != null && !subColPath.isEmpty()) {
            subColumnPaths = "." + String.join(".", subColPath);
        }
        if (tableNameInfo != null) {
            return tableNameInfo.toSql() + "." + label + subColumnPaths;
        } else if (label != null) {
            if (ConnectContext.get() != null
                    && ConnectContext.get().getState().isNereids()
                    && !ConnectContext.get().getState().isQuery()
                    && ConnectContext.get().getSessionVariable() != null
                    && desc != null) {
                return label + "[#" + desc.getId().asInt() + "]";
            } else {
                return label;
            }
        } else if (desc == null) {
            // virtual slot of an alias function
            // when we try to translate an alias function to Nereids style, the desc in the place holding slotRef
            // is null, and we just need the name of col.
            return "`" + col + "`";
        } else if (desc.getSourceExprs() != null) {
            if ((ToSqlContext.get() == null || ToSqlContext.get().isNeedSlotRefId())) {
                if (desc.getId().asInt() != 1) {
                    sb.append("<slot " + desc.getId().asInt() + ">");
                }
            }
            for (Expr expr : desc.getSourceExprs()) {
                sb.append(" ");
                sb.append(expr.toSql());
            }
            return sb.toString();
        } else {
            return "<slot " + desc.getId().asInt() + ">" + sb.toString();
        }
    }

    @Override
    public String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf inputTable) {
        if (needExternalSql) {
            return toExternalSqlImpl(tableType, inputTable);
        }

        if (disableTableName && label != null) {
            return label;
        }

        StringBuilder sb = new StringBuilder();
        String subColumnPaths = "";
        if (subColPath != null && !subColPath.isEmpty()) {
            subColumnPaths = "." + String.join(".", subColPath);
        }
        if (tableNameInfo != null) {
            return tableNameInfo.toSql() + "." + label + subColumnPaths;
        } else if (label != null) {
            if (ConnectContext.get() != null
                    && ConnectContext.get().getState().isNereids()
                    && !ConnectContext.get().getState().isQuery()
                    && ConnectContext.get().getSessionVariable() != null
                    && desc != null) {
                return label + "[#" + desc.getId().asInt() + "]";
            } else {
                return label;
            }
        } else if (desc == null) {
            // virtual slot of an alias function
            // when we try to translate an alias function to Nereids style, the desc in the place holding slotRef
            // is null, and we just need the name of col.
            return "`" + col + "`";
        } else if (desc.getSourceExprs() != null) {
            if (!disableTableName && (ToSqlContext.get() == null || ToSqlContext.get().isNeedSlotRefId())) {
                if (desc.getId().asInt() != 1) {
                    sb.append("<slot " + desc.getId().asInt() + ">");
                }
            }
            for (Expr expr : desc.getSourceExprs()) {
                if (!disableTableName) {
                    sb.append(" ");
                }
                sb.append(disableTableName ? expr.toSqlWithoutTbl() : expr.toSql());
            }
            return sb.toString();
        } else {
            return "<slot " + desc.getId().asInt() + ">" + sb.toString();
        }
    }

    private String toExternalSqlImpl(TableType tableType, TableIf inputTable) {
        if (col != null) {
            if (tableType.equals(TableType.JDBC_EXTERNAL_TABLE) || tableType.equals(TableType.JDBC) || tableType
                    .equals(TableType.ODBC)) {
                if (inputTable instanceof JdbcTable) {
                    return ((JdbcTable) inputTable).getProperRemoteColumnName(
                            ((JdbcTable) inputTable).getJdbcTableType(), col);
                } else if (inputTable instanceof OdbcTable) {
                    return JdbcTable.databaseProperName(((OdbcTable) inputTable).getOdbcTableType(), col);
                } else {
                    return col;
                }
            } else {
                return col;
            }
        } else {
            return "<slot " + Integer.toString(desc.getId().asInt()) + ">";
        }
    }

    @Override
    public String toColumnLabel() {
        return col;
    }

    @Override
    public String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(toColumnLabel());
        }
        return this.exprName.get();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        msg.slot_ref = new TSlotRef(desc.getId().asInt(), desc.getParent().getId().asInt());
        msg.slot_ref.setColUniqueId(desc.getUniqueId());
        msg.slot_ref.setIsVirtualSlot(desc.getVirtualColumn() != null);
        msg.setLabel(label);
    }

    @Override
    protected void normalize(TExprNode msg, Normalizer normalizer) {
        msg.node_type = TExprNodeType.SLOT_REF;
        // we should eliminate the different tuple id to reuse query cache
        msg.slot_ref = new TSlotRef(
                normalizer.normalizeSlotId(desc.getId().asInt()),
                0
        );
        msg.slot_ref.setColUniqueId(desc.getUniqueId());
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
        Preconditions.checkState(isAnalyzed);
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
            TableIf table = desc.getParent().getTable();
            if (table == null) {
                // Maybe this column comes from inline view.
                return;
            }
            Long tableId = table.getId();
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
    public boolean isNullable() {
        Preconditions.checkNotNull(desc);
        return desc.getIsNullable();
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
