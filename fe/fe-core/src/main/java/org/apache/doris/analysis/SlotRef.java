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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.ToSqlContext;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSlotRef;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public class SlotRef extends Expr {
    @SerializedName("tn")
    private TableName tblName;
    private TableIf table = null;
    private TupleId tupleId = null;
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

    public SlotRef(TableName tblName, String col) {
        super();
        this.tblName = tblName;
        this.col = col;
        this.label = "`" + col + "`";
    }

    public SlotRef(TableName tblName, String col, List<String> subColPath) {
        super();
        this.tblName = tblName;
        this.col = col;
        this.label = "`" + col + "`";
        this.subColPath = subColPath;
    }

    // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
    // a table's column.
    public SlotRef(SlotDescriptor desc) {
        super();
        this.tblName = null;
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
        tblName = other.tblName;
        col = other.col;
        label = other.label;
        desc = other.desc;
        tupleId = other.tupleId;
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
    // so we can to get the only column name when calling toSql
    public void setTblName(TableName name) {
        this.tblName = name;
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
        helper.add("tblName", tblName != null ? tblName.toSql() : "null");
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
        if (tblName != null) {
            return tblName.toSql() + "." + label + subColumnPaths;
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
        if (tblName != null) {
            return tblName.toSql() + "." + label + subColumnPaths;
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

    public TableName getTableName() {
        if (tblName == null) {
            Preconditions.checkState(isAnalyzed);
            Preconditions.checkNotNull(desc);
            Preconditions.checkNotNull(desc.getParent());
            if (desc.getParent().getRef() == null) {
                return null;
            }
            return desc.getParent().getRef().getName();
        }
        return tblName;
    }

    public TableName getOriginTableName() {
        return tblName;
    }

    @Override
    public String toColumnLabel() {
        // return tblName == null ? col : tblName.getTbl() + "." + col;
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
            return Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase());
        }
        int result = Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase());
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
        if ((tblName == null) != (other.tblName == null)) {
            return false;
        }
        if (tblName != null && !tblName.equals(other.tblName)) {
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

    public TupleId getTupleId() {
        return tupleId;
    }

    @Override
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        Preconditions.checkState(desc != null || tupleId != null);
        if (desc != null) {
            tupleId = desc.getParent().getId();
        }
        for (TupleId tid : tids) {
            if (tid.equals(tupleId)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBound(SlotId slotId) {
        Preconditions.checkState(isAnalyzed);
        return desc.getId().equals(slotId);
    }

    @Override
    public void getSlotRefsBoundByTupleIds(List<TupleId> tupleIds, Set<SlotRef> boundSlotRefs) {
        if (desc == null) {
            return;
        }
        if (tupleIds.contains(desc.getParent().getId())) {
            boundSlotRefs.add(this);
            return;
        }
        if (desc.getSourceExprs() == null) {
            return;
        }
        for (Expr sourceExpr : desc.getSourceExprs()) {
            sourceExpr.getSlotRefsBoundByTupleIds(tupleIds, boundSlotRefs);
        }
    }

    @Override
    public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
        Preconditions.checkState(!type.equals(Type.INVALID));
        Preconditions.checkState(desc != null);
        if (slotIds != null) {
            slotIds.add(desc.getId());
        }
        if (tupleIds != null) {
            tupleIds.add(desc.getParent().getId());
        }
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

    public void setTable(TableIf table) {
        this.table = table;
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
        if (tblName != null) {
            builder.append(tblName).append(".");
        }
        if (label != null) {
            builder.append(label);
        }
        return builder.toString();
    }

    @Override
    public Expr getResultValue(boolean forPushDownPredicatesToView) throws AnalysisException {
        if (!forPushDownPredicatesToView) {
            return this;
        }
        if (!isConstant() || desc == null) {
            return this;
        }
        List<Expr> exprs = desc.getSourceExprs();
        if (CollectionUtils.isEmpty(exprs)) {
            return this;
        }
        Expr expr = exprs.get(0);
        if (expr instanceof SlotRef) {
            return expr.getResultValue(forPushDownPredicatesToView);
        }
        if (expr.isConstant()) {
            return expr;
        }
        return this;
    }
}
