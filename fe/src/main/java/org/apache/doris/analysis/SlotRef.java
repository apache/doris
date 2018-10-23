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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSlotRef;

import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class SlotRef extends Expr {
    private static final Logger LOG = LogManager.getLogger(SlotRef.class);
    private TableName tblName;
    private String col;
    // Used in toSql
    private String label;

    // results of analysis
    private SlotDescriptor desc;

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

    // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
    // a table's column.
    public SlotRef(SlotDescriptor desc) {
        super();
        this.tblName = null;
        this.col = null;
        this.desc = desc;
        this.type = desc.getType();
        // TODO(zc): label is meaningful
        this.label = null;
        if (this.type == Type.CHAR) {
            this.type = Type.VARCHAR;
        }
        analysisDone();
    }

    protected SlotRef(SlotRef other) {
        super(other);
        tblName = other.tblName;
        col = other.col;
        label = other.label;
        desc = other.desc;
    }

    @Override
    public Expr clone() {
        return new SlotRef(this);
    }

    public boolean isHllType() {
        return this.type == Type.HLL;
    }

    public SlotDescriptor getDesc() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc;
    }

    public SlotId getSlotId() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }

    // NOTE: this is used to set tblName to null,
    // so we can to get the only column name when calling toSql
    public void setTblName(TableName name) {
        this.tblName = name;
    }

    @Override
    public void vectorizedAnalyze(Analyzer analyzer) {
        computeOutputColumn(analyzer);
    }

    @Override
    public void computeOutputColumn(Analyzer analyzer) {
        outputColumn = desc.getSlotOffset();
        LOG.debug("SlotRef: " + debugString() + " outputColumn: " + outputColumn);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        desc = analyzer.registerColumnRef(tblName, col);
        type = desc.getType();
        if (this.type == Type.CHAR) {
            this.type = Type.VARCHAR;
        }
        if (!type.isSupported()) {
            throw new AnalysisException(
                    "Unsupported type '" + type.toString() + "' in '" + toSql() + "'.");
        }
        numDistinctValues = desc.getStats().getNumDistinctValues();
        if (type == Type.BOOLEAN) {
            selectivity = DEFAULT_SELECTIVITY;
        }
    }

    @Override
    public String debugString() {
        ToStringHelper helper = Objects.toStringHelper(this);
        helper.add("slotDesc", desc != null ? desc.debugString() : "null");
        helper.add("col", col);
        helper.add("label", label);
        helper.add("tblName", tblName != null ? tblName.toSql() : "null");
        return helper.toString();
    }

    @Override
    public String toSqlImpl() {
        StringBuilder sb = new StringBuilder();

        // if (desc != null) {
        //     sb.append("[tupleId=");
        //     sb.append(desc.getParent().getId().asInt());
        //     sb.append(",SlotId=");
        //     sb.append(desc.getId().asInt());
        //     sb.append("]");
        // }
        if (tblName != null) {
            return tblName.toSql() + "." + label + sb.toString();
        } else if (label != null) {
            return label + sb.toString();
        } else {
            return "<slot " + Integer.toString(desc.getId().asInt()) + ">" + sb.toString();
        }
    }

    @Override
    public String toMySql() {
        if (col != null) {
            return col;
        } else {
            return "<slot " + Integer.toString(desc.getId().asInt()) + ">";
        }
    }

    public TableName getTableName() {
        return tblName;
    }

    @Override
    public String toColumnLabel() {
        // return tblName == null ? col : tblName.getTbl() + "." + col;
        return col;
    }


    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        msg.slot_ref = new TSlotRef(desc.getId().asInt(), desc.getParent().getId().asInt());
        msg.setOutput_column(outputColumn);
    }

    @Override
    public void markAgg() {
        desc.setIsAgg(true);
    }

    @Override
    public int hashCode() {
        if (desc != null) {
            return desc.getId().hashCode();
        }
        return Objects.hashCode((tblName.toSql() + "." + label).toLowerCase());
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
        if ((tblName == null) != (other.tblName == null)) {
            return false;
        }
        if (tblName != null && !tblName.equals(other.tblName)) {
            return false;
        }
        if ((col == null) != (other.col == null)) {
            return false;
        }
        if (col != null && !col.toLowerCase().equals(other.col.toLowerCase())) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() { return false; }

    @Override
    public boolean isBoundByTupleIds(List<TupleId> tids) {
        Preconditions.checkState(desc != null);
        for (TupleId tid: tids) {
            if (tid.equals(desc.getParent().getId())) return true;
        }
        return false;
    }

    @Override
    public boolean isBound(SlotId slotId) {
        Preconditions.checkState(isAnalyzed);
        return desc.getId().equals(slotId);
    }

    @Override
    public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
        Preconditions.checkState(type != Type.INVALID);
        Preconditions.checkState(desc != null);
        if (slotIds != null) {
            slotIds.add(desc.getId());
        }
        if (tupleIds != null) {
            tupleIds.add(desc.getParent().getId());
        }
    }

    public String getColumnName() {
        return col;
    }

    public void setCol(String col) {
        this.col = col;
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TableName
        if (tblName == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            tblName.write(out);
        }
        Text.writeString(out, col);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in.readBoolean()) {
            tblName = new TableName();
            tblName.readFields(in);
        }
        col = Text.readString(in);
    }

    public static SlotRef read(DataInput in) throws IOException {
        SlotRef slotRef = new SlotRef();
        slotRef.readFields(in);
        return slotRef;
    }
}
