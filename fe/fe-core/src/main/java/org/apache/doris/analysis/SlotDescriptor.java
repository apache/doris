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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/SlotDescriptor.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TSlotDescriptor;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

public class SlotDescriptor {
    private static final Logger LOG = LogManager.getLogger(SlotDescriptor.class);
    private final SlotId id;
    private final TupleDescriptor parent;
    private Type type;
    private Column column;  // underlying column, if there is one

    // for SlotRef.toSql() in the absence of a path
    private String label;

    // Expr(s) materialized into this slot; multiple exprs for unions. Should be empty if
    // path_ is set.
    private List<Expr> sourceExprs = Lists.newArrayList();

    // if false, this slot doesn't need to be materialized in parent tuple
    // (and physical layout parameters are invalid)
    private boolean isMaterialized;

    // if false, this slot cannot be NULL
    private boolean isNullable;

    // physical layout parameters
    private int byteSize;
    private int byteOffset;  // within tuple
    private int nullIndicatorByte;  // index into byte array
    private int nullIndicatorBit; // index within byte
    private int slotIdx;          // index within tuple struct
    private int slotOffset;       // index within slot array list

    private ColumnStats stats;  // only set if 'column' isn't set
    private boolean isAgg;
    private boolean isMultiRef;
    // used for load to get more information of varchar and decimal
    private Type originType;

    public SlotDescriptor(SlotId id, TupleDescriptor parent) {
        this.id = id;
        this.parent = parent;
        this.byteOffset = -1;  // invalid
        this.isMaterialized = false;
        this.isNullable = true;
        this.isAgg = false;
        this.isMultiRef = false;
    }

    public SlotDescriptor(SlotId id, TupleDescriptor parent, SlotDescriptor src) {
        this.id = id;
        this.parent = parent;
        this.byteOffset = src.byteOffset;
        this.nullIndicatorBit = src.nullIndicatorBit;
        this.nullIndicatorByte = src.nullIndicatorByte;
        this.slotIdx = src.slotIdx;
        this.isMaterialized = src.isMaterialized;
        this.column = src.column;
        this.isNullable = src.isNullable;
        this.byteSize = src.byteSize;
        this.isAgg = false;
        this.stats = src.stats;
        this.type = src.type;
    }

    public boolean isMultiRef() {
        return isMultiRef;
    }

    public void setMultiRef(boolean isMultiRef) {
        this.isMultiRef = isMultiRef;
    }

    public boolean getIsAgg() {
        return isAgg;
    }

    public void setIsAgg(boolean agg) {
        isAgg = agg;
    }

    public int getNullIndicatorByte() {
        return nullIndicatorByte;
    }

    public void setNullIndicatorByte(int nullIndicatorByte) {
        this.nullIndicatorByte = nullIndicatorByte;
    }

    public int getNullIndicatorBit() {
        return nullIndicatorBit;
    }

    public void setNullIndicatorBit(int nullIndicatorBit) {
        this.nullIndicatorBit = nullIndicatorBit;
    }

    public SlotId getId() {
        return id;
    }

    public TupleDescriptor getParent() {
        return parent;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
        this.type = column.getType();
        this.originType = column.getOriginType();
    }

    public void setSrcColumn(Column column) {
        this.column = column;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public void setIsMaterialized(boolean value) {
        isMaterialized = value;
    }

    public void materializeSrcExpr() {
        if (sourceExprs == null) {
            return;
        }
        for (Expr expr : sourceExprs) {
            if (!(expr instanceof SlotRef)) {
                expr.materializeSrcExpr();
                continue;
            }
            SlotRef slotRef = (SlotRef) expr;
            SlotDescriptor slotDesc = slotRef.getDesc();
            slotDesc.setIsMaterialized(true);
            slotDesc.materializeSrcExpr();
        }
    }

    public boolean getIsNullable() {
        return isNullable;
    }

    public void setIsNullable(boolean value) {
        isNullable = value;
    }

    public int getByteSize() {
        return byteSize;
    }

    public void setByteSize(int byteSize) {
        this.byteSize = byteSize;
    }

    public int getByteOffset() {
        return byteOffset;
    }

    public void setByteOffset(int byteOffset) {
        this.byteOffset = byteOffset;
    }

    public void setSlotIdx(int slotIdx) {
        this.slotIdx = slotIdx;
    }

    public void setStats(ColumnStats stats) {
        this.stats = stats;
    }

    public ColumnStats getStats() {
        if (stats == null) {
            if (column != null) {
                stats = column.getStats();
            } else {
                stats = new ColumnStats();
            }
        }
        // FIXME(dhc): mock ndv
        stats.setNumDistinctValues((long) parent.getCardinality());
        return stats;
    }

    public void setSlotOffset(int slotOffset) {
        this.slotOffset = slotOffset;
    }

    public int getSlotOffset() {
        return slotOffset;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setSourceExprs(List<Expr> exprs) {
        sourceExprs = exprs;
    }

    public void setSourceExpr(Expr expr) {
        sourceExprs = Collections.singletonList(expr);
    }

    public void addSourceExpr(Expr expr) {
        sourceExprs.add(expr);
    }

    public List<Expr> getSourceExprs() {
        return sourceExprs;
    }


    /**
     * Initializes a slot by setting its source expression information
     */
    public void initFromExpr(Expr expr) {
        setIsNullable(expr.isNullable());
        setLabel(expr.toSql());
        Preconditions.checkState(sourceExprs.isEmpty());
        setSourceExpr(expr);
        setStats(ColumnStats.fromExpr(expr));
        Preconditions.checkState(expr.getType().isValid());
        setType(expr.getType());
    }

    /**
     * Return true if the physical layout of this descriptor matches the physical layout
     * of the other descriptor, but not necessarily ids.
     */
    public boolean layoutEquals(SlotDescriptor other) {
        if (!getType().equals(other.getType())) {
            return false;
        }
        if (isNullable != other.isNullable) {
            return false;
        }
        if (getByteSize() != other.getByteSize()) {
            return false;
        }
        if (getByteOffset() != other.getByteOffset()) {
            return false;
        }
        if (getNullIndicatorByte() != other.getNullIndicatorByte()) {
            return false;
        }
        if (getNullIndicatorBit() != other.getNullIndicatorBit()) {
            return false;
        }
        return true;
    }

    // TODO
    public TSlotDescriptor toThrift() {

        TSlotDescriptor tSlotDescriptor = new TSlotDescriptor(id.asInt(), parent.getId().asInt(),
                (originType != null ? originType.toThrift() : type.toThrift()), -1, byteOffset, nullIndicatorByte,
                nullIndicatorBit, ((column != null) ? column.getNonShadowName() : ""), slotIdx, isMaterialized);

        if (column != null) {
            LOG.debug("column name:{}, column unique id:{}", column.getNonShadowName(), column.getUniqueId());
            tSlotDescriptor.setColUniqueId(column.getUniqueId());
            tSlotDescriptor.setPrimitiveType(column.getDataType().toThrift());
        }
        return tSlotDescriptor;
    }

    public String debugString() {
        String colStr = (column == null ? "null" : column.getName());
        String typeStr = (type == null ? "null" : type.toString());
        String parentTupleId = (parent == null) ? "null" : parent.getId().toString();
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("parent", parentTupleId).add("col", colStr)
                .add("type", typeStr).add("materialized", isMaterialized).add("byteSize", byteSize)
                .add("byteOffset", byteOffset).add("nullIndicatorByte", nullIndicatorByte)
                .add("nullIndicatorBit", nullIndicatorBit).add("slotIdx", slotIdx).toString();
    }

    @Override
    public String toString() {
        return debugString();
    }

    public String getExplainString(String prefix) {
        StringBuilder builder = new StringBuilder();
        String colStr = (column == null ? "null" : column.getName());
        String typeStr = (type == null ? "null" : type.toString());
        builder.append(prefix).append("SlotDescriptor{")
                .append("id=").append(id).append(", col=").append(colStr).append(", type=").append(typeStr)
                .append(", nullable=").append(isNullable).append("}");
        return builder.toString();
    }

    public boolean isScanSlot() {
        return parent.getTable() instanceof OlapTable;
    }

}
