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

import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.thrift.TTupleDescriptor;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TupleDescriptor {
    private static final Logger LOG = LogManager.getLogger(TupleDescriptor.class);
    private final TupleId id;
    private final String debugName; // debug only
    private final ArrayList<SlotDescriptor> slots;

    // underlying table, if there is one
    private Table table;
    // underlying table, if there is one
    private TableRef ref;

    // All legal aliases of this tuple.
    private String[] aliases_;

    // If true, requires that aliases_.length() == 1. However, aliases_.length() == 1
    // does not imply an explicit alias because nested collection refs have only a
    // single implicit alias.
    private boolean hasExplicitAlias_;

    // if false, this tuple doesn't need to be materialized
    private boolean isMaterialized = true;

    private int byteSize;  // of all slots plus null indicators
    private int numNullBytes;
    private int numNullableSlots;

    private float avgSerializedSize;  // in bytes; includes serialization overhead

    public TupleDescriptor(TupleId id) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.debugName = "";
    }

    public TupleDescriptor(TupleId id, String debugName) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.debugName = debugName;
    }

    public void addSlot(SlotDescriptor desc) {
        desc.setSlotOffset(slots.size());
        slots.add(desc);
    }

    public TupleId getId() {
        return id;
    }

    public TableRef getRef() {
        return ref;
    }

    public void setRef(TableRef new_ref) {
        ref = new_ref;
    }

    public ArrayList<SlotDescriptor> getSlots() {
        return slots;
    }

    public ArrayList<SlotDescriptor> getMaterializedSlots() {
        ArrayList<SlotDescriptor> result = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            if (slot.isMaterialized()) result.add(slot);
        }
        return result;
    }

    /**
     * Return slot descriptor corresponding to column referenced in the context
     * of tupleDesc, or null if no such reference exists.
     */
    public SlotDescriptor getColumnSlot(String columnName) {
        for (SlotDescriptor slotDesc : slots) {
            if (slotDesc.getColumn() != null && slotDesc.getColumn().getName().equalsIgnoreCase(columnName)) {
                return slotDesc;
            }    
        }    
        return null;
    } 

    public Table getTable() {
        return table;
    }

    public void setTable(Table tbl) {
        table = tbl;
    }

    public int getByteSize() {
        return byteSize;
    }

    public boolean getIsMaterialized() {
        return isMaterialized;
    }

    public boolean isMaterialized() { return isMaterialized; }
    public void setIsMaterialized(boolean value) {
        isMaterialized = value;
    }

    public float getAvgSerializedSize() {
        return avgSerializedSize;
    }

    public void setAliases(String[] aliases, boolean hasExplicitAlias) {
        aliases_ = aliases;
        hasExplicitAlias_ = hasExplicitAlias;
    }
    public boolean hasExplicitAlias() { return hasExplicitAlias_; }
    public String getAlias() { return (aliases_ != null) ? aliases_[0] : null; }
    public TableName getAliasAsName() {
        return (aliases_ != null) ? new TableName(null, aliases_[0]) : null;
    }

    public TTupleDescriptor toThrift() {
        TTupleDescriptor ttupleDesc = new TTupleDescriptor(id.asInt(), byteSize, numNullBytes);
        ttupleDesc.setNumNullSlots(numNullableSlots);
        if (table != null && table.getId() >= 0) {
            ttupleDesc.setTableId((int) table.getId());
        }
        return ttupleDesc;
    }

    public void computeMemLayout() {
        // sort slots by size
        List<List<SlotDescriptor>> slotsBySize = Lists.newArrayListWithCapacity(PrimitiveType.getMaxSlotSize());
        for (int i = 0; i <= PrimitiveType.getMaxSlotSize(); ++i) {
            slotsBySize.add(new ArrayList<SlotDescriptor>());
        }

        // populate slotsBySize; also compute avgSerializedSize
        numNullableSlots = 0;
        for (SlotDescriptor d : slots) {
            ColumnStats stats = d.getStats();
            if (stats.hasAvgSerializedSize()) {
                avgSerializedSize += d.getStats().getAvgSerializedSize();
            } else {
                // TODO: for computed slots, try to come up with stats estimates
                avgSerializedSize += d.getType().getSlotSize();
            }
            if (d.isMaterialized()) {
                slotsBySize.get(d.getType().getSlotSize()).add(d);
                if (d.getIsNullable()) {
                    ++numNullableSlots;
                }
            }
        }
        // we shouldn't have anything of size 0
        Preconditions.checkState(slotsBySize.get(0).isEmpty());

        // assign offsets to slots in order of ascending size
        numNullBytes = (numNullableSlots + 7) / 8;
        int offset = numNullBytes;
        int nullIndicatorByte = 0;
        int nullIndicatorBit = 0;
        // slotIdx is the index into the resulting tuple struct.  The first (smallest) field
        // is 0, next is 1, etc.
        int slotIdx = 0;
        for (int slotSize = 1; slotSize <= PrimitiveType.getMaxSlotSize(); ++slotSize) {
            if (slotsBySize.get(slotSize).isEmpty()) {
                continue;
            }
            if (slotSize > 1) {
                // insert padding
                int alignTo = Math.min(slotSize, 8);
                if (slotSize == 40) {
                    alignTo = 4;
                }
                offset = (offset + alignTo - 1) / alignTo * alignTo;
            }

            for (SlotDescriptor d : slotsBySize.get(slotSize)) {
                d.setByteSize(slotSize);
                d.setByteOffset(offset);
                d.setSlotIdx(slotIdx++);
                offset += slotSize;

                // assign null indicator
                if (d.getIsNullable()) {
                    d.setNullIndicatorByte(nullIndicatorByte);
                    d.setNullIndicatorBit(nullIndicatorBit);
                    nullIndicatorBit = (nullIndicatorBit + 1) % 8;
                    if (nullIndicatorBit == 0) {
                        ++nullIndicatorByte;
                    }
                } else {
                    // Non-nullable slots will have 0 for the byte offset and -1 for the bit mask
                    d.setNullIndicatorBit(-1);
                    d.setNullIndicatorByte(0);
                }
            }
        }

        this.byteSize = offset;
        // LOG.debug("tuple is {}", byteSize);
    }

    /**
     * Returns true if tuples of type 'this' can be assigned to tuples of type 'desc'
     * (checks that both have the same number of slots and that slots are of the same type)
     */
    public boolean isCompatible(TupleDescriptor desc) {
        if (slots.size() != desc.slots.size()) {
            return false;
        }
        for (int i = 0; i < slots.size(); ++i) {
            if (slots.get(i).getType() != desc.slots.get(i).getType()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Materialize all slots.
     */
    public void materializeSlots() {
        for (SlotDescriptor slot: slots) slot.setIsMaterialized(true);
    }

    public void getTableNameToColumnNames(Map<String, Set<String>> tupleDescToColumnNames) {
        for (SlotDescriptor slotDescriptor : slots) {
            if (!slotDescriptor.isMaterialized()) {
                continue;
            }
            if (slotDescriptor.getColumn() != null) {
                TupleDescriptor parent = slotDescriptor.getParent();
                Preconditions.checkState(parent != null);
                Table table = parent.getTable();
                Preconditions.checkState(table != null);
                String tableName = table.getName();
                Set<String> columnNames = tupleDescToColumnNames.get(tableName);
                if (columnNames == null) {
                    columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    tupleDescToColumnNames.put(tableName, columnNames);
                }
                columnNames.add(slotDescriptor.getColumn().getName());
            } else {
                for (Expr expr : slotDescriptor.getSourceExprs()) {
                    expr.getTableNameToColumnNames(tupleDescToColumnNames);
                }
            }
        }
    }

    @Override
    public String toString() {
        String tblStr = (table == null ? "null" : table.getName());
        List<String> slotStrings = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            slotStrings.add(slot.debugString());
        }
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("tbl", tblStr).add("byte_size", byteSize)
                   .add("is_materialized", isMaterialized).add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
                   .toString();
    }

    public String debugString() {
        // TODO(zc):
        // String tblStr = (getTable() == null ? "null" : getTable().getFullName());
        String tblStr = (getTable() == null ? "null" : getTable().getName());
        List<String> slotStrings = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            slotStrings.add(slot.debugString());
        }
        return MoreObjects.toStringHelper(this)
                .add("id", id.asInt())
                .add("name", debugName)
                .add("tbl", tblStr)
                .add("byte_size", byteSize)
                .add("is_materialized", isMaterialized)
                .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
                .toString();
    }

    public String getExplainString() {
        StringBuilder builder = new StringBuilder();
        String prefix = "  ";
        String tblStr = (getTable() == null ? "null" : getTable().getName());

        builder.append(MoreObjects.toStringHelper(this)
                .add("id", id.asInt())
                .add("tbl", tblStr)
                .add("byteSize", byteSize)
                .add("materialized", isMaterialized)
                .toString());
        builder.append("\n");
        for (SlotDescriptor slot : slots) {
            builder.append(slot.getExplainString(prefix)).append("\n");
        }
        return builder.toString();
    }
}
