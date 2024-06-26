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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/TupleDescriptor.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.ColumnStats;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.thrift.TTupleDescriptor;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
    private TableIf table;
    // underlying table, if there is one
    private TableRef ref;

    // All legal aliases of this tuple.
    private String[] aliases;

    // If true, requires that aliases_.length() == 1. However, aliases_.length() == 1
    // does not imply an explicit alias because nested collection refs have only a
    // single implicit alias.
    private boolean hasExplicitAlias;

    // if false, this tuple doesn't need to be materialized
    private boolean isMaterialized = true;

    private int byteSize;  // of all slots plus null indicators

    // This cardinality is only used to mock slot ndv.
    // Only tuple of olap scan node has this value.
    private long cardinality;

    private float avgSerializedSize;  // in bytes; includes serialization overhead

    private int tableId = -1;

    public TupleDescriptor(TupleId id) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.debugName = "";
        this.cardinality = -1;
    }

    public TupleDescriptor(TupleId id, String debugName) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.debugName = debugName;
        this.cardinality = -1;
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

    public void setRef(TableRef tableRef) {
        ref = tableRef;
    }

    public ArrayList<SlotDescriptor> getSlots() {
        return slots;
    }

    public void setTableId(int id) {
        tableId = id;
    }

    /**
     * get slot desc by slot id.
     *
     * @param slotId slot id
     * @return this slot's desc
     */
    public SlotDescriptor getSlot(int slotId) {
        for (SlotDescriptor slotDesc : slots) {
            if (slotDesc.getId().asInt() == slotId) {
                return slotDesc;
            }
        }
        return null;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    public ArrayList<SlotDescriptor> getMaterializedSlots() {
        ArrayList<SlotDescriptor> result = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            if (slot.isMaterialized()) {
                result.add(slot);
            }
        }
        return result;
    }

    public ArrayList<SlotId> getMaterializedSlotIds() {
        ArrayList<SlotId> result = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            if (slot.isMaterialized()) {
                result.add(slot.getId());
            }
        }
        return result;
    }

    public ArrayList<SlotId> getAllSlotIds() {
        ArrayList<SlotId> result = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            result.add(slot.getId());
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

    public boolean hasVariantCol() {
        for (SlotDescriptor slotDesc : slots) {
            if (slotDesc.getColumn() != null && slotDesc.getColumn().getType().isVariantType()) {
                return true;
            }
        }
        return false;
    }

    public TableIf getTable() {
        return table;
    }

    public void setTable(TableIf tbl) {
        table = tbl;
    }

    public int getByteSize() {
        return byteSize;
    }

    public void setIsMaterialized(boolean value) {
        isMaterialized = value;
    }

    public boolean isMaterialized() {
        return isMaterialized;
    }

    public float getAvgSerializedSize() {
        return avgSerializedSize;
    }

    public void setAliases(String[] aliases, boolean hasExplicitAlias) {
        this.aliases = aliases;
        this.hasExplicitAlias = hasExplicitAlias;
    }

    public boolean hasExplicitAlias() {
        return hasExplicitAlias;
    }

    public String getAlias() {
        return (aliases != null) ? aliases[0] : null;
    }

    public String getLastAlias() {
        return (aliases != null) ? aliases[aliases.length - 1] : null;
    }

    public TableName getAliasAsName() {
        return (aliases != null) ? new TableName(aliases[0]) : null;
    }

    public TTupleDescriptor toThrift() {
        TTupleDescriptor ttupleDesc = new TTupleDescriptor(id.asInt(), 0, 0);
        if (table != null && table.getId() >= 0) {
            ttupleDesc.setTableId((int) table.getId());
        }
        if (tableId > 0) {
            ttupleDesc.setTableId(tableId);
        }
        return ttupleDesc;
    }

    /**
     * This function is mainly used to calculate the statistics of the tuple and the layout information.
     * Generally, it occurs after the plan node materializes the slot and before calculating the plan node statistics.
     * PlanNode.init() {
     *     materializedSlot();
     *     tupleDesc.computeStatAndMemLayout();
     *     computeStat();
     * }
     */
    public void computeStatAndMemLayout() {
        computeStat();
        computeMemLayout();
    }

    /**
     * This function is mainly used to evaluate the statistics of the tuple,
     * such as the average size of each row.
     * This function will be used before the computeStat() of the plan node
     * and is the pre-work for evaluating the statistics of the plan node.
     *
     * This function is theoretically only called once when the plan node is init.
     * However, the current code structure is relatively confusing
     * In order to ensure that even if it is wrongly called a second time, no error will occur,
     * so it will be initialized again at the beginning of the function.
     *
     * @deprecated In the future this function will be changed to a private function.
     */
    @Deprecated
    public void computeStat() {
        // init stat
        avgSerializedSize = 0;

        // compute stat
        for (SlotDescriptor d : slots) {
            if (!d.isMaterialized()) {
                continue;
            }
            ColumnStats stats = d.getStats();
            if (stats.hasAvgSerializedSize()) {
                avgSerializedSize += d.getStats().getAvgSerializedSize();
            } else {
                // TODO: for computed slots, try to come up with stats estimates
                avgSerializedSize += d.getType().getSlotSize();
            }
        }
    }

    /**
     * @deprecated In the future this function will be changed to a private function.
     */
    @Deprecated
    public void computeMemLayout() {
        // sort slots by size
        List<List<SlotDescriptor>> slotsBySize = Lists.newArrayListWithCapacity(PrimitiveType.getMaxSlotSize());
        for (int i = 0; i <= PrimitiveType.getMaxSlotSize(); ++i) {
            slotsBySize.add(new ArrayList<SlotDescriptor>());
        }

        // populate slotsBySize; also compute avgSerializedSize
        for (SlotDescriptor d : slots) {
            if (d.isMaterialized()) {
                slotsBySize.get(d.getType().getSlotSize()).add(d);
            }
        }
        // we shouldn't have anything of size 0
        Preconditions.checkState(slotsBySize.get(0).isEmpty());

        // slotIdx is the index into the resulting tuple struct.  The first (smallest) field
        // is 0, next is 1, etc.
        int slotIdx = 0;
        for (int slotSize = 1; slotSize <= PrimitiveType.getMaxSlotSize(); ++slotSize) {
            if (slotsBySize.get(slotSize).isEmpty()) {
                continue;
            }

            for (SlotDescriptor d : slotsBySize.get(slotSize)) {
                d.setByteSize(slotSize);
                d.setSlotIdx(slotIdx++);
                byteSize += slotSize;
            }
        }
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
        for (SlotDescriptor slot : slots) {
            slot.setIsMaterialized(true);
        }
    }

    public void getTableIdToColumnNames(Map<Long, Set<String>> tableIdToColumnNames) {
        for (SlotDescriptor slotDescriptor : slots) {
            if (!slotDescriptor.isMaterialized()) {
                continue;
            }
            if (slotDescriptor.getColumn() != null) {
                TupleDescriptor parent = slotDescriptor.getParent();
                Preconditions.checkState(parent != null);
                TableIf table = parent.getTable();
                Preconditions.checkState(table != null);
                Long tableId = table.getId();
                Set<String> columnNames = tableIdToColumnNames.get(tableId);
                if (columnNames == null) {
                    columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    tableIdToColumnNames.put(tableId, columnNames);
                }
                columnNames.add(slotDescriptor.getColumn().getName());
            } else {
                for (Expr expr : slotDescriptor.getSourceExprs()) {
                    expr.getTableIdToColumnNames(tableIdToColumnNames);
                }
            }
        }
    }

    public Set<String> getColumnNames() {
        Map<Long, Set<String>> columnNamesInQueryOutput = Maps.newHashMap();
        getTableIdToColumnNames(columnNamesInQueryOutput);
        Set<String> columnNames = Sets.newHashSet();
        for (Set<String> names : columnNamesInQueryOutput.values()) {
            columnNames.addAll(names);
        }
        return columnNames;
    }

    @Override
    public String toString() {
        String tblStr = (table == null ? "null" : table.getName());
        List<String> slotStrings = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            slotStrings.add(slot.debugString());
        }
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("tbl", tblStr)
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
                .add("tbl", tblStr));
        builder.append("\n");
        for (SlotDescriptor slot : slots) {
            if (slot.isMaterialized()) {
                builder.append(slot.getExplainString(prefix)).append("\n");
            }
        }
        return builder.toString();
    }
}
