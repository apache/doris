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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.info.TableRefInfo;
import org.apache.doris.thrift.TTupleDescriptor;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TupleDescriptor {
    private final TupleId id;
    private final String debugName; // debug only
    private final ArrayList<SlotDescriptor> slots;
    private final HashMap<Integer, SlotDescriptor> idToSlotDesc;

    // underlying table, if there is one
    private TableIf table;
    // underlying table, if there is one
    private TableRefInfo ref;

    private int tableId = -1;

    public TupleDescriptor(TupleId id) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.idToSlotDesc = new HashMap<>();
        this.debugName = "";
    }

    public TupleDescriptor(TupleId id, String debugName) {
        this.id = id;
        this.slots = new ArrayList<SlotDescriptor>();
        this.idToSlotDesc = new HashMap<>();
        this.debugName = debugName;
    }

    public void addSlot(SlotDescriptor desc) {
        slots.add(desc);
        idToSlotDesc.putIfAbsent(desc.getId().asInt(), desc);
    }

    public TupleId getId() {
        return id;
    }

    public TableRefInfo getRef() {
        return ref;
    }

    public void setRef(TableRefInfo tableRefInfo) {
        ref = tableRefInfo;
    }

    public ArrayList<SlotDescriptor> getSlots() {
        return slots;
    }

    /**
     * get slot desc by slot id.
     *
     * @param slotId slot id
     * @return this slot's desc
     */
    public SlotDescriptor getSlot(int slotId) {
        return idToSlotDesc.get(slotId);
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

    public TableIf getTable() {
        return table;
    }

    public void setTable(TableIf tbl) {
        table = tbl;
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

    @Override
    public String toString() {
        String tblStr = (table == null ? "null" : table.getName());
        List<String> slotStrings = Lists.newArrayList();
        for (SlotDescriptor slot : slots) {
            slotStrings.add(slot.debugString());
        }
        return MoreObjects.toStringHelper(this).add("id", id.asInt()).add("tbl", tblStr)
                .add("slots", "[" + Joiner.on(", ").join(slotStrings) + "]")
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
            builder.append(slot.getExplainString(prefix)).append("\n");
        }
        return builder.toString();
    }
}
