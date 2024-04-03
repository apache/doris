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
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/DescriptorTable.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.thrift.TDescriptorTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Repository for tuple (and slot) descriptors.
 * Descriptors should only be created through this class, which assigns
 * them unique ids..
 */
public class DescriptorTable {
    private static final Logger LOG = LogManager.getLogger(DescriptorTable.class);

    private final HashMap<TupleId, TupleDescriptor> tupleDescs = new HashMap<TupleId, TupleDescriptor>();
    // List of referenced tables with no associated TupleDescriptor to ship to the BE.
    // For example, the output table of an insert query.
    private final List<TableIf> referencedTables = new ArrayList<TableIf>();
    private final IdGenerator<TupleId> tupleIdGenerator = TupleId.createGenerator();
    private final IdGenerator<SlotId> slotIdGenerator = SlotId.createGenerator();
    private final HashMap<SlotId, SlotDescriptor> slotDescs = Maps.newHashMap();

    private final HashMap<SlotDescriptor, SlotDescriptor> outToIntermediateSlots = new HashMap<>();

    private TDescriptorTable thriftDescTable = null; // serialized version of this

    public DescriptorTable() {
    }

    public TupleDescriptor createTupleDescriptor() {
        TupleDescriptor d = new TupleDescriptor(tupleIdGenerator.getNextId());
        tupleDescs.put(d.getId(), d);
        return d;
    }

    public TupleDescriptor createTupleDescriptor(String debugName) {
        TupleDescriptor d = new TupleDescriptor(tupleIdGenerator.getNextId(), debugName);
        tupleDescs.put(d.getId(), d);
        return d;
    }

    public SlotDescriptor addSlotDescriptor(TupleDescriptor d) {
        SlotDescriptor result = new SlotDescriptor(slotIdGenerator.getNextId(), d);
        d.addSlot(result);
        slotDescs.put(result.getId(), result);
        return result;
    }

    /**
     * Used by new optimizer.
     */
    public SlotDescriptor addSlotDescriptor(TupleDescriptor d, int id) {
        SlotDescriptor result = new SlotDescriptor(new SlotId(id), d);
        d.addSlot(result);
        slotDescs.put(result.getId(), result);
        return result;
    }

    /**
     * Create copy of src with new id. The returned descriptor has its mem layout
     * computed.
     */
    public TupleDescriptor copyTupleDescriptor(TupleId srcId, String debugName) {
        TupleDescriptor d = new TupleDescriptor(tupleIdGenerator.getNextId(), debugName);
        tupleDescs.put(d.getId(), d);
        // create copies of slots
        TupleDescriptor src = tupleDescs.get(srcId);
        for (SlotDescriptor slot : src.getSlots()) {
            copySlotDescriptor(d, slot);
        }
        d.computeStatAndMemLayout();
        return d;
    }

    /**
     * Append copy of src to dest.
     */
    public SlotDescriptor copySlotDescriptor(TupleDescriptor dest, SlotDescriptor src) {
        SlotDescriptor result = new SlotDescriptor(slotIdGenerator.getNextId(), dest, src);
        dest.addSlot(result);
        slotDescs.put(result.getId(), result);
        return result;
    }

    public TupleDescriptor getTupleDesc(TupleId id) {
        return tupleDescs.get(id);
    }

    public HashMap<SlotId, SlotDescriptor> getSlotDescs() {
        return slotDescs;
    }

    /**
     * Return all tuple desc by idList.
     */
    public List<TupleDescriptor> getTupleDesc(List<TupleId> idList) throws AnalysisException {
        List<TupleDescriptor> result = Lists.newArrayList();
        for (TupleId tupleId : idList) {
            TupleDescriptor tupleDescriptor = getTupleDesc(tupleId);
            if (tupleDescriptor == null) {
                throw new AnalysisException("Invalid tuple id:" + tupleId.toString());
            }
            result.add(tupleDescriptor);
        }
        return result;
    }

    public SlotDescriptor getSlotDesc(SlotId id) {
        return slotDescs.get(id);
    }

    public Collection<TupleDescriptor> getTupleDescs() {
        return tupleDescs.values();
    }

    public void addReferencedTable(TableIf table) {
        referencedTables.add(table);
    }

    /**
     * Marks all slots in list as materialized.
     */
    public void markSlotsMaterialized(List<SlotId> ids) {
        for (SlotId id : ids) {
            getSlotDesc(id).setIsMaterialized(true);
        }
    }

    @Deprecated
    public void computeMemLayout() {
        for (TupleDescriptor d : tupleDescs.values()) {
            d.computeMemLayout();
        }
    }

    // Computes physical layout parameters of all descriptors and calculate the statistics of the tuple.
    // Call this only after the last descriptor was added.
    public void computeStatAndMemLayout() {
        for (TupleDescriptor d : tupleDescs.values()) {
            d.computeStatAndMemLayout();
        }
    }

    public void addSlotMappingInfo(Map<SlotDescriptor, SlotDescriptor> mapping) {
        outToIntermediateSlots.putAll(mapping);
    }

    public void materializeIntermediateSlots() {
        for (Map.Entry<SlotDescriptor, SlotDescriptor> entry : outToIntermediateSlots.entrySet()) {
            entry.getValue().setIsMaterialized(entry.getKey().isMaterialized());
        }
    }

    public TDescriptorTable toThrift() {
        if (thriftDescTable != null) {
            return thriftDescTable;
        }

        TDescriptorTable result = new TDescriptorTable();
        Map<Long, TableIf> referencedTbls = Maps.newHashMap();
        for (TupleDescriptor tupleD : tupleDescs.values()) {
            // inline view of a non-constant select has a non-materialized tuple descriptor
            // in the descriptor table just for type checking, which we need to skip
            if (tupleD.isMaterialized()) {
                result.addToTupleDescriptors(tupleD.toThrift());
                // an inline view of a constant select has a materialized tuple
                // but its table has no id
                if (tupleD.getTable() != null
                        && tupleD.getTable().getId() >= 0) {
                    referencedTbls.put(tupleD.getTable().getId(), tupleD.getTable());
                }
                for (SlotDescriptor slotD : tupleD.getMaterializedSlots()) {
                    result.addToSlotDescriptors(slotD.toThrift());
                }
            }
        }

        for (TableIf tbl : referencedTables) {
            referencedTbls.put(tbl.getId(), tbl);
        }

        for (TableIf tbl : referencedTbls.values()) {
            result.addToTableDescriptors(tbl.toThrift());
        }
        thriftDescTable = result;
        return result;
    }

    public String debugString() {
        StringBuilder out = new StringBuilder();
        out.append("tuples:\n");
        for (TupleDescriptor desc : tupleDescs.values()) {
            out.append(desc).append("\n");
        }
        out.append("\n ");
        out.append("slotDesc size: ").append(slotDescs.size()).append("\n");
        for (SlotDescriptor desc : slotDescs.values()) {
            out.append(desc.debugString());
            out.append("\n");
        }
        out.append("\n ");
        return out.toString();
    }

    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        out.append("\nTuples:\n");
        for (TupleDescriptor desc : tupleDescs.values()) {
            if (desc.isMaterialized()) {
                out.append(desc.getExplainString()).append("\n");
            }
        }
        return out.toString();
    }
}
