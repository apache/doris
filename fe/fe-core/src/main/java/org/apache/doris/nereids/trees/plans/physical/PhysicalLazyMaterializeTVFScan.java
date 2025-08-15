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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 wrapper for FileScan used for lazy materialization
 */
public class PhysicalLazyMaterializeTVFScan extends PhysicalTVFRelation {
    private PhysicalTVFRelation scan;
    private SlotReference rowId;
    private final List<Slot> lazySlots;
    private List<Slot> output;

    /**
     * PhysicalLazyMaterializeTVFScan
     */
    public PhysicalLazyMaterializeTVFScan(PhysicalTVFRelation scan, SlotReference rowId, List<Slot> lazySlots) {
        super(scan.getRelationId(), scan.getFunction(), scan.getOperativeSlots(), scan.getLogicalProperties());
        this.scan = scan;
        this.rowId = rowId;
        this.lazySlots = lazySlots;
    }

    @Override
    public List<Slot> computeOutput() {
        if (output == null) {
            output = ImmutableList.<Slot>builder()
                    .addAll(scan.getOperativeSlots())
                    .add(rowId).build();
        }
        return output;
    }

    @Override
    public List<Slot> getOutput() {
        return computeOutput();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PhysicalLazyMaterializeTVFScan[")
                .append(scan.toString());

        if (!getAppliedRuntimeFilters().isEmpty()) {
            getAppliedRuntimeFilters().forEach(rf -> sb.append(" RF").append(rf.getId().asInt()));
        }
        sb.append("]");
        return sb.toString();
    }
}
