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

package org.apache.doris.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.optimizer.base.OptColumnRef;
import org.apache.doris.optimizer.base.OptColumnRefFactory;
import org.apache.doris.planner.PlanNodeId;

import java.util.List;
import java.util.Map;

// Used to record information when converting statement to optimization's expression
// and convert optimized expression back to plan node
public class OptConverterCtx {
    private OptColumnRefFactory columnRefFactory = new OptColumnRefFactory();
    private Map<Integer, OptColumnRef> slotIdToColumnRef = Maps.newHashMap();
    // map from OptColumnRef to SlotReference,
    private Map<Integer, SlotDescriptor> columnIdToSlotRef = Maps.newHashMap();
    private IdGenerator<PlanNodeId> planNodeIdGenerator = PlanNodeId.createGenerator();

    // Convert a TupleDescriptor to a list of OptColumnRef and record map information
    public List<OptColumnRef> convertTuple(TupleDescriptor tupleDesc) {
        List<OptColumnRef> columnRefs = Lists.newArrayList();
        for (SlotDescriptor slotDesc : tupleDesc.getSlots()) {
            columnRefs.add(createColumnRef(slotDesc));
        }
        return columnRefs;
    }

    // used to convert statement to OptExpression
    public OptColumnRef getColumnRef(int slotId) {
        return slotIdToColumnRef.get(slotId);
    }

    // used to convert OptExpression to PlanNode
    public SlotDescriptor getSlotRef(int columnId) {
        return columnIdToSlotRef.get(columnId);
    }

    public PlanNodeId nextPlanNodeId() {
        return planNodeIdGenerator.getNextId();
    }

    private OptColumnRef createColumnRef(SlotDescriptor slotDesc) {
        OptColumnRef columnRef = columnRefFactory.create(slotDesc.getLabel(), slotDesc.getType());
        slotIdToColumnRef.put(slotDesc.getId().asInt(), columnRef);
        columnIdToSlotRef.put(columnRef.getId(), slotDesc);
        return columnRef;
    }
}
