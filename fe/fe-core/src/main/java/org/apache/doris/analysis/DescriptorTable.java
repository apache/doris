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

import org.apache.doris.common.IdGenerator;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.HashMap;

/**
 * Repository for tuple (and slot) descriptors.
 * Descriptors should only be created through this class, which assigns
 * them unique ids.
 */
public class DescriptorTable {
    private final HashMap<TupleId, TupleDescriptor> tupleDescs = new HashMap<TupleId, TupleDescriptor>();
    private final IdGenerator<TupleId> tupleIdGenerator = TupleId.createGenerator();
    private final IdGenerator<SlotId> slotIdGenerator = SlotId.createGenerator();
    private final HashMap<SlotId, SlotDescriptor> slotDescs = Maps.newHashMap();

    public DescriptorTable() {
    }

    public TupleDescriptor createTupleDescriptor() {
        TupleDescriptor d = new TupleDescriptor(tupleIdGenerator.getNextId());
        tupleDescs.put(d.getId(), d);
        return d;
    }

    public SlotDescriptor addSlotDescriptor(TupleDescriptor d) {
        SlotDescriptor result = new SlotDescriptor(slotIdGenerator.getNextId(), d.getId());
        d.addSlot(result);
        slotDescs.put(result.getId(), result);
        return result;
    }

    public TupleDescriptor getTupleDesc(TupleId id) {
        return tupleDescs.get(id);
    }

    public Collection<TupleDescriptor> getTupleDescs() {
        return tupleDescs.values();
    }

    public String getExplainString() {
        StringBuilder out = new StringBuilder();
        out.append("\nTuples:\n");
        for (TupleDescriptor desc : tupleDescs.values()) {
            out.append(desc.getExplainString()).append("\n");
        }
        return out.toString();
    }
}
