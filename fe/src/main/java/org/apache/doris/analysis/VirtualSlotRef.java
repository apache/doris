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

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * It like a SlotRef except that it is not a real column exist in table.
 */
public class VirtualSlotRef extends SlotRef {
    private static final Logger LOG = LogManager.getLogger(VirtualSlotRef.class);
    // results of analysis slot

    private TupleDescriptor tupleDescriptor;
    private List<Expr> realSlots;

    public VirtualSlotRef(String col, Type type, TupleDescriptor tupleDescriptor, List<Expr> realSlots) {
        super(null, col);
        super.type = type;
        this.tupleDescriptor = tupleDescriptor;
        this.realSlots = realSlots;
    }

    protected VirtualSlotRef(VirtualSlotRef other) {
        super(other);
        if (other.realSlots != null) {
            realSlots = Expr.cloneList(other.realSlots);
        }
        tupleDescriptor = other.tupleDescriptor;
    }

    public static VirtualSlotRef read(DataInput in) throws IOException {
        VirtualSlotRef virtualSlotRef = new VirtualSlotRef(null, Type.BIGINT, null, new ArrayList<>());
        virtualSlotRef.readFields(in);
        return virtualSlotRef;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (CollectionUtils.isEmpty(realSlots)) {
            out.writeInt(0);
        } else {
            out.writeInt(realSlots.size());
            for (Expr slotRef : realSlots) {
                slotRef.write(out);
            }
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int realSlotsSize = in.readInt();
        if (realSlotsSize > 0) {
            for (int i = 0; i < realSlotsSize; i++) {
                realSlots.add(SlotRef.read(in));
            }
        }
    }

    public List<Expr> getRealSlots() {
        return realSlots;
    }

    public void setRealSlots(List<Expr> realSlots) {
        this.realSlots = realSlots;
    }

    @Override
    public Expr clone() {
        return new VirtualSlotRef(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        desc = analyzer.registerVirtualColumnRef(super.getColumnName(), type, tupleDescriptor);
        numDistinctValues = desc.getStats().getNumDistinctValues();
    }
}
