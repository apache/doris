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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * It like a SlotRef except that it is not a real column exist in table.
 */
public class VirtualSlotRef extends SlotRef {
    // results of analysis slot
    private TupleDescriptor tupleDescriptor;
    private List<Expr> realSlots;

    protected VirtualSlotRef(VirtualSlotRef other) {
        super(other);
        if (other.realSlots != null) {
            realSlots = Expr.cloneList(other.realSlots);
        }
        tupleDescriptor = other.tupleDescriptor;
    }

    public VirtualSlotRef(SlotDescriptor desc) {
        super(desc);
    }

    @Override
    public void getTableIdToColumnNames(Map<Long, Set<String>> tableIdToColumnNames) {
    }

    @Override
    public Expr clone() {
        return new VirtualSlotRef(this);
    }

    @Override
    public String getExprName() {
        return super.getExprName();
    }

    @Override
    public boolean supportSerializable() {
        return false;
    }
}
