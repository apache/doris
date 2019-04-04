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

package org.apache.doris.optimizer.base;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// BitSet used to accelerate column processing
public class OptColumnRefSet implements Cloneable {
    private BitSet bitSet;

    public OptColumnRefSet() {
        bitSet = new BitSet(1024);
    }

    public OptColumnRefSet(int id) {
        bitSet = new BitSet(1024);
        bitSet.set(id);
    }

    public OptColumnRefSet(List<OptColumnRef> refs) {
        bitSet = new BitSet(1024);
        for (OptColumnRef ref : refs) {
            bitSet.set(ref.getId());
        }
    }

    @Override
    public Object clone() {
        try {
            OptColumnRefSet result = (OptColumnRefSet) super.clone();
            result.bitSet = bitSet;
            return result;
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof OptColumnRefSet)) {
            return false;
        }
        OptColumnRefSet rhs = (OptColumnRefSet) obj;
        return bitSet.equals(rhs.bitSet);
    }

    public void include(OptColumnRef ref) { bitSet.set(ref.getId()); }
    public void inlcude(List<OptColumnRef> refs) {
        include(new OptColumnRefSet(refs));
    }

    public void include(OptColumnRefSet set) {
        bitSet.or(set.bitSet);
    }

    public void exclude(List<OptColumnRef> refs) {
        exclude(new OptColumnRefSet(refs));
    }

    public void exclude(OptColumnRefSet set) {
        bitSet.andNot(set.bitSet);
    }

    public boolean intersects(List<OptColumnRef> refs) {
        return intersects(new OptColumnRefSet(refs));
    }

    public boolean intersects(OptColumnRefSet set) {
        return bitSet.intersects(set.bitSet);
    }

    public List<OptColumnRef> getColumnRefs(Map<Integer, OptColumnRef> columnRefMap) {
        return bitSet.stream().mapToObj(columnRefMap::get).collect(Collectors.toList());
    }
}
