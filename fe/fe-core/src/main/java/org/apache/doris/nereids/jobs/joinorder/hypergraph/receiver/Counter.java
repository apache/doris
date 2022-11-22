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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.Edge;
import org.apache.doris.nereids.memo.Group;

import com.google.common.base.Preconditions;

import java.util.BitSet;
import java.util.HashMap;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class Counter implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver
    private int limit;
    private int emitCount = 0;
    private HashMap<BitSet, Integer> counter = new HashMap<>();

    public Counter() {
        this.limit = Integer.MAX_VALUE;
    }

    public Counter(int limit) {
        this.limit = limit;
    }

    /**
     * Emit a new plan from bottom to top
     *
     * @param left the bitmap of left child tree
     * @param right the bitmap of the right child tree
     * @param edge the join operator
     * @return the left and the right can be connected by the edge
     */
    public boolean emitCsgCmp(BitSet left, BitSet right, Edge edge) {
        Preconditions.checkArgument(counter.containsKey(left));
        Preconditions.checkArgument(counter.containsKey(right));
        emitCount += 1;
        if (emitCount > limit) {
            return false;
        }
        BitSet bitSet = new BitSet();
        bitSet.or(left);
        bitSet.or(right);
        if (!counter.containsKey(bitSet)) {
            counter.put(bitSet, counter.get(left) * counter.get(right));
        } else {
            counter.put(bitSet, counter.get(bitSet) + counter.get(left) * counter.get(right));
        }
        return true;
    }

    public void addGroup(BitSet bitSet, Group group) {
        counter.put(bitSet, 1);
    }

    public boolean contain(BitSet bitSet) {
        return counter.containsKey(bitSet);
    }

    public void reset() {
        this.counter.clear();
        emitCount = 0;
    }

    public Group getBestPlan(BitSet bitSet) {
        throw new RuntimeException("Counter does not support getBestPlan()");
    }

    public int getCount(BitSet bitSet) {
        return counter.get(bitSet);
    }

    public HashMap<BitSet, Integer> getAllCount() {
        return counter;
    }

    public int getLimit() {
        return limit;
    }
}
