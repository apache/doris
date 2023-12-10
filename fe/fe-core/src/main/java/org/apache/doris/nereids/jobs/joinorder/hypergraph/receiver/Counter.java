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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.Group;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class Counter implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver
    private final int limit;
    private int emitCount = 0;
    private final HashMap<Long, Integer> counter = new HashMap<>();

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
     * @param edges the join operator
     * @return the left and the right can be connected by the edge
     */
    public boolean emitCsgCmp(long left, long right, List<Edge> edges) {
        Preconditions.checkArgument(counter.containsKey(left));
        Preconditions.checkArgument(counter.containsKey(right));
        emitCount += 1;
        if (emitCount > limit) {
            return false;
        }
        long bitmap = LongBitmap.newBitmapUnion(left, right);
        if (!counter.containsKey(bitmap)) {
            counter.put(bitmap, counter.get(left) * counter.get(right));
        } else {
            counter.put(bitmap, counter.get(bitmap) + counter.get(left) * counter.get(right));
        }
        return true;
    }

    public void addGroup(long bitmap, Group group) {
        counter.put(bitmap, 1);
    }

    public boolean contain(long bitmap) {
        return counter.containsKey(bitmap);
    }

    public void reset() {
        this.counter.clear();
        emitCount = 0;
    }

    public Group getBestPlan(long bitmap) {
        throw new RuntimeException("Counter does not support getBestPlan()");
    }

    public int getCount(long bitmap) {
        return counter.get(bitmap);
    }

    public HashMap<Long, Integer> getAllCount() {
        return counter;
    }

    public int getLimit() {
        return limit;
    }

    public int getEmitCount() {
        return emitCount;
    }
}
