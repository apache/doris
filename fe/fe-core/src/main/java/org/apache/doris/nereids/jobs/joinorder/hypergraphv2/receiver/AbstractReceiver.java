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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2.receiver;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.memo.Group;

import java.util.List;

/**
 * A interface of receiver
 */
public abstract class AbstractReceiver {
    public enum EmitState {
        SUCCESS,
        FAIL,
        CONTINUE,
        NONE
    }

    public abstract EmitState emitCsgCmp(long csg, long cmp, List<Edge> edges);

    public abstract void addGroup(long bitSet, Group group);

    public abstract boolean contain(long bitSet);

    public abstract void reset();

    public abstract Group getBestPlan(long bitSet);

    boolean checkConflictRule(long left, long right, List<Edge> edges) {
        long joinedTables = LongBitmap.or(left, right);
        for (Edge edge : edges) {
            for (Pair<Long, Long> rule : edge.getConflictRules()) {
                if (LongBitmap.isOverlap(joinedTables, rule.first)
                        && !LongBitmap.isSubset(rule.second, joinedTables)) {
                    return false;
                }
            }
        }
        return true;
    }
}
