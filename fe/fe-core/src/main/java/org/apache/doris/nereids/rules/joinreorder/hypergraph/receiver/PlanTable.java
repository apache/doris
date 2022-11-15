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

package org.apache.doris.nereids.rules.joinreorder.hypergraph.receiver;

import org.apache.doris.nereids.rules.joinreorder.hypergraph.Edge;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.BitSet;

/**
 * The Receiver is used for cached the plan that has been emitted and build the new plan
 */
public class PlanTable implements AbstractReceiver {
    // limit define the max number of csg-cmp pair in this Receiver

    /**
     * Emit a new plan from bottom to top
     *
     * @param left the bitmap of left child tree
     * @param right the bitmap of the right child tree
     * @param edge the join operator
     * @return the left and the right can be connected by the edge
     */
    public boolean emitCsgCmp(BitSet left, BitSet right, Edge edge) {
        throw new RuntimeException("PlanTable does not support emitCsgCmp()");
    }

    public void addPlan(BitSet bitSet, Plan plan) {
        throw new RuntimeException("PlanTable does not support addPlan()");
    }

    public boolean contain(BitSet bitSet) {
        throw new RuntimeException("PlanTable does not support contain()");
    }

    @Override
    public void reset() {
        throw new RuntimeException("PlanTable does not support reset()");
    }

    public Plan getBestPlan(BitSet bitSet) {
        throw new RuntimeException("PlanTable does not support getBestPlan()");
    }
}

