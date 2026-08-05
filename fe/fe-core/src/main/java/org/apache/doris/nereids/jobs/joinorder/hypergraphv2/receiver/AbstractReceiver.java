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
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.memo.Group;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

/**
 * A interface of receiver
 */
public abstract class AbstractReceiver {

    public abstract EmitState emitCsgCmp(long csg, long cmp, List<Edge> edges);

    public abstract void addGroup(long bitSet, Group group);

    public abstract boolean contain(long bitSet);

    public abstract void reset();

    public abstract Group getBestPlan(long bitSet);

    /**
     * Find all edges that are missed by the current connection edges but whose
     * reference nodes are a subset of the joined nodes.  These missed edges
     * must be added to the emitted join (they become additional join
     * conditions).  If any missed edge is enforced-order, or references a
     * projected alias whose source spans both children, the csg-cmp pair is
     * rejected (returns false).
     *
     * <p>This logic is shared by {@link PlanReceiver} (which actually emits
     * the plan) and {@link Counter} (which counts csg-cmp pairs during graph
     * simplification).  Keeping them consistent is essential: GraphSimplifier
     * relies on Counter to decide how many simplification steps are needed to
     * satisfy {@code dphyperLimit}, and an over-count would over-constrain the
     * graph and change the enumeration output.
     */
    protected boolean processMissedEdges(HyperGraph hyperGraph, HashMap<Long, BitSet> usdEdges,
            long left, long right, List<Edge> edges, List<Edge> missingEdges) {
        // find all used edges
        BitSet usedEdgesBitmap = new BitSet();
        usedEdgesBitmap.or(usdEdges.get(left));
        usedEdgesBitmap.or(usdEdges.get(right));
        edges.forEach(edge -> usedEdgesBitmap.set(edge.getIndex()));

        // find all referenced nodes
        long allReferenceNodes = LongBitmap.or(left, right);

        // find the edge which is not in usedEdgesBitmap and its referenced nodes is subset of allReferenceNodes
        for (Edge edge : hyperGraph.getJoinEdges()) {
            if (LongBitmap.isSubset(edge.getReferenceNodes(), allReferenceNodes)
                    && !usedEdgesBitmap.get(edge.getIndex())) {
                if (edge.isEnforcedOrder()) {
                    return false;
                } else {
                    // Reject missed edges that reference a projected alias whose
                    // source bitmap spans both children. The alias layer is emitted
                    // by proposeProject (after proposeJoin), so the join predicate
                    // would reference a slot that does not exist in either child's
                    // output. Wait for a later join step where the alias source is
                    // fully contained in one child.
                    if (!hyperGraph.isEdgeSafeForJoin(edge, left, right)) {
                        return false;
                    }
                    // add the missed edge to edges
                    missingEdges.add(edge);
                }
            }
        }
        return true;
    }

    /**
     * checkConflictRule for CD-C
     */
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

    /**
     * EmitState
     */
    public enum EmitState {
        SUCCESS,
        FAIL,
        CONTINUE,
        NONE
    }
}
