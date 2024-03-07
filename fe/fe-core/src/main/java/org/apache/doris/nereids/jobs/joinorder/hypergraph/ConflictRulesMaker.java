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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.FilterEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.trees.plans.JoinType;

import java.util.BitSet;
import java.util.List;

/**
 * This is a conflict rule maker to
 */
public class ConflictRulesMaker {
    private ConflictRulesMaker() {}

    /**
     * make conflict rules for filter edges
     */
    public static void makeFilterConflictRules(
            JoinEdge joinEdge, List<JoinEdge> joinEdges, List<FilterEdge> filterEdges) {
        long leftSubNodes = joinEdge.getLeftSubNodes(joinEdges);
        long rightSubNodes = joinEdge.getRightSubNodes(joinEdges);
        filterEdges.forEach(e -> {
            if (LongBitmap.isSubset(e.getReferenceNodes(), leftSubNodes)
                    && !PushDownFilterThroughJoin.COULD_PUSH_THROUGH_LEFT.contains(joinEdge.getJoinType())) {
                e.addLeftRejectEdge(joinEdge);
            }
            if (LongBitmap.isSubset(e.getReferenceNodes(), rightSubNodes)
                    && !PushDownFilterThroughJoin.COULD_PUSH_THROUGH_RIGHT.contains(joinEdge.getJoinType())) {
                e.addRightRejectEdge(joinEdge);
            }
        });
    }

    /**
     * Make edge with CD-C algorithm in
     * On the correct and complete enumeration of the core search
     */
    public static void makeJoinConflictRules(JoinEdge edgeB, List<JoinEdge> joinEdges) {
        BitSet leftSubTreeEdges = subTreeEdges(edgeB.getLeftChildEdges(), joinEdges);
        BitSet rightSubTreeEdges = subTreeEdges(edgeB.getRightChildEdges(), joinEdges);
        long leftRequired = edgeB.getLeftRequiredNodes();
        long rightRequired = edgeB.getRightRequiredNodes();

        for (int i = leftSubTreeEdges.nextSetBit(0); i >= 0; i = leftSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!JoinType.isAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getLeftSubNodes(joinEdges));
                childA.addLeftRejectEdge(edgeB);
            }
            if (!JoinType.isLAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getRightSubNodes(joinEdges));
                childA.addLeftRejectEdge(edgeB);
            }
        }

        for (int i = rightSubTreeEdges.nextSetBit(0); i >= 0; i = rightSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!JoinType.isAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getRightSubNodes(joinEdges));
                childA.addRightRejectEdge(edgeB);
            }
            if (!JoinType.isRAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getLeftSubNodes(joinEdges));
                childA.addRightRejectEdge(edgeB);
            }
        }
        edgeB.setLeftExtendedNodes(leftRequired);
        edgeB.setRightExtendedNodes(rightRequired);
    }

    private static BitSet subTreeEdge(Edge edge, List<JoinEdge> joinEdges) {
        long subTreeNodes = edge.getSubTreeNodes();
        BitSet subEdges = new BitSet();
        joinEdges.stream()
                .filter(e -> LongBitmap.isSubset(subTreeNodes, e.getReferenceNodes()))
                .forEach(e -> subEdges.set(e.getIndex()));
        return subEdges;
    }

    private static BitSet subTreeEdges(BitSet edgeSet, List<JoinEdge> joinEdges) {
        BitSet bitSet = new BitSet();
        edgeSet.stream()
                .mapToObj(i -> subTreeEdge(joinEdges.get(i), joinEdges))
                .forEach(bitSet::or);
        return bitSet;
    }
}
