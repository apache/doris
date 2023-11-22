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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Edge in HyperGraph
 */
public class Edge {
    final int index;
    final LogicalJoin<? extends Plan, ? extends Plan> join;
    final double selectivity;

    // "RequiredNodes" refers to the nodes that can activate this edge based on
    // specific requirements. These requirements are established during the building process.
    // "ExtendNodes" encompasses both the "RequiredNodes" and any additional nodes
    // added by the graph simplifier.
    private long leftRequiredNodes = LongBitmap.newBitmap();
    private long rightRequiredNodes = LongBitmap.newBitmap();
    private long leftExtendedNodes = LongBitmap.newBitmap();
    private long rightExtendedNodes = LongBitmap.newBitmap();

    private long referenceNodes = LongBitmap.newBitmap();

    // record the left child edges and right child edges in origin plan tree
    private final BitSet leftChildEdges;
    private final BitSet rightChildEdges;

    // record the edges in the same operator
    private final BitSet curJoinEdges = new BitSet();
    // record all sub nodes behind in this operator. It's T function in paper
    private final long subTreeNodes;

    /**
     * Create simple edge.
     */
    public Edge(LogicalJoin join, int index, BitSet leftChildEdges, BitSet rightChildEdges, Long subTreeNodes) {
        this.index = index;
        this.join = join;
        this.selectivity = 1.0;
        this.leftChildEdges = leftChildEdges;
        this.rightChildEdges = rightChildEdges;
        this.subTreeNodes = subTreeNodes;
    }

    public LogicalJoin<? extends Plan, ? extends Plan> getJoin() {
        return join;
    }

    public JoinType getJoinType() {
        return join.getJoinType();
    }

    public boolean isSimple() {
        return LongBitmap.getCardinality(leftExtendedNodes) == 1 && LongBitmap.getCardinality(rightExtendedNodes) == 1;
    }

    public void addLeftNode(long left) {
        this.leftExtendedNodes = LongBitmap.or(this.leftExtendedNodes, left);
        referenceNodes = LongBitmap.or(referenceNodes, left);
    }

    public void addLeftNodes(long... bitmaps) {
        for (long bitmap : bitmaps) {
            this.leftExtendedNodes = LongBitmap.or(this.leftExtendedNodes, bitmap);
            referenceNodes = LongBitmap.or(referenceNodes, bitmap);
        }
    }

    public void addRightNode(long right) {
        this.rightExtendedNodes = LongBitmap.or(this.rightExtendedNodes, right);
        referenceNodes = LongBitmap.or(referenceNodes, right);
    }

    public void addRightNodes(long... bitmaps) {
        for (long bitmap : bitmaps) {
            LongBitmap.or(this.rightExtendedNodes, bitmap);
            LongBitmap.or(referenceNodes, bitmap);
        }
    }

    public long getSubTreeNodes() {
        return this.subTreeNodes;
    }

    public long getLeftExtendedNodes() {
        return leftExtendedNodes;
    }

    public BitSet getLeftChildEdges() {
        return leftChildEdges;
    }

    public Pair<BitSet, Long> getLeftEdgeNodes(List<Edge> edges) {
        return Pair.of(leftChildEdges, getLeftSubNodes(edges));
    }

    public Pair<BitSet, Long> getRightEdgeNodes(List<Edge> edges) {
        return Pair.of(rightChildEdges, getRightSubNodes(edges));
    }

    public long getLeftSubNodes(List<Edge> edges) {
        if (leftChildEdges.isEmpty()) {
            return leftRequiredNodes;
        }
        return edges.get(leftChildEdges.nextSetBit(0)).getSubTreeNodes();
    }

    public long getRightSubNodes(List<Edge> edges) {
        if (rightChildEdges.isEmpty()) {
            return rightRequiredNodes;
        }
        return edges.get(rightChildEdges.nextSetBit(0)).getSubTreeNodes();
    }

    public void setLeftExtendedNodes(long leftExtendedNodes) {
        referenceNodes = LongBitmap.clear(referenceNodes);
        this.leftExtendedNodes = leftExtendedNodes;
    }

    public long getRightExtendedNodes() {
        return rightExtendedNodes;
    }

    public BitSet getRightChildEdges() {
        return rightChildEdges;
    }

    public void setRightExtendedNodes(long rightExtendedNodes) {
        referenceNodes = LongBitmap.clear(referenceNodes);
        this.rightExtendedNodes = rightExtendedNodes;
    }

    public long getLeftRequiredNodes() {
        return leftRequiredNodes;
    }

    public void setLeftRequiredNodes(long left) {
        this.leftRequiredNodes = left;
    }

    public long getRightRequiredNodes() {
        return rightRequiredNodes;
    }

    public void setRightRequiredNodes(long right) {
        this.rightRequiredNodes = right;
    }

    public void addCurJoinEdges(BitSet edges) {
        curJoinEdges.or(edges);
    }

    public BitSet getCurJoinEdges() {
        return curJoinEdges;
    }

    public boolean isSub(Edge edge) {
        // When this join reference nodes is a subset of other join, then this join must appear before that join
        long otherBitmap = edge.getReferenceNodes();
        return LongBitmap.isSubset(getReferenceNodes(), otherBitmap);
    }

    public long getReferenceNodes() {
        if (LongBitmap.getCardinality(referenceNodes) == 0) {
            referenceNodes = LongBitmap.newBitmapUnion(leftExtendedNodes, rightExtendedNodes);
        }
        return referenceNodes;
    }

    public long getRequireNodes() {
        return LongBitmap.newBitmapUnion(leftRequiredNodes, rightRequiredNodes);
    }

    public int getIndex() {
        return index;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public Expression getExpression() {
        Preconditions.checkArgument(join.getExpressions().size() == 1);
        return join.getExpressions().get(0);
    }

    public List<Expression> getHashJoinConjuncts() {
        return join.getHashJoinConjuncts();
    }

    public List<Expression> getOtherJoinConjuncts() {
        return join.getOtherJoinConjuncts();
    }

    public final Set<Slot> getInputSlots() {
        Set<Slot> slots = new HashSet<>();
        join.getExpressions().stream().forEach(expression -> slots.addAll(expression.getInputSlots()));
        return slots;
    }

    @Override
    public String toString() {
        return String.format("<%s - %s>", LongBitmap.toString(leftExtendedNodes), LongBitmap.toString(
                rightExtendedNodes));
    }

    /**
     * extract join type and conjuncts from edges
     */
    public static @Nullable JoinType extractJoinTypeAndConjuncts(List<Edge> edges,
            List<Expression> hashConjuncts, List<Expression> otherConjuncts) {
        JoinType joinType = null;
        for (Edge edge : edges) {
            if (edge.getJoinType() != joinType && joinType != null) {
                return null;
            }
            Preconditions.checkArgument(joinType == null || joinType == edge.getJoinType());
            joinType = edge.getJoinType();
            hashConjuncts.addAll(edge.getHashJoinConjuncts());
            otherConjuncts.addAll(edge.getOtherJoinConjuncts());
        }
        return joinType;
    }

    public static Edge createTempEdge(LogicalJoin join) {
        return new Edge(join, -1, null, null, 0L);
    }
}

