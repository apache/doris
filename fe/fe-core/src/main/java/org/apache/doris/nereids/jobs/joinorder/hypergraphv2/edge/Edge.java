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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperElement;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Edge in HyperGraph, currently it's a join edge or filter edge
 * join edge is used for join reorder and mtmv match, and filter edge is only used for mtmv match
 * the original operator mentioned in comments is join operator or filter operator
 */
public class Edge implements HyperElement {
    private final LogicalJoin<? extends Plan, ? extends Plan> join;
    private final Set<Slot> leftInputSlots;
    private final Set<Slot> rightInputSlots;
    // record all left subtree nodes bellow the original operator.
    private final long leftSubtreeNodes;
    // record all right subtree nodes bellow the original operator.
    private final long rightSubtreeNodes;
    private final int index;
    private final double selectivity;

    // "RequiredNodes" refers to the nodes that can activate this edge based on
    // specific requirements. These requirements are established during the building process.
    // "ExtendNodes" encompasses both the "RequiredNodes" and any additional nodes
    // added by the graph simplifier.
    private final long leftRequiredNodes;
    private final long rightRequiredNodes;
    // The nodes needed which to prevent wrong association or l-association
    private long leftExtendedNodes;
    // The nodes needed which to prevent wrong association or r-association
    private long rightExtendedNodes;

    // record the left child edges and right child edges bellow the original operator, just first level child, not more
    // because we split inner join conjuncts to multiple inner join edges, we may have n : 1
    // as edges to original join node
    private final BitSet leftChildEdges;
    private final BitSet rightChildEdges;

    // record all sub nodes bellow the original operator. It's T function in paper
    private final long subTreeNodes;

    private List<Pair<Long, Long>> conflictRules;

    /**
     * Create simple edge.
     */
    public Edge(LogicalJoin<? extends Plan, ? extends Plan> join, int index,
                BitSet leftChildEdges, BitSet rightChildEdges, long leftSubtreeNodes, long rightSubtreeNodes,
                long leftRequiredNodes, long rightRequiredNodes, Set<Slot> leftInputSlots, Set<Slot> rightInputSlots) {
        this.index = index;
        this.selectivity = 1.0;
        this.leftChildEdges = leftChildEdges;
        this.rightChildEdges = rightChildEdges;
        this.leftExtendedNodes = leftRequiredNodes;
        this.rightExtendedNodes = rightRequiredNodes;
        this.leftRequiredNodes = leftRequiredNodes;
        this.rightRequiredNodes = rightRequiredNodes;
        this.subTreeNodes = LongBitmap.newBitmapUnion(leftSubtreeNodes, rightSubtreeNodes);
        this.join = join;
        this.leftInputSlots = leftInputSlots;
        this.rightInputSlots = rightInputSlots;
        this.leftSubtreeNodes = leftSubtreeNodes;
        this.rightSubtreeNodes = rightSubtreeNodes;
        this.conflictRules = new ArrayList<>();
    }

    public boolean isSimple() {
        return LongBitmap.getCardinality(leftExtendedNodes) == 1 && LongBitmap.getCardinality(rightExtendedNodes) == 1;
    }

    public void addLeftExtendNode(long left) {
        this.leftExtendedNodes = LongBitmap.or(this.leftExtendedNodes, left);
    }

    public void addRightExtendNode(long right) {
        this.rightExtendedNodes = LongBitmap.or(this.rightExtendedNodes, right);
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

    public void setLeftExtendedNodes(long leftExtendedNodes) {
        this.leftExtendedNodes = leftExtendedNodes;
    }

    public long getRightExtendedNodes() {
        return rightExtendedNodes;
    }

    public BitSet getRightChildEdges() {
        return rightChildEdges;
    }

    public void setRightExtendedNodes(long rightExtendedNodes) {
        this.rightExtendedNodes = rightExtendedNodes;
    }

    public long getLeftRequiredNodes() {
        return leftRequiredNodes;
    }

    public long getRightRequiredNodes() {
        return rightRequiredNodes;
    }

    public boolean isSub(Edge edge) {
        // When this join reference nodes is a subset of other join, then this join must appear before that join
        long otherBitmap = edge.getReferenceNodes();
        return LongBitmap.isSubset(getReferenceNodes(), otherBitmap);
    }

    @Override
    public long getReferenceNodes() {
        return LongBitmap.newBitmapUnion(leftExtendedNodes, rightExtendedNodes);
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

    public List<Expression> getHashJoinConjuncts() {
        return join.getHashJoinConjuncts();
    }

    public List<Expression> getOtherJoinConjuncts() {
        return join.getOtherJoinConjuncts();
    }

    public Set<Slot> getInputSlots() {
        Set<Slot> slots = new HashSet<>();
        join.getExpressions().forEach(expression -> slots.addAll(expression.getInputSlots()));
        return slots;
    }

    /**
     * swap the edge
     */
    public Edge swap() {
        Edge swapEdge = new
                Edge(join.swap(), getIndex(), getRightChildEdges(),
                getLeftChildEdges(), getRightSubtreeNodes(), getLeftSubtreeNodes(),
                getRightRequiredNodes(), getLeftRequiredNodes(),
                this.rightInputSlots, this.leftInputSlots);
        return swapEdge;
    }

    public JoinType getJoinType() {
        return join.getJoinType();
    }

    public long getLeftSubtreeNodes() {
        return leftSubtreeNodes;
    }

    public long getRightSubtreeNodes() {
        return rightSubtreeNodes;
    }

    public LogicalJoin<? extends Plan, ? extends Plan> getJoin() {
        return join;
    }

    /**
     * extract join type for edges and push them in hash conjuncts and other conjuncts
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

    public Expression getExpression() {
        Preconditions.checkArgument(join.getExpressions().size() == 1);
        return join.getExpressions().get(0);
    }

    public List<? extends Expression> getExpressions() {
        return join.getExpressions();
    }

    public Expression getExpression(int i) {
        return getExpressions().get(i);
    }

    public String getTypeName() {
        return ((Edge) this).getJoinType().toString();
    }

    @Override
    public String toString() {
        return String.format("<%s --%s-- %s>", LongBitmap.toString(leftExtendedNodes),
                this.getTypeName(), LongBitmap.toString(rightExtendedNodes));
    }

    public void setConflictRules(List<Pair<Long, Long>> conflictRules) {
        this.conflictRules = conflictRules;
    }

    public List<Pair<Long, Long>> getConflictRules() {
        return conflictRules;
    }

    public boolean isEnforcedOrder() {
        return leftExtendedNodes != leftRequiredNodes || rightExtendedNodes != rightRequiredNodes;
    }
}
