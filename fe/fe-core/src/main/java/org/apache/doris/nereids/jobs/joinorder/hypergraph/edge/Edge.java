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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.edge;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperElement;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableSet;

import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Edge in HyperGraph
 */
public abstract class Edge implements HyperElement {
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

    // record the left child edges and right child edges in origin plan tree
    private final BitSet leftChildEdges;
    private final BitSet rightChildEdges;

    // record the edges in the same operator
    private final BitSet curOperatorEdges = new BitSet();
    // record all sub nodes behind in this operator. It's T function in paper
    private final long subTreeNodes;
    // The edges which prevents association or l-association when join edge
    // and prevents push down or pull up when filter edge in the left of edge
    private final Set<JoinEdge> leftRejectEdges;
    // The edges which prevents association or r-association
    // and prevents push down or pull up when filter edge in the right of edge
    private final Set<JoinEdge> rightRejectEdges;

    /**
     * Create simple edge.
     */
    Edge(int index, BitSet leftChildEdges, BitSet rightChildEdges,
            long subTreeNodes, long leftRequiredNodes, Long rightRequiredNodes) {
        this.index = index;
        this.selectivity = 1.0;
        this.leftChildEdges = leftChildEdges;
        this.rightChildEdges = rightChildEdges;
        this.leftRequiredNodes = leftRequiredNodes;
        this.rightRequiredNodes = rightRequiredNodes;
        this.leftExtendedNodes = leftRequiredNodes;
        this.rightExtendedNodes = rightRequiredNodes;
        this.subTreeNodes = subTreeNodes;
        this.leftRejectEdges = new HashSet<>();
        this.rightRejectEdges = new HashSet<>();
    }

    public boolean isSimple() {
        return LongBitmap.getCardinality(leftExtendedNodes) == 1 && LongBitmap.getCardinality(rightExtendedNodes) == 1;
    }

    public boolean isRightSimple() {
        return LongBitmap.getCardinality(rightExtendedNodes) == 1;
    }

    public void addLeftRejectEdge(JoinEdge edge) {
        leftRejectEdges.add(edge);
    }

    public void addRightRejectEdge(JoinEdge edge) {
        rightRejectEdges.add(edge);
    }

    public void addLeftRejectEdges(Set<JoinEdge> edge) {
        leftRejectEdges.addAll(edge);
    }

    public void addRightRejectEdges(Set<JoinEdge> edge) {
        rightRejectEdges.addAll(edge);
    }

    public Set<JoinEdge> getLeftRejectEdge() {
        return ImmutableSet.copyOf(leftRejectEdges);
    }

    public Set<JoinEdge> getRightRejectEdge() {
        return ImmutableSet.copyOf(rightRejectEdges);
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

    public Pair<BitSet, Long> getLeftEdgeNodes(List<JoinEdge> edges) {
        return Pair.of(leftChildEdges, getLeftSubNodes(edges));
    }

    public Pair<BitSet, Long> getRightEdgeNodes(List<JoinEdge> edges) {
        return Pair.of(rightChildEdges, getRightSubNodes(edges));
    }

    public long getLeftSubNodes(List<JoinEdge> edges) {
        if (leftChildEdges.isEmpty()) {
            return leftRequiredNodes;
        }
        return edges.get(leftChildEdges.nextSetBit(0)).getSubTreeNodes();
    }

    public long getRightSubNodes(List<JoinEdge> edges) {
        if (rightChildEdges.isEmpty()) {
            return rightRequiredNodes;
        }
        return edges.get(rightChildEdges.nextSetBit(0)).getSubTreeNodes();
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

    public void addCurJoinEdges(BitSet edges) {
        curOperatorEdges.or(edges);
    }

    public BitSet getCurOperatorEdges() {
        return curOperatorEdges;
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

    public abstract Set<Slot> getInputSlots();

    public abstract List<? extends Expression> getExpressions();

    public Set<? extends Expression> getExpressionSet() {
        return ImmutableSet.copyOf(getExpressions());
    }

    public Expression getExpression(int i) {
        return getExpressions().get(i);
    }

    public String getTypeName() {
        if (this instanceof FilterEdge) {
            return "FILTER";
        } else {
            return ((JoinEdge) this).getJoinType().toString();
        }
    }

    @Override
    public String toString() {
        if (!leftRejectEdges.isEmpty() || !rightRejectEdges.isEmpty()) {
            return String.format("<%s --%s-- %s>[%s , %s]", LongBitmap.toString(leftExtendedNodes),
                    this.getTypeName(), LongBitmap.toString(rightExtendedNodes), leftRejectEdges, rightRejectEdges);
        }
        return String.format("<%s --%s-- %s>", LongBitmap.toString(leftExtendedNodes),
                this.getTypeName(), LongBitmap.toString(rightExtendedNodes));
    }
}
