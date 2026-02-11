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
 * Edge represents a join
 */
public class JoinEdge extends Edge {

    private final LogicalJoin<? extends Plan, ? extends Plan> join;
    private final Set<Slot> leftInputSlots;
    private final Set<Slot> rightInputSlots;

    public JoinEdge(LogicalJoin<? extends Plan, ? extends Plan> join, int index,
            BitSet leftChildEdges, BitSet rightChildEdges, long subTreeNodes,
            long leftRequireNodes, long rightRequireNodes, Set<Slot> leftInputSlots, Set<Slot> rightInputSlots) {
        super(index, leftChildEdges, rightChildEdges, subTreeNodes, leftRequireNodes, rightRequireNodes);
        this.join = join;
        this.leftInputSlots = leftInputSlots;
        this.rightInputSlots = rightInputSlots;
    }

    /**
     * swap the edge
     */
    public JoinEdge swap() {
        JoinEdge swapEdge = new
                JoinEdge(join.swap(), getIndex(), getRightChildEdges(),
                getLeftChildEdges(), getSubTreeNodes(), getRightRequiredNodes(), getLeftRequiredNodes(),
                this.rightInputSlots, this.leftInputSlots);
        swapEdge.addLeftRejectEdges(getLeftRejectEdge());
        swapEdge.addRightRejectEdges(getRightRejectEdge());
        return swapEdge;
    }

    public JoinType getJoinType() {
        return join.getJoinType();
    }

    public JoinEdge withJoinTypeAndCleanCR(JoinType joinType) {
        return new JoinEdge(join.withJoinType(joinType), getIndex(), getLeftChildEdges(), getRightChildEdges(),
                getSubTreeNodes(), getLeftRequiredNodes(), getRightRequiredNodes(), leftInputSlots, rightInputSlots);
    }

    public LogicalJoin<? extends Plan, ? extends Plan> getJoin() {
        return join;
    }

    /**
     * extract join type for edges and push them in hash conjuncts and other conjuncts
     */
    public static @Nullable JoinType extractJoinTypeAndConjuncts(List<JoinEdge> edges,
            List<Expression> hashConjuncts, List<Expression> otherConjuncts) {
        JoinType joinType = null;
        for (JoinEdge edge : edges) {
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

    @Override
    public List<? extends Expression> getExpressions() {
        return join.getExpressions();
    }

    public List<Expression> getHashJoinConjuncts() {
        return join.getHashJoinConjuncts();
    }

    public List<Expression> getOtherJoinConjuncts() {
        return join.getOtherJoinConjuncts();
    }

    @Override
    public Set<Slot> getInputSlots() {
        Set<Slot> slots = new HashSet<>();
        join.getExpressions().forEach(expression -> slots.addAll(expression.getInputSlots()));
        return slots;
    }

    public Set<Slot> getLeftInputSlots() {
        return leftInputSlots;
    }

    public Set<Slot> getRightInputSlots() {
        return rightInputSlots;
    }
}
