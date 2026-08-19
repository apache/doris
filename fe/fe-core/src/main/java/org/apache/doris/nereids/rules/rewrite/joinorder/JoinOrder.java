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

package org.apache.doris.nereids.rules.rewrite.joinorder;

import org.apache.doris.nereids.rules.rewrite.StatsDerive;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**JoinOrder*/
public abstract class JoinOrder {
    private static final double MAXIMUM_COST = Double.MAX_VALUE / Math.pow(10, 50);
    private static final double EXECUTE_COST_PENALTY = 2;
    private static final double CROSS_JOIN_PENALTY = 1_000_000;

    protected int atomSize;
    protected int edgeSize;
    protected List<JoinLevel> joinLevels = Lists.newArrayList();
    protected List<Edge> edges = Lists.newArrayList();
    protected final Map<BitSet, GroupInfo> bitSetToGroupInfo = Maps.newHashMap();

    static class ExpressionInfo {
        Plan expr;
        GroupInfo leftChild;
        GroupInfo rightChild;
        double cost = -1L;
        double rowCount = -1L;

        public ExpressionInfo(Plan expr) {
            this.expr = expr;
        }

        public ExpressionInfo(Plan expr,
                GroupInfo leftChild,
                GroupInfo rightChild) {
            this.expr = expr;
            this.leftChild = leftChild;
            this.rightChild = rightChild;
        }

        public double getCost() {
            return cost;
        }

        @Override
        public int hashCode() {
            return Objects.hash(expr.hashCode(), leftChild, rightChild);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof JoinOrder)) {
                return false;
            }

            ExpressionInfo other = (ExpressionInfo) obj;
            return Objects.equals(expr, other.expr)
                    && Objects.equals(leftChild, other.leftChild)
                    && Objects.equals(rightChild, other.rightChild);
        }
    }

    static class GroupInfo {
        final BitSet atoms;
        ExpressionInfo bestExprInfo = null;
        double lowestExprCost = Double.MAX_VALUE;

        public GroupInfo(BitSet atoms) {
            this.atoms = atoms;
        }

        @Override
        public int hashCode() {
            return atoms.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            GroupInfo other = (GroupInfo) obj;
            return atoms.equals(other.atoms);
        }
    }

    /**
     * The join level from bottom to top
     * For A Join B Join C Join D
     * Level 1 groups are: A, B, C, D
     * Level 2 groups are: AB, AC, AD, BC ...
     * Level 3 groups are: ABC, ABD, BCD ...
     * Level 4 groups are: ABCD
     */
    static class JoinLevel {
        final int level;
        List<GroupInfo> groups = Lists.newArrayList();

        public JoinLevel(int level) {
            this.level = level;
        }
    }

    /**
     * The Edge represents the join on predicate
     * For A.id = B.id
     * The predicate is A.id = B.id,
     * The vertexes are A and B
     */
    static class Edge {
        final BitSet vertexes = new BitSet();
        final Expression predicate;

        public Edge(Expression predicate) {
            this.predicate = predicate;
        }
    }

    // Different join order algorithms should have different implementations
    protected abstract void enumerate();

    //Get reorder result
    public abstract List<Plan> getResult();

    public void reorder(List<Plan> atoms, List<Expression> predicates) {
        init(atoms, predicates);
        enumerate();
    }

    void init(List<Plan> atoms, List<Expression> predicates) {

        // 1. calculate statistics for each atom expression
        for (Plan atom : atoms) {
            atom.accept(new StatsDerive(false), new DeriveContext());
        }

        // 2. build join graph
        atomSize = atoms.size();
        for (Expression predicate : predicates) {
            edges.add(new Edge(predicate));
        }
        edgeSize = edges.size();
        computeEdgeCover(atoms);

        // 3. init join levels
        // For human read easily, the join level start with 1, not 0.
        for (int i = 0; i <= atomSize; ++i) {
            joinLevels.add(new JoinLevel(i));
        }

        // 4.init join group info
        JoinLevel atomLevel = joinLevels.get(1);
        for (int i = 0; i < atomSize; ++i) {
            BitSet atomBit = new BitSet();
            atomBit.set(i);
            ExpressionInfo atomExprInfo = new ExpressionInfo(atoms.get(i));
            computeCost(atomExprInfo);

            GroupInfo groupInfo = new GroupInfo(atomBit);
            groupInfo.bestExprInfo = atomExprInfo;
            groupInfo.lowestExprCost = atomExprInfo.cost;
            atomLevel.groups.add(groupInfo);
        }
    }

    void computeCost(ExpressionInfo exprInfo) {
        double cost = exprInfo.expr.getStats().getRowCount();
        exprInfo.rowCount = cost;
        if (exprInfo.leftChild != null) {
            cost = cost > (MAXIMUM_COST - exprInfo.leftChild.bestExprInfo.cost)
                    ? MAXIMUM_COST : cost + exprInfo.leftChild.bestExprInfo.cost;
            cost = cost > (MAXIMUM_COST - exprInfo.rightChild.bestExprInfo.cost)
                    ? MAXIMUM_COST : cost + exprInfo.rightChild.bestExprInfo.cost;
            LogicalJoin join = (LogicalJoin) exprInfo.expr;
            if (join.getJoinType().isCrossJoin()) {
                // punish cross join
                cost = cost > (MAXIMUM_COST / CROSS_JOIN_PENALTY) ? MAXIMUM_COST : cost * CROSS_JOIN_PENALTY;
            } else if (join.getHashJoinConjuncts().isEmpty()) {
                // punish nestloop join
                cost = cost > (MAXIMUM_COST / EXECUTE_COST_PENALTY) ? MAXIMUM_COST : cost * EXECUTE_COST_PENALTY;
            }
        }
        exprInfo.cost = cost;
    }

    protected void computeEdgeCover(List<Plan> atoms) {
        for (int i = 0; i < edgeSize; ++i) {
            Expression predicate = edges.get(i).predicate;
            Set<Slot> predicateSlots = predicate.getInputSlots();
            for (int j = 0; j < atomSize; ++j) {
                Plan atom = atoms.get(j);
                Set<Slot> outputSlots = atom.getOutputSet();
                if (!Collections.disjoint(predicateSlots, outputSlots)) {
                    edges.get(i).vertexes.set(j);
                }
            }
        }
    }

    protected List<Expression> buildInnerJoinPredicate(BitSet left, BitSet right) {
        List<Expression> onPredicates = Lists.newArrayList();
        BitSet joinBitSet = new BitSet();
        joinBitSet.or(left);
        joinBitSet.or(right);
        for (int i = 0; i < edgeSize; ++i) {
            Edge edge = edges.get(i);
            if (contains(joinBitSet, edge.vertexes) && left.intersects(edge.vertexes)
                    && right.intersects(edge.vertexes)) {
                onPredicates.add(edge.predicate);
            }
        }
        return onPredicates;
    }

    private boolean contains(BitSet left, BitSet right) {
        for (int b = right.nextSetBit(0); b >= 0; b = right.nextSetBit(b + 1)) {
            if (!left.get(b)) {
                return false;
            }
        }
        return true;
    }
}
