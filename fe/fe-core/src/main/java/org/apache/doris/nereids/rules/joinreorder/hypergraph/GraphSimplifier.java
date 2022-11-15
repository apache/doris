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

package org.apache.doris.nereids.rules.joinreorder.hypergraph;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * GraphSimplifier is used to simplify HyperGraph {@link HyperGraph}
 */
public class GraphSimplifier {
    // The graph we are simplifying
    HyperGraph graph;
    // It stores the children plan of one edge.
    // we don't store it in hyper graph, because it's just used for simulating join.
    // In fact, the graph simplifier just generate the partial order of join operator.
    List<Pair<Plan, Plan>> joinPlans = new ArrayList<>();
    // This is used for cache the intermediate results when calculate the benefit
    // Note that we store it for the after. Because if we put B after A (t1 Join_A t2 Join_B t3),
    // B is changed. Therefore, Any step that involves B need to be recalculated.
    List<BestSimplification> simplifications = new ArrayList<>();
    PriorityQueue<BestSimplification> priorityQueue = new PriorityQueue<>();
    // Note that each index in the graph simplifier is the half of the actual index
    private final int edgeSize;

    GraphSimplifier(HyperGraph graph) {
        this.graph = graph;
        edgeSize = graph.getEdges().size();
        for (Edge edge : graph.getEdges()) {
            BestSimplification bestSimplification = new BestSimplification();
            Plan leftPlan = graph.getPlan(edge.getLeft());
            Plan rightPlan = graph.getPlan(edge.getRight());
            joinPlans.add(Pair.of(leftPlan, rightPlan));
            simplifications.add(bestSimplification);
        }
    }

    /**
     * This function init the first simplification step
     */
    public void initFirstStep() {
        for (int i = 0; i < edgeSize; i += 1) {
            processNeighbors(i, i + 1, edgeSize);
        }
    }

    /**
     * This function will repeatedly apply simplification steps util the number of csg-cmp pair
     * is under the limit.
     * In hyper graph, counting the csg-cmp pair is more expensive than the apply simplification step, therefore
     * we use binary search to find the least step we should apply.
     *
     * @param limit the limit number of the csg-cmp pair
     */
    public boolean simplifyGraph(int limit) {
        // Now we only support simplify graph until the hyperGraph only contains one plan
        Preconditions.checkArgument(limit == 1);
        while (applySimplificationStep()) {
        }
        return true;
    }

    /**
     * This function apply a simplification step
     *
     * @return If there is no other steps, return false.
     */
    public boolean applySimplificationStep() {
        if (priorityQueue.isEmpty()) {
            return false;
        }
        BestSimplification bestSimplification = priorityQueue.poll();
        bestSimplification.isInQueue = false;
        SimplificationStep bestStep = bestSimplification.getStep();
        // TODO: add circle detector to refuse the edge that can cause a circle
        graph.modifyEdge(bestStep.afterIndex, bestStep.newLeft, bestStep.newRight);
        joinPlans.set(bestStep.afterIndex, Pair.of(bestStep.leftPlan, bestStep.rightPlan));
        processNeighbors(bestStep.afterIndex, 0, edgeSize);
        return true;
    }

    /**
     * Process all neighbors and try to make simplification step
     * Note that when a given ordering is less advantageous and dropped out,
     * we need to call this function recursively to update all relative steps.
     *
     * @param edgeIndex1 The index of join operator that we need to process its neighbors
     * @param beginIndex The beginning index of the processing join operators
     * @param endIndex The end index of the processing join operators
     */
    private void processNeighbors(int edgeIndex1, int beginIndex, int endIndex) {
        // Go through the neighbors with lower index, where the best simplification is stored
        // Because the best simplification is stored in the edge with lower index, we need to update it recursively
        // when we can't reset that best simplification
        for (int edgeIndex2 = beginIndex; edgeIndex2 < Integer.min(endIndex, edgeIndex1); edgeIndex2 += 1) {
            BestSimplification bestSimplification = simplifications.get(edgeIndex2);
            Optional<SimplificationStep> optionalStep = makeSimplificationStep(edgeIndex1, edgeIndex2);
            if (optionalStep.isPresent() && trySetSimplificationStep(optionalStep.get(), bestSimplification, edgeIndex2,
                    edgeIndex1)) {
                continue;
            }
            if (bestSimplification.bestNeighbor == edgeIndex1) {
                processNeighbors(edgeIndex2, 0, edgeSize);
            }
        }

        // Go through the neighbors with higher index, we only need to recalculate all related steps
        for (int edgeIndex2 = Integer.max(beginIndex, edgeIndex1 + 1); edgeIndex2 < endIndex; edgeIndex2 += 1) {
            BestSimplification bestSimplification = simplifications.get(edgeIndex1);
            bestSimplification.bestNeighbor = -1;
            Optional<SimplificationStep> optionalStep = makeSimplificationStep(edgeIndex1, edgeIndex2);
            if (optionalStep.isPresent()) {
                trySetSimplificationStep(optionalStep.get(), bestSimplification, edgeIndex1, edgeIndex2);
            }
        }
    }

    private boolean trySetSimplificationStep(SimplificationStep step, BestSimplification bestSimplification,
            int index, int neighborIndex) {
        if (bestSimplification.bestNeighbor == -1 || bestSimplification.getBenefit() <= step.getBenefit()) {
            bestSimplification.bestNeighbor = neighborIndex;
            bestSimplification.setStep(step);
            updatePriorityQueue(index);
            return true;
        }
        return false;
    }

    private void updatePriorityQueue(int index) {
        BestSimplification bestSimplification = simplifications.get(index);
        if (!bestSimplification.isInQueue) {
            if (bestSimplification.bestNeighbor != -1) {
                priorityQueue.add(bestSimplification);
                bestSimplification.isInQueue = true;
            }
        } else {
            priorityQueue.remove(bestSimplification);
            if (bestSimplification.bestNeighbor == -1) {
                bestSimplification.isInQueue = false;
            } else {
                priorityQueue.add(bestSimplification);
            }
        }
    }

    private Optional<SimplificationStep> makeSimplificationStep(int edgeIndex1, int edgeIndex2) {
        Edge edge1 = graph.getEdge(edgeIndex1);
        Edge edge2 = graph.getEdge(edgeIndex2);
        if (edge1.isBefore(edge2) || edge2.isBefore(edge1)) {
            return Optional.empty();
        }

        Plan leftPlan1 = joinPlans.get(edgeIndex1).first;
        Plan rightPlan1 = joinPlans.get(edgeIndex1).second;
        Plan leftPlan2 = joinPlans.get(edgeIndex2).first;
        Plan rightPlan2 = joinPlans.get(edgeIndex2).second;
        Edge edge1Before2;
        Edge edge2Before1;
        List<Plan> commonPlan = new ArrayList<>();
        if (isSubPlan(leftPlan1, edge1.getLeft(), leftPlan2, edge2.getLeft(), commonPlan)) {
            // (common Join1 right1) Join2 right2
            edge1Before2 = threeLeftJoin(commonPlan.get(0), edge1, rightPlan1, edge2, rightPlan2);
            // (common Join2 right2) Join1 right1
            edge2Before1 = threeLeftJoin(commonPlan.get(0), edge2, rightPlan2, edge1, rightPlan1);
        } else if (isSubPlan(leftPlan1, edge1.getLeft(), rightPlan2, edge2.getRight(), commonPlan)) {
            // left2 Join2 (common Join1 right1)
            edge1Before2 = threeRightJoin(leftPlan2, edge2, commonPlan.get(0), edge1, rightPlan1);
            // (left2 Join2 common) join1 right1
            edge2Before1 = threeLeftJoin(leftPlan2, edge2, commonPlan.get(0), edge1, rightPlan1);
        } else if (isSubPlan(rightPlan1, edge1.getRight(), leftPlan2, edge2.getLeft(), commonPlan)) {
            // (left1 Join1 common) Join2 right2
            edge1Before2 = threeLeftJoin(leftPlan1, edge1, commonPlan.get(0), edge2, rightPlan2);
            // left1 Join1 (common Join2 right2)
            edge2Before1 = threeRightJoin(leftPlan1, edge1, commonPlan.get(0), edge2, rightPlan2);
        } else if (isSubPlan(rightPlan1, edge1.getRight(), rightPlan2, edge2.getRight(), commonPlan)) {
            // left2 Join2 (left1 Join1 common)
            edge1Before2 = threeRightJoin(leftPlan2, edge2, leftPlan1, edge1, commonPlan.get(0));
            // left1 Join1 (left2 Join2 common)
            edge2Before1 = threeRightJoin(leftPlan1, edge1, leftPlan2, edge2, commonPlan.get(0));
        } else {
            return Optional.empty();
        }
        // edge1 is not the neighborhood of edge2
        return Optional.of(orderJoin(edge1Before2, edge2Before1, edgeIndex1, edgeIndex2));
    }

    Edge threeLeftJoin(Plan plan1, Edge edge1, Plan plan2, Edge edge2, Plan plan3) {
        // (plan1 edge1 plan2) edge2 plan3
        LogicalJoin join = simulateJoin(simulateJoin(plan1, edge1.getJoin(), plan2), edge2.getJoin(), plan3);
        Edge edge = new Edge(join, -1);
        edge.addLeftNodes(edge1.getLeft(), edge1.getRight(), edge2.getLeft());
        edge.addRightNode(edge2.getRight());
        return edge;
    }

    Edge threeRightJoin(Plan plan1, Edge edge1, Plan plan2, Edge edge2, Plan plan3) {
        // plan1 edge1 (plan2 edge2 plan3)
        LogicalJoin join = simulateJoin(plan1, edge1.getJoin(), simulateJoin(plan2, edge2.getJoin(), plan3));
        Edge edge = new Edge(join, -1);
        edge.addLeftNode(edge1.getLeft());
        edge.addRightNodes(edge2.getRight(), edge2.getLeft(), edge1.getRight());
        return edge;
    }

    private Group getGroup(Plan plan) {
        if (plan instanceof GroupPlan) {
            return ((GroupPlan) plan).getGroup();
        }
        assert plan.getGroupExpression().isPresent() : "All plan in GraphSimplifier must have a group";
        return plan.getGroupExpression().get().getOwnerGroup();
    }

    private SimplificationStep orderJoin(Edge edge1Before2, Edge edge2Before1, int edgeIndex1, int edgeIndex2) {
        double cost1Before2 = getSimpleCost(edge1Before2.getJoin());
        double cost2Before1 = getSimpleCost(edge2Before1.getJoin());
        double benefit = Double.MAX_VALUE;
        SimplificationStep step;
        // Choose the plan with smaller cost and make the simplification step to replace the old edge by it.
        if (cost1Before2 < cost2Before1) {
            if (cost1Before2 != 0) {
                benefit = cost2Before1 / cost1Before2;
            }
            step = new SimplificationStep(benefit, edgeIndex1, edgeIndex2, edge1Before2.getJoin().left(),
                    edge1Before2.getJoin().right(), edge1Before2.getLeft(), edge1Before2.getRight());
        } else {
            if (cost2Before1 != 0) {
                benefit = cost1Before2 / cost2Before1;
            }
            step = new SimplificationStep(benefit, edgeIndex2, edgeIndex1, edge2Before1.getJoin().left(),
                    edge2Before1.getJoin().right(), edge2Before1.getLeft(), edge2Before1.getRight());
        }
        return step;
    }

    /**
     * This function check whether the plan1 is a sub-plan of plan2 or the opposite
     *
     * @param plan1 the fist plan
     * @param bitSet1 the reference nodes of the first plan
     * @param plan2 the second plan
     * @param bitSet2 the reference nodes of the second plan
     * @param commonPlan the super plan is store here
     * @return whether the plan1 is a sub-plan of plan2 or the opposite
     */
    private boolean isSubPlan(Plan plan1, BitSet bitSet1, Plan plan2, BitSet bitSet2, List<Plan> commonPlan) {
        if (isSubset(bitSet1, bitSet2)) {
            commonPlan.add(plan2);
            return true;
        } else if (isSubset(bitSet2, bitSet1)) {
            commonPlan.add(plan1);
            return true;
        }
        return false;
    }

    private double getSimpleCost(Plan plan) {
        if (plan instanceof GroupPlan) {
            return ((GroupPlan) plan).getGroup().getStatistics().getRowCount();
        }
        return plan.getGroupExpression().get().getCostByProperties(PhysicalProperties.ANY);
    }

    private LogicalJoin simulateJoin(Plan plan1, LogicalJoin join, Plan plan2) {
        // In Graph Simplifier, we use the simple cost model, that is
        //      Plan.cost = Plan.rowCount + Plan.children1.cost + Plan.children2.cost
        // The reason is that this cost model has ASI (adjacent sequence interchange) property.
        // TODO: consider network, data distribution cost
        LogicalJoin newJoin = new LogicalJoin(join.getJoinType(), plan1, plan2);
        List<Group> children = new ArrayList<>();
        children.add(getGroup(plan1));
        children.add(getGroup(plan2));
        double cost = getSimpleCost(plan1) + getSimpleCost(plan2);
        GroupExpression groupExpression = new GroupExpression(newJoin, children);
        Group group = new Group();
        group.addGroupExpression(groupExpression);
        StatsCalculator.estimate(groupExpression);
        cost += group.getStatistics().getRowCount();

        List<PhysicalProperties> inputs = new ArrayList<>();
        inputs.add(PhysicalProperties.ANY);
        inputs.add(PhysicalProperties.ANY);
        groupExpression.updateLowestCostTable(PhysicalProperties.ANY, inputs, cost);

        return (LogicalJoin) newJoin.withGroupExpression(Optional.of(groupExpression));
    }

    private boolean isSubset(BitSet bitSet1, BitSet bitSet2) {
        BitSet bitSet = new BitSet();
        bitSet.or(bitSet1);
        bitSet.or(bitSet2);
        return bitSet.equals(bitSet2);
    }

    class SimplificationStep {
        double benefit;
        int beforeIndex;
        int afterIndex;
        BitSet newLeft;
        BitSet newRight;
        Plan leftPlan;
        Plan rightPlan;

        SimplificationStep(double benefit, int beforeIndex, int afterIndex, Plan leftPlan, Plan rightPlan,
                BitSet newLeft, BitSet newRight) {
            this.afterIndex = afterIndex;
            this.beforeIndex = beforeIndex;
            this.benefit = benefit;
            this.leftPlan = leftPlan;
            this.rightPlan = rightPlan;
            this.newLeft = newLeft;
            this.newRight = newRight;
        }

        public double getBenefit() {
            return benefit;
        }

        @Override
        public String toString() {
            return String.format("%d -> %d %.2f", beforeIndex, afterIndex, benefit);
        }
    }

    class BestSimplification implements Comparable<BestSimplification> {
        int bestNeighbor = -1;
        Optional<SimplificationStep> step = Optional.empty();
        // This data whether to be added to the queue
        boolean isInQueue = false;

        @Override
        public int compareTo(GraphSimplifier.BestSimplification o) {
            Preconditions.checkArgument(step.isPresent());
            return Double.compare(getBenefit(), o.getBenefit());
        }

        public double getBenefit() {
            return step.get().getBenefit();
        }

        public SimplificationStep getStep() {
            return step.get();
        }

        public void setStep(SimplificationStep step) {
            this.step = Optional.of(step);
        }

        @Override
        public String toString() {
            return step.toString();
        }
    }
}
