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

import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.bitmap.Bitmap;
import org.apache.doris.nereids.stats.StatsCalculator;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * GraphSimplifier is used to simplify HyperGraph {@link HyperGraph}
 */
public class GraphSimplifier {
    // Note that each index in the graph simplifier is the half of the actual index
    private final int edgeSize;
    // This is used for cache the intermediate results when calculate the benefit
    // Note that we store it for the after. Because if we put B after A (t1 Join_A t2 Join_B t3),
    // B is changed. Therefore, Any step that involves B need to be recalculated.
    private List<BestSimplification> simplifications = new ArrayList<>();
    private PriorityQueue<BestSimplification> priorityQueue = new PriorityQueue<>();
    // The graph we are simplifying
    private HyperGraph graph;
    // Detect the circle when order join
    private CircleDetector circleDetector;
    // It cached the plan in simplification. we don't store it in hyper graph,
    // because it's just used for simulating join. In fact, the graph simplifier
    // just generate the partial order of join operator.
    private HashMap<BitSet, Plan> cachePlan = new HashMap<>();

    GraphSimplifier(HyperGraph graph) {
        this.graph = graph;
        edgeSize = graph.getEdges().size();
        for (int i = 0; i < edgeSize; i++) {
            BestSimplification bestSimplification = new BestSimplification();
            simplifications.add(bestSimplification);
        }
        for (Node node : graph.getNodes()) {
            cachePlan.put(node.getBitSet(), node.getPlan());
        }
        circleDetector = new CircleDetector(edgeSize);
    }

    /**
     * This function init the first simplification step
     */
    public void initFirstStep() {
        extractJoinDependencies();
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
        if (circleDetector.checkCircleWithEdge(bestStep.beforeIndex, bestStep.afterIndex)) {
            bestStep.reverse();
        }
        bestStep.concretize(graph);
        circleDetector.tryAddDirectedEdge(bestStep.beforeIndex, bestStep.afterIndex);
        graph.modifyEdge(bestStep.afterIndex, bestStep.newLeft, bestStep.newRight);
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
            Optional<SimplificationStep> optionalStep = makeSimplificationStep(edgeIndex1, edgeIndex2);
            if (optionalStep.isPresent()) {
                bestSimplification.bestNeighbor = -1;
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
        if (edge1.isSub(edge2) || edge2.isSub(edge1)) {
            return Optional.empty();
        }
        BitSet left1 = edge1.getLeft();
        BitSet right1 = edge1.getRight();
        BitSet left2 = edge2.getLeft();
        BitSet right2 = edge2.getRight();
        Edge edge1Before2;
        Edge edge2Before1;
        List<BitSet> superBitset = new ArrayList<>();
        boolean isLeftJoin1 = true;
        boolean isLeftJoin2 = true;
        if (tryGetSuperset(left1, left2, superBitset)) {
            // (common Join1 right1) Join2 right2
            edge1Before2 = threeLeftJoin(superBitset.get(0), edge1, right1, edge2, right2);
            // (common Join2 right2) Join1 right1
            edge2Before1 = threeLeftJoin(superBitset.get(0), edge2, right2, edge1, right1);
        } else if (tryGetSuperset(left1, right2, superBitset)) {
            // left2 Join2 (common Join1 right1)
            edge1Before2 = threeRightJoin(left2, edge2, superBitset.get(0), edge1, right1);
            isLeftJoin1 = false;
            // (left2 Join2 common) join1 right1
            edge2Before1 = threeLeftJoin(left2, edge2, superBitset.get(0), edge1, right1);
        } else if (tryGetSuperset(right1, left2, superBitset)) {
            // (left1 Join1 common) Join2 right2
            edge1Before2 = threeLeftJoin(left1, edge1, superBitset.get(0), edge2, right2);
            // left1 Join1 (common Join2 right2)
            edge2Before1 = threeRightJoin(left1, edge1, superBitset.get(0), edge2, right2);
            isLeftJoin2 = false;
        } else if (tryGetSuperset(right1, right2, superBitset)) {
            // left2 Join2 (left1 Join1 common)
            edge1Before2 = threeRightJoin(left2, edge2, left1, edge1, superBitset.get(0));
            isLeftJoin1 = false;
            // left1 Join1 (left2 Join2 common)
            edge2Before1 = threeRightJoin(left1, edge1, left2, edge2, superBitset.get(0));
            isLeftJoin2 = false;
        } else {
            return Optional.empty();
        }
        // edge1 is not the neighborhood of edge2
        return Optional.of(orderJoin(edge1Before2, isLeftJoin1, edge2Before1, isLeftJoin2, edgeIndex1, edgeIndex2));
    }

    Edge threeLeftJoin(BitSet bitSet1, Edge edge1, BitSet bitSet2, Edge edge2, BitSet bitSet3) {
        // (plan1 edge1 plan2) edge2 plan3
        // The join may have redundant table, e.g., t1,t2 join t3 join t2,t4
        // Therefore, the cost is not accurate
        LogicalJoin leftPlan = simulateJoin(cachePlan.get(bitSet1), edge1.getJoin(), cachePlan.get(bitSet2));
        LogicalJoin join = simulateJoin(leftPlan, edge2.getJoin(), cachePlan.get(bitSet3));
        Edge edge = new Edge(join, -1);
        BitSet newLeft = new BitSet();
        newLeft.or(bitSet1);
        newLeft.or(bitSet2);
        // To avoid overlapping the left and the right, the newLeft is calculated, Note the
        // newLeft is not totally include the bitset1 and bitset2, we use circle detector to trace the dependency
        newLeft.andNot(bitSet3);
        edge.addLeftNodes(newLeft);
        edge.addRightNode(edge2.getRight());
        cachePlan.put(newLeft, leftPlan);
        return edge;
    }

    Edge threeRightJoin(BitSet bitSet1, Edge edge1, BitSet bitSet2, Edge edge2, BitSet bitSet3) {
        // plan1 edge1 (plan2 edge2 plan3)
        LogicalJoin rightPlan = simulateJoin(cachePlan.get(bitSet2), edge2.getJoin(), cachePlan.get(bitSet3));
        LogicalJoin join = simulateJoin(cachePlan.get(bitSet1), edge1.getJoin(), rightPlan);
        Edge edge = new Edge(join, -1);

        BitSet newRight = new BitSet();
        newRight.or(bitSet2);
        newRight.or(bitSet3);
        newRight.andNot(bitSet1);
        edge.addLeftNode(edge1.getLeft());
        edge.addRightNode(newRight);
        cachePlan.put(newRight, rightPlan);
        return edge;
    }

    private Group getGroup(Plan plan) {
        if (plan instanceof GroupPlan) {
            return ((GroupPlan) plan).getGroup();
        }
        Preconditions.checkArgument(plan.getGroupExpression().isPresent(),
                "All plan in GraphSimplifier must have a group");
        return plan.getGroupExpression().get().getOwnerGroup();
    }

    private SimplificationStep orderJoin(Edge edge1Before2, boolean isLeftJoin1, Edge edge2Before1, boolean isLeftJoin2,
            int edgeIndex1, int edgeIndex2) {
        double cost1Before2 = getSimpleCost(edge1Before2.getJoin());
        double cost2Before1 = getSimpleCost(edge2Before1.getJoin());
        double benefit = Double.MAX_VALUE;
        SimplificationStep step;
        // Choose the plan with smaller cost and make the simplification step to replace the old edge by it.
        if (cost1Before2 < cost2Before1) {
            if (cost1Before2 != 0) {
                benefit = cost2Before1 / cost1Before2;
            }
            // choose edge1Before2
            step = new SimplificationStep(benefit, edgeIndex1, edgeIndex2, isLeftJoin1);
        } else {
            if (cost2Before1 != 0) {
                benefit = cost1Before2 / cost2Before1;
            }
            // choose edge2Before1
            step = new SimplificationStep(benefit, edgeIndex2, edgeIndex1, isLeftJoin2);
        }
        return step;
    }

    private boolean tryGetSuperset(BitSet bitSet1, BitSet bitSet2, List<BitSet> superset) {
        if (Bitmap.isSubset(bitSet1, bitSet2)) {
            superset.add(bitSet2);
            return true;
        } else if (Bitmap.isSubset(bitSet2, bitSet1)) {
            superset.add(bitSet1);
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

    private void extractJoinDependencies() {
        for (int i = 0; i < edgeSize; i++) {
            for (int j = i + 1; j < edgeSize; j++) {
                Edge edge1 = graph.getEdge(i);
                Edge edge2 = graph.getEdge(j);
                if (edge1.isSub(edge2)) {
                    Preconditions.checkArgument(circleDetector.tryAddDirectedEdge(i, j),
                            String.format("Edge %s violates Edge %s", edge1, edge2));
                } else if (edge2.isSub(edge1)) {
                    Preconditions.checkArgument(circleDetector.tryAddDirectedEdge(j, i),
                            String.format("Edge %s violates Edge %s", edge2, edge1));
                }
            }
        }
    }

    class SimplificationStep {
        double benefit;
        int beforeIndex;
        int afterIndex;
        // if true: (t1 join t2) join t3
        // if false: t1 join (t2 join t3)
        boolean isLeftJoin;
        BitSet newLeft;
        BitSet newRight;

        SimplificationStep(double benefit, int beforeIndex, int afterIndex, boolean isLeftJoin) {
            this.afterIndex = afterIndex;
            this.beforeIndex = beforeIndex;
            this.benefit = benefit;
            this.newLeft = new BitSet();
            this.newRight = new BitSet();
            this.isLeftJoin = isLeftJoin;
        }

        public void concretize(HyperGraph graph) {
            Edge beforeEdge = graph.getEdge(beforeIndex);
            Edge afterEdge = graph.getEdge(afterIndex);
            newLeft.or(afterEdge.getLeft());
            newRight.or(afterEdge.getRight());
            if (isLeftJoin) {
                // three left join
                newLeft.or(beforeEdge.getLeft());
                newLeft.or(beforeEdge.getRight());
                newLeft.andNot(afterEdge.getRight());
            } else {
                // three right join
                newRight.or(beforeEdge.getLeft());
                newRight.or(beforeEdge.getRight());
                newRight.andNot(afterEdge.getLeft());
            }
        }

        public void reverse() {
            int temp = beforeIndex;
            beforeIndex = afterIndex;
            afterIndex = temp;
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
            if (step.isPresent()) {
                return String.format("[%s, %s, %d]", step.get(), isInQueue, bestNeighbor);
            }
            return String.format("[%s, empty, %d]", isInQueue, bestNeighbor);
        }
    }
}
