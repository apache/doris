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
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.Counter;
import org.apache.doris.nereids.stats.JoinEstimation;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Stack;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * GraphSimplifier is used to simplify HyperGraph {@link HyperGraph}.
 * <p>
 * Related paper:
 * - [Neu09] Neumann: “Query Simplification: Graceful Degradation for Join-Order Optimization”.
 * - [Rad19] Radke and Neumann: “LinDP++: Generalizing Linearized DP to Crossproducts and Non-Inner Joins”.
 */
public class GraphSimplifier {
    // Note that each index in the graph simplifier is the half of the actual index
    private final int edgeSize;
    // Detect the circle when order join
    private final CircleDetector circleDetector;
    // This is used for cache the intermediate results when calculate the benefit
    // Note that we store it for the after. Because if we put B after A (t1 Join_A t2 Join_B t3),
    // B is changed. Therefore, Any step that involves B need to be recalculated.
    private final List<BestSimplification> simplifications = new ArrayList<>();
    private final PriorityQueue<BestSimplification> priorityQueue = new PriorityQueue<>();
    // The graph we are simplifying
    private final HyperGraph graph;
    // It cached the plan stats in simplification. we don't store it in hyper graph,
    // because it's just used for simulating join. In fact, the graph simplifier
    // just generate the partial order of join operator.
    private final HashMap<Long, Statistics> cacheStats = new HashMap<>();
    private final HashMap<Long, Cost> cacheCost = new HashMap<>();

    private final Stack<SimplificationStep> appliedSteps = new Stack<>();
    private final Stack<SimplificationStep> unAppliedSteps = new Stack<>();

    /**
     * Create a graph simplifier
     *
     * @param graph create a graph simplifier to simplify the graph
     */
    public GraphSimplifier(HyperGraph graph) {
        this.graph = graph;
        edgeSize = graph.getEdges().size();
        for (int i = 0; i < edgeSize; i++) {
            BestSimplification bestSimplification = new BestSimplification();
            simplifications.add(bestSimplification);
        }
        for (Node node : graph.getNodes()) {
            cacheStats.put(node.getNodeMap(), node.getGroup().getStatistics());
            cacheCost.put(node.getNodeMap(), Cost.withRowCount(node.getRowCount()));
        }
        circleDetector = new CircleDetector(edgeSize);

        // init first simplification step
        initFirstStep();
    }

    private void initFirstStep() {
        extractJoinDependencies();
        for (int i = 0; i < edgeSize; i += 1) {
            processNeighbors(i, i + 1, edgeSize);
        }
    }

    /**
     * Check whether all edge has been ordered
     *
     * @return if true, all edges has a total order
     */
    public boolean isTotalOrder() {
        for (int i = 0; i < edgeSize; i++) {
            for (int j = i + 1; j < edgeSize; j++) {
                Edge edge1 = graph.getEdge(i);
                Edge edge2 = graph.getEdge(j);
                List<Long> superset = new ArrayList<>();
                tryGetSuperset(edge1.getLeftExtendedNodes(), edge2.getLeftExtendedNodes(), superset);
                tryGetSuperset(edge1.getLeftExtendedNodes(), edge2.getRightExtendedNodes(), superset);
                tryGetSuperset(edge1.getRightExtendedNodes(), edge2.getLeftExtendedNodes(), superset);
                tryGetSuperset(edge1.getRightExtendedNodes(), edge2.getRightExtendedNodes(), superset);
                if (!circleDetector.checkCircleWithEdge(i, j) && !circleDetector.checkCircleWithEdge(j, i)
                        && !edge2.isSub(edge1) && !edge1.isSub(edge2) && !superset.isEmpty()) {
                    return false;
                }
            }
        }
        return true;
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
        Preconditions.checkArgument(limit >= 1);
        int lowerBound = 0;
        int upperBound = 1;

        // Try to probe the largest number of steps to satisfy the limit
        int numApplySteps = 0;
        Counter counter = new Counter(limit);
        SubgraphEnumerator enumerator = new SubgraphEnumerator(counter, graph);
        while (true) {
            while (numApplySteps < upperBound) {
                if (!applySimplificationStep()) {
                    // If we have done all steps but still has more sub graphs
                    // Just return
                    if (!enumerator.enumerate()) {
                        return false;
                    }
                    break;
                }
                numApplySteps += 1;
            }
            if (numApplySteps < upperBound || enumerator.enumerate()) {
                break;
            }
            upperBound *= 2;
        }

        // Try to search the lowest number of steps to satisfy the limit
        upperBound = numApplySteps + 1;
        while (lowerBound < upperBound) {
            int mid = lowerBound + (upperBound - lowerBound) / 2;
            applyStepsWithNum(mid);
            if (enumerator.enumerate()) {
                upperBound = mid;
            } else {
                lowerBound = mid + 1;
            }
        }
        applyStepsWithNum(upperBound);
        return true;
    }

    /**
     * This function apply a simplification step
     *
     * @return If there is no other steps, return false.
     */
    public boolean applySimplificationStep() {
        boolean needProcessNeighbor = unAppliedSteps.isEmpty();
        SimplificationStep bestStep = fetchSimplificationStep();
        if (bestStep == null) {
            return false;
        }
        appliedSteps.push(bestStep);
        Preconditions.checkArgument(
                cacheStats.containsKey(bestStep.newLeft) && cacheStats.containsKey(bestStep.newRight),
                String.format("%s - %s", bestStep.newLeft, bestStep.newRight));
        graph.modifyEdge(bestStep.afterIndex, bestStep.newLeft, bestStep.newRight);
        if (needProcessNeighbor) {
            processNeighbors(bestStep.afterIndex, 0, edgeSize);
        }
        return true;
    }

    private boolean unApplySimplificationStep() {
        Preconditions.checkArgument(appliedSteps.size() > 0);
        SimplificationStep bestStep = appliedSteps.pop();
        unAppliedSteps.push(bestStep);
        graph.modifyEdge(bestStep.afterIndex, bestStep.oldLeft, bestStep.oldRight);
        // Note we don't need to delete this edge in circleDetector, because we don't need to
        // recalculate neighbors until all steps applied
        return true;
    }

    private @Nullable SimplificationStep fetchSimplificationStep() {
        if (!unAppliedSteps.isEmpty()) {
            return unAppliedSteps.pop();
        }
        if (priorityQueue.isEmpty()) {
            return null;
        }
        BestSimplification bestSimplification = priorityQueue.poll();
        bestSimplification.isInQueue = false;
        SimplificationStep bestStep = bestSimplification.getStep();
        while (bestSimplification.bestNeighbor == -1
                || !circleDetector.tryAddDirectedEdge(bestStep.beforeIndex, bestStep.afterIndex)) {
            processNeighbors(bestStep.afterIndex, 0, edgeSize);
            if (priorityQueue.isEmpty()) {
                return null;
            }
            bestSimplification = priorityQueue.poll();
            bestSimplification.isInQueue = false;
            bestStep = bestSimplification.getStep();
        }
        return bestStep;
    }

    private void applyStepsWithNum(int num) {
        while (appliedSteps.size() < num) {
            applySimplificationStep();
        }
        while (appliedSteps.size() > num) {
            unApplySimplificationStep();
        }
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
        BestSimplification bestSimplification = simplifications.get(edgeIndex1);
        bestSimplification.bestNeighbor = -1;
        for (int edgeIndex2 = Integer.max(beginIndex, edgeIndex1 + 1); edgeIndex2 < endIndex; edgeIndex2 += 1) {
            Optional<SimplificationStep> optionalStep = makeSimplificationStep(edgeIndex1, edgeIndex2);
            if (optionalStep.isPresent()) {
                trySetSimplificationStep(optionalStep.get(), bestSimplification, edgeIndex1, edgeIndex2);
            }
        }
    }

    private boolean trySetSimplificationStep(SimplificationStep step, BestSimplification bestSimplification,
            int index, int neighborIndex) {
        if (bestSimplification.bestNeighbor == -1 || !bestSimplification.isInQueue
                || bestSimplification.getBenefit() <= step.getBenefit()) {
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
        if (edge1.isSub(edge2) || edge2.isSub(edge1) || circleDetector.checkCircleWithEdge(edgeIndex1, edgeIndex2)
                || circleDetector.checkCircleWithEdge(edgeIndex2, edgeIndex1)) {
            return Optional.empty();
        }
        long left1 = edge1.getLeftExtendedNodes();
        long right1 = edge1.getRightExtendedNodes();
        long left2 = edge2.getLeftExtendedNodes();
        long right2 = edge2.getRightExtendedNodes();
        if (!cacheStats.containsKey(left1) || !cacheStats.containsKey(right1)
                || !cacheStats.containsKey(left2) || !cacheStats.containsKey(right2)) {
            return Optional.empty();
        }
        Pair<Statistics, Edge> edge1Before2;
        Pair<Statistics, Edge> edge2Before1;
        List<Long> superBitset = new ArrayList<>();
        if (tryGetSuperset(left1, left2, superBitset)) {
            // (common Join1 right1) Join2 right2
            edge1Before2 = threeLeftJoin(superBitset.get(0), edge1, right1, edge2, right2);
            // (common Join2 right2) Join1 right1
            edge2Before1 = threeLeftJoin(superBitset.get(0), edge2, right2, edge1, right1);
        } else if (tryGetSuperset(left1, right2, superBitset)) {
            // left2 Join2 (common Join1 right1)
            edge1Before2 = threeRightJoin(left2, edge2, superBitset.get(0), edge1, right1);
            // (left2 Join2 common) join1 right1
            edge2Before1 = threeLeftJoin(left2, edge2, superBitset.get(0), edge1, right1);
        } else if (tryGetSuperset(right1, left2, superBitset)) {
            // (left1 Join1 common) Join2 right2
            edge1Before2 = threeLeftJoin(left1, edge1, superBitset.get(0), edge2, right2);
            // left1 Join1 (common Join2 right2)
            edge2Before1 = threeRightJoin(left1, edge1, superBitset.get(0), edge2, right2);
        } else if (tryGetSuperset(right1, right2, superBitset)) {
            // left2 Join2 (left1 Join1 common)
            edge1Before2 = threeRightJoin(left2, edge2, left1, edge1, superBitset.get(0));
            // left1 Join1 (left2 Join2 common)
            edge2Before1 = threeRightJoin(left1, edge1, left2, edge2, superBitset.get(0));
        } else {
            return Optional.empty();
        }
        // edge1 is not the neighborhood of edge2
        SimplificationStep simplificationStep = orderJoin(edge1Before2, edge2Before1, edgeIndex1, edgeIndex2);
        return Optional.of(simplificationStep);
    }

    Pair<Statistics, Edge> threeLeftJoin(long bitmap1, Edge edge1, long bitmap2, Edge edge2, long bitmap3) {
        // (plan1 edge1 plan2) edge2 plan3
        // The join may have redundant table, e.g., t1,t2 join t3 join t2,t4
        // Therefore, the cost is not accurate
        Preconditions.checkArgument(
                cacheStats.containsKey(bitmap1) && cacheStats.containsKey(bitmap2) && cacheStats.containsKey(bitmap3));
        Statistics leftStats = JoinEstimation.estimate(cacheStats.get(bitmap1), cacheStats.get(bitmap2),
                edge1.getJoin());
        Statistics joinStats = JoinEstimation.estimate(leftStats, cacheStats.get(bitmap3), edge2.getJoin());
        Edge edge = new Edge(
                edge2.getJoin(), -1, edge2.getLeftChildEdges(), edge2.getRightChildEdges(), edge2.getSubTreeNodes());
        long newLeft = LongBitmap.newBitmapUnion(bitmap1, bitmap2);
        // To avoid overlapping the left and the right, the newLeft is calculated, Note the
        // newLeft is not totally include the bitset1 and bitset2, we use circle detector to trace the dependency
        newLeft = LongBitmap.andNot(newLeft, bitmap3);
        edge.addLeftNodes(newLeft);
        edge.addRightNode(edge2.getRightExtendedNodes());
        cacheStats.put(newLeft, leftStats);
        cacheCost.put(newLeft, calCost(edge2, leftStats, cacheStats.get(bitmap1), cacheStats.get(bitmap2)));
        return Pair.of(joinStats, edge);
    }

    Pair<Statistics, Edge> threeRightJoin(long bitmap1, Edge edge1, long bitmap2, Edge edge2, long bitmap3) {
        Preconditions.checkArgument(
                cacheStats.containsKey(bitmap1) && cacheStats.containsKey(bitmap2) && cacheStats.containsKey(bitmap3));
        // plan1 edge1 (plan2 edge2 plan3)
        Statistics rightStats = JoinEstimation.estimate(cacheStats.get(bitmap2), cacheStats.get(bitmap3),
                edge2.getJoin());
        Statistics joinStats = JoinEstimation.estimate(cacheStats.get(bitmap1), rightStats, edge1.getJoin());
        Edge edge = new Edge(
                edge1.getJoin(), -1, edge1.getLeftChildEdges(), edge1.getRightChildEdges(), edge1.getSubTreeNodes());

        long newRight = LongBitmap.newBitmapUnion(bitmap2, bitmap3);
        newRight = LongBitmap.andNot(newRight, bitmap1);
        edge.addLeftNode(edge1.getLeftExtendedNodes());
        edge.addRightNode(newRight);
        cacheStats.put(newRight, rightStats);
        cacheCost.put(newRight, calCost(edge2, rightStats, cacheStats.get(bitmap2), cacheStats.get(bitmap3)));
        return Pair.of(joinStats, edge);
    }

    private Edge processMissedEdges(int edgeIndex1, int edgeIndex2, Edge edge) {
        List<Edge> edges = Lists.newArrayList(edge);
        edges.addAll(graph.getEdges().stream()
                .filter(e -> e.getIndex() != edgeIndex1 && e.getIndex() != edgeIndex2
                        && LongBitmap.isSubset(e.getReferenceNodes(), edge.getReferenceNodes())
                        && !LongBitmap.isSubset(e.getReferenceNodes(), edge.getLeftExtendedNodes())
                        && !LongBitmap.isSubset(e.getReferenceNodes(), edge.getRightExtendedNodes()))
                .collect(Collectors.toList()));
        if (edges.size() > 1) {
            List<Expression> hashConjuncts = new ArrayList<>();
            List<Expression> otherConjuncts = new ArrayList<>();
            JoinType joinType = Edge.extractJoinTypeAndConjuncts(edges, hashConjuncts, otherConjuncts);
            LogicalJoin oldJoin = edge.getJoin();
            LogicalJoin newJoin = new LogicalJoin<>(joinType, hashConjuncts,
                    otherConjuncts, oldJoin.getHint(), oldJoin.left(), oldJoin.right());
            Edge newEdge = Edge.createTempEdge(newJoin);
            newEdge.setLeftExtendedNodes(edge.getLeftExtendedNodes());
            newEdge.setRightExtendedNodes(edge.getRightExtendedNodes());
            return newEdge;
        } else {
            return edge;
        }
    }

    private SimplificationStep orderJoin(Pair<Statistics, Edge> edge1Before2,
                                         Pair<Statistics, Edge> edge2Before1, int edgeIndex1, int edgeIndex2) {
        // TODO: Consider miss edges when construct join.
        // considering
        //          a
        //         /  \
        //        b  - c
        // when constructing edge_ab before edge_bc. edge_ac should be added on top join
        Cost cost1Before2 = calCost(edge1Before2.second, edge1Before2.first,
                cacheStats.get(edge1Before2.second.getLeftExtendedNodes()),
                cacheStats.get(edge1Before2.second.getRightExtendedNodes()));
        Cost cost2Before1 = calCost(edge1Before2.second, edge2Before1.first,
                cacheStats.get(edge2Before1.second.getLeftExtendedNodes()),
                cacheStats.get(edge2Before1.second.getRightExtendedNodes()));
        double benefit = Double.MAX_VALUE;
        SimplificationStep step;
        // Choose the plan with smaller cost and make the simplification step to replace the old edge by it.
        if (cost1Before2.getValue() < cost2Before1.getValue()) {
            if (cost1Before2.getValue() != 0) {
                benefit = cost2Before1.getValue() / cost1Before2.getValue();
            }
            // choose edge1Before2
            step = new SimplificationStep(benefit, edgeIndex1, edgeIndex2, edge1Before2.second.getLeftExtendedNodes(),
                    edge1Before2.second.getRightExtendedNodes(), graph.getEdge(edgeIndex2).getLeftExtendedNodes(),
                    graph.getEdge(edgeIndex2).getRightExtendedNodes());
        } else {
            if (cost2Before1.getValue() != 0) {
                benefit = cost1Before2.getValue() / cost2Before1.getValue();
            }
            // choose edge2Before1
            step = new SimplificationStep(benefit, edgeIndex2, edgeIndex1, edge2Before1.second.getLeftExtendedNodes(),
                    edge2Before1.second.getRightExtendedNodes(), graph.getEdge(edgeIndex1).getLeftExtendedNodes(),
                    graph.getEdge(edgeIndex1).getRightExtendedNodes());
        }
        return step;
    }

    private boolean tryGetSuperset(long bitmap1, long bitmap2, List<Long> superset) {
        if (LongBitmap.isSubset(bitmap1, bitmap2)) {
            superset.add(bitmap2);
            return true;
        } else if (LongBitmap.isSubset(bitmap2, bitmap1)) {
            superset.add(bitmap1);
            return true;
        }
        return false;
    }

    private Cost calCost(Edge edge, Statistics stats,
            Statistics leftStats, Statistics rightStats) {
        LogicalJoin join = edge.getJoin();
        PlanContext planContext = new PlanContext(stats, ImmutableList.of(leftStats, rightStats));
        Cost cost;
        if (JoinUtils.shouldNestedLoopJoin(join)) {
            PhysicalNestedLoopJoin nestedLoopJoin = new PhysicalNestedLoopJoin<>(
                    join.getJoinType(),
                    join.getHashJoinConjuncts(),
                    join.getOtherJoinConjuncts(),
                    join.getMarkJoinSlotReference(),
                    join.getLogicalProperties(),
                    join.left(),
                    join.right());
            cost = CostCalculator.calculateCost(nestedLoopJoin, planContext);
            cost = CostCalculator.addChildCost(nestedLoopJoin, cost, cacheCost.get(edge.getLeftExtendedNodes()), 0);
            cost = CostCalculator.addChildCost(nestedLoopJoin, cost, cacheCost.get(edge.getRightExtendedNodes()), 1);
        } else {
            PhysicalHashJoin hashJoin = new PhysicalHashJoin<>(
                    join.getJoinType(),
                    join.getHashJoinConjuncts(),
                    join.getOtherJoinConjuncts(),
                    join.getHint(),
                    join.getMarkJoinSlotReference(),
                    join.getLogicalProperties(),
                    join.left(),
                    join.right());
            cost = CostCalculator.calculateCost(hashJoin, planContext);
            cost = CostCalculator.addChildCost(hashJoin, cost, cacheCost.get(edge.getLeftExtendedNodes()), 0);
            cost = CostCalculator.addChildCost(hashJoin, cost, cacheCost.get(edge.getRightExtendedNodes()), 1);
        }

        return cost;
    }

    /**
     * Put join dependencies into circle detector.
     */
    private void extractJoinDependencies() {
        for (int i = 0; i < edgeSize; i++) {
            Edge edge1 = graph.getEdge(i);
            for (int j = i + 1; j < edgeSize; j++) {
                Edge edge2 = graph.getEdge(j);
                if (edge1.isSub(edge2)) {
                    Preconditions.checkArgument(circleDetector.tryAddDirectedEdge(i, j),
                            "Edge %s violates Edge %s", edge1, edge2);
                } else if (edge2.isSub(edge1)) {
                    Preconditions.checkArgument(circleDetector.tryAddDirectedEdge(j, i),
                            "Edge %s violates Edge %s", edge2, edge1);
                }
            }
        }
    }

    static class SimplificationStep {
        double benefit;
        int beforeIndex;
        int afterIndex;
        long newLeft;
        long newRight;
        long oldLeft;
        long oldRight;

        SimplificationStep(double benefit, int beforeIndex, int afterIndex, long newLeft, long newRight,
                long oldLeft, long oldRight) {
            this.afterIndex = afterIndex;
            this.beforeIndex = beforeIndex;
            this.benefit = benefit;
            this.oldLeft = oldLeft;
            this.oldRight = oldRight;
            this.newLeft = newLeft;
            this.newRight = newRight;
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
            return String.format("%d -> %d", beforeIndex, afterIndex);
        }
    }

    static class BestSimplification implements Comparable<BestSimplification> {
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
