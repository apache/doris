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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.Counter;
import org.apache.doris.nereids.stats.JoinEstimation;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
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
    private final HashMap<Long, Double> cacheCost = new HashMap<>();
    private final Deque<SimplificationStep> appliedSteps = new ArrayDeque<>();
    private final Deque<SimplificationStep> unAppliedSteps = new ArrayDeque<>();

    private final Set<Edge> validEdges;

    /**
     * Create a graph simplifier
     *
     * @param graph create a graph simplifier to simplify the graph
     */
    public GraphSimplifier(HyperGraph graph) {
        this.graph = graph;
        edgeSize = graph.getJoinEdges().size();
        for (int i = 0; i < edgeSize; i++) {
            BestSimplification bestSimplification = new BestSimplification();
            simplifications.add(bestSimplification);
        }
        for (AbstractNode node : graph.getNodes()) {
            DPhyperNode dPhyperNode = (DPhyperNode) node;
            cacheStats.put(node.getNodeMap(), dPhyperNode.getGroup().getStatistics());
            cacheCost.put(node.getNodeMap(), dPhyperNode.getRowCount());
        }
        validEdges = graph.getJoinEdges().stream()
                .filter(e -> {
                    for (Slot slot : e.getJoin().getConditionSlot()) {
                        boolean contains = false;
                        for (int nodeIdx : LongBitmap.getIterator(e.getReferenceNodes())) {
                            if (graph.getNode(nodeIdx).getOutput().contains(slot)) {
                                contains = true;
                                break;
                            }
                        }
                        if (!contains) {
                            return false;
                        }
                    }
                    return true;
                })
                .collect(Collectors.toSet());
        circleDetector = new CircleDetector(edgeSize);

        // init first simplification step
        initFirstStep();
    }

    private boolean isOverlap(Edge edge1, Edge edge2) {
        return (LongBitmap.isOverlap(edge1.getLeftExtendedNodes(), edge2.getLeftExtendedNodes())
                && LongBitmap.isOverlap(edge1.getRightExtendedNodes(), edge2.getRightExtendedNodes()))
                || (LongBitmap.isOverlap(edge1.getLeftExtendedNodes(), edge2.getRightExtendedNodes())
                && LongBitmap.isOverlap(edge1.getRightExtendedNodes(), edge2.getLeftExtendedNodes()));
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
                Edge edge1 = graph.getJoinEdge(i);
                Edge edge2 = graph.getJoinEdge(j);
                List<Long> superset = new ArrayList<>();
                tryGetSuperset(edge1.getLeftExtendedNodes(), edge2.getLeftExtendedNodes(), superset);
                tryGetSuperset(edge1.getLeftExtendedNodes(), edge2.getRightExtendedNodes(), superset);
                tryGetSuperset(edge1.getRightExtendedNodes(), edge2.getLeftExtendedNodes(), superset);
                tryGetSuperset(edge1.getRightExtendedNodes(), edge2.getRightExtendedNodes(), superset);
                if (edge2.isSub(edge1) || edge1.isSub(edge2) || superset.isEmpty() || isOverlap(edge1, edge2)) {
                    continue;
                }
                if (!(circleDetector.checkCircleWithEdge(i, j) || circleDetector.checkCircleWithEdge(j, i))) {
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
                "<%s - %s> must has been stats derived", bestStep.newLeft, bestStep.newRight);
        graph.modifyEdge(bestStep.afterIndex, bestStep.newLeft, bestStep.newRight);
        if (needProcessNeighbor) {
            processNeighbors(bestStep.afterIndex, 0, edgeSize);
        }
        return true;
    }

    private boolean unApplySimplificationStep() {
        Preconditions.checkArgument(!appliedSteps.isEmpty(),
                "try to unapply a simplification step but there is no step applied");
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

    public @Nullable Pair<Long, Long> getLastAppliedSteps() {
        if (appliedSteps.isEmpty()) {
            return null;
        }
        return Pair.of(appliedSteps.peek().newLeft, appliedSteps.peek().newRight);
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
        JoinEdge edge1 = graph.getJoinEdge(edgeIndex1);
        JoinEdge edge2 = graph.getJoinEdge(edgeIndex2);
        if (edge1.isSub(edge2) || edge2.isSub(edge1)
                || circleDetector.checkCircleWithEdge(edgeIndex1, edgeIndex2)
                || circleDetector.checkCircleWithEdge(edgeIndex2, edgeIndex1)
                || !validEdges.contains(edge1) || !validEdges.contains(edge2)) {
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
        JoinEdge edge1Before2;
        JoinEdge edge2Before1;
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

        if (edge1Before2 == null || edge2Before1 == null) {
            return Optional.empty();
        }

        // edge1 is not the neighborhood of edge2
        SimplificationStep simplificationStep = orderJoin(edge1Before2, edge2Before1, edgeIndex1, edgeIndex2);
        return Optional.of(simplificationStep);
    }

    private JoinEdge constructEdge(long leftNodes, JoinEdge edge, long rightNodes) {
        LogicalJoin<? extends Plan, ? extends Plan> join;
        if (graph.getJoinEdges().size() > 64 * 63 / 8) {
            // If there are too many edges, it is advisable to return the "edge" directly
            // to avoid lengthy enumeration time.
            join = edge.getJoin();
        } else {
            BitSet validEdgesMap = graph.getEdgesInOperator(leftNodes, rightNodes);
            List<Expression> hashConditions = validEdgesMap.stream()
                    .mapToObj(i -> graph.getJoinEdge(i).getJoin().getHashJoinConjuncts())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            List<Expression> otherConditions = validEdgesMap.stream()
                    .mapToObj(i -> graph.getJoinEdge(i).getJoin().getHashJoinConjuncts())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            join = edge.getJoin().withJoinConjuncts(hashConditions, otherConditions, null);
        }

        JoinEdge newEdge = new JoinEdge(join, edge.getIndex(),
                edge.getLeftChildEdges(), edge.getRightChildEdges(), edge.getSubTreeNodes(),
                edge.getLeftRequiredNodes(), edge.getRightRequiredNodes(), ImmutableSet.of(), ImmutableSet.of());
        newEdge.addLeftExtendNode(leftNodes);
        newEdge.addRightExtendNode(rightNodes);
        return newEdge;
    }

    private void deriveStats(JoinEdge edge, long leftBitmap, long rightBitmap) {
        // The bitmap may differ from the edge's reference slots.
        // Taking into account the order: edge1<{1} - {2}> edge2<{1,3} - {4}>.
        // Actually, we are considering the sequence {1,3} - {2} - {4}
        long bitmap = LongBitmap.newBitmapUnion(leftBitmap, rightBitmap);
        if (cacheStats.containsKey(bitmap)) {
            return;
        }
        // Note the edge in graphSimplifier contains all tree nodes
        Statistics joinStats = JoinEstimation
                .estimate(cacheStats.get(leftBitmap),
                        cacheStats.get(rightBitmap), edge.getJoin());
        cacheStats.put(bitmap, joinStats);
    }

    private double calCost(JoinEdge edge, long leftBitmap, long rightBitmap) {
        long bitmap = LongBitmap.newBitmapUnion(leftBitmap, rightBitmap);
        Preconditions.checkArgument(cacheStats.containsKey(leftBitmap) && cacheStats.containsKey(rightBitmap)
                        && cacheStats.containsKey(bitmap),
                "graph simplifier meet an edge %s that have not been derived stats", edge);
        LogicalJoin<? extends Plan, ? extends Plan> join = edge.getJoin();
        Statistics leftStats = cacheStats.get(leftBitmap);
        Statistics rightStats = cacheStats.get(rightBitmap);
        double cost;
        if (JoinUtils.shouldNestedLoopJoin(join)) {
            cost = cacheCost.get(leftBitmap) + cacheCost.get(rightBitmap)
                    + rightStats.getRowCount() + 1 / leftStats.getRowCount();
        } else {
            cost = cacheCost.get(leftBitmap) + cacheCost.get(rightBitmap)
                    + (rightStats.getRowCount() + 1 / leftStats.getRowCount()) * 1.2;
        }

        if (!cacheCost.containsKey(bitmap) || cacheCost.get(bitmap) > cost) {
            cacheCost.put(bitmap, cost);
        }
        return cost;
    }

    private @Nullable JoinEdge threeLeftJoin(long bitmap1, JoinEdge edge1, long bitmap2, JoinEdge edge2, long bitmap3) {
        // (plan1 edge1 plan2) edge2 plan3
        // if the left and right is overlapping, just return null.
        Preconditions.checkArgument(
                cacheStats.containsKey(bitmap1) && cacheStats.containsKey(bitmap2) && cacheStats.containsKey(bitmap3));
        // construct new Edge
        long newLeft = LongBitmap.newBitmapUnion(bitmap1, bitmap2);
        if (LongBitmap.isOverlap(newLeft, bitmap3)) {
            return null;
        }
        JoinEdge newEdge = constructEdge(newLeft, edge2, bitmap3);

        deriveStats(edge1, bitmap1, bitmap2);
        deriveStats(newEdge, newLeft, bitmap3);

        calCost(edge1, bitmap1, bitmap2);

        return newEdge;
    }

    private @Nullable JoinEdge threeRightJoin(long bitmap1, JoinEdge edge1, long bitmap2,
            JoinEdge edge2, long bitmap3) {
        Preconditions.checkArgument(cacheStats.containsKey(bitmap1)
                        && cacheStats.containsKey(bitmap2) && cacheStats.containsKey(bitmap3));
        // plan1 edge1 (plan2 edge2 plan3)
        long newRight = LongBitmap.newBitmapUnion(bitmap2, bitmap3);
        if (LongBitmap.isOverlap(bitmap1, newRight)) {
            return null;
        }
        JoinEdge newEdge = constructEdge(bitmap1, edge1, newRight);

        deriveStats(edge2, bitmap2, bitmap3);
        deriveStats(newEdge, bitmap1, newRight);

        calCost(edge1, bitmap2, bitmap3);
        return newEdge;
    }

    private SimplificationStep orderJoin(JoinEdge edge1Before2,
            JoinEdge edge2Before1, int edgeIndex1, int edgeIndex2) {
        double cost1Before2 = calCost(edge1Before2,
                edge1Before2.getLeftExtendedNodes(), edge1Before2.getRightExtendedNodes());
        double cost2Before1 = calCost(edge2Before1,
                edge2Before1.getLeftExtendedNodes(), edge2Before1.getRightExtendedNodes());
        double benefit = Double.MAX_VALUE;
        SimplificationStep step;
        // Choose the plan with smaller cost and make the simplification step to replace the old edge by it.
        if (cost1Before2 < cost2Before1) {
            if (cost1Before2 != 0) {
                benefit = cost2Before1 / cost1Before2;
            }
            // choose edge1Before2
            step = new SimplificationStep(benefit, edgeIndex1, edgeIndex2,
                    edge1Before2.getLeftExtendedNodes(),
                    edge1Before2.getRightExtendedNodes(),
                    graph.getJoinEdge(edgeIndex2).getLeftExtendedNodes(),
                    graph.getJoinEdge(edgeIndex2).getRightExtendedNodes());
        } else {
            if (cost2Before1 != 0) {
                benefit = cost1Before2 / cost2Before1;
            }
            // choose edge2Before1
            step = new SimplificationStep(benefit, edgeIndex2, edgeIndex1, edge2Before1.getLeftExtendedNodes(),
                    edge2Before1.getRightExtendedNodes(), graph.getJoinEdge(edgeIndex1).getLeftExtendedNodes(),
                    graph.getJoinEdge(edgeIndex1).getRightExtendedNodes());
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

    /**
     * Put join dependencies into circle detector.
     */
    private void extractJoinDependencies() {
        for (int i = 0; i < edgeSize; i++) {
            Edge edge1 = graph.getJoinEdge(i);
            for (int j = i + 1; j < edgeSize; j++) {
                Edge edge2 = graph.getJoinEdge(j);
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
            return Double.compare(o.getBenefit(), getBenefit());
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
