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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.receiver.Counter;
import org.apache.doris.nereids.stats.JoinEstimation;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
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

    private static int MAX_SIMPLIFICATION_STEPS = 1000000;
    // It cached the plan stats in simplification. we don't store it in hyper graph,
    // because it's just used for simulating join. In fact, the graph simplifier
    // just generate the partial order of join operator.
    private final HashMap<Long, Statistics> cacheStats = new HashMap<>();
    private final HashMap<Long, Double> cacheCost = new HashMap<>();
    private Deque<SimplificationStep> appliedSteps = new ArrayDeque<>();
    private Deque<SimplificationStep> unAppliedSteps = new ArrayDeque<>();
    private HyperGraph graph;
    private CircleDetector cycles;
    private List<BestSimplification> simplifications;
    private PriorityQueue<BestSimplification> priorityQueue = new PriorityQueue<>();

    /**
     * GraphSimplifier
     */
    public GraphSimplifier(HyperGraph graph) {
        this.graph = graph;
        int edgeCount = graph.getJoinEdges().size();

        // init join dependency
        this.cycles = new CircleDetector(edgeCount);
        extractJoinDependencies(cycles);

        // init simplifications with empty values
        this.simplifications = new ArrayList<>(edgeCount);
        for (int i = 0; i < edgeCount; i++) {
            simplifications.add(new BestSimplification());
        }

        // init cacheStats and cacheCost
        for (AbstractNode node : graph.getNodes()) {
            DPhyperNode dPhyperNode = (DPhyperNode) node;
            cacheStats.put(node.getNodeMap(), dPhyperNode.getGroup().getStatistics());
            cacheCost.put(node.getNodeMap(), dPhyperNode.getRowCount());
        }

        //
        for (int edgeIdx = 0; edgeIdx < edgeCount; edgeIdx++) {
            recalculateNeighbors(edgeIdx, edgeIdx + 1, edgeCount);
        }
    }

    /**
     * Checks if the current hypergraph is joinable, i.e., all tables can be connected via joins.
     */
    private static boolean graphIsJoinable(HyperGraph graph, CircleDetector cycles) {
        int nodeCount = graph.getNodes().size();
        long[] components = new long[nodeCount];
        int[] inComponent = new int[nodeCount];

        for (int nodeIdx = 0; nodeIdx < nodeCount; nodeIdx++) {
            components[nodeIdx] = LongBitmap.newBitmap(nodeIdx);
            inComponent[nodeIdx] = nodeIdx;
        }
        return connectComponentsThroughJoins(graph, cycles, components, inComponent) == nodeCount;
    }

    /**
     * Connects components through join edges, calling callback on each join.
     */
    private static int connectComponentsThroughJoins(
            HyperGraph graph,
            CircleDetector cycles,
            long[] components,
            int[] inComponent) {
        int nodeCount = graph.getNodes().size();
        int numInComponent = 1;
        boolean didAnything;
        int usedEdges = 0;
        int edgeCount = cycles.getOrder().length;
        do {
            didAnything = false;
            for (int edgeIdx : cycles.getOrder()) {
                Edge e = graph.getJoinEdge(edgeIdx);
                long rightNodes = e.getRightExtendedNodes();
                int leftComponent = getComponent(components, inComponent, e.getLeftExtendedNodes());
                if (leftComponent == -1) {
                    continue;
                }
                if (LongBitmap.isOverlap(rightNodes, components[leftComponent])) {
                    //TODO return ???
                    return -1;
                    //                    continue;
                }
                int rightComponent = getComponent(components, inComponent, e.getRightExtendedNodes());
                if (rightComponent == -1
                        || combiningWouldViolateConflictRules(e.getConflictRules(), inComponent, leftComponent,
                        rightComponent)) {
                    continue;
                }

                if (rightComponent < leftComponent) {
                    int tmp = leftComponent;
                    leftComponent = rightComponent;
                    rightComponent = tmp;
                }
                int numChanged = 0;
                for (int tableIdx : LongBitmap.getIterator(components[rightComponent])) {
                    inComponent[tableIdx] = leftComponent;
                    ++numChanged;
                }

                long combinedNodes = components[leftComponent] | components[rightComponent];
                components[leftComponent] = combinedNodes;
                usedEdges++;
                if (leftComponent == 0) {
                    numInComponent += numChanged;
                    if (numInComponent == nodeCount && usedEdges == edgeCount) {
                        return numInComponent;
                    }
                }
                didAnything = true;
            }
        } while (didAnything);
        return numInComponent;
    }

    private static int getComponent(long[] components, int[] inComponent, long tables) {
        if (tables == 0) {
            return -1;
        }
        int idx = Long.numberOfTrailingZeros(tables);
        int component = inComponent[idx];
        if (component >= 0 && (tables & ~components[component]) == 0) {
            return component;
        }
        return -1;
    }

    private static boolean combiningWouldViolateConflictRules(
            List<Pair<Long, Long>> conflictRules,
            int[] inComponent,
            int leftComponent,
            int rightComponent) {
        for (Pair<Long, Long> cr : conflictRules) {
            boolean applies = false;
            for (int nodeIdx : LongBitmap.getIterator(cr.first)) {
                if (inComponent[nodeIdx] == leftComponent
                        || inComponent[nodeIdx] == rightComponent) {
                    applies = true;
                    break;
                }
            }
            if (applies) {
                for (int nodeIdx : LongBitmap.getIterator(cr.second)) {
                    if (inComponent[nodeIdx] != leftComponent
                            && inComponent[nodeIdx] != rightComponent) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void extractJoinDependencies(CircleDetector circleDetector) {
        int edgeCount = graph.getJoinEdges().size();
        for (int i = 0; i < edgeCount; i++) {
            Edge edge1 = graph.getJoinEdge(i);
            for (int j = i + 1; j < edgeCount; j++) {
                Edge edge2 = graph.getJoinEdge(j);
                if (edge1.isSub(edge2)) {
                    Preconditions.checkArgument(circleDetector.addEdge(i, j),
                            "Edge %s violates Edge %s", edge1, edge2);
                } else if (edge2.isSub(edge1)) {
                    Preconditions.checkArgument(circleDetector.addEdge(j, i),
                            "Edge %s violates Edge %s", edge2, edge1);
                }
            }
        }
    }

    /**
     * simplifyGraph
     */
    public boolean simplifyGraph(int limit) {
        Preconditions.checkArgument(limit >= 1);
        int lowerBound = 0;
        int upperBound = 1;

        // Try to probe the largest number of steps to satisfy the limit
        Counter counter = new Counter(limit);
        SubgraphEnumerator enumerator = new SubgraphEnumerator(counter, graph);
        while (true) {
            boolean hitUpperLimit = false;
            while (numStepsDone() < upperBound) {
                if (applySimplificationStep() == SimplificationResult.NO_SIMPLIFICATION_POSSIBLE) {
                    // If we have done all steps but still has more sub graphs
                    // Just return
                    if (!enumerator.enumerate()) {
                        return false;
                    }
                    upperBound = numStepsDone();
                    hitUpperLimit = true;
                    break;
                }
            }
            if (hitUpperLimit) {
                break;
            }
            if (enumerator.enumerate()) {
                break;
            }
            lowerBound = upperBound;
            upperBound *= 2;
            if (upperBound > MAX_SIMPLIFICATION_STEPS) {
                return false;
            }
        }

        // Try to search the lowest number of steps to satisfy the limit
        while (upperBound - lowerBound > 1) {
            int mid = (upperBound + lowerBound) / 2;
            applyStepsWithNum(mid);
            if (enumerator.enumerate()) {
                upperBound = mid;
            } else {
                lowerBound = mid;
            }
        }
        applyStepsWithNum(upperBound);
        if (!isTotalOrder()) {
            return false;
        }
        return true;
    }

    /**
     * Check whether all edge has been ordered
     *
     * @return if true, all edges has a total order
     */
    public boolean isTotalOrder() {
        return cycles.getOrder().length == graph.getJoinEdges().size() && graphIsJoinable(graph, cycles);
    }

    private boolean isOverlap(Edge edge1, Edge edge2) {
        return (LongBitmap.isOverlap(edge1.getLeftExtendedNodes(), edge2.getLeftExtendedNodes())
                && LongBitmap.isOverlap(edge1.getRightExtendedNodes(), edge2.getRightExtendedNodes()))
                || (LongBitmap.isOverlap(edge1.getLeftExtendedNodes(), edge2.getRightExtendedNodes())
                && LongBitmap.isOverlap(edge1.getRightExtendedNodes(), edge2.getLeftExtendedNodes()));
    }

    private boolean unApplySimplificationStep() {
        Preconditions.checkArgument(!appliedSteps.isEmpty(),
                "try to unapply a simplification step but there is no step applied");
        SimplificationStep bestStep = appliedSteps.poll();
        unAppliedSteps.push(bestStep);
        graph.modifyEdge(bestStep.afterEdgeIdx, bestStep.oldEdgeLeft, bestStep.oldEdgeRight);
        // Note we don't need to delete this edge in circleDetector, because we don't need to
        // recalculate neighbors until all steps applied
        return true;
    }

    private void applyStepsWithNum(int num) {
        while (appliedSteps.size() < num) {
            SimplificationResult result = applySimplificationStep();
            Preconditions.checkState(result != SimplificationResult.NO_SIMPLIFICATION_POSSIBLE);
        }
        while (appliedSteps.size() > num) {
            unApplySimplificationStep();
        }
    }

    public @Nullable Pair<Long, Long> getLastAppliedSteps() {
        if (appliedSteps.isEmpty()) {
            return null;
        }
        return Pair.of(appliedSteps.peek().newEdgeLeft, appliedSteps.peek().newEdgeRight);
    }

    public int numStepsDone() {
        return appliedSteps.size();
    }

    public int numStepsUndone() {
        return unAppliedSteps.size();
    }

    @VisibleForTesting
    protected SimplificationResult applySimplificationStep() {
        if (!unAppliedSteps.isEmpty()) {
            SimplificationStep step = unAppliedSteps.poll();
            graph.modifyEdge(step.afterEdgeIdx, step.newEdgeLeft, step.newEdgeRight);
            appliedSteps.push(step);
            return SimplificationResult.APPLIED_REDO_STEP;
        }

        BestSimplification cacheNode = priorityQueue.peek();
        if (cacheNode == null) {
            return SimplificationResult.NO_SIMPLIFICATION_POSSIBLE;
        }
        ProposedSimplificationStep bestStep = cacheNode.bestStep;
        boolean forced = false;
        if (cycles.edgeWouldCreateCycle(bestStep.beforeEdgeIdx, bestStep.afterEdgeIdx)) {
            int tmp = bestStep.beforeEdgeIdx;
            bestStep.beforeEdgeIdx = bestStep.afterEdgeIdx;
            bestStep.afterEdgeIdx = tmp;
            forced = true;
        }

        SimplificationStep fullStep = concretizeSimplificationStep(bestStep);

        boolean added = cycles.addEdge(bestStep.beforeEdgeIdx, bestStep.afterEdgeIdx);
        Preconditions.checkState(added, "Cycle detected");
        graph.modifyEdge(bestStep.afterEdgeIdx, fullStep.newEdgeLeft, fullStep.newEdgeRight);

        if (!graphIsJoinable(graph, cycles)) {
            // Undo change
            cycles.deleteEdge(bestStep.beforeEdgeIdx, bestStep.afterEdgeIdx);
            graph.modifyEdge(bestStep.afterEdgeIdx, fullStep.oldEdgeLeft, fullStep.oldEdgeRight);

            // Insert opposite constraint and try again
            if (!cycles.addEdge(fullStep.afterEdgeIdx, fullStep.beforeEdgeIdx)) {
                priorityQueue.poll();
                cacheNode.isInPriorityQueue = false;
            }
            return applySimplificationStep();
        }
        recalculateNeighbors(bestStep.afterEdgeIdx, 0, graph.getJoinEdges().size());
        appliedSteps.push(fullStep);
        return forced ? SimplificationResult.APPLIED_NOOP : SimplificationResult.APPLIED_SIMPLIFICATION;
    }

    private void updatePQ(int edgeIdx) {
        BestSimplification cacheNode = simplifications.get(edgeIdx);
        Preconditions.checkState(!Double.isNaN(cacheNode.bestStep.benefit), "bestStep has invalid benefit value");
        if (!cacheNode.isInPriorityQueue) {
            if (cacheNode.bestNeighbor != -1) {
                priorityQueue.add(cacheNode);
                cacheNode.isInPriorityQueue = true;
            }
        } else {
            if (cacheNode.bestNeighbor == -1) {
                priorityQueue.remove(cacheNode);
                cacheNode.isInPriorityQueue = false;
            }
        }
    }

    private void recalculateNeighbors(int edge1Idx, int begin, int end) {
        for (int edge2Idx = begin; edge2Idx < Math.min(edge1Idx, end); edge2Idx++) {
            BestSimplification otherCache = simplifications.get(edge2Idx);
            ProposedSimplificationStep step = new ProposedSimplificationStep();
            if (edgesAreNeighboring(edge2Idx, edge1Idx, step)) {
                if (otherCache.bestNeighbor == -1 || step.benefit >= otherCache.bestStep.benefit) {
                    otherCache.bestNeighbor = edge1Idx;
                    otherCache.bestStep = step;
                    updatePQ(edge2Idx);
                    continue;
                }
            }
            if (otherCache.bestNeighbor == edge1Idx) {
                recalculateNeighbors(edge2Idx, 0, graph.getJoinEdges().size());
            }
        }
        BestSimplification cacheNode = simplifications.get(edge1Idx);
        cacheNode.bestNeighbor = -1;
        cacheNode.bestStep.benefit = Double.NEGATIVE_INFINITY;
        for (int edge2Idx = Math.max(begin, edge1Idx + 1); edge2Idx < end; edge2Idx++) {
            ProposedSimplificationStep step = new ProposedSimplificationStep();
            if (edgesAreNeighboring(edge1Idx, edge2Idx, step)) {
                if (cacheNode.bestNeighbor == -1 || step.benefit > cacheNode.bestStep.benefit) {
                    cacheNode.bestNeighbor = edge2Idx;
                    cacheNode.bestStep = step;
                }
            }
        }
        updatePQ(edge1Idx);
    }

    private boolean edgesAreNeighboring(int edge1Idx, int edge2Idx, ProposedSimplificationStep step) {
        Edge edge1 = graph.getJoinEdge(edge1Idx);
        Edge edge2 = graph.getJoinEdge(edge2Idx);
        if (edge1.isSub(edge2) || edge2.isSub(edge1)) {
            return false;
        }
        long left1 = edge1.getLeftExtendedNodes();
        long right1 = edge1.getRightExtendedNodes();
        long left2 = edge2.getLeftExtendedNodes();
        long right2 = edge2.getRightExtendedNodes();
        if (!cacheStats.containsKey(left1) || !cacheStats.containsKey(right1)
                || !cacheStats.containsKey(left2) || !cacheStats.containsKey(right2)) {
            return false;
        }
        Edge edge1Before2;
        Edge edge2Before1;
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
            return false;
        }

        if (edge1Before2 == null || edge2Before1 == null) {
            return false;
        }

        double cost1Before2 = calCost(edge1Before2,
                edge1Before2.getLeftExtendedNodes(), edge1Before2.getRightExtendedNodes());
        double cost2Before1 = calCost(edge2Before1,
                edge2Before1.getLeftExtendedNodes(), edge2Before1.getRightExtendedNodes());
        double benefit = Double.MAX_VALUE;
        // Choose the plan with smaller cost and make the simplification step to replace the old edge by it.
        if (cost1Before2 < cost2Before1) {
            if (cost1Before2 != 0) {
                benefit = cost2Before1 / cost1Before2;
            }
            // choose edge1Before2
            step.benefit = benefit;
            step.beforeEdgeIdx = edge1Idx;
            step.afterEdgeIdx = edge2Idx;
        } else {
            if (cost2Before1 != 0) {
                benefit = cost1Before2 / cost2Before1;
            }
            // choose edge2Before1
            step.benefit = benefit;
            step.beforeEdgeIdx = edge2Idx;
            step.afterEdgeIdx = edge1Idx;
        }
        return true;
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

    private Edge constructEdge(long leftNodes, Edge edge, long rightNodes) {
        LogicalJoin<? extends Plan, ? extends Plan> join;
        if (graph.getJoinEdges().size() > 64 * 63 / 8) {
            // If there are too many edges, it is advisable to return the "edge" directly
            // to avoid lengthy enumeration time.
            join = edge.getJoin();
        } else {
            BitSet connectionEdges = graph.findConnectionEdges(leftNodes, rightNodes);
            List<Expression> hashConditions = connectionEdges.stream()
                    .mapToObj(i -> graph.getJoinEdge(i).getJoin().getHashJoinConjuncts())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            List<Expression> otherConditions = connectionEdges.stream()
                    .mapToObj(i -> graph.getJoinEdge(i).getJoin().getHashJoinConjuncts())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            join = edge.getJoin().withJoinConjuncts(hashConditions, otherConditions, null);
        }

        Edge newEdge = new Edge(join, edge.getIndex(), edge.getLeftChildEdges(), edge.getRightChildEdges(),
                edge.getLeftSubtreeNodes(), edge.getRightSubtreeNodes(),
                edge.getLeftRequiredNodes(), edge.getRightRequiredNodes());
        newEdge.addLeftExtendNode(leftNodes);
        newEdge.addRightExtendNode(rightNodes);
        return newEdge;
    }

    private void deriveStats(Edge edge, long leftBitmap, long rightBitmap) {
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

    private double calCost(Edge edge, long leftBitmap, long rightBitmap) {
        long bitmap = LongBitmap.newBitmapUnion(leftBitmap, rightBitmap);
        Preconditions.checkArgument(cacheStats.containsKey(leftBitmap) && cacheStats.containsKey(rightBitmap)
                        && cacheStats.containsKey(bitmap),
                "graph simplifier meet an edge %s that have not been derived stats", edge);
        LogicalJoin<? extends Plan, ? extends Plan> join = edge.getJoin();
        Statistics leftStats = cacheStats.get(leftBitmap);
        Statistics rightStats = cacheStats.get(rightBitmap);
        // Ensure base costs exist for left and right bitmaps. If missing, initialize
        // with a default cost derived from row count to avoid null lookups.
        if (!cacheCost.containsKey(leftBitmap) && leftStats != null) {
            cacheCost.put(leftBitmap, Math.max(1.0, leftStats.getRowCount()));
        }
        if (!cacheCost.containsKey(rightBitmap) && rightStats != null) {
            cacheCost.put(rightBitmap, Math.max(1.0, rightStats.getRowCount()));
        }
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

    // --- Supporting classes ---

    private @Nullable Edge threeLeftJoin(long bitmap1, Edge edge1, long bitmap2, Edge edge2, long bitmap3) {
        // (plan1 edge1 plan2) edge2 plan3
        // if the left and right is overlapping, just return null.
        Preconditions.checkArgument(
                cacheStats.containsKey(bitmap1) && cacheStats.containsKey(bitmap2) && cacheStats.containsKey(bitmap3));
        // construct new Edge
        long newLeft = LongBitmap.newBitmapUnion(bitmap1, bitmap2);
        if (LongBitmap.isOverlap(newLeft, bitmap3)) {
            return null;
        }
        Edge newEdge = constructEdge(newLeft, edge2, bitmap3);

        deriveStats(edge1, bitmap1, bitmap2);
        deriveStats(newEdge, newLeft, bitmap3);

        calCost(edge1, bitmap1, bitmap2);

        return newEdge;
    }

    private @Nullable Edge threeRightJoin(long bitmap1, Edge edge1, long bitmap2,
                                          Edge edge2, long bitmap3) {
        Preconditions.checkArgument(cacheStats.containsKey(bitmap1)
                && cacheStats.containsKey(bitmap2) && cacheStats.containsKey(bitmap3));
        // plan1 edge1 (plan2 edge2 plan3)
        long newRight = LongBitmap.newBitmapUnion(bitmap2, bitmap3);
        if (LongBitmap.isOverlap(bitmap1, newRight)) {
            return null;
        }
        Edge newEdge = constructEdge(bitmap1, edge1, newRight);

        deriveStats(edge2, bitmap2, bitmap3);
        deriveStats(newEdge, bitmap1, newRight);

        calCost(edge1, bitmap2, bitmap3);
        return newEdge;
    }

    private SimplificationStep concretizeSimplificationStep(ProposedSimplificationStep step) {
        Edge e1 = graph.getJoinEdges().get(step.beforeEdgeIdx);
        Edge e2 = graph.getJoinEdges().get(step.afterEdgeIdx);

        SimplificationStep fullStep = new SimplificationStep();
        fullStep.beforeEdgeIdx = step.beforeEdgeIdx;
        fullStep.afterEdgeIdx = step.afterEdgeIdx;
        fullStep.oldEdgeLeft = e2.getLeftExtendedNodes();
        fullStep.oldEdgeRight = e2.getRightExtendedNodes();
        fullStep.newEdgeLeft = fullStep.oldEdgeLeft;
        fullStep.newEdgeRight = fullStep.oldEdgeRight;
        long e1Left = e1.getLeftExtendedNodes();
        long e1Right = e1.getRightExtendedNodes();
        long e2Left = e2.getLeftExtendedNodes();
        long e2Right = e2.getRightExtendedNodes();
        if (LongBitmap.isSubset(e1Left, e2Left) || LongBitmap.isSubset(e2Left, e1Left)
                || LongBitmap.isSubset(e1Right, e2Left) || LongBitmap.isSubset(e2Left, e1Right)) {
            if (!LongBitmap.isOverlap(e2Right, e1Left | e1Right)) {
                fullStep.newEdgeLeft |= (e1Left | e1Right);
            } else {
                fullStep.newEdgeLeft |= (e1Left | e1Right) & ~e2Right;
            }
        } else {
            if (!LongBitmap.isOverlap(e2Left, e1Left | e1Right)) {
                fullStep.newEdgeRight |= (e1Left | e1Right);
            } else {
                fullStep.newEdgeLeft |= (e1Left | e1Right) & ~e2Left;
            }
        }
        return fullStep;
    }

    /**
     * SimplificationResult
     */
    public enum SimplificationResult {
        NO_SIMPLIFICATION_POSSIBLE,
        APPLIED_SIMPLIFICATION,
        APPLIED_NOOP,
        APPLIED_REDO_STEP
    }

    private static class SimplificationStep {
        public int beforeEdgeIdx;
        public int afterEdgeIdx;
        public long oldEdgeLeft;
        public long oldEdgeRight;
        public long newEdgeLeft;
        public long newEdgeRight;
    }

    private static class ProposedSimplificationStep {
        public double benefit = Double.NEGATIVE_INFINITY;
        public int beforeEdgeIdx;
        public int afterEdgeIdx;
    }

    private static class BestSimplification implements Comparable<BestSimplification> {
        public int bestNeighbor = -1;
        public ProposedSimplificationStep bestStep = new ProposedSimplificationStep();
        public boolean isInPriorityQueue = false;

        @Override
        public int compareTo(BestSimplification o) {
            return Double.compare(o.bestStep.benefit, bestStep.benefit);
        }
    }
}
