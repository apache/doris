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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.FilterEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The graph is a join graph, whose node is the leaf plan and edge is a join operator.
 * It's used for join ordering
 */
public class HyperGraph {
    // record all edges that can be placed on the subgraph
    private final Map<Long, BitSet> treeEdgesCache = new LinkedHashMap<>();
    private final List<JoinEdge> joinEdges;
    private final List<FilterEdge> filterEdges;
    private final List<AbstractNode> nodes;
    private final List<NamedExpression> finalProjects;

    // Record the complex project expression for some subgraph
    // e.g. project (a + b)
    //         |-- join(t1.a = t2.b)
    private final Map<Long, List<NamedExpression>> complexProject;

    private final CascadesContext ctx;

    HyperGraph(List<NamedExpression> finalProjects, List<JoinEdge> joinEdges, List<AbstractNode> nodes, List<FilterEdge> filterEdges,
            Map<Long, List<NamedExpression>> complexProject, CascadesContext ctx) {
        this.finalProjects = ImmutableList.copyOf(finalProjects);
        this.joinEdges = ImmutableList.copyOf(joinEdges);
        this.nodes = ImmutableList.copyOf(nodes);
        this.complexProject = ImmutableMap.copyOf(complexProject);
        this.filterEdges = ImmutableList.copyOf(filterEdges);
        this.ctx = ctx;
    }

    public CascadesContext getCtx() {
        return ctx;
    }

    public List<NamedExpression> getFinalProjects() {
        return finalProjects;
    }

    public List<JoinEdge> getJoinEdges() {
        return joinEdges;
    }

    public List<FilterEdge> getFilterEdges() {
        return filterEdges;
    }

    public List<AbstractNode> getNodes() {
        return nodes;
    }

    public long getNodesMap() {
        return LongBitmap.newBitmapBetween(0, nodes.size());
    }

    public JoinEdge getJoinEdge(int index) {
        return joinEdges.get(index);
    }

    public FilterEdge getFilterEdge(int index) {
        return filterEdges.get(index);
    }

    public AbstractNode getNode(int index) {
        return nodes.get(index);
    }

    public Map<Long, List<NamedExpression>> getComplexProject() {
        return complexProject;
    }

    public BitSet getEdgesInOperator(long left, long right) {
        BitSet operatorEdgesMap = new BitSet();
        operatorEdgesMap.or(getEdgesInTree(LongBitmap.or(left, right)));
        operatorEdgesMap.andNot(getEdgesInTree(left));
        operatorEdgesMap.andNot(getEdgesInTree(right));
        return operatorEdgesMap;
    }

    /**
     * Returns all edges in the tree
     */
    public BitSet getEdgesInTree(long treeNodesMap) {
        if (!treeEdgesCache.containsKey(treeNodesMap)) {
            BitSet edgesMap = new BitSet();
            for (Edge edge : joinEdges) {
                if (LongBitmap.isSubset(edge.getReferenceNodes(), treeNodesMap)) {
                    edgesMap.set(edge.getIndex());
                }
            }
            treeEdgesCache.put(treeNodesMap, edgesMap);
        }
        return treeEdgesCache.get(treeNodesMap);
    }

    /**
     * Graph simplifier need to update the edge for join ordering
     *
     * @param edgeIndex The index of updated edge
     * @param newLeft The new left of updated edge
     * @param newRight The new right of update edge
     */
    public void modifyEdge(int edgeIndex, long newLeft, long newRight) {
        // When modify an edge in hyper graph, we need to update the left and right nodes
        // For these nodes that are only in the old edge, we need remove the edge from them
        // For these nodes that are only in the new edge, we need to add the edge to them
        Edge edge = joinEdges.get(edgeIndex);
        if (treeEdgesCache.containsKey(edge.getReferenceNodes())) {
            treeEdgesCache.get(edge.getReferenceNodes()).set(edgeIndex, false);
        }
        joinEdges.get(edgeIndex).setLeftExtendedNodes(newLeft);
        joinEdges.get(edgeIndex).setRightExtendedNodes(newRight);
        if (treeEdgesCache.containsKey(edge.getReferenceNodes())) {
            treeEdgesCache.get(edge.getReferenceNodes()).set(edgeIndex, true);
        }

    }

    /**
     * inner join group without mark slot
     */
    public static boolean isValidJoinForStructInfo(Plan plan) {
        if (!(plan instanceof LogicalJoin)) {
            return false;
        }

        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
        return !join.isMarkJoin()
                && !join.getExpressions().isEmpty();
    }

    public static boolean isValidFilter(Plan plan) {
        return plan instanceof LogicalFilter;
    }

    /**
     * the project with alias and slot
     */
    public static boolean isValidProject(Plan plan) {
        if (!(plan instanceof LogicalProject)) {
            return false;
        }
        return ((LogicalProject<? extends Plan>) plan).getProjects().stream()
                .allMatch(e -> e instanceof Slot || e instanceof Alias);
    }

    /**
     * inner join group without mark slot
     */
    public static boolean isValidJoin(Plan plan) {
        if (!(plan instanceof LogicalJoin)) {
            return false;
        }
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
        return !join.isMarkJoin()
//                && !join.getExpressions().isEmpty()
                && !join.isLeadingJoin();
    }

    /**
     * For the given hyperGraph, make a textual representation in the form
     * of a dotty graph. You can save this to a file and then use Graphviz
     * to render this it a graphical representation of the hyperGraph for
     * easier debugging, e.g. like this:
     * <p>
     * dot -Tps graph.dot > graph.ps
     * display graph.ps
     */
    public String toDottyHyperGraph() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("digraph G {  # %d edges%n", joinEdges.size()));
        List<String> graphvisNodes = new ArrayList<>();
        for (AbstractNode node : nodes) {
            String nodeName = node.getName();
            // nodeID is used to identify the node with the same name
            String nodeID = nodeName;
            while (graphvisNodes.contains(nodeID)) {
                nodeID += "_";
            }
            double rowCount = (node instanceof DPhyperNode)
                    ? ((DPhyperNode) node).getRowCount()
                    : -1;
            builder.append(String.format("  %s [label=\"%s %n rowCount=%.2f\"];%n",
                    nodeID, nodeName, rowCount));
            graphvisNodes.add(nodeName);
        }
        for (int i = 0; i < joinEdges.size(); i += 1) {
            JoinEdge edge = joinEdges.get(i);
            // TODO: add cardinality to label
            String label = String.format("%.2f", edge.getSelectivity());
            if (joinEdges.get(i).isSimple()) {
                String arrowHead = "";
                if (edge.getJoin().getJoinType() == JoinType.INNER_JOIN) {
                    arrowHead = ",arrowhead=none";
                }

                int leftIndex = LongBitmap.lowestOneIndex(edge.getLeftExtendedNodes());
                int rightIndex = LongBitmap.lowestOneIndex(edge.getRightExtendedNodes());
                builder.append(String.format("%s -> %s [label=\"%s\"%s]%n", graphvisNodes.get(leftIndex),
                        graphvisNodes.get(rightIndex), label, arrowHead));
            } else {
                // Hyper edge is considered as a tiny virtual node
                builder.append(String.format("e%d [shape=circle, width=.001, label=\"\"]%n", i));

                String leftLabel = "";
                String rightLabel = "";
                if (LongBitmap.getCardinality(edge.getLeftExtendedNodes()) == 1) {
                    rightLabel = label;
                } else {
                    leftLabel = label;
                }

                String finalLeftLabel = leftLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getLeftExtendedNodes())) {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]%n",
                            graphvisNodes.get(nodeIndex), i, finalLeftLabel));
                }

                String finalRightLabel = rightLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getRightExtendedNodes())) {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]%n",
                            graphvisNodes.get(nodeIndex), i, finalRightLabel));
                }
            }
        }
        builder.append("}\n");
        return builder.toString();
    }

    public static HyperGraph.Builder builderForDPhyper(Group group, CascadesContext ctx) {
        return new HyperGraph.Builder().buildHyperGraphForDPhyper(group, ctx);
    }

    public static HyperGraph.Builder builderForMv(Plan plan) {
        return new HyperGraph.Builder().buildHyperGraphForMv(plan);
    }

    /**
     * map output to requires output and construct named expressions
     */
    public @Nullable List<NamedExpression> getNamedExpressions(
            long nodeMap, Set<Slot> outputSet, Set<Slot> requireOutputs) {
        List<NamedExpression> output = new ArrayList<>();
        List<NamedExpression> projects = getComplexProject().get(nodeMap);
        if (projects == null) {
            return null;
        }
        for (Slot slot : requireOutputs) {
            if (outputSet.contains(slot)) {
                output.add(slot);
            } else {
                Optional<NamedExpression> expr = projects.stream()
                        .filter(p -> p.toSlot().equals(slot))
                        .findFirst();
                if (!expr.isPresent()) {
                    return null;
                }
                // TODO: consider cascades alias
                if (!outputSet.containsAll(expr.get().getInputSlots())) {
                    return null;
                }
                output.add(expr.get());
            }
        }
        return output;
    }

    /**
     * Builder of HyperGraph
     */
    public static class Builder {
        private final List<JoinEdge> joinEdges = new ArrayList<>();
        private final List<FilterEdge> filterEdges = new ArrayList<>();
        private final List<AbstractNode> nodes = new ArrayList<>();

        // These hyperGraphs should be replaced nodes when building all
        private final Map<Long, List<HyperGraph>> replacedHyperGraphs = new LinkedHashMap<>();
        // Key: hyper node's output slots, the slots come from simple slotReference or Alias
        // value: the long bitmap value representing hyper node(simple node or joined nodes)
        // addDPHyperNode method add slots from simple node
        // addAlias method add slots from both simple node and joined nodes, depending on the alias's input slots
        private final HashMap<Slot, Long> slotToHyperNodeMap = new LinkedHashMap<>();
        // key: the long bitmap value representing hyper node(simple node or joined nodes)
        // value: hyper node's complex project outputs coming from Alias in LogicalProject node
        private final Map<Long, List<NamedExpression>> complexProject = new LinkedHashMap<>();
        private Set<Slot> finalOutputs;

        private CascadesContext ctx;

        private ExpressionRewriteContext expressionRewriteCtx;

        private Map<Slot, Expression> aliasReplaceMap = new LinkedHashMap<>();

        private List<NamedExpression> finalProjects = new ArrayList<>();

        private long outerJoinedNoes = 0;

        public List<AbstractNode> getNodes() {
            return nodes;
        }

        private HyperGraph.Builder buildHyperGraphForDPhyper(Group group, CascadesContext ctx) {
            this.ctx = ctx;
            this.expressionRewriteCtx = new ExpressionRewriteContext(ctx);
            finalOutputs = group.getLogicalProperties().getOutputSet();
            this.buildForDPhyper(group.getLogicalExpression());
            for (Slot slot : finalOutputs) {
                Expression expression = aliasReplaceMap.get(slot);
                if (expression != null) {
                    finalProjects.add(new Alias(slot.getExprId(), expression, slot.getName()));
                } else {
                    finalProjects.add(slot);
                }
            }
            complexProject.clear();
            return this;
        }

        private HyperGraph.Builder buildHyperGraphForMv(Plan plan) {
            finalOutputs = plan.getOutputSet();
            this.buildForMv(plan);
            return this;
        }

        public HyperGraph build() {
            return new HyperGraph(finalProjects, joinEdges, nodes, filterEdges, complexProject, ctx);
        }

        public void updateNode(int idx, Group group) {
            Preconditions.checkArgument(nodes.get(idx) instanceof DPhyperNode);
            nodes.set(idx, ((DPhyperNode) nodes.get(idx)).withGroup(group));
        }

        /**
         * Build Graph for DPhyper in bottom-up way, it means build node first, then edges
         *
         * @param groupExpression the groupExpression contains root plan node of this join cluster
         * @return Pair<BitSet, Long>, BitSet is the latest join edges' index in joinEdges, Long is all the subtree
         * nodes of the latest join edges' index
         */
        private Pair<BitSet, Long> buildForDPhyper(GroupExpression groupExpression) {
            // process Project
            if (isValidProject(groupExpression.getPlan())) {
                LogicalProject<?> project = (LogicalProject<?>) groupExpression.getPlan();
                Pair<BitSet, Long> res = this.buildForDPhyper(groupExpression.child(0).getLogicalExpressions().get(0));
                for (NamedExpression expr : project.getProjects()) {
                    if (expr instanceof Alias) {
                        this.addAlias((Alias) expr, res.second);
                    }
                }
                return res;
            }

            // process Join
            if (isValidJoin(groupExpression.getPlan())) {
                LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) groupExpression.getPlan();
                Pair<BitSet, Long> left =
                        this.buildForDPhyper(groupExpression.child(0).getLogicalExpressions().get(0));
                Pair<BitSet, Long> right =
                        this.buildForDPhyper(groupExpression.child(1).getLogicalExpressions().get(0));
                return Pair.of(this.addJoin(join, left, right),
                        LongBitmap.or(left.second, right.second));
            }

            // process Other Node
            int idx = this.addDPHyperNode(groupExpression.getOwnerGroup());
            return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
        }

        // Build Graph for matching mv, return join edge set and nodes in this plan
        private Pair<BitSet, Long> buildForMv(Plan plan) {
            if (plan instanceof GroupPlan) {
                Group group = ((GroupPlan) plan).getGroup();
                GroupExpression groupExpression = group.getLogicalExpressions().get(0);
                return buildForMv(groupExpression.getPlan()
                        .withChildren(groupExpression.children().stream()
                                .map(Group::getGroupPlan)
                                .collect(Collectors.toList())));
            }
            // process Project
            if (isValidProject(plan)) {
                LogicalProject<?> project = (LogicalProject<?>) plan;
                Pair<BitSet, Long> res = this.buildForMv(plan.child(0));
                for (NamedExpression expr : project.getProjects()) {
                    if (expr instanceof Alias) {
                        this.addAlias((Alias) expr, res.second);
                    }
                }
                return res;
            }

            // process Join
            if (isValidJoinForStructInfo(plan)) {
                LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
                Pair<BitSet, Long> left = this.buildForMv(plan.child(0));
                Pair<BitSet, Long> right = this.buildForMv(plan.child(1));
                return Pair.of(this.addJoin(join, left, right),
                        LongBitmap.or(left.second, right.second));
            }

            if (isValidFilter(plan)) {
                LogicalFilter<?> filter = (LogicalFilter<?>) plan;
                Pair<BitSet, Long> child = this.buildForMv(filter.child());
                this.addFilter(filter, child);
                return Pair.of(child.first, child.second);
            }
            if (plan instanceof LogicalLimit && ((LogicalLimit<?>) plan).getPhase() == LimitPhase.LOCAL) {
                return this.buildForMv(plan.child(0));
            }
            // process Other Node
            int idx = this.addStructInfoNode(plan);
            return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
        }

        /**
         * Store the relation between Alias Slot and Original Slot and its expression
         * e.g.,
         * a = b
         * |--- project((c + d) as b)
         * <p>
         * a = b
         * |--- project((c + 1) as b)
         *
         * @param alias The alias Expression in project Operator
         */
        public boolean addAlias(Alias alias, long subTreeNodes) {
            Slot aliasSlot = alias.toSlot();
            if (slotToHyperNodeMap.containsKey(aliasSlot)) {
                return true;
            }
            long bitmap = LongBitmap.newBitmap();
            // find the nodes produce the input slots, the nodes is subset of subTreeNodes
            for (Slot slot : alias.getInputSlots()) {
                bitmap = LongBitmap.or(bitmap, slotToHyperNodeMap.get(slot));
            }
            // The case hit when there are some constant aliases such as:
            // select * from t1 join (
            //          select *, 1 as b1 from t2)
            //              on t1.b = b1
            // just reference them all for this slot
            if (bitmap == 0) {
                bitmap = subTreeNodes;
            }
            Preconditions.checkArgument(bitmap > 0, "slot must belong to some table");
            slotToHyperNodeMap.put(aliasSlot, bitmap);
            alias = (Alias) ExpressionUtils.replaceNameExpression(alias, aliasReplaceMap);
            aliasReplaceMap.put(aliasSlot, alias.child());
            return true;
        }

        /**
         * add end node to HyperGraph
         *
         * @param group The group that is the end node in graph
         * @return return the node index
         */
        private int addDPHyperNode(Group group) {
            for (Slot slot : group.getLogicalExpression().getPlan().getOutput()) {
                Preconditions.checkArgument(!slotToHyperNodeMap.containsKey(slot));
                slotToHyperNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
            }
            nodes.add(new DPhyperNode(nodes.size(), group));
            return nodes.size() - 1;
        }

        /**
         * add end node to HyperGraph
         *
         * @param plan The plan that is the end node in graph
         * @return return the node index
         */
        private int addStructInfoNode(Plan plan) {
            for (Slot slot : plan.getOutput()) {
                Preconditions.checkArgument(!slotToHyperNodeMap.containsKey(slot));
                slotToHyperNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
            }
            nodes.add(new StructInfoNode(nodes.size(), plan));
            return nodes.size() - 1;
        }

        private long calReferencedNodesBySlots(Set<Slot> slots) {
            Preconditions.checkArgument(slots.size() != 0);
            long bitmap = LongBitmap.newBitmap();
            for (Slot slot : slots) {
                Preconditions.checkArgument(slotToHyperNodeMap.containsKey(slot));
                bitmap = LongBitmap.or(bitmap, slotToHyperNodeMap.get(slot));
            }
            return bitmap;
        }

        /**
         * try to add edge for join group ( only works for inner join!!! )
         * 1. consider original join node's conjuncts:
         *      inner join conjuncts: t1.a = t2.a and t1.b = t3.b
         * 2. extract join conjuncts:
         *      nodeToConjuncts contains 2 elements : (t1 and t2, t1.a = t2.a), (t1 and t3, t1.b = t3.b)
         * 3. based on nodeToConjuncts, create 2 new join node and edge:
         *      inner join1 : t1.a = t2.a
         *      inner join2 : t1.b = t3.b
         *
         * @param join The join plan
         * @param leftChildEdgeNodes first level left child edges and all nodes in the left tree
         * @param rightChildEdgeNodes first level right child edges and all nodes in the right tree
         * @return BitSet indicate the join edges' index in joinEdges produced by @param join
         */
        private BitSet addJoin(LogicalJoin<?, ?> join,
                Pair<BitSet, Long> leftChildEdgeNodes, Pair<BitSet, Long> rightChildEdgeNodes) {
            // the join conjuncts may only use subset of leftNodes and rightNodes
            // extract inner join conjuncts to requiredNodesToConjuncts map
            // requiredNodesToConjuncts:
            // Pair<Long, Long> -> [leftNodes, rightNodes] only contains conjuncts' input slots referenced nodes
            // Pair<List<Expression>, List<Expression>> -> [hashConjuncts, otherConjuncts]
            // TODO: need handle mark conjuncts here
            Map<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> requiredNodesToConjuncts = new LinkedHashMap<>();
            if (join.getJoinType().isInnerJoin()) {
                Pair<Long, Long> ends = calculateEnds(calReferencedNodesBySlots(join.getInputSlots()),
                        leftChildEdgeNodes, rightChildEdgeNodes);
//                requiredNodesToConjuncts.put(ends, Pair.of(join.getHashJoinConjuncts(), join.getOtherJoinConjuncts()));
                if (LongBitmap.isOverlap(outerJoinedNoes, ends.first) || LongBitmap.isOverlap(outerJoinedNoes, ends.second)) {
                    requiredNodesToConjuncts.put(ends, Pair.of(join.getHashJoinConjuncts(), join.getOtherJoinConjuncts()));
                } else {
                    for (Expression expression : join.getHashJoinConjuncts()) {
                        // TODO: avoid calling calculateEnds if calNodeMap's results are same
                        // split join conjunct's input slots into left child and right child group
                        // ends.first is left child nodes, ends.second is right child nodes
                        ends = calculateEnds(calReferencedNodesBySlots(expression.getInputSlots()),
                                leftChildEdgeNodes, rightChildEdgeNodes);
                        if (!requiredNodesToConjuncts.containsKey(ends)) {
                            requiredNodesToConjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
                        }
                        requiredNodesToConjuncts.get(ends).first.add(expression);
                    }
                    for (Expression expression : join.getOtherJoinConjuncts()) {
                        ends = calculateEnds(calReferencedNodesBySlots(expression.getInputSlots()),
                                leftChildEdgeNodes, rightChildEdgeNodes);
                        if (!requiredNodesToConjuncts.containsKey(ends)) {
                            requiredNodesToConjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
                        }
                        requiredNodesToConjuncts.get(ends).second.add(expression);
                    }
                }
            } else if (join.getJoinType().isCrossJoin()) {
                return addCrossJoin(join, leftChildEdgeNodes, rightChildEdgeNodes);
            } else {
                outerJoinedNoes = LongBitmap.or(leftChildEdgeNodes.second, rightChildEdgeNodes.second);
                Pair<Long, Long> ends = calculateEnds(calReferencedNodesBySlots(join.getInputSlots()),
                        leftChildEdgeNodes, rightChildEdgeNodes);
                requiredNodesToConjuncts.put(ends, Pair.of(join.getHashJoinConjuncts(), join.getOtherJoinConjuncts()));
            }

            BitSet curJoinEdges = new BitSet();
            Set<Slot> leftInputSlots = ImmutableSet.copyOf(
                    Sets.intersection(join.getInputSlots(), join.left().getOutputSet()));
            Set<Slot> rightInputSlots = ImmutableSet.copyOf(
                    Sets.intersection(join.getInputSlots(), join.right().getOutputSet()));
            for (Map.Entry<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> entry : requiredNodesToConjuncts
                    .entrySet()) {
                List<Expression> newHashConjuncts = ExpressionUtils.replace(entry.getValue().first, aliasReplaceMap);
                List<Expression> newOtherConjuncts = ExpressionUtils.replace(entry.getValue().second, aliasReplaceMap);
                LogicalJoin<?, ?> singleJoin = new LogicalJoin<>(join.getJoinType(), newHashConjuncts,
                        newOtherConjuncts,
                        new DistributeHint(DistributeType.NONE), join.getMarkJoinSlotReference(),
                        Lists.newArrayList(join.left(), join.right()), null);
                Pair<Long, Long> ends = entry.getKey();
                // we can see JoinEdge contains following info
                // 1. LogicalJoin operator
                // 2. edgeIndex in joinEdges
                // 3. first level left child edge and right child edge
                // 4. all subtree nodes
                // 5. left and right required nodes(produced by join conjuncts)
                // 6. left and right input slots(used for struct info)
                JoinEdge edge = new JoinEdge(singleJoin, joinEdges.size(), leftChildEdgeNodes.first,
                        rightChildEdgeNodes.first, leftChildEdgeNodes.second,
                        rightChildEdgeNodes.second, ends.first, ends.second, leftInputSlots, rightInputSlots);
                curJoinEdges.set(edge.getIndex());
                joinEdges.add(edge);
            }
            curJoinEdges.stream().forEach(i -> joinEdges.get(i).addCurJoinEdges(curJoinEdges));
            curJoinEdges.stream().forEach(i -> ConflictRulesMaker.makeJoinConflictRules(joinEdges.get(i), joinEdges, expressionRewriteCtx));
            curJoinEdges.stream().forEach(i ->
                    ConflictRulesMaker.makeFilterConflictRules(joinEdges.get(i), joinEdges, filterEdges));
            return curJoinEdges;
            // In MySQL, each edge is reversed and store in edges again for reducing the branch miss
            // We don't implement this trick now.
        }

        private BitSet addCrossJoin(LogicalJoin<?, ?> join,
                               Pair<BitSet, Long> leftChildEdgeNodes, Pair<BitSet, Long> rightChildEdgeNodes) {
            BitSet curJoinEdges = new BitSet();
            Pair<Long, Long> ends = calculateEnds(calReferencedNodesBySlots(Sets.union(join.left().getOutputSet(), join.right().getOutputSet())),
                    leftChildEdgeNodes, rightChildEdgeNodes);
            JoinEdge edge = new JoinEdge(join, joinEdges.size(), leftChildEdgeNodes.first,
                    rightChildEdgeNodes.first, leftChildEdgeNodes.second,
                    rightChildEdgeNodes.second, ends.first, ends.second, join.left().getOutputSet(), join.right().getOutputSet());
            curJoinEdges.set(edge.getIndex());
            joinEdges.add(edge);
            curJoinEdges.stream().forEach(i -> joinEdges.get(i).addCurJoinEdges(curJoinEdges));
            curJoinEdges.stream().forEach(i -> ConflictRulesMaker.makeJoinConflictRules(joinEdges.get(i), joinEdges, expressionRewriteCtx));
            curJoinEdges.stream().forEach(i ->
                    ConflictRulesMaker.makeFilterConflictRules(joinEdges.get(i), joinEdges, filterEdges));
            return curJoinEdges;
        }

        private BitSet addFilter(LogicalFilter<?> filter, Pair<BitSet, Long> childEdgeNodes) {
            FilterEdge edge = new FilterEdge(filter, filterEdges.size(), childEdgeNodes.first, childEdgeNodes.second,
                    childEdgeNodes.second);
            filterEdges.add(edge);
            BitSet bitSet = new BitSet();
            bitSet.set(edge.getIndex());
            return bitSet;
        }

        // Try to calculate the ends of an expression.
        // left = ref_nodes \cap left_tree , right = ref_nodes \cap right_tree
        // if left = 0, recursively calculate it in left tree
        private Pair<Long, Long> calculateEnds(long allNodes, Pair<BitSet, Long> leftEdgeNodes,
                Pair<BitSet, Long> rightEdgeNodes) {
            long left = LongBitmap.newBitmapIntersect(allNodes, leftEdgeNodes.second);
            long right = LongBitmap.newBitmapIntersect(allNodes, rightEdgeNodes.second);
            // handle degenerate predicates
            if (left == 0) {
                left = leftEdgeNodes.second;
            }
            if (right == 0) {
                right = rightEdgeNodes.second;
            }
            return Pair.of(left, right);
        }
    }
}
