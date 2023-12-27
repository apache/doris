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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.FilterEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.JoinEdge;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.StructInfoNode;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The graph is a join graph, whose node is the leaf plan and edge is a join operator.
 * It's used for join ordering
 */
public class HyperGraph {
    private final List<JoinEdge> joinEdges = new ArrayList<>();
    private final List<FilterEdge> filterEdges = new ArrayList<>();
    private final List<AbstractNode> nodes = new ArrayList<>();
    private final HashMap<Slot, Long> slotToNodeMap = new HashMap<>();
    // record all edges that can be placed on the subgraph
    private final Map<Long, BitSet> treeEdgesCache = new HashMap<>();
    private final Set<Slot> finalOutputs;

    // Record the complex project expression for some subgraph
    // e.g. project (a + b)
    //         |-- join(t1.a = t2.b)
    private final HashMap<Long, List<NamedExpression>> complexProject = new HashMap<>();

    HyperGraph(Set<Slot> finalOutputs) {
        this.finalOutputs = ImmutableSet.copyOf(finalOutputs);
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
        if (slotToNodeMap.containsKey(aliasSlot)) {
            return true;
        }
        long bitmap = LongBitmap.newBitmap();
        for (Slot slot : alias.getInputSlots()) {
            bitmap = LongBitmap.or(bitmap, slotToNodeMap.get(slot));
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
        slotToNodeMap.put(aliasSlot, bitmap);
        if (!complexProject.containsKey(bitmap)) {
            complexProject.put(bitmap, new ArrayList<>());
        }
        alias = (Alias) PlanUtils.mergeProjections(complexProject.get(bitmap), Lists.newArrayList(alias)).get(0);

        complexProject.get(bitmap).add(alias);
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
            Preconditions.checkArgument(!slotToNodeMap.containsKey(slot));
            slotToNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
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
            Preconditions.checkArgument(!slotToNodeMap.containsKey(slot));
            slotToNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
        }
        nodes.add(new StructInfoNode(nodes.size(), plan));
        return nodes.size() - 1;
    }

    private int addStructInfoNode(List<HyperGraph> childGraphs) {
        for (Slot slot : childGraphs.get(0).finalOutputs) {
            Preconditions.checkArgument(!slotToNodeMap.containsKey(slot));
            slotToNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
        }
        nodes.add(new StructInfoNode(nodes.size(), childGraphs));
        return nodes.size() - 1;
    }

    public void updateNode(int idx, Group group) {
        Preconditions.checkArgument(nodes.get(idx) instanceof DPhyperNode);
        nodes.set(idx, ((DPhyperNode) nodes.get(idx)).withGroup(group));
    }

    public HashMap<Long, List<NamedExpression>> getComplexProject() {
        return complexProject;
    }

    private void addEdgeOfInfo(JoinEdge edge) {
        long nodeMap = calNodeMap(edge.getInputSlots());
        Preconditions.checkArgument(LongBitmap.getCardinality(nodeMap) > 1,
                "edge must have more than one ends");
        long left = LongBitmap.newBitmap(LongBitmap.nextSetBit(nodeMap, 0));
        long right = LongBitmap.newBitmapDiff(nodeMap, left);
        this.joinEdges.add(new JoinEdge(edge.getJoin(), joinEdges.size(),
                null, null, 0, left, right));
    }

    /**
     * try to add edge for join group
     *
     * @param join The join plan
     */
    private BitSet addJoin(LogicalJoin<?, ?> join,
            Pair<BitSet, Long> leftEdgeNodes, Pair<BitSet, Long> rightEdgeNodes) {
        HashMap<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> conjuncts = new HashMap<>();
        for (Expression expression : join.getHashJoinConjuncts()) {
            // TODO: avoid calling calculateEnds if calNodeMap's results are same
            Pair<Long, Long> ends = calculateEnds(calNodeMap(expression.getInputSlots()), leftEdgeNodes,
                    rightEdgeNodes);
            if (!conjuncts.containsKey(ends)) {
                conjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
            }
            conjuncts.get(ends).first.add(expression);
        }
        for (Expression expression : join.getOtherJoinConjuncts()) {
            Pair<Long, Long> ends = calculateEnds(calNodeMap(expression.getInputSlots()), leftEdgeNodes,
                    rightEdgeNodes);
            if (!conjuncts.containsKey(ends)) {
                conjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
            }
            conjuncts.get(ends).second.add(expression);
        }

        BitSet curJoinEdges = new BitSet();
        for (Map.Entry<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> entry : conjuncts
                .entrySet()) {
            LogicalJoin<?, ?> singleJoin = new LogicalJoin<>(join.getJoinType(), entry.getValue().first,
                    entry.getValue().second, JoinHint.NONE, join.getMarkJoinSlotReference(),
                    Lists.newArrayList(join.left(), join.right()));
            Pair<Long, Long> ends = entry.getKey();
            JoinEdge edge = new JoinEdge(singleJoin, joinEdges.size(), leftEdgeNodes.first, rightEdgeNodes.first,
                    LongBitmap.newBitmapUnion(leftEdgeNodes.second, rightEdgeNodes.second), ends.first, ends.second);
            for (int nodeIndex : LongBitmap.getIterator(edge.getReferenceNodes())) {
                nodes.get(nodeIndex).attachEdge(edge);
            }
            curJoinEdges.set(edge.getIndex());
            joinEdges.add(edge);
        }
        curJoinEdges.stream().forEach(i -> joinEdges.get(i).addCurJoinEdges(curJoinEdges));
        curJoinEdges.stream().forEach(i -> makeJoinConflictRules(joinEdges.get(i)));
        curJoinEdges.stream().forEach(i -> makeFilterConflictRules(joinEdges.get(i)));
        return curJoinEdges;
        // In MySQL, each edge is reversed and store in edges again for reducing the branch miss
        // We don't implement this trick now.
    }

    private BitSet addFilter(LogicalFilter<?> filter, Pair<BitSet, Long> childEdgeNodes) {
        FilterEdge edge = new FilterEdge(filter, filterEdges.size(), childEdgeNodes.first, childEdgeNodes.second,
                childEdgeNodes.second);
        filterEdges.add(edge);
        BitSet bitSet = new BitSet();
        bitSet.set(edge.getIndex());
        return bitSet;
    }

    private void makeFilterConflictRules(JoinEdge joinEdge) {
        long leftSubNodes = joinEdge.getLeftSubNodes(joinEdges);
        long rightSubNodes = joinEdge.getRightSubNodes(joinEdges);
        filterEdges.forEach(e -> {
            if (LongBitmap.isSubset(e.getReferenceNodes(), leftSubNodes)
                    && !PushDownFilterThroughJoin.COULD_PUSH_THROUGH_LEFT.contains(joinEdge.getJoinType())) {
                e.addLeftRejectEdge(joinEdge);
            }
            if (LongBitmap.isSubset(e.getReferenceNodes(), rightSubNodes)
                    && !PushDownFilterThroughJoin.COULD_PUSH_THROUGH_RIGHT.contains(joinEdge.getJoinType())) {
                e.addRightRejectEdge(joinEdge);
            }
        });
    }

    // Make edge with CD-C algorithm in
    // On the correct and complete enumeration of the core search
    private void makeJoinConflictRules(JoinEdge edgeB) {
        BitSet leftSubTreeEdges = subTreeEdges(edgeB.getLeftChildEdges());
        BitSet rightSubTreeEdges = subTreeEdges(edgeB.getRightChildEdges());
        long leftRequired = edgeB.getLeftRequiredNodes();
        long rightRequired = edgeB.getRightRequiredNodes();

        for (int i = leftSubTreeEdges.nextSetBit(0); i >= 0; i = leftSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!JoinType.isAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getLeftSubNodes(joinEdges));
                childA.addLeftRejectEdge(edgeB);
            }
            if (!JoinType.isLAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getRightSubNodes(joinEdges));
                childA.addLeftRejectEdge(edgeB);
            }
        }

        for (int i = rightSubTreeEdges.nextSetBit(0); i >= 0; i = rightSubTreeEdges.nextSetBit(i + 1)) {
            JoinEdge childA = joinEdges.get(i);
            if (!JoinType.isAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getRightSubNodes(joinEdges));
                childA.addRightRejectEdge(edgeB);
            }
            if (!JoinType.isRAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getLeftSubNodes(joinEdges));
                childA.addRightRejectEdge(edgeB);
            }
        }
        edgeB.setLeftExtendedNodes(leftRequired);
        edgeB.setRightExtendedNodes(rightRequired);
    }

    private BitSet subTreeEdge(Edge edge) {
        long subTreeNodes = edge.getSubTreeNodes();
        BitSet subEdges = new BitSet();
        joinEdges.stream()
                .filter(e -> LongBitmap.isSubset(subTreeNodes, e.getReferenceNodes()))
                .forEach(e -> subEdges.set(e.getIndex()));
        return subEdges;
    }

    private BitSet subTreeEdges(BitSet edgeSet) {
        BitSet bitSet = new BitSet();
        edgeSet.stream()
                .mapToObj(i -> subTreeEdge(joinEdges.get(i)))
                .forEach(bitSet::or);
        return bitSet;
    }

    // Try to calculate the ends of an expression.
    // left = ref_nodes \cap left_tree , right = ref_nodes \cap right_tree
    // if left = 0, recursively calculate it in left tree
    private Pair<Long, Long> calculateEnds(long allNodes, Pair<BitSet, Long> leftEdgeNodes,
            Pair<BitSet, Long> rightEdgeNodes) {
        long left = LongBitmap.newBitmapIntersect(allNodes, leftEdgeNodes.second);
        long right = LongBitmap.newBitmapIntersect(allNodes, rightEdgeNodes.second);
        if (left == 0) {
            Preconditions.checkArgument(leftEdgeNodes.first.cardinality() > 0,
                    "the number of the table which expression reference is less 2");
            Pair<BitSet, Long> llEdgesNodes = joinEdges.get(leftEdgeNodes.first.nextSetBit(0)).getLeftEdgeNodes(
                    joinEdges);
            Pair<BitSet, Long> lrEdgesNodes = joinEdges.get(leftEdgeNodes.first.nextSetBit(0)).getRightEdgeNodes(
                    joinEdges);
            return calculateEnds(allNodes, llEdgesNodes, lrEdgesNodes);
        }
        if (right == 0) {
            Preconditions.checkArgument(rightEdgeNodes.first.cardinality() > 0,
                    "the number of the table which expression reference is less 2");
            Pair<BitSet, Long> rlEdgesNodes = joinEdges.get(rightEdgeNodes.first.nextSetBit(0)).getLeftEdgeNodes(
                    joinEdges);
            Pair<BitSet, Long> rrEdgesNodes = joinEdges.get(rightEdgeNodes.first.nextSetBit(0)).getRightEdgeNodes(
                    joinEdges);
            return calculateEnds(allNodes, rlEdgesNodes, rrEdgesNodes);
        }
        return Pair.of(left, right);
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

    private long calNodeMap(Set<Slot> slots) {
        Preconditions.checkArgument(slots.size() != 0);
        long bitmap = LongBitmap.newBitmap();
        for (Slot slot : slots) {
            Preconditions.checkArgument(slotToNodeMap.containsKey(slot));
            bitmap = LongBitmap.or(bitmap, slotToNodeMap.get(slot));
        }
        return bitmap;
    }

    public static List<HyperGraph> toStructInfo(Plan plan) {
        HyperGraph hyperGraph = new HyperGraph(plan.getOutputSet());
        hyperGraph.buildStructInfo(plan);
        return hyperGraph.flatChildren();
    }

    private List<HyperGraph> flatChildren() {
        if (nodes.stream().noneMatch(n -> ((StructInfoNode) n).needToFlat())) {
            return Lists.newArrayList(this);
        }
        List<HyperGraph> res = new ArrayList<>();
        res.add(new HyperGraph(finalOutputs));
        for (AbstractNode node : nodes) {
            res = flatChild((StructInfoNode) node, res);
        }
        for (JoinEdge edge : joinEdges) {
            res.forEach(g -> g.addEdgeOfInfo(edge));
        }
        return res;
    }

    private List<HyperGraph> flatChild(StructInfoNode infoNode, List<HyperGraph> hyperGraphs) {
        if (!infoNode.needToFlat()) {
            hyperGraphs.forEach(g -> g.addStructInfoNode(infoNode.getPlan()));
            return hyperGraphs;
        }
        return hyperGraphs.stream().flatMap(g ->
                infoNode.getGraphs().stream().map(subGraph -> {
                    HyperGraph hyperGraph = new HyperGraph(g.finalOutputs);
                    hyperGraph.addStructInfo(g);
                    hyperGraph.addStructInfo(subGraph);
                    return hyperGraph;
                })
        ).collect(Collectors.toList());
    }

    public static HyperGraph toDPhyperGraph(Group group) {
        HyperGraph hyperGraph = new HyperGraph(group.getLogicalProperties().getOutputSet());
        hyperGraph.buildDPhyperGraph(group.getLogicalExpressions().get(0));
        return hyperGraph;
    }

    // Build Graph for DPhyper
    private Pair<BitSet, Long> buildDPhyperGraph(GroupExpression groupExpression) {
        // process Project
        if (isValidProject(groupExpression.getPlan())) {
            LogicalProject<?> project = (LogicalProject<?>) groupExpression.getPlan();
            Pair<BitSet, Long> res = this.buildDPhyperGraph(groupExpression.child(0).getLogicalExpressions().get(0));
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
            Pair<BitSet, Long> left = this.buildDPhyperGraph(groupExpression.child(0).getLogicalExpressions().get(0));
            Pair<BitSet, Long> right = this.buildDPhyperGraph(groupExpression.child(1).getLogicalExpressions().get(0));
            return Pair.of(this.addJoin(join, left, right),
                    LongBitmap.or(left.second, right.second));
        }

        // process Other Node
        int idx = this.addDPHyperNode(groupExpression.getOwnerGroup());
        return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
    }

    private void addStructInfo(HyperGraph other) {
        int offset = this.getNodes().size();
        other.getNodes().forEach(n -> this.addStructInfoNode(n.getPlan()));
        other.getComplexProject().forEach((t, projectList) ->
                projectList.forEach(e -> this.addAlias((Alias) e, t << offset)));
        other.getJoinEdges().forEach(this::addEdgeOfInfo);
    }

    // Build Graph for matching mv, return join edge set and nodes in this plan
    private Pair<BitSet, Long> buildStructInfo(Plan plan) {
        if (plan instanceof GroupPlan) {
            Group group = ((GroupPlan) plan).getGroup();
            List<HyperGraph> childGraphs = ((GroupPlan) plan).getGroup().getHyperGraphs();
            if (childGraphs.size() != 0) {
                int idx = addStructInfoNode(childGraphs);
                return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
            }
            GroupExpression groupExpression = group.getLogicalExpressions().get(0);
            return buildStructInfo(groupExpression.getPlan()
                    .withChildren(
                            groupExpression.children().stream().map(GroupPlan::new).collect(Collectors.toList())));
        }
        // process Project
        if (isValidProject(plan)) {
            LogicalProject<?> project = (LogicalProject<?>) plan;
            Pair<BitSet, Long> res = this.buildStructInfo(plan.child(0));
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
            Pair<BitSet, Long> left = this.buildStructInfo(plan.child(0));
            Pair<BitSet, Long> right = this.buildStructInfo(plan.child(1));
            return Pair.of(this.addJoin(join, left, right),
                    LongBitmap.or(left.second, right.second));
        }

        if (isValidFilter(plan)) {
            LogicalFilter<?> filter = (LogicalFilter<?>) plan;
            Pair<BitSet, Long> child = this.buildStructInfo(filter.child());
            this.addFilter(filter, child);
            return Pair.of(new BitSet(), child.second);
        }

        // process Other Node
        int idx = this.addStructInfoNode(plan);
        return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
    }

    /**
     * inner join group without mark slot
     */
    public static boolean isValidJoin(Plan plan) {
        if (!(plan instanceof LogicalJoin)) {
            return false;
        }
        LogicalJoin<?, ?> join = (LogicalJoin<?, ?>) plan;
        return join.getJoinType() == JoinType.INNER_JOIN
                && !join.isMarkJoin()
                && !join.getExpressions().isEmpty();
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
        updateEdges(edge, edge.getLeftExtendedNodes(), newLeft);
        updateEdges(edge, edge.getRightExtendedNodes(), newRight);
        joinEdges.get(edgeIndex).setLeftExtendedNodes(newLeft);
        joinEdges.get(edgeIndex).setRightExtendedNodes(newRight);
        if (treeEdgesCache.containsKey(edge.getReferenceNodes())) {
            treeEdgesCache.get(edge.getReferenceNodes()).set(edgeIndex, true);
        }
    }

    private void updateEdges(Edge edge, long oldNodes, long newNodes) {
        long removeNodes = LongBitmap.newBitmapDiff(oldNodes, newNodes);
        LongBitmap.getIterator(removeNodes).forEach(index -> nodes.get(index).removeEdge(edge));

        long addedNodes = LongBitmap.newBitmapDiff(newNodes, oldNodes);
        LongBitmap.getIterator(addedNodes).forEach(index -> nodes.get(index).attachEdge(edge));
    }

    public int edgeSize() {
        return joinEdges.size() + filterEdges.size();
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
        builder.append(String.format("digraph G {  # %d edges\n", joinEdges.size()));
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
            builder.append(String.format("  %s [label=\"%s \n rowCount=%.2f\"];\n",
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
                builder.append(String.format("%s -> %s [label=\"%s\"%s]\n", graphvisNodes.get(leftIndex),
                        graphvisNodes.get(rightIndex), label, arrowHead));
            } else {
                // Hyper edge is considered as a tiny virtual node
                builder.append(String.format("e%d [shape=circle, width=.001, label=\"\"]\n", i));

                String leftLabel = "";
                String rightLabel = "";
                if (LongBitmap.getCardinality(edge.getLeftExtendedNodes()) == 1) {
                    rightLabel = label;
                } else {
                    leftLabel = label;
                }

                int finalI = i;
                String finalLeftLabel = leftLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getLeftExtendedNodes())) {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]\n",
                            graphvisNodes.get(nodeIndex), finalI, finalLeftLabel));
                }

                String finalRightLabel = rightLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getRightExtendedNodes())) {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]\n",
                            graphvisNodes.get(nodeIndex), finalI, finalRightLabel));
                }
            }
        }
        builder.append("}\n");
        return builder.toString();
    }
}
