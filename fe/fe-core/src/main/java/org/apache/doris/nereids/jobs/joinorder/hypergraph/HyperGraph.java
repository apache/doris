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
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The graph is a join graph, whose node is the leaf plan and edge is a join operator.
 * It's used for join ordering
 */
public class HyperGraph {
    private final List<Edge> edges = new ArrayList<>();
    private final List<Node> nodes = new ArrayList<>();
    private final HashSet<Group> nodeSet = new HashSet<>();
    private final HashMap<Slot, Long> slotToNodeMap = new HashMap<>();
    // record all edges that can be placed on the subgraph
    private final Map<Long, BitSet> treeEdgesCache = new HashMap<>();

    // Record the complex project expression for some subgraph
    // e.g. project (a + b)
    //         |-- join(t1.a = t2.b)
    private final HashMap<Long, List<NamedExpression>> complexProject = new HashMap<>();

    public List<Edge> getEdges() {
        return edges;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public long getNodesMap() {
        return LongBitmap.newBitmapBetween(0, nodes.size());
    }

    public Edge getEdge(int index) {
        return edges.get(index);
    }

    public Node getNode(int index) {
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
    public int addNode(Group group) {
        Preconditions.checkArgument(!group.isValidJoinGroup());
        for (Slot slot : group.getLogicalExpression().getPlan().getOutput()) {
            Preconditions.checkArgument(!slotToNodeMap.containsKey(slot));
            slotToNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
        }
        nodeSet.add(group);
        nodes.add(new Node(nodes.size(), group));
        return nodes.size() - 1;
    }

    public boolean isNodeGroup(Group group) {
        return nodeSet.contains(group);
    }

    public HashMap<Long, List<NamedExpression>> getComplexProject() {
        return complexProject;
    }

    /**
     * try to add edge for join group
     *
     * @param group The join group
     */
    public BitSet addEdge(Group group, Pair<BitSet, Long> leftEdgeNodes, Pair<BitSet, Long> rightEdgeNodes) {
        Preconditions.checkArgument(group.isValidJoinGroup());
        LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin) group.getLogicalExpression().getPlan();
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
            LogicalJoin singleJoin = new LogicalJoin<>(join.getJoinType(), entry.getValue().first,
                    entry.getValue().second, JoinHint.NONE, join.getMarkJoinSlotReference(),
                    Lists.newArrayList(join.left(), join.right()));
            Edge edge = new Edge(singleJoin, edges.size(), leftEdgeNodes.first, rightEdgeNodes.first,
                    LongBitmap.newBitmapUnion(leftEdgeNodes.second, rightEdgeNodes.second));
            Pair<Long, Long> ends = entry.getKey();
            edge.setLeftRequiredNodes(ends.first);
            edge.setLeftExtendedNodes(ends.first);
            edge.setRightRequiredNodes(ends.second);
            edge.setRightExtendedNodes(ends.second);
            for (int nodeIndex : LongBitmap.getIterator(edge.getReferenceNodes())) {
                nodes.get(nodeIndex).attachEdge(edge);
            }
            curJoinEdges.set(edge.getIndex());
            edges.add(edge);
        }
        curJoinEdges.stream().forEach(i -> edges.get(i).addCurJoinEdges(curJoinEdges));
        curJoinEdges.stream().forEach(i -> edges.get(i).addCurJoinEdges(curJoinEdges));
        curJoinEdges.stream().forEach(i -> makeConflictRules(edges.get(i)));
        return curJoinEdges;
        // In MySQL, each edge is reversed and store in edges again for reducing the branch miss
        // We don't implement this trick now.
    }

    // Make edge with CD-C algorithm in
    // On the correct and complete enumeration of the core search
    private void makeConflictRules(Edge edgeB) {
        BitSet leftSubTreeEdges = subTreeEdges(edgeB.getLeftChildEdges());
        BitSet rightSubTreeEdges = subTreeEdges(edgeB.getRightChildEdges());
        long leftRequired = edgeB.getLeftRequiredNodes();
        long rightRequired = edgeB.getRightRequiredNodes();

        for (int i = leftSubTreeEdges.nextSetBit(0); i >= 0; i = leftSubTreeEdges.nextSetBit(i + 1)) {
            Edge childA = edges.get(i);
            if (!JoinType.isAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getLeftSubNodes(edges));
            }
            if (!JoinType.isLAssoc(childA.getJoinType(), edgeB.getJoinType())) {
                leftRequired = LongBitmap.newBitmapUnion(leftRequired, childA.getRightSubNodes(edges));
            }
        }

        for (int i = rightSubTreeEdges.nextSetBit(0); i >= 0; i = rightSubTreeEdges.nextSetBit(i + 1)) {
            Edge childA = edges.get(i);
            if (!JoinType.isAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getRightSubNodes(edges));
            }
            if (!JoinType.isRAssoc(edgeB.getJoinType(), childA.getJoinType())) {
                rightRequired = LongBitmap.newBitmapUnion(rightRequired, childA.getLeftSubNodes(edges));
            }
        }
        edgeB.setLeftRequiredNodes(leftRequired);
        edgeB.setRightRequiredNodes(rightRequired);
        edgeB.setLeftExtendedNodes(leftRequired);
        edgeB.setRightExtendedNodes(rightRequired);
    }

    private BitSet subTreeEdge(Edge edge) {
        long subTreeNodes = edge.getSubTreeNodes();
        BitSet subEdges = new BitSet();
        edges.stream()
                .filter(e -> LongBitmap.isSubset(subTreeNodes, e.getReferenceNodes()))
                .forEach(e -> subEdges.set(e.getIndex()));
        return subEdges;
    }

    private BitSet subTreeEdges(BitSet edgeSet) {
        BitSet bitSet = new BitSet();
        edgeSet.stream()
                .mapToObj(i -> subTreeEdge(edges.get(i)))
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
            Pair<BitSet, Long> llEdgesNodes = edges.get(leftEdgeNodes.first.nextSetBit(0)).getLeftEdgeNodes(edges);
            Pair<BitSet, Long> lrEdgesNodes = edges.get(leftEdgeNodes.first.nextSetBit(0)).getRightEdgeNodes(edges);
            return calculateEnds(allNodes, llEdgesNodes, lrEdgesNodes);
        }
        if (right == 0) {
            Preconditions.checkArgument(rightEdgeNodes.first.cardinality() > 0,
                    "the number of the table which expression reference is less 2");
            Pair<BitSet, Long> rlEdgesNodes = edges.get(rightEdgeNodes.first.nextSetBit(0)).getLeftEdgeNodes(edges);
            Pair<BitSet, Long> rrEdgesNodes = edges.get(rightEdgeNodes.first.nextSetBit(0)).getRightEdgeNodes(edges);
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
            for (Edge edge : edges) {
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
        Edge edge = edges.get(edgeIndex);
        if (treeEdgesCache.containsKey(edge.getReferenceNodes())) {
            treeEdgesCache.get(edge.getReferenceNodes()).set(edgeIndex, false);
        }
        updateEdges(edge, edge.getLeftExtendedNodes(), newLeft);
        updateEdges(edge, edge.getRightExtendedNodes(), newRight);
        edges.get(edgeIndex).setLeftExtendedNodes(newLeft);
        edges.get(edgeIndex).setRightExtendedNodes(newRight);
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
        builder.append(String.format("digraph G {  # %d edges\n", edges.size()));
        List<String> graphvisNodes = new ArrayList<>();
        for (Node node : nodes) {
            String nodeName = node.getName();
            // nodeID is used to identify the node with the same name
            String nodeID = nodeName;
            while (graphvisNodes.contains(nodeID)) {
                nodeID += "_";
            }
            builder.append(String.format("  %s [label=\"%s \n rowCount=%.2f\"];\n",
                    nodeID, nodeName, node.getRowCount()));
            graphvisNodes.add(nodeName);
        }
        for (int i = 0; i < edges.size(); i += 1) {
            Edge edge = edges.get(i);
            // TODO: add cardinality to label
            String label = String.format("%.2f", edge.getSelectivity());
            if (edges.get(i).isSimple()) {
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
