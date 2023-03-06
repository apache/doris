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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
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
    public boolean addAlias(Alias alias) {
        Slot aliasSlot = alias.toSlot();
        if (slotToNodeMap.containsKey(aliasSlot)) {
            return true;
        }
        long bitmap = LongBitmap.newBitmap();
        for (Slot slot : alias.getInputSlots()) {
            bitmap = LongBitmap.or(bitmap, slotToNodeMap.get(slot));
        }
        slotToNodeMap.put(aliasSlot, bitmap);
        if (!complexProject.containsKey(bitmap)) {
            complexProject.put(bitmap, new ArrayList<>());
        }
        complexProject.get(bitmap).add(alias);
        return true;
    }

    /**
     * add end node to HyperGraph
     *
     * @param group The group that is the end node in graph
     */
    public void addNode(Group group) {
        Preconditions.checkArgument(!group.isJoinGroup());
        for (Slot slot : group.getLogicalExpression().getPlan().getOutput()) {
            Preconditions.checkArgument(!slotToNodeMap.containsKey(slot));
            slotToNodeMap.put(slot, LongBitmap.newBitmap(nodes.size()));
        }
        nodeSet.add(group);
        nodes.add(new Node(nodes.size(), group));
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
    public void addEdge(Group group) {
        Preconditions.checkArgument(group.isJoinGroup());
        LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin) group.getLogicalExpression().getPlan();
        HashMap<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> conjuncts = new HashMap<>();
        for (Expression expression : join.getHashJoinConjuncts()) {
            Pair<Long, Long> ends = findEnds(expression);
            if (!conjuncts.containsKey(ends)) {
                conjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
            }
            conjuncts.get(ends).first.add(expression);
        }
        for (Expression expression : join.getOtherJoinConjuncts()) {
            Pair<Long, Long> ends = findEnds(expression);
            if (!conjuncts.containsKey(ends)) {
                conjuncts.put(ends, Pair.of(new ArrayList<>(), new ArrayList<>()));
            }
            conjuncts.get(ends).second.add(expression);
        }
        for (Map.Entry<Pair<Long, Long>, Pair<List<Expression>, List<Expression>>> entry : conjuncts
                .entrySet()) {
            LogicalJoin singleJoin = new LogicalJoin<>(join.getJoinType(), entry.getValue().first,
                    entry.getValue().second, JoinHint.NONE, join.left(), join.right());
            Edge edge = new Edge(singleJoin, edges.size());
            Pair<Long, Long> ends = entry.getKey();
            edge.setLeft(ends.first);
            edge.setOriginalLeft(ends.first);
            edge.setRight(ends.second);
            edge.setOriginalRight(ends.second);
            for (int nodeIndex : LongBitmap.getIterator(edge.getReferenceNodes())) {
                nodes.get(nodeIndex).attachEdge(edge);
            }
            edges.add(edge);
        }
        // In MySQL, each edge is reversed and store in edges again for reducing the branch miss
        // We don't implement this trick now.
    }

    private int findRoot(List<Integer> parent, int idx) {
        int root = parent.get(idx);
        if (root != idx) {
            root = findRoot(parent, root);
        }
        parent.set(idx, root);
        return root;
    }

    private boolean isConnected(long bitmap, long excludeBitmap) {
        if (LongBitmap.getCardinality(bitmap) == 1) {
            return true;
        }

        // use unionSet to check whether the bitmap is connected
        List<Integer> parent = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            parent.add(i, i);
        }
        for (Edge edge : edges) {
            if (LongBitmap.isOverlap(edge.getLeft(), excludeBitmap)
                    || LongBitmap.isOverlap(edge.getRight(), excludeBitmap)) {
                continue;
            }

            int root = findRoot(parent, LongBitmap.nextSetBit(edge.getLeft(), 0));
            for (int idx : LongBitmap.getIterator(edge.getLeft())) {
                parent.set(idx, root);
            }
            for (int idx : LongBitmap.getIterator(edge.getRight())) {
                parent.set(idx, root);
            }
        }

        int root = findRoot(parent, LongBitmap.nextSetBit(bitmap, 0));
        for (int idx : LongBitmap.getIterator(bitmap)) {
            if (root != findRoot(parent, idx)) {
                return false;
            }
        }
        return true;
    }

    private Pair<Long, Long> findEnds(Expression expression) {
        long bitmap = calNodeMap(expression.getInputSlots());
        int cardinality = LongBitmap.getCardinality(bitmap);
        Preconditions.checkArgument(cardinality > 1);
        for (long subset : LongBitmap.getSubsetIterator(bitmap)) {
            long left = subset;
            long right = LongBitmap.newBitmapDiff(bitmap, left);
            // when the graph without right node has a connected-sub-graph contains left nodes
            // and the graph without left node has a connected-sub-graph contains right nodes.
            // we can generate an edge for this expression
            if (isConnected(left, right) && isConnected(right, left)) {
                return Pair.of(left, right);
            }
        }
        throw new RuntimeException("DPhyper meets unconnected subgraph");
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
        updateEdges(edge, edge.getLeft(), newLeft);
        updateEdges(edge, edge.getRight(), newRight);
        edges.get(edgeIndex).setLeft(newLeft);
        edges.get(edgeIndex).setRight(newRight);
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

                int leftIndex = LongBitmap.lowestOneIndex(edge.getLeft());
                int rightIndex = LongBitmap.lowestOneIndex(edge.getRight());
                builder.append(String.format("%s -> %s [label=\"%s\"%s]\n", graphvisNodes.get(leftIndex),
                        graphvisNodes.get(rightIndex), label, arrowHead));
            } else {
                // Hyper edge is considered as a tiny virtual node
                builder.append(String.format("e%d [shape=circle, width=.001, label=\"\"]\n", i));

                String leftLabel = "";
                String rightLabel = "";
                if (LongBitmap.getCardinality(edge.getLeft()) == 1) {
                    rightLabel = label;
                } else {
                    leftLabel = label;
                }

                int finalI = i;
                String finalLeftLabel = leftLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getLeft())) {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]\n",
                            graphvisNodes.get(nodeIndex), finalI, finalLeftLabel));
                }

                String finalRightLabel = rightLabel;
                for (int nodeIndex : LongBitmap.getIterator(edge.getRight())) {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]\n",
                            graphvisNodes.get(nodeIndex), finalI, finalRightLabel));
                }
            }
        }
        builder.append("}\n");
        return builder.toString();
    }
}
