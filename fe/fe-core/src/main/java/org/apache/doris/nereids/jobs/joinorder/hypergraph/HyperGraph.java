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

import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    private final HashMap<Long, NamedExpression> complexProject = new HashMap<>();

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
     * e.g. a = b
     * project((c + d) as b)
     * Note if the alias if the alias only associated with one endNode,
     * e.g. a = b
     * project((c + 1) as b)
     * we need to replace the group of that node with this project group.
     *
     * @param alias The alias Expression in project Operator
     */
    public boolean addAlias(Alias alias, Group group) {
        long bitmap = LongBitmap.newBitmap();
        for (Slot slot : alias.getInputSlots()) {
            bitmap = LongBitmap.or(bitmap, slotToNodeMap.get(slot));
        }
        Slot aliasSlot = alias.toSlot();
        Preconditions.checkArgument(!slotToNodeMap.containsKey(aliasSlot));
        slotToNodeMap.put(aliasSlot, bitmap);
        if (LongBitmap.getCardinality(bitmap) == 1) {
            int index = LongBitmap.lowestOneIndex(bitmap);
            nodeSet.remove(nodes.get(index).getGroup());
            nodeSet.add(group);
            nodes.get(index).replaceGroupWith(group);
            return false;
        }
        complexProject.put(bitmap, alias);
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

    public HashMap<Long, NamedExpression> getComplexProject() {
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
        for (Expression expression : join.getExpressions()) {
            LogicalJoin singleJoin = new LogicalJoin(join.getJoinType(), ImmutableList.of(expression), join.left(),
                    join.right());
            Edge edge = new Edge(singleJoin, edges.size());
            Preconditions.checkArgument(expression.children().size() == 2);

            long left = calNodeMap(expression.child(0).getInputSlots());
            edge.setLeft(left);
            long right = calNodeMap(expression.child(1).getInputSlots());
            edge.setRight(right);
            for (int nodeIndex : LongBitmap.getIterator(edge.getReferenceNodes())) {
                nodes.get(nodeIndex).attachEdge(edge);
            }
            edges.add(edge);
        }
        // In MySQL, each edge is reversed and store in edges again for reducing the branch miss
        // We don't implement this trick now.
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
