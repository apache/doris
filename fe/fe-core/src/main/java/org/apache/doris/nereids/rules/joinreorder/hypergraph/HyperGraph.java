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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

/**
 * The graph is a join graph, whose node is the leaf plan and edge is a join operator.
 * It's used for join ordering
 */
public class HyperGraph {
    List<Edge> edges = new ArrayList<>();
    List<Node> nodes = new ArrayList<>();
    // TODO: add system arg: limit
    Receiver receiver = new Receiver(100);

    public static HyperGraph fromPlan(Plan plan) {
        HyperGraph graph = new HyperGraph();
        graph.buildGraph(plan);
        return graph;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public Edge getEdge(int index) {
        return edges.get(index);
    }

    public Node getNode(int index) {
        return nodes.get(index);
    }

    public Plan getPlan(BitSet bitSet) {
        // In HyperGraph, we assume that each edge is a simple edge at first.
        // Therefore, it only supports to get simple plan
        Preconditions.checkArgument(bitSet.cardinality() == 1);
        int index = bitSet.nextSetBit(0);
        return nodes.get(index).getPlan();
    }

    public Plan toPlan() {
        BitSet bitSet = new BitSet();
        bitSet.set(0, nodes.size());
        return receiver.getBestPlan(bitSet);
    }

    public boolean simplify() {
        GraphSimplifier graphSimplifier = new GraphSimplifier(this);
        graphSimplifier.initFirstStep();
        return graphSimplifier.simplifyGraph(1);
    }

    public boolean emitPlan() {
        return false;
    }

    public boolean optimize() {
        return simplify() && emitPlan();
    }

    private void buildGraph(Plan plan) {
        if ((plan instanceof LogicalProject && plan.child(0) instanceof GroupPlan)
                || plan instanceof GroupPlan) {
            nodes.add(new Node(nodes.size(), plan));
            return;
        }

        LogicalJoin<? extends Plan, ? extends Plan> join;
        if (plan instanceof LogicalProject) {
            LogicalProject<? extends Plan> project = (LogicalProject<? extends Plan>) plan;
            join = (LogicalJoin<? extends Plan, ? extends Plan>) project.child();

            // Handle project
            // Ignore the projection expression just using for selection column.
            // TODO: how to handle Alias and complex project expression
        } else {
            join = (LogicalJoin<? extends Plan, ? extends Plan>) plan;
        }

        // Now we only support inner join with Inside-Project
        // TODO: Other joins can be added according CD-C algorithm
        if (join.getJoinType() != JoinType.INNER_JOIN) {
            Node node = new Node(nodes.size(), plan);
            receiver.addNode(node);
            nodes.add(node);
            return;
        }

        buildGraph(join.left());
        buildGraph(join.right());
        addEdge(join);
    }

    private BitSet findNodes(Set<Slot> slots) {
        BitSet bitSet = new BitSet();
        for (Node node : nodes) {
            for (Slot slot : node.getPlan().getOutput()) {
                if (slots.contains(slot)) {
                    bitSet.set(node.getIndex());
                    break;
                }
            }
        }
        return bitSet;
    }

    private void addEdge(LogicalJoin<? extends Plan, ? extends Plan> join) {
        for (Expression expression : join.getExpressions()) {
            LogicalJoin singleJoin = new LogicalJoin(join.getJoinType(), ImmutableList.of(expression), join.left(),
                    join.right());
            Edge edge = new Edge(singleJoin, edges.size());
            BitSet bitSet = findNodes(expression.getInputSlots());
            assert bitSet.cardinality() == 2 : String.format("HyperGraph has not supported polynomial %s yet",
                    expression);
            int leftIndex = bitSet.nextSetBit(0);
            BitSet left = new BitSet();
            left.set(leftIndex);
            edge.addLeftNode(left);
            int rightIndex = bitSet.nextSetBit(leftIndex + 1);
            BitSet right = new BitSet();
            right.set(rightIndex);
            edge.addRightNode(right);
            edge.getReferenceNodes().stream().forEach(index -> nodes.get(index).attachEdge(edge));
            edges.add(edge);
        }
        // In MySQL, each edge is reversed and store in edges again for reducing the branch miss
        // We don't implement this trick now.
    }

    /**
     * Graph simplifier need to update the edge for join ordering
     *
     * @param edgeIndex The index of updated edge
     * @param newLeft The new left of updated edge
     * @param newRight The new right of update edge
     */
    public void modifyEdge(int edgeIndex, BitSet newLeft, BitSet newRight) {
        // When modify an edge in hyper graph, we need to update the left and right nodes
        // For these nodes that are only in the old edge, we need remove the edge from them
        // For these nodes that are only in the new edge, we need to add the edge to them
        Edge edge = edges.get(edgeIndex);
        updateEdges(edge, edge.getLeft(), newLeft);
        updateEdges(edge, edge.getRight(), newRight);
        edges.get(edgeIndex).setLeft(newLeft);
        edges.get(edgeIndex).setRight(newRight);
    }

    private void updateEdges(Edge edge, BitSet oldNodes, BitSet newNodes) {
        BitSet removeNodes = new BitSet();
        removeNodes.or(oldNodes);
        removeNodes.andNot(newNodes);
        removeNodes.stream().forEach(index -> nodes.get(index).removeEdge(edge));

        BitSet addedNodes = new BitSet();
        addedNodes.or(newNodes);
        addedNodes.andNot(oldNodes);
        addedNodes.stream().forEach(index -> nodes.get(index).attachEdge(edge));
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

                int leftIndex = edge.getLeft().nextSetBit(0);
                int rightIndex = edge.getRight().nextSetBit(0);
                builder.append(String.format("%s -> %s [label=\"%s\"%s]\n", graphvisNodes.get(leftIndex),
                        graphvisNodes.get(rightIndex), label, arrowHead));
            } else {
                // Hyper edge is considered as a tiny virtual node
                builder.append(String.format("e%d [shape=circle, width=.001, label=\"\"]\n", i));

                String leftLabel = "";
                String rightLabel = "";
                if (edge.getLeft().cardinality() == 1) {
                    rightLabel = label;
                } else {
                    leftLabel = label;
                }

                int finalI = i;
                String finalLeftLabel = leftLabel;
                edge.getLeft().stream().forEach(nodeIndex -> {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]\n",
                            graphvisNodes.get(nodeIndex), finalI, finalLeftLabel));
                });

                String finalRightLabel = rightLabel;
                edge.getRight().stream().forEach(nodeIndex -> {
                    builder.append(String.format("%s -> e%d [arrowhead=none, label=\"%s\"]\n",
                            graphvisNodes.get(nodeIndex), finalI, finalRightLabel));
                });
            }
        }
        builder.append("}\n");
        return builder.toString();
    }
}
