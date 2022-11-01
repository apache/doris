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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

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

    public Plan toPlan() {
        BitSet bitSet = new BitSet();
        bitSet.set(0, nodes.size());
        return receiver.getBestPlan(bitSet);
    }

    public boolean simplify() {
        return false;
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
            nodes.add(new Node(nodes.size(), plan));
            return;
        }

        buildGraph(join.left());
        buildGraph(join.right());
        addEdge(join);
    }

    private BitSet findNode(Set<Slot> slots) {
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
        Edge edge = new Edge(edges.size(), join);
        for (Expression expression : join.getHashJoinConjuncts()) {
            EqualTo equal = (EqualTo) expression;
            edge.addLeftNode(findNode(equal.left().getInputSlots()));
            edge.addRightNode(findNode(equal.right().getInputSlots()));
        }

        for (Expression expression : join.getOtherJoinConjuncts()) {
            edge.addConstraintNode(findNode(expression.getInputSlots()));
        }

        edge.getReferenceNodes().stream().forEach(index -> nodes.get(index).attachEdge(edge));
        edges.add(edge);
        edges.add(edge.reverse());
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
        builder.append(String.format("digraph G {  # %d edges\n", edges.size() / 2));
        List<String> graphvisNodes = new ArrayList<>();
        for (Node node : nodes) {
            String nodeName = node.getPlan().getType().name() + node.getIndex();
            // nodeID is used to identify the node with the same name
            String nodeID = nodeName;
            while (graphvisNodes.contains(nodeID)) {
                nodeID += "_";
            }
            builder.append(String.format("  %s [label=\"%s\"];\n", nodeID, nodeName));
            graphvisNodes.add(nodeName);
        }
        for (int i = 0; i < edges.size(); i += 2) {
            Edge edge = edges.get(i);
            String label = String.valueOf(edge.getSelectivity());
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
                builder.append(String.format("e%d [shape=circle, width=.001, label=\"\"\n", i));

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
