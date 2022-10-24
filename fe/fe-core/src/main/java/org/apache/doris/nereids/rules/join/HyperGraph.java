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

package org.apache.doris.nereids.rules.join;

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hyper Graph to present Join Cluster.
 */
public class HyperGraph {
    private final List<Plan> plans = Lists.newArrayList();
    private final List<Node> nodes = Lists.newArrayList();

    private final List<JoinType> joinTypes = Lists.newArrayList();
    private final List<Expression> conditions = Lists.newArrayList();
    private final List<Edge> edges = Lists.newArrayList();

    public List<Plan> plans() {
        return plans;
    }

    public Plan plan(int index) {
        return plans.get(index);
    }

    public List<Node> nodes() {
        return nodes;
    }

    public Node node(int index) {
        return nodes.get(index);
    }

    public List<JoinType> joinTypes() {
        return joinTypes;
    }

    public JoinType joinTypes(int index) {
        return joinTypes.get(index);
    }

    public List<Expression> conditions() {
        return conditions;
    }

    public List<Expression> conditions(List<Integer> indexs) {
        return indexs.stream().map(conditions::get).collect(Collectors.toList());
    }

    public List<Edge> edges() {
        return edges;
    }

    public List<Edge> edges(List<Integer> indexs) {
        return indexs.stream().map(edges::get).collect(Collectors.toList());
    }

    public Edge edge(int index) {
        return edges.get(index);
    }

    /**
     * new graph from Join Cluster.
     * Support Join.
     * TODO: support project/filter inside.
     */
    public static HyperGraph from(LogicalJoin<? extends Plan, ? extends Plan> join) {
        HyperGraph graph = new HyperGraph();

        graph.buildGraph(join);

        return graph;
    }

    private void buildGraph(Plan plan) {
        Preconditions.checkArgument(plan instanceof LogicalJoin);
        LogicalJoin<? extends Plan, ? extends Plan> join = (LogicalJoin<? extends Plan, ? extends Plan>) plan;

        if (!(join.left() instanceof GroupPlan)) {
            // recursively handle left.
            Preconditions.checkState(join.left() instanceof LogicalJoin);
            buildGraph(join.left());
        } else {
            // meet end point, add node in graph.
            createNode(join.left());
        }
        if (!(join.right() instanceof GroupPlan)) {
            Preconditions.checkState(join.right() instanceof LogicalJoin);
            buildGraph(join.right());
        } else {
            createNode(join.right());
        }

        // TODO: current just consider Equal.
        Preconditions.checkArgument(!join.getHashJoinConjuncts().isEmpty());
        for (Expression expr : join.getHashJoinConjuncts()) {
            // TODO: consider OR
            Preconditions.checkState(expr instanceof EqualTo);
            EqualTo equal = (EqualTo) expr;
            Node leftNode = findNode(equal.left().getInputSlots());
            Node rightNode = findNode(equal.right().getInputSlots());

            createEdge(join.getJoinType(), leftNode, rightNode, equal);
        }
    }

    private void createNode(Plan plan) {
        int newId = nodes.size();
        Node newNode = new Node(newId);
        nodes.add(newNode);
        plans.add(plan);
    }

    private void createEdge(JoinType joinType, Node leftNode, Node rightNode, Expression condition) {
        Edge edge = new Edge(edges.size(), leftNode.index, rightNode.index);
        edges.add(edge);
        conditions.add(condition);
        joinTypes.add(joinType);

        // TODO: current just consider simpleEdges.
        leftNode.addSimpleEdge(rightNode.index, edge.index);
        rightNode.addSimpleEdge(leftNode.index, edge.index);
    }

    private Node findNode(Set<Slot> slots) {
        for (Node node : nodes) {
            Plan plan = plans.get(node.index);
            // TODO: consider (A join B) join (B join C)
            if (plan.getOutputSet().containsAll(slots)) {
                return node;
            }
        }

        throw new RuntimeException("can't find Node contain current slots.");
    }

    /**
     * find all edges from leftSet -> right.
     */
    public List<Integer> findEdges(BitSet left, int right) {
        return nodes.get(right).simpleEdges.stream()
                .map(edges::get)
                .filter(edge -> (edge.left.intersects(left) && edge.right.get(right))
                        || (edge.right.intersects(left) && edge.left.get(right)))
                .map(edge -> edge.index)
                .collect(Collectors.toList());
    }

    private static boolean containsAll(BitSet s1, BitSet s2) {
        BitSet intersection = (BitSet) s1.clone();
        intersection.and(s2);
        return intersection.equals(s2);
    }
}
