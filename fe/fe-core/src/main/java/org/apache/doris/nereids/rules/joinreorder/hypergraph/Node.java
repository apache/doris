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

import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.Lists;

import java.util.BitSet;
import java.util.List;

/**
 * HyperGraph Node.
 */
class Node {
    private final int index;
    private Plan plan;
    // We split these into simple edges (only one node on each side) and complex edges (others)
    // because we can often quickly discard all simple edges by testing the set of interesting nodes
    // against the “simple_neighborhood” bitmap.
    private List<Edge> complexEdges = Lists.newArrayList();
    private List<Edge> simpleEdges = Lists.newArrayList();

    private BitSet simpleNeighborhood = new BitSet();

    public Node(int index, Plan plan) {
        this.plan = plan;
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public Plan getPlan() {
        return plan;
    }

    public void setPlan(Plan plan) {
        this.plan = plan;
    }

    public List<Edge> getComplexEdges() {
        return complexEdges;
    }

    public void setComplexEdges(List<Edge> complexEdges) {
        this.complexEdges = complexEdges;
    }

    public List<Edge> getSimpleEdges() {
        return simpleEdges;
    }

    public void setSimpleEdges(List<Edge> simpleEdges) {
        this.simpleEdges = simpleEdges;
    }

    public BitSet getSimpleNeighborhood() {
        return simpleNeighborhood;
    }

    public void setSimpleNeighborhood(BitSet simpleNeighborhood) {
        this.simpleNeighborhood = simpleNeighborhood;
    }

    public void attachEdge(Edge edge) {
        if (edge.isSimple()) {
            simpleEdges.add(edge);
            edge.getLeft().stream().forEach(index -> simpleNeighborhood.set(index));
            edge.getRight().stream().forEach(index -> simpleNeighborhood.set(index));
            simpleNeighborhood.clear(index);
        } else {
            complexEdges.add(edge);
        }
    }

    public void removeEdge(Edge edge) {
        if (edge.isSimple()) {
            simpleEdges.remove(edge);
        } else {
            complexEdges.remove(edge);
        }
    }

    public String getName() {
        assert plan instanceof GroupPlan : "Each node is a group plan in child";
        return ((GroupPlan) plan).getGroup().getLogicalExpression().getPlan().getType().name() + index;
    }

    public double getRowCount() {
        assert plan instanceof GroupPlan : "Each node is a group plan in child";
        return ((GroupPlan) plan).getGroup().getStatistics().getRowCount();
    }
}
