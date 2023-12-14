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

package org.apache.doris.nereids.jobs.joinorder.hypergraph.node;

import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.edge.Edge;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * HyperGraph Node.
 */
public class StructInfoNode extends AbstractNode {

    private List<HyperGraph> graphs = new ArrayList<>();

    public StructInfoNode(int index, Plan plan, List<Edge> edges) {
        super(extractPlan(plan), index, edges);
    }

    public StructInfoNode(int index, Plan plan) {
        this(index, plan, new ArrayList<>());
    }

    public StructInfoNode(int index, List<HyperGraph> graphs) {
        this(index, graphs.get(0).getNode(0).getPlan(), new ArrayList<>());
        this.graphs = graphs;
    }

    private static Plan extractPlan(Plan plan) {
        if (plan instanceof GroupPlan) {
            //TODO: Note mv can be in logicalExpression, how can we choose it
            plan = ((GroupPlan) plan).getGroup().getLogicalExpressions().get(0)
                    .getPlan();
        }
        List<Plan> children = plan.children().stream()
                .map(StructInfoNode::extractPlan)
                .collect(ImmutableList.toImmutableList());
        return plan.withChildren(children);
    }

    public boolean needToFlat() {
        return !graphs.isEmpty();
    }

    public List<HyperGraph> getGraphs() {
        return graphs;
    }

}
