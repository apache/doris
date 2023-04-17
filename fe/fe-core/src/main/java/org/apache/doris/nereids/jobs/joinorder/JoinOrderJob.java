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

package org.apache.doris.nereids.jobs.joinorder;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.GraphSimplifier;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.SubgraphEnumerator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.PlanReceiver;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Join Order job with DPHyp
 */
public class JoinOrderJob extends Job {
    private final Group group;
    private final Set<NamedExpression> otherProject = new HashSet<>();

    public JoinOrderJob(Group group, JobContext context) {
        super(JobType.JOIN_ORDER, context);
        this.group = group;
    }

    @Override
    public void execute() throws AnalysisException {
        GroupExpression rootExpr = group.getLogicalExpression();
        int arity = rootExpr.arity();
        for (int i = 0; i < arity; i++) {
            rootExpr.setChild(i, optimizePlan(rootExpr.child(i)));
        }
        CascadesContext cascadesContext = context.getCascadesContext();
        cascadesContext.pushJob(
                new DeriveStatsJob(group.getLogicalExpression(), cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    private Group optimizePlan(Group group) {
        if (group.isInnerJoinGroup()) {
            return optimizeJoin(group);
        }
        GroupExpression rootExpr = group.getLogicalExpression();
        int arity = rootExpr.arity();
        for (int i = 0; i < arity; i++) {
            rootExpr.setChild(i, optimizePlan(rootExpr.child(i)));
        }
        return group;
    }

    private Group optimizeJoin(Group group) {
        HyperGraph hyperGraph = new HyperGraph();
        buildGraph(group, hyperGraph);
        // TODO: Right now, we just hardcode the limit with 10000, maybe we need a better way to set it
        int limit = 1000;
        PlanReceiver planReceiver = new PlanReceiver(this.context, limit, hyperGraph,
                group.getLogicalProperties().getOutputSet());
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(planReceiver, hyperGraph);
        if (!subgraphEnumerator.enumerate()) {
            GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
            graphSimplifier.simplifyGraph(limit);
            if (!subgraphEnumerator.enumerate()) {
                throw new RuntimeException("DPHyp can not enumerate all sub graphs with limit=" + limit);
            }
        }
        Group optimized = planReceiver.getBestPlan(hyperGraph.getNodesMap());

        // For other projects, such as project constant or project nullable, we construct a new project above root
        if (otherProject.size() != 0) {
            otherProject.addAll(optimized.getLogicalExpression().getPlan().getOutput());
            LogicalProject logicalProject = new LogicalProject<>(new ArrayList<>(otherProject),
                    optimized.getLogicalExpression().getPlan());
            GroupExpression groupExpression = new GroupExpression(logicalProject, Lists.newArrayList(group));
            optimized = context.getCascadesContext().getMemo().copyInGroupExpression(groupExpression);
        }
        return optimized;
    }

    /**
     * build a hyperGraph for the root group
     *
     * @param group root group, should be join type
     * @param hyperGraph build hyperGraph
     */
    public void buildGraph(Group group, HyperGraph hyperGraph) {
        if (group.isProjectGroup()) {
            buildGraph(group.getLogicalExpression().child(0), hyperGraph);
            processProjectPlan(hyperGraph, group);
            return;
        }
        if (!group.isInnerJoinGroup()) {
            hyperGraph.addNode(optimizePlan(group));
            return;
        }
        buildGraph(group.getLogicalExpression().child(0), hyperGraph);
        buildGraph(group.getLogicalExpression().child(1), hyperGraph);
        hyperGraph.addEdge(group);
    }

    /**
     * Process project expression in HyperGraph
     * 1. If it's a simple expression for column pruning, we just ignore it
     * 2. If it's an alias that may be used in the join operator, we need to add it to graph
     * 3. If it's other expression, we can ignore them and add it after optimizing
     */
    private void processProjectPlan(HyperGraph hyperGraph, Group group) {
        LogicalProject<? extends Plan> logicalProject
                = (LogicalProject<? extends Plan>) group.getLogicalExpression()
                .getPlan();

        for (NamedExpression expr : logicalProject.getProjects()) {
            if (expr instanceof Alias) {
                hyperGraph.addAlias((Alias) expr);
            } else if (!expr.isSlot()) {
                otherProject.add(expr);
            }
        }
    }
}
