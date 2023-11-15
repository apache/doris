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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.GraphSimplifier;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.SubgraphEnumerator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.PlanReceiver;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Join Order job with DPHyp
 */
public class JoinOrderJob extends Job {
    public static final Logger LOG = LogManager.getLogger(JoinOrderJob.class);
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
        if (group.isValidJoinGroup()) {
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
        if (!tryEnumerateJoin(hyperGraph, planReceiver, limit)) {
            return group;
        }
        Group optimized = planReceiver.getBestPlan(hyperGraph.getNodesMap());
        // For other projects, such as project constant or project nullable, we construct a new project above root
        if (!otherProject.isEmpty()) {
            otherProject.addAll(optimized.getLogicalExpression().getPlan().getOutput());
            LogicalProject<Plan> logicalProject = new LogicalProject<>(new ArrayList<>(otherProject),
                    optimized.getLogicalExpression().getPlan());
            GroupExpression groupExpression = new GroupExpression(logicalProject, Lists.newArrayList(group));
            optimized = context.getCascadesContext().getMemo().copyInGroupExpression(groupExpression);
        }
        return optimized;
    }

    private boolean tryEnumerateJoin(HyperGraph hyperGraph, PlanReceiver planReceiver, int limit) {
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(planReceiver, hyperGraph);
        if (!subgraphEnumerator.enumerate()) {
            GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
            return graphSimplifier.simplifyGraph(limit) && subgraphEnumerator.enumerate();
        }
        return true;
    }

    /**
     * build a hyperGraph for the root group
     *
     * @param group root group, should be join type
     * @param hyperGraph build hyperGraph
     *
     * @return return edges of group's child and subTreeNodes of this group
     */
    public Pair<BitSet, Long> buildGraph(Group group, HyperGraph hyperGraph) {
        if (group.isProjectGroup()) {
            Pair<BitSet, Long> res = buildGraph(group.getLogicalExpression().child(0), hyperGraph);
            processProjectPlan(hyperGraph, group, res.second);
            return res;
        }
        if (!group.isValidJoinGroup()) {
            int idx = hyperGraph.addNode(optimizePlan(group));
            return Pair.of(new BitSet(), LongBitmap.newBitmap(idx));
        }
        Pair<BitSet, Long> left = buildGraph(group.getLogicalExpression().child(0), hyperGraph);
        Pair<BitSet, Long> right = buildGraph(group.getLogicalExpression().child(1), hyperGraph);
        return Pair.of(hyperGraph.addEdge(group, left, right), LongBitmap.or(left.second, right.second));
    }

    /**
     * Process project expression in HyperGraph
     * 1. If it's a simple expression for column pruning, we just ignore it
     * 2. If it's an alias that may be used in the join operator, we need to add it to graph
     * 3. If it's other expression, we can ignore them and add it after optimizing
     */
    private void processProjectPlan(HyperGraph hyperGraph, Group group, long subTreeNodes) {
        LogicalProject<? extends Plan> logicalProject
                = (LogicalProject<? extends Plan>) group.getLogicalExpression()
                .getPlan();

        for (NamedExpression expr : logicalProject.getProjects()) {
            if (expr instanceof Alias) {
                hyperGraph.addAlias((Alias) expr, subTreeNodes);
            } else if (!expr.isSlot()) {
                otherProject.add(expr);
            }
        }
    }
}
