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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.GraphSimplifier;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.SubgraphEnumerator;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.PlanReceiver;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;

import com.google.common.base.Preconditions;

/**
 * Join Order job with DPHyp
 */
public class JoinOrderJob extends Job {
    private final Group group;

    public JoinOrderJob(Group group, JobContext context) {
        super(JobType.JOIN_ORDER, context);
        this.group = group;
    }

    @Override
    public void execute() throws AnalysisException {
        Preconditions.checkArgument(!group.isJoinGroup());
        GroupExpression rootExpr = group.getLogicalExpression();
        int arity = rootExpr.arity();
        for (int i = 0; i < arity; i++) {
            rootExpr.setChild(i, optimizePlan(rootExpr.child(i)));
        }
    }

    private Group optimizePlan(Group group) {
        if (group.isJoinGroup()) {
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
        // Right now, we just hardcode the limit with 10000, maybe we need a better way to set it
        int limit = 10000;
        PlanReceiver planReceiver = new PlanReceiver(limit);
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(planReceiver, hyperGraph);
        if (!subgraphEnumerator.enumerate()) {
            GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
            graphSimplifier.simplifyGraph(limit);
            if (!subgraphEnumerator.enumerate()) {
                throw new RuntimeException("DPHyp can not enumerate all sub graphs with limit=" + limit);
            }
        }

        Group optimized = planReceiver.getBestPlan(hyperGraph.getNodesMap());
        return copyToMemo(optimized);
    }

    private Group copyToMemo(Group root) {
        if (!root.isJoinGroup()) {
            return root;
        }
        GroupExpression groupExpression = root.getLogicalExpression();
        int arity = groupExpression.arity();
        for (int i = 0; i < arity; i++) {
            Group childGroup = groupExpression.child(i);
            Group newChildGroup = copyToMemo(childGroup);
            groupExpression.setChild(i, newChildGroup);
        }
        Group newRoot = context.getCascadesContext().getMemo().copyInGroupExpression(groupExpression);
        newRoot.setStatistics(root.getStatistics());
        return newRoot;
    }

    /**
     * build a hyperGraph for the root group
     *
     * @param group root group, should be join type
     * @param hyperGraph build hyperGraph
     */
    public void buildGraph(Group group, HyperGraph hyperGraph) {
        if (!group.isJoinGroup()) {
            hyperGraph.addNode(optimizePlan(group));
            return;
        }
        buildGraph(group.getLogicalExpression().child(0), hyperGraph);
        buildGraph(group.getLogicalExpression().child(1), hyperGraph);
        hyperGraph.addEdge(group);
    }
}
