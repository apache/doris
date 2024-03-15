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
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.receiver.PlanReceiver;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Join Order job with DPHyp
 */
public class JoinOrderJob extends Job {
    public static final Logger LOG = LogManager.getLogger(JoinOrderJob.class);
    private final Group group;

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
        if (HyperGraph.isValidJoin(group.getLogicalExpression().getPlan())) {
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
        HyperGraph.Builder builder = HyperGraph.builderForDPhyper(group);
        for (AbstractNode node : builder.getNodes()) {
            DPhyperNode dPhyperNode = (DPhyperNode) node;
            builder.updateNode(node.getIndex(), optimizePlan(dPhyperNode.getGroup()));
        }
        HyperGraph hyperGraph = builder.build();
        // TODO: Right now, we just hardcode the limit with 10000, maybe we need a better way to set it
        int limit = 1000;
        PlanReceiver planReceiver = new PlanReceiver(this.context, limit, hyperGraph,
                group.getLogicalProperties().getOutputSet());
        if (!tryEnumerateJoin(hyperGraph, planReceiver, limit)) {
            return group;
        }
        return planReceiver.getBestPlan(hyperGraph.getNodesMap());
    }

    private boolean tryEnumerateJoin(HyperGraph hyperGraph, PlanReceiver planReceiver, int limit) {
        SubgraphEnumerator subgraphEnumerator = new SubgraphEnumerator(planReceiver, hyperGraph);
        if (!subgraphEnumerator.enumerate()) {
            GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
            return graphSimplifier.simplifyGraph(limit) && subgraphEnumerator.enumerate();
        }
        return true;
    }
}
