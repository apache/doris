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
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.GraphSimplifier;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.HyperGraph;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.SubgraphEnumerator;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.AbstractNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.node.DPhyperNode;
import org.apache.doris.nereids.jobs.joinorder.hypergraphv2.receiver.PlanReceiver;
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

    //自底向上记录每个hyper node是否经过了outer join，然后在addjoin中对inner join多个and条件拆分时做判断，引用的node必须只经过了inner或cross join时才能拆分
    //能拆分的就拆分，拆分不了的，就和外连接一样原地保留
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
        HyperGraph.Builder builder = HyperGraph.builderForDPhyper(group, context.getCascadesContext());
        for (AbstractNode node : builder.getNodes()) {
            DPhyperNode dPhyperNode = (DPhyperNode) node;
            builder.updateNode(node.getIndex(), optimizePlan(dPhyperNode.getGroup()));
        }
        HyperGraph hyperGraph = builder.build();
        int limit = 1000;
        if (this.context.getCascadesContext().getConnectContext() != null) {
            limit = this.context.getCascadesContext().getConnectContext().getSessionVariable().dphyperLimit;
        }
//        limit = 1;
        PlanReceiver planReceiver = new PlanReceiver(this.context, limit, hyperGraph);
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
