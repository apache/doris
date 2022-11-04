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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.joinreorder.HyperGraphJoinReorderGroupPlan;
import org.apache.doris.nereids.rules.joinreorder.hypergraph.HyperGraph;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.statistics.StatsDeriveResult;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HyperGraphBuilder {
    HashMap<String, Integer> tableRowCount = new HashMap<>();
    LogicalPlan plan;

    public HyperGraph build() {
        Plan planWithStats = extractJoinCluster(this.plan);
        HyperGraph graph = HyperGraph.fromPlan(planWithStats);
        return graph;
    }

    public HyperGraphBuilder init(String name, int rowCount) {
        assert !tableRowCount.containsKey(name) : "The join table must be new";
        plan = PlanConstructor.newLogicalOlapScan(tableRowCount.size(), name, 0);
        tableRowCount.put(name, rowCount);
        return this;
    }

    public HyperGraphBuilder join(JoinType joinType, String name, int rowCount) {
        assert !tableRowCount.containsKey(name) : "The join table must be new";
        tableRowCount.put(name, rowCount);
        Plan scan = PlanConstructor.newLogicalOlapScan(tableRowCount.size(), name, 0);
        ImmutableList<EqualTo> hashConjunts = ImmutableList.of(
                new EqualTo(this.plan.getOutput().get(0), scan.getOutput().get(0)));
        plan = new LogicalJoin<>(joinType, new ArrayList<>(hashConjunts),
                this.plan, scan);
        return this;
    }

    private Plan extractJoinCluster(Plan plan) {
        Rule rule = new HyperGraphJoinReorderGroupPlan().build();
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        deriveStats(cascadesContext);
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(rule.getPattern(),
                cascadesContext.getMemo().getRoot().getLogicalExpression());
        List<Plan> planList = new ArrayList<>();
        for (Plan matchingPlan : groupExpressionMatching) {
            planList.add(matchingPlan);
        }
        assert planList.size() == 1 : "Now we only support one join cluster";
        injectRowcount(planList.get(0));
        return planList.get(0);
    }

    private void deriveStats(CascadesContext cascadesContext) {
        cascadesContext.pushJob(
                new DeriveStatsJob(cascadesContext.getMemo().getRoot().getLogicalExpression(),
                        cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    private void injectRowcount(Plan plan) {
        if (plan instanceof GroupPlan) {
            GroupPlan olapGroupPlan = (GroupPlan) plan;
            LogicalOlapScan scanPlan = (LogicalOlapScan) olapGroupPlan.getGroup().getLogicalExpression().getPlan();
            StatsDeriveResult stats = olapGroupPlan.getGroup().getStatistics();
            stats.setRowCount(tableRowCount.get(scanPlan.getTable().getName()));
            return;
        }
        LogicalJoin join = (LogicalJoin) plan;
        assert join.getGroupExpression().get().isStatDerived() : "All plan in HyperGraph has been derived stats";
        injectRowcount(join.left());
        injectRowcount(join.right());
    }
}
