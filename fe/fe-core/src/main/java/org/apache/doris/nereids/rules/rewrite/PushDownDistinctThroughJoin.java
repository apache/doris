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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.util.Set;
import java.util.function.Function;

/**
 * PushdownDistinctThroughJoin
 */
public class PushDownDistinctThroughJoin extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext context) {
        Set<Integer> enableNereidsRules = context.getCascadesContext().getConnectContext()
                .getSessionVariable().getEnableNereidsRules();
        if (!enableNereidsRules.contains(RuleType.PUSH_DOWN_DISTINCT_THROUGH_JOIN.type())) {
            return null;
        }
        return plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> agg, JobContext context) {
        agg = visitChildren(this, agg, context);
        if (agg.hasPushed() || !agg.isDistinct() || isLeaf(agg.child())) {
            return agg;
        }

        // After we push down distinct, if this distinct is generated, we will eliminate this distinct
        if (agg.isGenerated()) {
            return skipProjectPushDistinct(agg.child());
        } else {
            return agg.withChildren(skipProjectPushDistinct(agg.child()));
        }
    }

    private Plan skipProjectPushDistinct(Plan plan) {
        if (plan instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) plan;
            Plan pushJoin = pushDistinct((LogicalJoin<? extends Plan, ? extends Plan>) project.child());
            return project.withChildren(ImmutableList.of(pushJoin));
        } else {
            Plan pushJoin = pushDistinct((LogicalJoin<? extends Plan, ? extends Plan>) plan);
            return pushJoin;
        }
    }

    private Plan pushDistinct(LogicalJoin<? extends Plan, ? extends Plan> join) {
        Function<Plan, Plan> pushChild = (Plan plan) -> {
            if (isLeaf(plan)) {
                return withDistinct(plan);
            } else {
                // Due to there isn't statistics during Rewrite, so we just push down through 1 join.
                // return skipProjectPushDistinct(plan);
                return withDistinct(plan);
            }
        };
        Plan left = pushChild.apply(join.left());
        Plan right = pushChild.apply(join.right());
        return join.withChildren(ImmutableList.of(left, right));
    }

    private Plan withDistinct(Plan plan) {
        if (PlanUtils.skipProjectFilterLimit(plan) instanceof Relation) {
            return plan;
        }
        return new LogicalAggregate<>(ImmutableList.copyOf(plan.getOutput()), true, true, plan);
    }

    private boolean isLeaf(Plan plan) {
        if (plan instanceof LogicalProject && ((LogicalProject<?>) plan).isAllSlots()) {
            plan = plan.child(0);
        }
        return !(plan instanceof LogicalJoin);
    }
}
