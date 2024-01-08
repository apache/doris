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
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * pull up LogicalCteAnchor to the top of plan to avoid CteAnchor break other rewrite rules pattern
 * The front producer may depend on the back producer in {@code List<LogicalCTEProducer<Plan>>}
 * After this rule, we normalize all CteAnchor in plan, all CteAnchor under CteProducer should pull out
 * and put all of them to the top of plan depends on dependency tree of them.
 */
public class PullUpCteAnchor extends DefaultPlanRewriter<List<LogicalCTEProducer<Plan>>> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        List<LogicalCTEProducer<Plan>> producers = Lists.newArrayList();
        return rewriteRoot(plan, producers);
    }

    public Plan rewriteRoot(Plan plan, List<LogicalCTEProducer<Plan>> producers) {
        Plan root = plan.accept(this, producers);
        for (LogicalCTEProducer<Plan> producer : producers) {
            root = new LogicalCTEAnchor<>(producer.getCteId(), producer, root);
        }
        return root;
    }

    @Override
    public Plan visitLogicalCTEAnchor(LogicalCTEAnchor<? extends Plan, ? extends Plan> cteAnchor,
            List<LogicalCTEProducer<Plan>> producers) {
        // 1. process child side
        Plan root = cteAnchor.child(1).accept(this, producers);
        // 2. process producers side, need to collect all producer
        cteAnchor.child(0).accept(this, producers);
        return root;
    }

    @Override
    public LogicalCTEProducer<Plan> visitLogicalCTEProducer(LogicalCTEProducer<? extends Plan> cteProducer,
            List<LogicalCTEProducer<Plan>> producers) {
        List<LogicalCTEProducer<Plan>> childProducers = Lists.newArrayList();
        Plan child = cteProducer.child().accept(this, childProducers);
        LogicalCTEProducer<Plan> newProducer = (LogicalCTEProducer<Plan>) cteProducer.withChildren(child);
        // because current producer relay on it child's producers, so add current producer first.
        producers.add(newProducer);
        producers.addAll(childProducers);
        return newProducer;
    }

    @Override
    public Plan visitLogicalApply(LogicalApply<? extends Plan, ? extends Plan> apply,
            List<LogicalCTEProducer<Plan>> producers) {
        SubqueryExpr subqueryExpr = apply.getSubqueryExpr();
        PullUpCteAnchor pullSubqueryExpr = new PullUpCteAnchor();
        List<LogicalCTEProducer<Plan>> subqueryExprProducers = Lists.newArrayList();
        Plan newPlanInExpr = pullSubqueryExpr.rewriteRoot(subqueryExpr.getQueryPlan(), subqueryExprProducers);
        while (newPlanInExpr instanceof LogicalCTEAnchor) {
            newPlanInExpr = ((LogicalCTEAnchor<?, ?>) newPlanInExpr).right();
        }
        SubqueryExpr newSubqueryExpr = subqueryExpr.withSubquery((LogicalPlan) newPlanInExpr);

        Plan newApplyLeft = apply.left().accept(this, producers);

        Plan applyRight = apply.right();
        PullUpCteAnchor pullApplyRight = new PullUpCteAnchor();
        List<LogicalCTEProducer<Plan>> childProducers = Lists.newArrayList();
        Plan newApplyRight = pullApplyRight.rewriteRoot(applyRight, childProducers);
        while (newApplyRight instanceof LogicalCTEAnchor) {
            newApplyRight = ((LogicalCTEAnchor<?, ?>) newApplyRight).right();
        }
        producers.addAll(childProducers);
        return apply.withSubqueryExprAndChildren(newSubqueryExpr,
                ImmutableList.of(newApplyLeft, newApplyRight));
    }
}
