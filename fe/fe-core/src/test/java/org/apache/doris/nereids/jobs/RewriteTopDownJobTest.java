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

package org.apache.doris.nereids.jobs;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.OptimizerContext;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.jobs.rewrite.RewriteTopDownJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.logical.LogicalProject;
import org.apache.doris.nereids.operators.plans.logical.LogicalRelation;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.Plans;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RewriteTopDownJobTest implements Plans {
    public static class FakeRule extends OneRewriteRuleFactory {
        @Override
        public Rule<Plan> build() {
            return unboundRelation().then(unboundRelation -> plan(
                new LogicalRelation(new OlapTable(), Lists.newArrayList("test"))
            )).toRule(RuleType.BINDING_UNBOUND_RELATION_RULE);
        }
    }

    @Test
    public void testSimplestScene() throws AnalysisException {
        UnboundRelation unboundRelation = new UnboundRelation(Lists.newArrayList("test"));
        Plan leaf = plan(unboundRelation);
        LogicalProject project = new LogicalProject(Lists.newArrayList());
        Plan root = plan(project, leaf);
        Memo<Plan> memo = new Memo();
        memo.initialize(root);

        OptimizerContext<Plan> optimizerContext = new OptimizerContext<>(memo);
        PlannerContext plannerContext = new PlannerContext(optimizerContext, null, new PhysicalProperties());
        List<Rule<Plan>> fakeRules = Lists.newArrayList(new FakeRule().build());
        RewriteTopDownJob<Plan> rewriteTopDownJob = new RewriteTopDownJob<>(memo.getRoot(), fakeRules, plannerContext);
        plannerContext.getOptimizerContext().pushJob(rewriteTopDownJob);
        plannerContext.getOptimizerContext().getJobScheduler().executeJobPool(plannerContext);

        Group rootGroup = memo.getRoot();
        Assertions.assertEquals(1, rootGroup.getLogicalExpressions().size());
        GroupExpression rootGroupExpression = rootGroup.getLogicalExpression();
        Assertions.assertEquals(1, rootGroupExpression.children().size());
        Assertions.assertEquals(OperatorType.LOGICAL_PROJECT, rootGroupExpression.getOperator().getType());
        Group leafGroup = rootGroupExpression.child(0);
        Assertions.assertEquals(1, leafGroup.getLogicalExpressions().size());
        GroupExpression leafGroupExpression = leafGroup.getLogicalExpression();
        Assertions.assertEquals(OperatorType.LOGICAL_BOUND_RELATION, leafGroupExpression.getOperator().getType());
    }
}
