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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.batch.NereidsRewriter;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Set;

public class CTEProducerRewrite extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalCTEProducer().thenApply(ctx -> {
            LogicalCTEProducer<? extends Plan> cteProducer = ctx.root;
            Set<Expression> filters = ctx.cascadesContext.findFilterForProducer(cteProducer.getCteId());
            Set<Expression> projects = ctx.cascadesContext.findProjectForProducer(cteProducer.getCteId());
            LogicalPlan child = (LogicalPlan) cteProducer.child();
            if (CollectionUtils.isNotEmpty(filters)) {
                Expression or = ExpressionUtils.or(new ArrayList<>(filters));
                LogicalFilter logicalFilter = new LogicalFilter(ImmutableSet.of(or), child);
                child = logicalFilter;
            }
            if (CollectionUtils.isNotEmpty(projects)) {
                LogicalProject project = new LogicalProject(ImmutableList.copyOf(projects), child);
                child = project;
            }
            CascadesContext rewrittenCtx = ctx.cascadesContext.forkForCTEProducer(child);
            NereidsRewriter rewriter = new NereidsRewriter(rewrittenCtx);
            rewriter.execute();
            return cteProducer.withChildren(ImmutableList.of(rewrittenCtx.getRewritePlan()));
        }).toRule(RuleType.CTE_PRODUCER_REWRITE);
    }
}
