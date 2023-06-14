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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.List;

/**
 * A CTEConsumer would be converted to a inlined plan if corresponding CTE referenced less than or
 * equal inline_cte_referenced_threshold (it's a session variable, by default is 1).
 */
public class InlineCTE extends OneRewriteRuleFactory {

    private static final int INLINE_CTE_REFERENCED_THRESHOLD = 1;

    @Override
    public Rule build() {
        return logicalCTEConsumer().thenApply(ctx -> {
            LogicalCTEConsumer cteConsumer = ctx.root;
            int refCount = ctx.cascadesContext.cteReferencedCount(cteConsumer.getCteId());
            /*
             *  Current we only implement CTE Materialize on pipeline engine and only materialize those CTE whose
             *  refCount > NereidsRewriter.INLINE_CTE_REFERENCED_THRESHOLD.
             */
            if (ConnectContext.get().getSessionVariable().enablePipelineEngine
                    && ConnectContext.get().getSessionVariable().enableCTEMaterialize
                    && refCount > INLINE_CTE_REFERENCED_THRESHOLD) {
                return cteConsumer;
            }
            LogicalPlan inlinedPlan = ctx.cascadesContext.findCTEPlanForInline(cteConsumer.getCteId());
            List<Slot> inlinedPlanOutput = inlinedPlan.getOutput();
            List<Slot> cteConsumerOutput = cteConsumer.getOutput();
            List<NamedExpression> projects = new ArrayList<>();
            for (Slot inlineSlot : inlinedPlanOutput) {
                String name = inlineSlot.getName();
                for (Slot consumerSlot : cteConsumerOutput) {
                    if (consumerSlot.getName().equals(name)) {
                        Alias alias = new Alias(consumerSlot.getExprId(), inlineSlot, name);
                        projects.add(alias);
                        break;
                    }
                }
            }
            return new LogicalProject<>(projects,
                    inlinedPlan);
        }).toRule(RuleType.INLINE_CTE);
    }
}
