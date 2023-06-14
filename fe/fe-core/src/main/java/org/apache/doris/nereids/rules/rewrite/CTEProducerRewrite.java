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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  Rewrite CTE Producer recursively.
 */
public class CTEProducerRewrite extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalCTEProducer().when(p -> !p.isRewritten()).thenApply(ctx -> {
            LogicalCTEProducer<? extends Plan> cteProducer = ctx.root;
            Set<Expression> projects = ctx.cascadesContext.getProjectForProducer(cteProducer.getCteId());
            LogicalPlan child = tryToConstructFilter(ctx.cascadesContext, cteProducer.getCteId(),
                    (LogicalPlan) ctx.root.child());
            if (CollectionUtils.isNotEmpty(projects)
                    && ctx.cascadesContext.couldPruneColumnOnProducer(cteProducer.getCteId())) {
                child = new LogicalProject(ImmutableList.copyOf(projects), child);
            }
            CascadesContext rewrittenCtx = ctx.cascadesContext.forkForCTEProducer(child);
            Rewriter rewriter = new Rewriter(rewrittenCtx);
            rewriter.execute();
            return cteProducer.withChildrenAndProjects(ImmutableList.of(rewrittenCtx.getRewritePlan()),
                    new ArrayList<>(child.getOutput()), true);
        }).toRule(RuleType.CTE_PRODUCER_REWRITE);
    }

    /*
     * An expression can only be pushed down if it has filter expressions on all consumers that reference the slot.
     * For example, let's assume a producer has two consumers, consumer1 and consumer2:
     *
     * filter(a > 5 and b < 1) -> consumer1
     * filter(a < 8) -> consumer2
     *
     * In this case, the only expression that can be pushed down to the producer is filter(a > 5 or a < 8).
     */
    private LogicalPlan tryToConstructFilter(CascadesContext cascadesContext, CTEId cteId, LogicalPlan child) {
        Set<Integer> consumerIds = cascadesContext.getCteIdToConsumers().get(cteId).stream()
                .map(LogicalCTEConsumer::getConsumerId)
                .collect(Collectors.toSet());
        Set<Set<Expression>> filtersAboveEachConsumer = cascadesContext.getConsumerIdToFilters().entrySet().stream()
                .filter(kv -> consumerIds.contains(kv.getKey()))
                .map(Entry::getValue)
                .collect(Collectors.toSet());
        Set<Expression> someone = filtersAboveEachConsumer.stream().findFirst().orElse(null);
        if (someone == null) {
            return child;
        }
        int filterSize = cascadesContext.getCteIdToConsumers().get(cteId).size();
        Set<Expression> filter = new HashSet<>();
        for (Expression f : someone) {
            int matchCount = 1;
            Set<SlotReference> slots = f.collect(e -> e instanceof SlotReference);
            Set<Expression> mightBeJoined = new HashSet<>();
            for (Set<Expression> another : filtersAboveEachConsumer) {
                if (another.equals(someone)) {
                    continue;
                }
                Set<Expression> matched = new HashSet<>();
                for (Expression e : another) {
                    Set<SlotReference> otherSlots = e.collect(ae -> ae instanceof SlotReference);
                    if (otherSlots.equals(slots)) {
                        matched.add(e);
                    }
                }
                if (!matched.isEmpty()) {
                    matchCount++;
                }
                mightBeJoined.addAll(matched);
            }
            if (matchCount >= filterSize) {
                mightBeJoined.add(f);
                filter.add(ExpressionUtils.or(mightBeJoined));
            }
        }
        if (!filter.isEmpty()) {
            return new LogicalFilter(ImmutableSet.of(ExpressionUtils.and(filter)), child);
        }
        return child;
    }
}
