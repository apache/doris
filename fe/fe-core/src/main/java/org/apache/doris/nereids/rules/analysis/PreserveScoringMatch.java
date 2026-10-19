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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.ScoringMatch;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Preserve each scoring MATCH before binding splits AND predicates into a set. */
public class PreserveScoringMatch implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(any().when(plan -> plan.getExpressions().stream()
                .anyMatch(PreserveScoringMatch::containsScore))
                .then(plan -> {
                    Plan preserved = preserve(plan);
                    return preserved == plan ? null : preserved;
                }).toRule(RuleType.PRESERVE_SCORING_MATCH));
    }

    static LogicalCTE<Plan> preserveCte(LogicalCTE<Plan> cte) {
        // A later CTE can request scores from an earlier definition. Inspect all definitions before binding.
        if (!containsScoreInPlan(cte)) {
            return cte;
        }
        ImmutableList.Builder<LogicalSubQueryAlias<Plan>> aliases = ImmutableList.builder();
        boolean changed = false;
        for (LogicalSubQueryAlias<Plan> alias : cte.getAliasQueries()) {
            Plan child = preserve(alias.child());
            changed |= child != alias.child();
            aliases.add(child == alias.child() ? alias : alias.withChildren(ImmutableList.of(child)));
        }
        return changed ? new LogicalCTE<>(cte.isRecursive(), aliases.build(), cte.child()) : cte;
    }

    private static boolean containsScoreInPlan(Plan plan) {
        return plan.getExpressions().stream().anyMatch(PreserveScoringMatch::containsScore)
                || Stream.concat(plan.children().stream(), plan.extraPlans().stream())
                        .anyMatch(PreserveScoringMatch::containsScoreInPlan);
    }

    private static boolean containsScore(Expression expression) {
        return expression.anyMatch(e -> e instanceof Score || (e instanceof UnboundFunction
                && ((UnboundFunction) e).getName().equalsIgnoreCase("score")));
    }

    private static Plan preserve(Plan plan) {
        Plan rewritten = plan.withChildren(PreserveScoringMatch::preserve);
        if (rewritten instanceof LogicalCTE) {
            LogicalCTE<?> cte = (LogicalCTE<?>) rewritten;
            List<LogicalSubQueryAlias<Plan>> aliases = new ArrayList<>();
            boolean changed = false;
            for (LogicalSubQueryAlias<Plan> alias : cte.getAliasQueries()) {
                Plan child = preserve(alias.child());
                changed |= child != alias.child();
                aliases.add(child == alias.child() ? alias : alias.withChildren(ImmutableList.of(child)));
            }
            return changed ? new LogicalCTE<>(cte.isRecursive(), aliases, cte.child()) : rewritten;
        }
        if (rewritten instanceof LogicalFilter) {
            LogicalFilter<?> filter = (LogicalFilter<?>) rewritten;
            ImmutableSet.Builder<Expression> conjuncts = ImmutableSet.builder();
            boolean changed = false;
            for (Expression conjunct : filter.getConjuncts()) {
                Expression preserved = conjunct.rewriteDownShortCircuit(e ->
                        e instanceof Match && !(e instanceof ScoringMatch) ? new ScoringMatch((Match) e) : e);
                conjuncts.add(preserved);
                changed |= preserved != conjunct;
            }
            return changed ? new LogicalFilter<>(conjuncts.build(), filter.child()) : rewritten;
        }
        return rewritten;
    }
}
