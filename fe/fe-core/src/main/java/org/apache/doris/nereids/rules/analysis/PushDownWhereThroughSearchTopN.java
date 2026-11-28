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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.table.ExternalSearch;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Moves the WHERE written on a search table function ({@link ExternalSearch}) below the TopN that merges the
 * candidates of the scan's ranges, so that it filters every range's candidates before the merge:
 *
 * <pre>
 * Filter                         [Filter: the conjuncts that stay]
 *   [SubQueryAlias]                [SubQueryAlias]
 *     TopN               ->          TopN
 *       search TVF                     Filter
 *                                        search TVF
 * </pre>
 *
 * <p>It runs while the statement is analyzed, where a filter over the merge TopN (or over the alias of the search
 * relation) can only be the WHERE of the query block whose FROM clause is the search. Every other filter applies to
 * the search's result, after the merge, and later reaches the same place: an outer query's WHERE once the user's
 * own ORDER BY ... LIMIT is folded into this TopN, the filters of a CTE's consumers, the WHERE of a query that joins
 * the search, predicates the optimizer infers. No rewrite rule moves a filter below this TopN.
 *
 * <p>A conjunct also stays above the TopN when it is volatile or must not move, as in the filter pushdowns, when it
 * contains a subquery, or when it reads a column of an outer query: these apply to the search's result. The WHERE
 * stays a filter of Doris on the candidates; it never becomes the search's own filter, which is the function's
 * {@code filter} argument, so the result can hold fewer than top_k rows.
 */
public class PushDownWhereThroughSearchTopN implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.PUSH_DOWN_WHERE_THROUGH_SEARCH_TOPN.build(
                        logicalFilter(logicalTopN(logicalTVFRelation()))
                                .when(filter -> filtersCandidates(filter, filter.child()))
                                .then(filter -> pushDown(filter, filter.child(), merge -> merge))),
                RuleType.PUSH_DOWN_WHERE_THROUGH_ALIASED_SEARCH_TOPN.build(
                        logicalFilter(logicalSubQueryAlias(logicalTopN(logicalTVFRelation())))
                                .when(filter -> filtersCandidates(filter, filter.child().child()))
                                .then(filter -> pushDown(filter, filter.child().child(),
                                        merge -> filter.child().withChildren(merge)))));
    }

    /** Whether the TopN merges a search and the filter has a conjunct for its candidates. */
    private static boolean filtersCandidates(LogicalFilter<?> filter, LogicalTopN<LogicalTVFRelation> topN) {
        LogicalTVFRelation relation = topN.child();
        return relation.getFunction() instanceof ExternalSearch
                && filter.getConjuncts().stream().anyMatch(conjunct -> filtersCandidates(conjunct, relation));
    }

    private static boolean filtersCandidates(Expression conjunct, LogicalTVFRelation relation) {
        return !conjunct.containsVolatileOrNoneMovableExpression()
                && !conjunct.containsType(SubqueryExpr.class)
                && relation.getOutputExprIdSet().containsAll(conjunct.getInputSlotExprIds());
    }

    private static Plan pushDown(LogicalFilter<?> filter, LogicalTopN<LogicalTVFRelation> merge,
            Function<Plan, Plan> aboveMerge) {
        LogicalTVFRelation relation = merge.child();
        Map<Boolean, Set<Expression>> conjuncts = filter.getConjuncts().stream().collect(Collectors.partitioningBy(
                conjunct -> filtersCandidates(conjunct, relation), Collectors.toCollection(LinkedHashSet::new)));
        Plan filteredMerge = merge.withChildren(new LogicalFilter<>(conjuncts.get(true), relation));
        return PlanUtils.filterOrSelf(conjuncts.get(false), aboveMerge.apply(filteredMerge));
    }
}
