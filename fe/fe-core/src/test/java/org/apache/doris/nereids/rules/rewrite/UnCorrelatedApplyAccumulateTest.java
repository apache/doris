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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

/**
 * The UnCorrelatedApply* rules rebuild LogicalApply and must AND the newly extracted
 * correlated predicate into the already accumulated correlationFilter instead of
 * overwriting it. this covers the non-Project accumulation path:
 *
 * <pre>
 *   Apply(correlationSlot=[x])
 *     +-- L
 *     +-- Filter(r2 = x)                      // correlated HAVING
 *          +-- Aggregate(group by [r1, r2])
 *               +-- Filter(r1 = x)            // correlated WHERE
 *                    +-- R
 * </pre>
 *
 * UnCorrelatedApplyFilter first pulls the correlated HAVING predicate (r2 = x) into the
 * apply, then UnCorrelatedApplyAggregateFilter must merge the correlated WHERE predicate
 * (r1 = x) into the same correlationFilter instead of replacing it, otherwise the HAVING
 * predicate silently disappears from the final join and the query returns a wrong (too
 * true) result.
 */
class UnCorrelatedApplyAccumulateTest implements MemoPatternMatchSupported {

    @Test
    public void testFilterThenAggregateFilterAccumulateCorrelationFilter() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        Slot r2 = right.getOutput().get(1); // t2.name

        // correlated WHERE: r1 = x, below the aggregate
        LogicalFilter<LogicalOlapScan> where =
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(r1, x)), right);
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(ImmutableList.of(r1, r2), ImmutableList.of(r1, r2), where);
        // correlated HAVING: r2 = x, above the aggregate
        LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>> having =
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(r2, x)), agg);
        LogicalApply<LogicalOlapScan, LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>>> apply =
                new LogicalApply<>(ImmutableList.of(x),
                        LogicalApply.SubQueryType.EXITS_SUBQUERY, false, Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(),
                        false, false, left, having);

        PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .applyBottomUp(new UnCorrelatedApplyAggregateFilter())
                .matches(logicalApply().when(a -> {
                    Optional<Expression> correlationFilter = a.getCorrelationFilter();
                    if (!correlationFilter.isPresent()) {
                        return false;
                    }
                    List<Expression> conjuncts = ExpressionUtils.extractConjunction(correlationFilter.get());
                    // the correlated HAVING (r2 = x) extracted by UnCorrelatedApplyFilter and the
                    // correlated WHERE (r1 = x) extracted by UnCorrelatedApplyAggregateFilter must
                    // both be kept in the accumulated correlation filter
                    return conjuncts.size() == 2
                            && conjuncts.stream().anyMatch(c -> c.toSql().contains("name"))
                            && conjuncts.stream().anyMatch(c -> c.toSql().contains("id"));
                }));
    }

    @Test
    public void testAggregateFilterThenFilterAccumulateCorrelationFilter() {
        LogicalOlapScan left = PlanConstructor.newLogicalOlapScan(0, "t1", 1);
        Slot x = left.getOutput().get(0); // t1.id
        LogicalOlapScan right = PlanConstructor.newLogicalOlapScan(1, "t2", 1);
        Slot r1 = right.getOutput().get(0); // t2.id
        Slot r2 = right.getOutput().get(1); // t2.name

        // correlated WHERE: r1 = x, below the aggregate
        LogicalFilter<LogicalOlapScan> where =
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(r1, x)), right);
        LogicalAggregate<LogicalFilter<LogicalOlapScan>> agg =
                new LogicalAggregate<>(ImmutableList.of(r1, r2), ImmutableList.of(r1, r2), where);
        // correlated HAVING: r2 = x, above the aggregate
        LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>> having =
                new LogicalFilter<>(ImmutableSet.of(new EqualTo(r2, x)), agg);
        LogicalApply<LogicalOlapScan, LogicalFilter<LogicalAggregate<LogicalFilter<LogicalOlapScan>>>> apply =
                new LogicalApply<>(ImmutableList.of(x),
                        LogicalApply.SubQueryType.EXITS_SUBQUERY, false, Optional.empty(), Optional.empty(),
                        Optional.empty(), Optional.empty(),
                        false, false, left, having);

        // reversed application order: UnCorrelatedApplyAggregateFilter pulls the correlated
        // WHERE (r1 = x) into the apply first, then UnCorrelatedApplyFilter must merge the
        // correlated HAVING (r2 = x) instead of overwriting it
        PlanChecker.from(MemoTestUtils.createConnectContext(), apply)
                .applyBottomUp(new UnCorrelatedApplyAggregateFilter())
                .applyBottomUp(new UnCorrelatedApplyFilter())
                .matches(logicalApply().when(a -> {
                    Optional<Expression> correlationFilter = a.getCorrelationFilter();
                    if (!correlationFilter.isPresent()) {
                        return false;
                    }
                    List<Expression> conjuncts = ExpressionUtils.extractConjunction(correlationFilter.get());
                    return conjuncts.size() == 2
                            && conjuncts.stream().anyMatch(c -> c.toSql().contains("name"))
                            && conjuncts.stream().anyMatch(c -> c.toSql().contains("id"));
                }));
    }
}
