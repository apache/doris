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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Random;
import org.apache.doris.nereids.trees.expressions.functions.table.FullTextSearch;
import org.apache.doris.nereids.trees.expressions.functions.table.Numbers;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.VectorSearch;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

/**
 * While a statement is analyzed, a filter right over the TopN that merges a search's candidates (or over the alias
 * of the search relation) is the WHERE written on the search, and it moves below that TopN to filter the candidates.
 * What applies to the search's result stays above: a volatile conjunct, a subquery, a column of an outer query, and
 * any filter of an enclosing query block.
 */
public class PushDownWhereThroughSearchTopNTest implements MemoPatternMatchSupported {

    @Test
    public void testMovesTheWhereBelowTheMergeOfEitherSearchFunction() {
        for (Class<? extends TableValuedFunction> search : Arrays.asList(VectorSearch.class, FullTextSearch.class)) {
            LogicalTopN<LogicalTVFRelation> merge = mergeOf(relation(search));
            PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(idAbove(merge, 1), merge))
                    .applyTopDown(new PushDownWhereThroughSearchTopN())
                    .matchesFromRoot(
                            logicalTopN(
                                    logicalFilter(
                                            logicalTVFRelation()
                                    ).when(filter -> filter.getConjuncts().equals(idAbove(merge, 1)))
                            ).when(topN -> topN.getLimit() == 5 && topN.getOffset() == 2));
        }
    }

    @Test
    public void testMovesTheWhereThroughTheAliasOfTheSearchRelation() {
        // SELECT ... FROM vector_search(...) v WHERE v.id > 1
        LogicalTopN<LogicalTVFRelation> merge = mergeOf(relation(VectorSearch.class));
        LogicalSubQueryAlias<Plan> alias = new LogicalSubQueryAlias<>("v", merge);
        PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(idAbove(merge, 1), alias))
                .applyTopDown(new PushDownWhereThroughSearchTopN())
                .matchesFromRoot(
                        logicalSubQueryAlias(
                                logicalTopN(
                                        logicalFilter(
                                                logicalTVFRelation()
                                        ).when(filter -> filter.getConjuncts().equals(idAbove(merge, 1)))
                                )
                        ).when(subQueryAlias -> subQueryAlias.getAlias().equals("v")));
    }

    @Test
    public void testKeepsAVolatileConjunctAboveTheMerge() {
        LogicalTopN<LogicalTVFRelation> merge = mergeOf(relation(VectorSearch.class));
        Expression random = new GreaterThan(new Random(), Literal.of(0.5));
        PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(
                        ImmutableSet.of(idAbove(merge, 1).iterator().next(), random), merge))
                .applyTopDown(new PushDownWhereThroughSearchTopN())
                .matchesFromRoot(
                        logicalFilter(
                                logicalTopN(
                                        logicalFilter(
                                                logicalTVFRelation()
                                        ).when(below -> below.getConjuncts().equals(idAbove(merge, 1)))
                                )
                        ).when(above -> above.getConjuncts().equals(ImmutableSet.of(random))));

        PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(ImmutableSet.of(random), merge))
                .applyTopDown(new PushDownWhereThroughSearchTopN())
                .matchesFromRoot(logicalFilter(logicalTopN(logicalTVFRelation())));
    }

    @Test
    public void testKeepsASubqueryOrAnOuterQuerysColumnAboveTheMerge() {
        LogicalTopN<LogicalTVFRelation> merge = mergeOf(relation(VectorSearch.class));
        // WHERE EXISTS (...) and a correlated WHERE v.id = outer.k both apply to the search's result.
        Expression exists = new Exists(relation(Numbers.class), false);
        Expression correlated = new EqualTo(merge.getOutput().get(0),
                new SlotReference("k", BigIntType.INSTANCE));
        PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(
                        ImmutableSet.of(exists, idAbove(merge, 1).iterator().next(), correlated), merge))
                .applyTopDown(new PushDownWhereThroughSearchTopN())
                .matchesFromRoot(
                        logicalFilter(
                                logicalTopN(
                                        logicalFilter(
                                                logicalTVFRelation()
                                        ).when(below -> below.getConjuncts().equals(idAbove(merge, 1)))
                                )
                        ).when(above -> above.getConjuncts().equals(ImmutableSet.of(exists, correlated))));
    }

    @Test
    public void testLeavesTheFilterOfAnotherQueryBlockOrRelationAboveTheTopN() {
        // SELECT * FROM (SELECT id, _distance FROM vector_search(...)) s WHERE s.id > 1: the outer WHERE sees the
        // search's result through the inner select list.
        LogicalTopN<LogicalTVFRelation> merge = mergeOf(relation(VectorSearch.class));
        LogicalSubQueryAlias<Plan> outer = new LogicalSubQueryAlias<>("s",
                new LogicalProject<>(ImmutableList.<NamedExpression>copyOf(merge.getOutput()), merge));
        PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(idAbove(merge, 1), outer))
                .applyTopDown(new PushDownWhereThroughSearchTopN())
                .matchesFromRoot(logicalFilter(logicalSubQueryAlias(logicalProject(logicalTopN(
                        logicalTVFRelation())))));

        LogicalTopN<LogicalTVFRelation> numbers = mergeOf(relation(Numbers.class));
        PlanChecker.from(MemoTestUtils.createConnectContext(), new LogicalFilter<>(idAbove(numbers, 1), numbers))
                .applyTopDown(new PushDownWhereThroughSearchTopN())
                .matchesFromRoot(logicalFilter(logicalTopN(logicalTVFRelation())));
    }

    private static ImmutableSet<Expression> idAbove(Plan plan, long value) {
        return ImmutableSet.of(new GreaterThan(plan.getOutput().get(0), Literal.of(value)));
    }

    private static LogicalTopN<LogicalTVFRelation> mergeOf(LogicalTVFRelation relation) {
        Slot result = relation.getOutput().get(1);
        return new LogicalTopN<>(ImmutableList.of(new OrderKey(result, true, false)), 5, 2, relation);
    }

    private static LogicalTVFRelation relation(Class<? extends TableValuedFunction> type) {
        TableValuedFunction function = Mockito.mock(type);
        Mockito.when(function.getName()).thenReturn(type.getSimpleName());
        Mockito.when(function.getTable()).thenReturn(new FunctionGenTable(-1, type.getSimpleName(),
                TableIf.TableType.TABLE_VALUED_FUNCTION, Arrays.asList(new Column("id", Type.BIGINT),
                        new Column("_distance", Type.FLOAT)), null));
        return new LogicalTVFRelation(StatementScopeIdGenerator.newRelationId(), function, ImmutableList.of());
    }
}
