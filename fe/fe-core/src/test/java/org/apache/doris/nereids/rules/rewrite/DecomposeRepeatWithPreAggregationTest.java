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

import org.apache.doris.nereids.rules.rewrite.DistinctAggStrategySelector.DistinctSelectorContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * UT for {@link DecomposeRepeatWithPreAggregation}.
 */
public class DecomposeRepeatWithPreAggregationTest extends TestWithFeService implements MemoPatternMatchSupported {
    private DecomposeRepeatWithPreAggregation rule;
    private DistinctSelectorContext ctx;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("decompose_repeat_with_preagg");
        createTable(
                "create table decompose_repeat_with_preagg.t1 (\n"
                        + "a int, b int, c int, d int\n"
                        + ")\n"
                        + "distributed by hash(a) buckets 1\n"
                        + "properties('replication_num' = '1');"
        );
        connectContext.setDatabase("default_cluster:decompose_repeat_with_preagg");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        rule = DecomposeRepeatWithPreAggregation.INSTANCE;
        ctx = new DistinctSelectorContext(
                MemoTestUtils.createCascadesContext(
                        new LogicalEmptyRelation(org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                                ImmutableList.of())).getStatementContext(),
                MemoTestUtils.createCascadesContext(
                        new LogicalEmptyRelation(org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                                ImmutableList.of())));
    }

    @Test
    void rewriteRollupSumShouldGenerateCteAndUnion() {
        String sql = "select a,b,c,sum(d) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEAnchor());
    }

    @Test
    void noRewriteWhenGroupingSetsSizeLe3() {
        String sql = "select a,b,sum(d) from t1 group by rollup(a,b);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());
    }

    @Test
    void noRewriteWhenDistinctAgg() {
        String sql = "select a,b,c,sum(distinct d) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());
    }

    @Test
    void noRewriteWhenUnsupportedAgg() {
        String sql = "select a,b,c,avg(d) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalCTEAnchor());

    }

    @Test
    void noRewriteWhenHasGroupingScalarFunction() {
        String sql = "select a,b,c,sum(d),grouping_id(a) from t1 group by rollup(a,b,c);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEAnchor());
    }

    @Test
    void rewriteWhenMaxGroupingSetNotFirst() {
        String sql = "select a,b,c,sum(d) from t1 group by grouping sets((a),(a,b,c),(a,b),());";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEAnchor());
    }

    @Test
    void rewriteWhenMaxGroupingSetFindMaxGroup() {
        String sql = "select a,b,c,sum(d) from t1 group by grouping sets((a,b),(c,d),(a,b,c,d),());";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalCTEAnchor());
    }


    @Test
    public void testFindMaxGroupingSetIndex() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("findMaxGroupingSetIndex", List.class);
        method.setAccessible(true);

        // Test case 1: Empty list
        List<List<Expression>> emptyList = new ArrayList<>();
        int result = (int) method.invoke(rule, emptyList);
        Assertions.assertEquals(-1, result);

        // Test case 2: Single grouping set
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        List<List<Expression>> singleSet = ImmutableList.of(ImmutableList.of(a, b, c));
        result = (int) method.invoke(rule, singleSet);
        Assertions.assertEquals(0, result);

        // Test case 3: Max grouping set contains all others (rollup scenario)
        List<List<Expression>> rollupSets = ImmutableList.of(
                ImmutableList.of(a, b, c),  // index 0 - max
                ImmutableList.of(a, b),     // index 1
                ImmutableList.of(a),       // index 2
                ImmutableList.of()          // index 3
        );
        result = (int) method.invoke(rule, rollupSets);
        Assertions.assertEquals(0, result);

        // Test case 4: Max grouping set not at first position
        List<List<Expression>> mixedSets = ImmutableList.of(
                ImmutableList.of(a),        // index 0
                ImmutableList.of(a, b),     // index 1
                ImmutableList.of(a, b, c),  // index 2 - max
                ImmutableList.of()          // index 3
        );
        result = (int) method.invoke(rule, mixedSets);
        Assertions.assertEquals(2, result);

        // Test case 5: No grouping set contains all others
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE);
        List<List<Expression>> disjointSets = ImmutableList.of(
                ImmutableList.of(a, b),     // index 0
                ImmutableList.of(c, d)      // index 1
        );
        result = (int) method.invoke(rule, disjointSets);
        Assertions.assertEquals(-1, result);

        // Test case 6: Multiple sets with same max size, should take first one
        List<List<Expression>> sameSizeSets = ImmutableList.of(
                ImmutableList.of(a, b, c),  // index 0 - first max
                ImmutableList.of(a, b, d),  // index 1 - same size
                ImmutableList.of(a, b)      // index 2
        );
        result = (int) method.invoke(rule, sameSizeSets);
        // Should return 0 if it contains all others, otherwise -1
        // In this case, (a,b,c) doesn't contain (a,b,d), so should return -1
        Assertions.assertEquals(-1, result);
    }

    @Test
    public void testGetAggFuncSlotMap() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("getAggFuncSlotMap", List.class, Map.class);
        method.setAccessible(true);

        SlotReference slot1 = new SlotReference("slot1", IntegerType.INSTANCE);
        SlotReference slot2 = new SlotReference("slot2", IntegerType.INSTANCE);
        SlotReference consumerSlot1 = new SlotReference("consumer_slot1", IntegerType.INSTANCE);
        SlotReference consumerSlot2 = new SlotReference("consumer_slot2", IntegerType.INSTANCE);

        Sum sumFunc = new Sum(slot1);
        Max maxFunc = new Max(slot2);
        Alias sumAlias = new Alias(sumFunc, "sum_alias");
        Alias maxAlias = new Alias(maxFunc, "max_alias");

        List<NamedExpression> outputExpressions = ImmutableList.of(sumAlias, maxAlias);
        Map<Slot, Slot> pToc = new HashMap<>();
        pToc.put(sumAlias.toSlot(), consumerSlot1);
        pToc.put(maxAlias.toSlot(), consumerSlot2);

        @SuppressWarnings("unchecked")
        Map<AggregateFunction, Slot> result = (Map<AggregateFunction, Slot>) method.invoke(rule, outputExpressions, pToc);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(consumerSlot1, result.get(sumFunc));
        Assertions.assertEquals(consumerSlot2, result.get(maxFunc));
    }

    @Test
    public void testGetNeedAddNullExpressions() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("getNeedAddNullExpressions",
                LogicalRepeat.class, List.class, int.class);
        method.setAccessible(true);

        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);

        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(a, b, c),  // index 0 - max
                ImmutableList.of(a, b),     // index 1
                ImmutableList.of(a)         // index 2
        );

        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of());
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                (List) ImmutableList.of(a, b, c),
                null,
                emptyRelation);

        List<List<Expression>> newGroupingSets = ImmutableList.of(
                ImmutableList.of(a, b),
                ImmutableList.of(a)
        );

        @SuppressWarnings("unchecked")
        Set<Expression> result = (Set<Expression>) method.invoke(rule, repeat, newGroupingSets, 0);

        // c should be in the result since it's in max group but not in other groups
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(c));
    }

    @Test
    public void testCanOptimize() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("canOptimize", LogicalAggregate.class);
        method.setAccessible(true);

        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE);

        // Test case 1: Valid rollup with Sum
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(a, b, c, d),
                ImmutableList.of(a, b, c),
                ImmutableList.of(a, b),
                ImmutableList.of(a),
                ImmutableList.of()
        );
        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of());
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                (List) ImmutableList.of(a, b, c, d),
                null,
                emptyRelation);
        Sum sumFunc = new Sum(d);
        Alias sumAlias = new Alias(sumFunc, "sum_d");
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(a, b, c, d),
                ImmutableList.of(a, b, c, d, sumAlias),
                repeat);

        int result = (int) method.invoke(rule, aggregate);
        Assertions.assertEquals(0, result);

        // Test case 2: Child is not LogicalRepeat
        LogicalAggregate<Plan> aggregateWithNonRepeat = new LogicalAggregate<>(
                ImmutableList.of(a),
                ImmutableList.of(a, sumAlias),
                emptyRelation);
        result = (int) method.invoke(rule, aggregateWithNonRepeat);
        Assertions.assertEquals(-1, result);

        // Test case 3: Unsupported aggregate function (Avg)
        org.apache.doris.nereids.trees.expressions.functions.agg.Avg avgFunc =
                new org.apache.doris.nereids.trees.expressions.functions.agg.Avg(d);
        Alias avgAlias = new Alias(avgFunc, "avg_d");
        LogicalAggregate<Plan> aggregateWithCount = new LogicalAggregate<>(
                ImmutableList.of(a, b, c, d),
                ImmutableList.of(a, b, c, d, avgAlias),
                repeat);
        result = (int) method.invoke(rule, aggregateWithCount);
        Assertions.assertEquals(-1, result);

        // Test case 4: Grouping sets size <= 3
        List<List<Expression>> smallGroupingSets = ImmutableList.of(
                ImmutableList.of(a, b),
                ImmutableList.of(a),
                ImmutableList.of()
        );
        LogicalRepeat<Plan> smallRepeat = new LogicalRepeat<>(
                smallGroupingSets,
                (List) ImmutableList.of(a, b),
                null,
                emptyRelation);
        LogicalAggregate<Plan> aggregateWithSmallRepeat = new LogicalAggregate<>(
                ImmutableList.of(a, b),
                ImmutableList.of(a, b, sumAlias),
                smallRepeat);
        result = (int) method.invoke(rule, aggregateWithSmallRepeat);
        Assertions.assertEquals(-1, result);
    }

    @Test
    public void testConstructUnion() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("constructUnion",
                org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class,
                org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class,
                LogicalAggregate.class);
        method.setAccessible(true);

        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        Sum sumFunc = new Sum(b);
        Alias sumAlias = new Alias(sumFunc, "sum_b");

        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of());
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(a, b),
                ImmutableList.of(a),
                ImmutableList.of()
        );
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                (List) ImmutableList.of(a, b),
                new SlotReference("grouping_id", IntegerType.INSTANCE),
                emptyRelation);
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(a, b),
                ImmutableList.of(a, b, sumAlias),
                repeat);

        LogicalProject<Plan> project = new LogicalProject<>(
                ImmutableList.of(a, b, sumAlias.toSlot()),
                aggregate);
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                new CTEId(1), "", new LogicalCTEProducer<>(new CTEId(1), emptyRelation));

        LogicalUnion result = (LogicalUnion) method.invoke(rule, project, consumer, aggregate);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.children().size());
        Assertions.assertTrue(aggregate.getOutputSet().containsAll(result.getOutputSet()));
    }

    @Test
    public void testConstructProducer() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("constructProducer",
                LogicalAggregate.class, int.class, DistinctSelectorContext.class, Map.class);
        method.setAccessible(true);

        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE);

        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.of(a, b, c, d),  // index 0 - max
                ImmutableList.of(a, b, c),
                ImmutableList.of(a, b),
                ImmutableList.of(a),
                ImmutableList.of()
        );
        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of());
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                groupingSets,
                (List) ImmutableList.of(a, b, c, d),
                null,
                emptyRelation);
        Sum sumFunc = new Sum(d);
        Alias sumAlias = new Alias(sumFunc, "sum_d");
        LogicalAggregate<Plan> aggregate = new LogicalAggregate<>(
                ImmutableList.of(a, b, c, d),
                ImmutableList.of(a, b, c, d, sumAlias),
                repeat);

        Map<Slot, Slot> preToCloneSlotMap = new HashMap<>();
        LogicalCTEProducer<LogicalAggregate<Plan>> result = (LogicalCTEProducer<LogicalAggregate<Plan>>)
                method.invoke(rule, aggregate, 0, ctx, preToCloneSlotMap);

        Assertions.assertNotNull(result);
        Assertions.assertNotNull(result.child());
        Assertions.assertInstanceOf(LogicalAggregate.class, result.child());
    }

    @Test
    public void testConstructRepeat() throws Exception {
        Method method = rule.getClass().getDeclaredMethod("constructRepeat",
                LogicalRepeat.class,
                org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class,
                List.class,
                Map.class,
                List.class);
        method.setAccessible(true);

        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        SlotReference consumerA = new SlotReference("consumer_a", IntegerType.INSTANCE);
        SlotReference consumerB = new SlotReference("consumer_b", IntegerType.INSTANCE);
        SlotReference consumerC = new SlotReference("consumer_c", IntegerType.INSTANCE);

        List<List<Expression>> originalGroupingSets = ImmutableList.of(
                ImmutableList.of(a, b, c),
                ImmutableList.of(a, b),
                ImmutableList.of(a)
        );
        LogicalEmptyRelation emptyRelation = new LogicalEmptyRelation(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                ImmutableList.of());
        LogicalRepeat<Plan> originalRepeat = new LogicalRepeat<>(
                originalGroupingSets,
                (List) ImmutableList.of(a, b, c),
                new SlotReference("grouping_id", IntegerType.INSTANCE),
                emptyRelation);

        List<List<Expression>> newGroupingSets = ImmutableList.of(
                ImmutableList.of(a, b),
                ImmutableList.of(a)
        );

        Map<Slot, Slot> producerToConsumerSlotMap = new HashMap<>();
        producerToConsumerSlotMap.put(a, consumerA);
        producerToConsumerSlotMap.put(b, consumerB);
        producerToConsumerSlotMap.put(c, consumerC);

        LogicalCTEConsumer consumer = new LogicalCTEConsumer(
                org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator.newRelationId(),
                new CTEId(1), "", new LogicalCTEProducer<>(new CTEId(1), emptyRelation));
        List<NamedExpression> groupingFunctionSlots = new ArrayList<>();
        LogicalRepeat<Plan> result = (LogicalRepeat<Plan>) method.invoke(rule,
                originalRepeat, consumer, newGroupingSets, producerToConsumerSlotMap, groupingFunctionSlots);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.getGroupingSets().size());
        Assertions.assertTrue(groupingFunctionSlots.isEmpty());
    }
}
