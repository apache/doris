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

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.rules.OneListPartitionEvaluator;
import org.apache.doris.nereids.rules.expression.rules.OnePartitionEvaluator;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class PartitionPrunerTest extends TestWithFeService {
    private Method canBePrunedOutMethod;
    private final Column partitionColumn = new Column("a", PrimitiveType.INT);
    private CascadesContext cascadesContext;
    private final SlotReference slotA = new SlotReference("a", IntegerType.INSTANCE);
    private final SlotReference slotB = new SlotReference("b", IntegerType.INSTANCE);
    private final SlotReference slotC = new SlotReference("c", IntegerType.INSTANCE);

    @Override
    protected void runBeforeAll() throws Exception {
        Class<?> clazz = PartitionPruner.class;
        canBePrunedOutMethod = clazz.getDeclaredMethod("canBePrunedOut", Expression.class, OnePartitionEvaluator.class);
        canBePrunedOutMethod.setAccessible(true);
        cascadesContext = createCascadesContext("select * from t1");
    }

    // test canBePrunedOut res
    // list partition p1, partition value is 1
    // predicate: a = 1
    @Test
    public void testEqualPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);
        Expression predicate = new EqualTo(slotA, Literal.of(1));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertTrue(result.second);
    }

    // list partition p1, partition value is 1, 2
    // predicate: a = 1
    @Test
    public void testEqualPredicate2()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1),
                ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2),
                ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);
        Expression predicate = new EqualTo(slotA, Literal.of(1));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1
    // predicate: a = 2
    @Test
    public void testEqualPredicate3()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);
        Expression predicate = new EqualTo(slotA, Literal.of(2));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertTrue(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1
    // predicate: a = NULL
    @Test
    public void testNullPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new EqualTo(slotA, NullLiteral.INSTANCE);
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertTrue(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2, 3
    // predicate: a IN (1, 2)
    @Test
    public void testInPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionValue partitionValue3 = new PartitionValue("3");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey3 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue3), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2, partitionKey3);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new InPredicate(slotA, ImmutableList.of(Literal.of(1), Literal.of(2)));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2
    // predicate: a IN (1, 2)
    @Test
    public void testInPredicateExactMatch()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new InPredicate(slotA, ImmutableList.of(Literal.of(1), Literal.of(2)));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertTrue(result.second);
    }


    // list partition p1, partition value (1, "a"), (2, "b")
    // predicate: a = 1 AND b = "a"
    @Test
    public void testMultiColumnPartition()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        Column partitionColumn2 = new Column("b", PrimitiveType.VARCHAR);
        SlotReference slot2 = new SlotReference("b", VarcharType.createVarcharType(10));

        PartitionValue partitionValue1a = new PartitionValue("1");
        PartitionValue partitionValue1b = new PartitionValue("a");
        PartitionValue partitionValue2a = new PartitionValue("2");
        PartitionValue partitionValue2b = new PartitionValue("b");

        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(
                ImmutableList.of(partitionValue1a, partitionValue1b),
                ImmutableList.of(partitionColumn, partitionColumn2));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(
                ImmutableList.of(partitionValue2a, partitionValue2b),
                ImmutableList.of(partitionColumn, partitionColumn2));

        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);

        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA, slot2), partitionItem, cascadesContext);

        Expression predicate = new And(
                new EqualTo(slotA, Literal.of(1)),
                new EqualTo(slot2, Literal.of("a"))
        );
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2
    // predicate: a = 1 OR a = 3
    @Test
    public void testOrPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new Or(
                new EqualTo(slotA, Literal.of(1)),
                new EqualTo(slotA, Literal.of(3))
        );
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1
    // predicate: NOT (a = 1)
    @Test
    public void testNotPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue = new PartitionValue("1");
        PartitionKey partitionKey = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new Not(new EqualTo(slotA, Literal.of(1)));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertTrue(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2, 3
    // predicate: a > 2
    @Test
    public void testGreaterThanPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionValue partitionValue3 = new PartitionValue("3");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey3 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue3), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2, partitionKey3);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new GreaterThan(slotA, Literal.of(2));
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // list partition p1, partition value is 1, 2, 3
    // predicate: (a = 1 OR a = 2) AND a > 0
    @Test
    public void testComplexNestedPredicate()
            throws AnalysisException, InvocationTargetException, IllegalAccessException {
        PartitionValue partitionValue1 = new PartitionValue("1");
        PartitionValue partitionValue2 = new PartitionValue("2");
        PartitionValue partitionValue3 = new PartitionValue("3");
        PartitionKey partitionKey1 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue1), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey2 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue2), ImmutableList.of(partitionColumn));
        PartitionKey partitionKey3 = PartitionKey.createPartitionKey(ImmutableList.of(partitionValue3), ImmutableList.of(partitionColumn));
        List<PartitionKey> partitionKeys = ImmutableList.of(partitionKey1, partitionKey2, partitionKey3);
        ListPartitionItem partitionItem = new ListPartitionItem(partitionKeys);
        OneListPartitionEvaluator<String> partitionEvaluator = new OneListPartitionEvaluator<>(
                "p1", ImmutableList.of(slotA), partitionItem, cascadesContext);

        Expression predicate = new And(
                new Or(
                        new EqualTo(slotA, Literal.of(1)),
                        new EqualTo(slotA, Literal.of(2))
                ),
                new GreaterThan(slotA, Literal.of(0))
        );
        Pair<Boolean, Boolean> result = (Pair<Boolean, Boolean>) canBePrunedOutMethod.invoke(null, predicate, partitionEvaluator);
        Assertions.assertFalse(result.first);
        Assertions.assertFalse(result.second);
    }

    // test prune predicate
    // Test basis: some predicates are pruned
    @Test
    public void testPrunePartialPredicates() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        LessThan lt = new LessThan(slotB, Literal.of(20));
        predicates.add(gt);
        predicates.add(lt);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(gt), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(lt));
        Assertions.assertFalse(prunedFilter.getConjuncts().contains(gt));
    }

    // all predicates are pruned
    @Test
    public void testPruneAllPredicates() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        predicates.add(gt);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(gt), filter, scan);

        Assertions.assertInstanceOf(LogicalOlapScan.class, prunedPlan);
    }

    // no predicates are pruned
    @Test
    public void testPruneNoPredicates() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        LessThan lt = new LessThan(slotB, Literal.of(20));
        predicates.add(gt);
        predicates.add(lt);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        EqualTo nonExistentPredicate = new EqualTo(slotC, Literal.of(30));
        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(nonExistentPredicate), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(2, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(gt));
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(lt));
    }

    @Test
    public void testPruneCompoundPredicate() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        LessThan lt = new LessThan(slotB, Literal.of(20));
        EqualTo eq = new EqualTo(slotC, Literal.of(30));
        predicates.add(gt);
        predicates.add(lt);
        predicates.add(eq);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        // (a > 10 AND b < 20)
        And compoundPredicate = new And(gt, lt);
        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(compoundPredicate), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(eq));
        Assertions.assertFalse(prunedFilter.getConjuncts().contains(gt));
        Assertions.assertFalse(prunedFilter.getConjuncts().contains(lt));
    }

    @Test
    public void testSkipPrunePredicate() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        predicates.add(gt);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        Plan prunedPlan = PartitionPruner.prunePredicate(true, Optional.of(gt), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(gt));
    }

    @Test
    public void testEmptyPrunedPredicates() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        predicates.add(gt);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        // prunedPredicates is empty
        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.empty(), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(gt));
    }

    @Test
    public void testPruneDuplicatePredicates() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt1 = new GreaterThan(slotA, Literal.of(10));
        GreaterThan gt2 = new GreaterThan(slotA, Literal.of(10)); // duplicated predicate
        predicates.add(gt1);
        predicates.add(gt2);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(gt1), filter, scan);

        Assertions.assertInstanceOf(LogicalOlapScan.class, prunedPlan);
    }

    @Test
    public void testPruneWithNullLiteral() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        EqualTo nullEq = new EqualTo(slotB, new NullLiteral());
        predicates.add(gt);
        predicates.add(nullEq);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(gt), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(nullEq));
    }

    @Test
    public void testPruneMultiplePredicatesPartially() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        LessThan lt = new LessThan(slotB, Literal.of(20));
        EqualTo eq1 = new EqualTo(slotC, Literal.of(30));
        EqualTo eq2 = new EqualTo(slotC, Literal.of(40));
        predicates.add(gt);
        predicates.add(lt);
        predicates.add(eq1);
        predicates.add(eq2);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        // (a > 10 AND b < 20)
        And compoundPredicate = new And(gt, lt);
        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(compoundPredicate), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(2, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(eq1));
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(eq2));
        Assertions.assertFalse(prunedFilter.getConjuncts().contains(gt));
        Assertions.assertFalse(prunedFilter.getConjuncts().contains(lt));
    }

    @Test
    public void testPruneNestedCompoundPredicate() {
        Set<Expression> predicates = new LinkedHashSet<>();
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        LessThan lt = new LessThan(slotB, Literal.of(20));
        EqualTo eq = new EqualTo(slotC, Literal.of(30));
        predicates.add(gt);
        predicates.add(lt);
        predicates.add(eq);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        // (a > 10 AND (b < 20 AND c = 30))
        And innerAnd = new And(lt, eq);
        And outerAnd = new And(gt, innerAnd);

        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(outerAnd), filter, scan);

        Assertions.assertInstanceOf(LogicalOlapScan.class, prunedPlan);
    }

    @Test
    public void testPruneWhenFilterContainsOr() {
        Set<Expression> predicates = new LinkedHashSet<>();
        Or orPredicate = new Or(
                new GreaterThan(slotA, Literal.of(10)),
                new LessThan(slotB, Literal.of(20))
        );
        predicates.add(orPredicate);
        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);

        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));
        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(gt), filter, scan);

        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;
        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(orPredicate));
    }

    @Test
    public void testPruneWhenFilterContainsAndOrMix() {
        Set<Expression> predicates = new LinkedHashSet<>();
        // filter :a > 10 AND (b < 20 OR c = 30)
        Or orPredicate = new Or(
                new LessThan(slotB, Literal.of(20)),
                new EqualTo(slotC, Literal.of(30))
        );
        GreaterThan gt = new GreaterThan(slotA, Literal.of(10));

        predicates.add(gt);
        predicates.add(orPredicate);

        LogicalOlapScan scan = new LogicalOlapScan(new RelationId(1), new OlapTable());
        LogicalFilter<Plan> filter = new LogicalFilter<>(predicates, scan);
        // a > 10
        Plan prunedPlan = PartitionPruner.prunePredicate(false, Optional.of(gt), filter, scan);
        Assertions.assertInstanceOf(LogicalFilter.class, prunedPlan);
        LogicalFilter<?> prunedFilter = (LogicalFilter<?>) prunedPlan;

        Assertions.assertEquals(1, prunedFilter.getConjuncts().size());
        Assertions.assertTrue(prunedFilter.getConjuncts().contains(orPredicate));
    }
}


