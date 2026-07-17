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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Unit tests for {@link WriteConstraintExtractor} (P6.3-T07b, O5-2 production half).
 *
 * <p>The extractor walks an analyzed DELETE/UPDATE/MERGE plan and keeps only the conjuncts that reference
 * solely the target table's own columns (slot origin-table == target), excluding synthetic / metadata
 * columns via an injected {@link Predicate} (the iceberg-specific exclusion is supplied by the row-level DML
 * transform in T07c — here a generic name-based predicate stands in). It mirrors the generic collection half
 * of legacy {@code IcebergConflictDetectionFilterUtils} but is connector-neutral
 * ({@code long targetTableId} + neutral {@link ConnectorPredicate} output).</p>
 */
public class WriteConstraintExtractorTest {

    private static final long TARGET_ID = 1L;
    private static final Predicate<SlotReference> NO_EXCLUSION = s -> false;

    private TableIf targetTable;

    @Before
    public void setUp() {
        targetTable = Mockito.mock(TableIf.class);
        Mockito.when(targetTable.getId()).thenReturn(TARGET_ID);
    }

    private SlotReference slot(TableIf table, String name, ScalarType type) {
        Column column = new Column(name, type);
        return SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table, column, ImmutableList.of());
    }

    private Plan filterOver(Set<Expression> conjuncts, SlotReference output) {
        LogicalEmptyRelation child = new LogicalEmptyRelation(new RelationId(0),
                ImmutableList.of((NamedExpression) output));
        return new LogicalFilter<>(conjuncts, child);
    }

    @Test
    public void targetOnlyPredicateIsKept() {
        SlotReference slot = slot(targetTable, "id", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1))), slot);

        Optional<ConnectorPredicate> result = WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION);

        Assert.assertTrue(result.isPresent());
        ConnectorExpression expr = result.get().getExpression();
        Assert.assertTrue(expr instanceof ConnectorComparison);
        ConnectorComparison cmp = (ConnectorComparison) expr;
        Assert.assertEquals(ConnectorComparison.Operator.EQ, cmp.getOperator());
        Assert.assertEquals("id", ((ConnectorColumnRef) cmp.getLeft()).getColumnName());
    }

    @Test
    public void crossTablePredicateIsDropped() {
        TableIf other = Mockito.mock(TableIf.class);
        Mockito.when(other.getId()).thenReturn(2L);
        SlotReference slot = slot(other, "id", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1))), slot);

        Assert.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
    }

    @Test
    public void mixedConjunctsKeepOnlyTargetArm() {
        TableIf other = Mockito.mock(TableIf.class);
        Mockito.when(other.getId()).thenReturn(2L);
        SlotReference targetSlot = slot(targetTable, "id", ScalarType.INT);
        SlotReference otherSlot = slot(other, "id", ScalarType.INT);
        Set<Expression> conjuncts = ImmutableSet.of(
                new EqualTo(targetSlot, new IntegerLiteral(1)),
                new EqualTo(otherSlot, new IntegerLiteral(2)));
        Plan plan = filterOver(conjuncts, targetSlot);

        Optional<ConnectorPredicate> result = WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION);

        Assert.assertTrue(result.isPresent());
        // only the single target-arm survives -> a lone comparison, not an AND of both
        Assert.assertTrue(result.get().getExpression() instanceof ConnectorComparison);
    }

    @Test
    public void multipleTargetConjunctsAreAnded() {
        SlotReference a = slot(targetTable, "id", ScalarType.INT);
        SlotReference b = slot(targetTable, "v", ScalarType.INT);
        Set<Expression> conjuncts = ImmutableSet.of(
                new EqualTo(a, new IntegerLiteral(1)),
                new GreaterThan(b, new IntegerLiteral(2)));
        Plan plan = filterOver(conjuncts, a);

        Optional<ConnectorPredicate> result = WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION);

        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get().getExpression() instanceof ConnectorAnd);
        Assert.assertEquals(2, ((ConnectorAnd) result.get().getExpression()).getConjuncts().size());
    }

    @Test
    public void injectedExclusionDropsSyntheticColumnConjunct() {
        // Load-bearing (critic finding 1 = BLOCKER): a synthetic column slot built via fromColumn has
        // getOriginalTable() == target, so the origin-table check alone would let it through. Only the
        // injected exclusion predicate drops it. Prove both halves.
        SlotReference synthetic = slot(targetTable, "rowid_col", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(synthetic, new IntegerLiteral(1))), synthetic);

        Assert.assertTrue("without exclusion the synthetic-column conjunct slips through",
                WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());

        Predicate<SlotReference> excludeRowId = s -> "rowid_col".equalsIgnoreCase(s.getName());
        Assert.assertFalse("the injected exclusion predicate must drop the synthetic-column conjunct",
                WriteConstraintExtractor.extract(plan, TARGET_ID, excludeRowId).isPresent());
    }

    @Test
    public void targetConjunctUnrepresentableByConverterIsDropped() {
        // a target-only predicate the neutral converter cannot represent (column-to-column) yields nothing
        SlotReference a = slot(targetTable, "id", ScalarType.INT);
        SlotReference b = slot(targetTable, "v", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(a, b)), a);

        Assert.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
    }

    @Test
    public void targetConjunctsDropOnlyTheUnconvertibleArm() {
        // O5-2-GAP-001: with two TARGET-only conjuncts where one is convertible (id = 1) and the other is an
        // unconvertible column-to-column comparison (v = w), the converter drops only the unconvertible conjunct
        // (per-conjunct, NOT the whole AND) -> a lone surviving comparison. multipleTargetConjunctsAreAnded covers
        // two convertible arms; targetConjunctUnrepresentableByConverterIsDropped covers a single unconvertible
        // arm; neither covers per-conjunct drop inside a multi-conjunct AND (which only widens the filter -> safe).
        SlotReference id = slot(targetTable, "id", ScalarType.INT);
        SlotReference v = slot(targetTable, "v", ScalarType.INT);
        SlotReference w = slot(targetTable, "w", ScalarType.INT);
        Set<Expression> conjuncts = ImmutableSet.of(
                new EqualTo(id, new IntegerLiteral(1)),     // convertible
                new EqualTo(v, w));                          // target-only, column-to-column -> unconvertible

        Optional<ConnectorPredicate> result =
                WriteConstraintExtractor.extract(filterOver(conjuncts, id), TARGET_ID, NO_EXCLUSION);

        Assert.assertTrue("the convertible target conjunct survives", result.isPresent());
        Assert.assertTrue("only the convertible arm remains -> a lone comparison, not an AND of one",
                result.get().getExpression() instanceof ConnectorComparison);
        Assert.assertEquals("id",
                ((ConnectorColumnRef) ((ConnectorComparison) result.get().getExpression()).getLeft())
                        .getColumnName());
    }

    @Test
    public void conjunctWithoutInputSlotsIsDropped() {
        SlotReference slot = slot(targetTable, "id", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(BooleanLiteral.of(true)), slot);

        Assert.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
    }

    @Test
    public void recursesIntoChildFilters() {
        SlotReference slot = slot(targetTable, "id", ScalarType.INT);
        LogicalEmptyRelation leaf = new LogicalEmptyRelation(new RelationId(0),
                ImmutableList.of((NamedExpression) slot));
        LogicalFilter<?> inner = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1))), leaf);
        LogicalFilter<?> outer = new LogicalFilter<>(
                ImmutableSet.of(new GreaterThan(slot, new IntegerLiteral(0))), inner);

        Optional<ConnectorPredicate> result = WriteConstraintExtractor.extract(outer, TARGET_ID, NO_EXCLUSION);

        Assert.assertTrue(result.isPresent());
        Assert.assertTrue(result.get().getExpression() instanceof ConnectorAnd);
        Assert.assertEquals(2, ((ConnectorAnd) result.get().getExpression()).getConjuncts().size());
    }

    @Test
    public void planWithoutFilterReturnsEmpty() {
        SlotReference slot = slot(targetTable, "id", ScalarType.INT);
        Plan plan = new LogicalEmptyRelation(new RelationId(0), ImmutableList.of((NamedExpression) slot));

        Assert.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
    }

    @Test
    public void nullPlanReturnsEmpty() {
        Assert.assertFalse(WriteConstraintExtractor.extract(null, TARGET_ID, NO_EXCLUSION).isPresent());
    }
}
