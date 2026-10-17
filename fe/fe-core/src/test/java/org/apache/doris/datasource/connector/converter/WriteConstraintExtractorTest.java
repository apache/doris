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
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
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

    @BeforeEach
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

        Assertions.assertTrue(result.isPresent());
        ConnectorExpression expr = result.get().getExpression();
        Assertions.assertTrue(expr instanceof ConnectorComparison);
        ConnectorComparison cmp = (ConnectorComparison) expr;
        Assertions.assertEquals(ConnectorComparison.Operator.EQ, cmp.getOperator());
        Assertions.assertEquals("id", ((ConnectorColumnRef) cmp.getLeft()).getColumnName());
    }

    @Test
    public void crossTablePredicateIsDropped() {
        TableIf other = Mockito.mock(TableIf.class);
        Mockito.when(other.getId()).thenReturn(2L);
        SlotReference slot = slot(other, "id", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(slot, new IntegerLiteral(1))), slot);

        Assertions.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
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

        Assertions.assertTrue(result.isPresent());
        // only the single target-arm survives -> a lone comparison, not an AND of both
        Assertions.assertTrue(result.get().getExpression() instanceof ConnectorComparison);
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

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get().getExpression() instanceof ConnectorAnd);
        Assertions.assertEquals(2, ((ConnectorAnd) result.get().getExpression()).getConjuncts().size());
    }

    @Test
    public void injectedExclusionDropsSyntheticColumnConjunct() {
        // Load-bearing (critic finding 1 = BLOCKER): a synthetic column slot built via fromColumn has
        // getOriginalTable() == target, so the origin-table check alone would let it through. Only the
        // injected exclusion predicate drops it. Prove both halves.
        SlotReference synthetic = slot(targetTable, "rowid_col", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(synthetic, new IntegerLiteral(1))), synthetic);

        Assertions.assertTrue(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent(), "without exclusion the synthetic-column conjunct slips through");

        Predicate<SlotReference> excludeRowId = s -> "rowid_col".equalsIgnoreCase(s.getName());
        Assertions.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, excludeRowId).isPresent(), "the injected exclusion predicate must drop the synthetic-column conjunct");
    }

    @Test
    public void targetConjunctUnrepresentableByConverterIsDropped() {
        // a target-only predicate the neutral converter cannot represent (column-to-column) yields nothing
        SlotReference a = slot(targetTable, "id", ScalarType.INT);
        SlotReference b = slot(targetTable, "v", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(a, b)), a);

        Assertions.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
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

        Assertions.assertTrue(result.isPresent(), "the convertible target conjunct survives");
        Assertions.assertTrue(result.get().getExpression() instanceof ConnectorComparison, "only the convertible arm remains -> a lone comparison, not an AND of one");
        Assertions.assertEquals("id",
                ((ConnectorColumnRef) ((ConnectorComparison) result.get().getExpression()).getLeft())
                        .getColumnName());
    }

    @Test
    public void conjunctWithoutInputSlotsIsDropped() {
        SlotReference slot = slot(targetTable, "id", ScalarType.INT);
        Plan plan = filterOver(ImmutableSet.of(BooleanLiteral.of(true)), slot);

        Assertions.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
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

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get().getExpression() instanceof ConnectorAnd);
        Assertions.assertEquals(2, ((ConnectorAnd) result.get().getExpression()).getConjuncts().size());
    }

    @Test
    public void planWithoutFilterReturnsEmpty() {
        SlotReference slot = slot(targetTable, "id", ScalarType.INT);
        Plan plan = new LogicalEmptyRelation(new RelationId(0), ImmutableList.of((NamedExpression) slot));

        Assertions.assertFalse(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent());
    }

    @Test
    public void nullPlanReturnsEmpty() {
        Assertions.assertFalse(WriteConstraintExtractor.extract(null, TARGET_ID, NO_EXCLUSION).isPresent());
    }

    // ───────── complete-predicate extraction (the whole-file "metadata delete" fast path) ─────────

    /**
     * The complete variant only accepts a plan that is provably "target table → filter(s) → sink": the leaf must
     * be the target {@link org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation}, so a
     * {@link LogicalEmptyRelation} (which cannot even arise from a DELETE, but would mean "no rows") is rejected
     * rather than read as an unfiltered delete of everything.
     */
    private Plan completePlanOver(Set<Expression> conjuncts, SlotReference output) {
        ExternalTable externalTarget = targetTableWithId();
        return new LogicalFilter<>(conjuncts, new TargetScan(externalTarget, ImmutableList.of(output)));
    }

    private SlotReference externalSlot(ExternalTable table, String name) {
        return SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table,
                new Column(name, ScalarType.INT), ImmutableList.of());
    }

    private ExternalTable targetTableWithId() {
        ExternalTable externalTarget = Mockito.mock(ExternalTable.class);
        Mockito.when(externalTarget.getId()).thenReturn(TARGET_ID);
        return externalTarget;
    }

    /** A minimal concrete target-table scan standing in for the DELETE's {@code LogicalFileScan} leaf. */
    private static final class TargetScan extends LogicalCatalogRelation {
        private final List<Slot> output;

        TargetScan(TableIf table, List<Slot> output) {
            super(new RelationId(0), PlanType.LOGICAL_FILE_SCAN, table, ImmutableList.of("db1"));
            this.output = ImmutableList.copyOf(output);
        }

        @Override
        public List<Slot> computeOutput() {
            return output;
        }

        @Override
        public LogicalCatalogRelation withRelationId(RelationId relationId) {
            return this;
        }

        @Override
        public LogicalCatalogRelation withTableAlias(String tableAlias) {
            return this;
        }

        @Override
        public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                Optional<LogicalProperties> logicalProperties, List<Plan> children) {
            return this;
        }

        @Override
        public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
            return this;
        }
    }

    @Test
    public void completePredicateIsExtractedOverTheTargetScan() {
        ExternalTable externalTarget = targetTableWithId();
        SlotReference id = externalSlot(externalTarget, "id");

        Optional<ConnectorExpression> result = WriteConstraintExtractor.extractCompletePredicate(
                completePlanOver(ImmutableSet.of(new EqualTo(id, new IntegerLiteral(1))), id),
                TARGET_ID, NO_EXCLUSION);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof ConnectorComparison);
        Assertions.assertEquals("id",
                ((ConnectorColumnRef) ((ConnectorComparison) result.get()).getLeft()).getColumnName());
    }

    @Test
    public void completePredicateWithoutWhereClauseIsAlwaysTrue() {
        ExternalTable externalTarget = targetTableWithId();
        TargetScan scan = new TargetScan(externalTarget, ImmutableList.of());

        Optional<ConnectorExpression> result =
                WriteConstraintExtractor.extractCompletePredicate(scan, TARGET_ID, NO_EXCLUSION);

        Assertions.assertTrue(result.isPresent(), "DELETE without WHERE is an always-true predicate");
        Assertions.assertTrue(result.get() instanceof ConnectorLiteral);
        Assertions.assertEquals(Boolean.TRUE, ((ConnectorLiteral) result.get()).getValue());
    }

    @Test
    public void completePredicateBailsOutWhenAnyConjunctIsUnrepresentable() {
        // The loose variant drops the column-to-column arm (safe there: it only widens a conflict filter), but a
        // whole-file delete must not: a dropped conjunct would drop files that still hold live rows.
        ExternalTable externalTarget = targetTableWithId();
        SlotReference id = externalSlot(externalTarget, "id");
        SlotReference v = externalSlot(externalTarget, "v");
        SlotReference w = externalSlot(externalTarget, "w");
        Plan plan = completePlanOver(ImmutableSet.of(
                new EqualTo(id, new IntegerLiteral(1)),
                new EqualTo(v, w)), id);

        Assertions.assertTrue(WriteConstraintExtractor.extract(plan, TARGET_ID, NO_EXCLUSION).isPresent(),
                "the loose extractor keeps the representable arm");
        Assertions.assertFalse(
                WriteConstraintExtractor.extractCompletePredicate(plan, TARGET_ID, NO_EXCLUSION).isPresent(),
                "the complete extractor must refuse the whole predicate instead of guessing");
    }

    @Test
    public void completePredicateBailsOutOnCrossTableConjunct() {
        ExternalTable externalTarget = targetTableWithId();
        TableIf other = Mockito.mock(TableIf.class);
        Mockito.when(other.getId()).thenReturn(2L);
        SlotReference id = externalSlot(externalTarget, "id");
        SlotReference otherId = slot(other, "id", ScalarType.INT);

        Plan plan = completePlanOver(ImmutableSet.of(
                new EqualTo(id, new IntegerLiteral(1)),
                new EqualTo(otherId, new IntegerLiteral(2))), id);

        Assertions.assertFalse(WriteConstraintExtractor.extractCompletePredicate(plan, TARGET_ID, NO_EXCLUSION)
                .isPresent(), "a conjunct over another table cannot be honoured by a whole-file delete");
    }

    @Test
    public void completePredicateIgnoresSyntheticConjuncts() {
        ExternalTable externalTarget = targetTableWithId();
        SlotReference id = externalSlot(externalTarget, "id");
        SlotReference rowId = externalSlot(externalTarget, "__DORIS_ICEBERG_ROWID_COL__");
        Plan plan = completePlanOver(ImmutableSet.of(
                new EqualTo(id, new IntegerLiteral(1)),
                new EqualTo(rowId, new IntegerLiteral(0))), id);
        Predicate<SlotReference> excludeRowId = s -> "__DORIS_ICEBERG_ROWID_COL__".equalsIgnoreCase(s.getName());

        Optional<ConnectorExpression> result =
                WriteConstraintExtractor.extractCompletePredicate(plan, TARGET_ID, excludeRowId);

        Assertions.assertTrue(result.isPresent(), "engine-injected synthetic conjuncts are not user predicates");
        Assertions.assertTrue(result.get() instanceof ConnectorComparison);
        Assertions.assertEquals("id",
                ((ConnectorColumnRef) ((ConnectorComparison) result.get()).getLeft()).getColumnName());
    }

    @Test
    public void completePredicateBailsOutOnNonLinearPlan() {
        // Anything that can restrict rows without showing up as a LogicalFilter (a join, set operation,
        // aggregate, limit, ...) must disable the fast path: its predicate is not part of what we can see.
        Plan join = Mockito.mock(LogicalJoin.class);

        Assertions.assertFalse(WriteConstraintExtractor.extractCompletePredicate(join, TARGET_ID, NO_EXCLUSION)
                .isPresent());
    }

    @Test
    public void completePredicateBailsOutOnEmptyRelationLeaf() {
        SlotReference id = externalSlot(targetTableWithId(), "id");
        Plan plan = filterOver(ImmutableSet.of(new EqualTo(id, new IntegerLiteral(1))), id);

        Assertions.assertFalse(WriteConstraintExtractor.extractCompletePredicate(plan, TARGET_ID, NO_EXCLUSION)
                .isPresent(), "a non-table leaf cannot prove the delete touches whole table files");
    }
}
