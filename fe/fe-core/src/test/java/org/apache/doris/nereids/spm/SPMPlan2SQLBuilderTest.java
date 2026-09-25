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

package org.apache.doris.nereids.spm;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.spm.builder.SPMExprSqlBuilder;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.spm.builder.SQLRelation;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BitNot;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.generator.Explode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalExcept;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalGenerate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIntersect;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterializeFileScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWorkTableReference;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * M1 milestone test: SPMPlan2SQLBuilder (physical plan decompiler).
 *
 * Verifies three levels:
 *
 * 1. The data structure behavior of SQLRelation (inline / subquery wrap / toSQL)
 * 2. The expression printing of SPMExprSqlBuilder (column mapping, predicates, functions)
 * 3. The whole-tree decompilation of SPMPlan2SQLBuilder over a physical plan
 */
public class SPMPlan2SQLBuilderTest {

    /** SQLRelation's table-alias sequence is per-thread and reset at each decompile;
     * reset it before each test so alias assertions (t_0 ...) are deterministic
     * regardless of test execution order. */
    @BeforeEach
    public void resetAliasCounter() {
        SQLRelation.resetAliasCounter();
    }

    // ==================== SQLRelation tests ====================

    @Test
    public void testRelationInline() {
        SQLRelation relation = new SQLRelation();
        relation.setFrom("t1");
        // no newAlias -> inline, toRelationSQL returns the table name directly
        Assertions.assertEquals("t1", relation.toRelationSQL());
    }

    @Test
    public void testRelationWrap() {
        SQLRelation relation = new SQLRelation();
        relation.setFrom("t1");
        relation.setWhere("c_3 > 100");
        relation.newAlias();
        // after newAlias -> wrapped as (SELECT * FROM t1 WHERE c_3 > 100) t_0
        Assertions.assertEquals(
                "(SELECT * FROM t1 WHERE c_3 > 100) t_0",
                relation.toRelationSQL());
    }

    @Test
    public void testRelationToSqlFields() {
        SQLRelation relation = new SQLRelation();
        relation.setFrom("t1");
        relation.setWhere("a > 100");
        relation.setGroupBy("a");
        relation.setHaving("sum(b) > 0");
        relation.setOrderBy("a ASC");
        relation.setLimit("10");
        Assertions.assertEquals(
                "SELECT * FROM t1 WHERE a > 100 GROUP BY a HAVING sum(b) > 0 ORDER BY a ASC LIMIT 10",
                relation.toSQL());
    }

    // ==================== SPMExprSqlBuilder tests ====================

    @Test
    public void testExprSlotRename() {
        // After registering the column mapping, SlotReference prints the mapped name
        SQLRelation relation = new SQLRelation();
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        relation.registerRef(a.getExprId(), "c_5");
        Assertions.assertEquals("c_5", new SPMExprSqlBuilder().print(a, relation));
    }

    @Test
    public void testExprComparison() {
        SQLRelation relation = new SQLRelation();
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        relation.registerRef(a.getExprId(), "c_5");
        // a > 100 -> (c_5 > 100)
        Expression pred = new GreaterThan(a, new IntegerLiteral(100));
        Assertions.assertEquals("(c_5 > 100)", new SPMExprSqlBuilder().print(pred, relation));
    }

    @Test
    public void testExprJoinPredicate() {
        SQLRelation relation = new SQLRelation();
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        relation.registerRef(a.getExprId(), "a");
        relation.registerRef(b.getExprId(), "b");
        // a = b -> (a = b)
        Expression pred = new EqualTo(a, b);
        Assertions.assertEquals("(a = b)", new SPMExprSqlBuilder().print(pred, relation));
    }

    // ==================== SPMPlan2SQLBuilder decompile tests ====================

    @Test
    public void testDecompileScanFilterProject() {
        // Build the physical plan tree (Mockito mocks):
        //   PhysicalProject [a, b]
        //     - PhysicalFilter [a > 100]
        //         - PhysicalOlapScan [t1]
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan scan = mockScan("t1", List.of(a, b));
        PhysicalFilter filter = mockFilter(new GreaterThan(a, new IntegerLiteral(100)), scan);
        PhysicalProject project = mockProject(List.of(a, b), filter);

        String sql = new SPMPlan2SQLBuilder().toSQL(project);
        // expected: SELECT a, b FROM (SELECT * FROM t1 WHERE (a > 100)) t_N
        Assertions.assertTrue(sql.contains("SELECT a, b FROM (SELECT * FROM t1 WHERE (a > 100))"));
    }

    @Test
    public void testDecompileJoin() {
        // Build a two-table JOIN physical plan:
        //   PhysicalHashJoin(INNER) [t1.a = t2.b]
        //     - PhysicalOlapScan [t1]
        //     - PhysicalOlapScan [t2]
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t2", List.of(b));
        PhysicalHashJoin join = mockJoin(left, right, new EqualTo(a, b));

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        // expected to contain INNER JOIN and the ON condition
        Assertions.assertTrue(sql.contains("INNER JOIN"));
        Assertions.assertTrue(sql.contains("ON (a = b)"));
    }

    @Test
    public void testDecompileSelfJoinForcesWrap() {
        // Both sides scan the same table (t1) -> same relation alias -> the decompiler
        // forces both sides to wrap as subqueries so the FROM clause stays unambiguous
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t1", List.of(b));
        PhysicalHashJoin join = mockJoin(left, right, new EqualTo(a, b));

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        // no inline "t1 INNER JOIN t1"
        Assertions.assertFalse(sql.contains("t1 INNER JOIN t1"),
                "self join must not inline both sides: " + sql);
        Assertions.assertTrue(sql.contains("(SELECT * FROM t1) t_0"), sql);
        Assertions.assertTrue(sql.contains("(SELECT * FROM t1) t_1"), sql);
    }

    @Test
    public void testDecompileColumnNameConflictQualifies() {
        // Both sides register a column under the same SQL name ("a") -> the join
        // relation qualifies the references with the side alias
        SlotReference a1 = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference a2 = new SlotReference("a", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockScan("t1", List.of(a1));
        PhysicalOlapScan right = mockScan("t2", List.of(a2));
        PhysicalHashJoin join = mockJoin(left, right, new EqualTo(a1, a2));

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("t1.a") && sql.contains("t2.a"),
                "conflicting column references must be qualified: " + sql);
    }

    // ==================== MARK / NULL_AWARE join decompile ====================
    // 2-valued EXISTS / NOT EXISTS MARK joins (empty mark conjuncts) decompile to the
    // native SEMI/ANTI MARK JOIN keyword so the shape is pinned and replayed; the
    // ==================== MARK / NULL_AWARE join decompile ====================
    // A MARK join is any SEMI/ANTI join carrying a mark slot; the MARK /
    // MARK_CONDITION / MARK_SLOT keywords are the SQL surface that passes (markSlot,
    // markConjuncts) back into the same SEMI/ANTI join, so EVERY MARK join decompiles
    // natively (comments2):
    //  1. correlation in ON, no mark key : SEMI/ANTI MARK JOIN ... MARK_SLOT m ON <conds>
    //  2. only a mark key                : SEMI/ANTI MARK JOIN ... MARK_CONDITION(<key>) MARK_SLOT m ON true
    //  3. correlation in ON + mark key   : SEMI/ANTI MARK JOIN ... MARK_CONDITION(<key>) MARK_SLOT m ON <conds>
    // ASOF is LEFT-direction only. The non-mark NULL_AWARE_LEFT_ANTI (WHERE NOT IN
    // filter) with residual conjuncts / no key equality stays on the NOT IN rewrite
    // (decompileNullAwareAnti).

    @Test
    public void testDecompileNativeSemiMarkJoin() {
        //   PhysicalHashJoin LEFT_SEMI isMarkJoin=true, mark slot m, hash [t1.a = t2.b]
        //     - PhysicalOlapScan [t1]
        //     - PhysicalOlapScan [t2]
        //   source query: SELECT * FROM t1 LEFT SEMI MARK JOIN t2 MARK_SLOT m ON t1.a = t2.b
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        MarkJoinSlotReference markSlot = new MarkJoinSlotReference("m");

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t2", List.of(b));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(a, b)));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.of(markSlot));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.LEFT_SEMI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(true);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("LEFT SEMI MARK JOIN"),
                "native mark join keyword expected: " + sql);
        Assertions.assertTrue(sql.contains("MARK_SLOT c_"),
                "mark slot must be decompiled natively: " + sql);
        Assertions.assertTrue(sql.contains("ON (a = b)"), sql);
        Assertions.assertFalse(sql.contains("EXISTS"), "no EXISTS rewrite expected: " + sql);
    }

    @Test
    public void testDecompileAntiMarkJoinMarkKeyOnTrue() {
        // Shape 2: a mark join with NO correlation and only the (three-valued) NOT IN
        // key in its mark conjuncts (a standalone "x NOT IN (sub)" boolean output). It
        // decompiles natively as LEFT ANTI MARK JOIN ... MARK_CONDITION(<key>) MARK_SLOT
        // m ON true - the literal-true ON keeps the SEMI/ANTI join parseable while hash /
        // other stay empty, so the translator derives the null-aware operator for BE.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        MarkJoinSlotReference markSlot = new MarkJoinSlotReference("m");

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t2", List.of(b));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of(new EqualTo(a, b)));
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.of(markSlot));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.LEFT_ANTI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(true);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("LEFT ANTI MARK JOIN"), sql);
        Assertions.assertTrue(sql.contains("MARK_CONDITION((a = b))"),
                "mark key must be carried by MARK_CONDITION: " + sql);
        Assertions.assertTrue(sql.contains("MARK_SLOT c_"), sql);
        Assertions.assertTrue(sql.contains("ON true"),
                "mark-key-only join needs ON true to stay parseable: " + sql);
        Assertions.assertFalse(sql.contains("NOT IN (SELECT"), "no NOT IN rewrite expected: " + sql);
    }

    @Test
    public void testDecompileSemiMarkJoinMarkKeyAndOn() {
        // Shape 3: correlated IN mark - the hash conjunct carries the correlation
        // (t1.a = t2.c), the mark conjunct the (three-valued) IN key (t1.b = t2.d). It
        // decompiles natively as SEMI MARK JOIN ... MARK_CONDITION(<key>) MARK_SLOT m
        // ON <correlation>.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE); // t1.a correlation key
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE); // t1.b IN probe
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE); // t2.c correlation build
        SlotReference d = new SlotReference("d", IntegerType.INSTANCE); // t2.d IN build
        MarkJoinSlotReference markSlot = new MarkJoinSlotReference("m");

        PhysicalOlapScan left = mockScan("t1", List.of(a, b));
        PhysicalOlapScan right = mockScan("t2", List.of(c, d));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(a, c)));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of(new EqualTo(b, d)));
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.of(markSlot));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.LEFT_SEMI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(true);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("LEFT SEMI MARK JOIN"), sql);
        Assertions.assertTrue(sql.contains("MARK_CONDITION((b = d))"), sql);
        Assertions.assertTrue(sql.contains("MARK_SLOT c_"), sql);
        Assertions.assertTrue(sql.contains("ON (a = c)"),
                "correlation must be the ON clause: " + sql);
        Assertions.assertFalse(sql.contains(" IN (SELECT"), "no IN rewrite expected: " + sql);
    }

    @Test
    public void testDecompileNativeNullAwareAnti() {
        // Clean equi-key NULL-AWARE anti (no residual conjuncts, one hash key): native
        // LEFT NULL_AWARE ANTI JOIN keyword. A standard ANTI JOIN is not three-valued,
        // so the keyword is what keeps the NOT IN semantics during a replay.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t2", List.of(b));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(a, b)));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.empty());
        Mockito.when(join.getJoinType()).thenReturn(JoinType.NULL_AWARE_LEFT_ANTI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(false);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("LEFT NULL_AWARE ANTI JOIN"),
                "native NULL_AWARE keyword expected: " + sql);
        Assertions.assertTrue(sql.contains("ON (a = b)"), sql);
        Assertions.assertFalse(sql.contains("NOT IN"), "no NOT IN rewrite expected: " + sql);
    }

    @Test
    public void testDecompileNullAwareAntiResidualStaysInSubquery() {
        // NULL-AWARE anti whose build side carries a residual conjunct: the conjunct
        // must stay INSIDE the NOT IN subquery (three-valued), so this corner case
        // still uses the subquery rewrite instead of the native keyword.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t2", List.of(a, b));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(a, b)));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of(new GreaterThan(b, new IntegerLiteral(1))));
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.empty());
        Mockito.when(join.getJoinType()).thenReturn(JoinType.NULL_AWARE_LEFT_ANTI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(false);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("NOT IN (SELECT"),
                "residual conjunct forces the NOT IN subquery rewrite: " + sql);
        Assertions.assertFalse(sql.contains("NULL_AWARE"), sql);
    }

    @Test
    public void testDecompileNullAwareTypedMarkJoinUnsupported() {
        // The optimizer never produces a NULL_AWARE-typed MARK join (SELECT-list IN /
        // NOT IN marks keep the LEFT/RIGHT SEMI/ANTI types); if one ever reached the
        // decompiler it has no SQL keyword, so it must fail loudly instead of silently
        // freezing an unrepresentable shape.
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        MarkJoinSlotReference markSlot = new MarkJoinSlotReference("m");

        PhysicalOlapScan left = mockScan("t1", List.of(a));
        PhysicalOlapScan right = mockScan("t2", List.of(b));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of(new EqualTo(a, b)));
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.of(markSlot));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.NULL_AWARE_LEFT_ANTI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(true);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new SPMPlan2SQLBuilder().toSQL(join));
    }

    // ==================== GROUPING SETS (PhysicalRepeat) ====================

    @Test
    public void testDecompileGroupingSets() {
        // PhysicalHashAggregate(GLOBAL) over PhysicalRepeat over scan:
        // GROUP BY GROUPING SETS((a), (a, b))
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan scan = mockScan("t1", List.of(a, b));

        PhysicalRepeat<?> repeat = Mockito.mock(PhysicalRepeat.class);
        Mockito.when(repeat.child(0)).thenReturn(scan);
        Mockito.when(repeat.getGroupingSets())
                .thenReturn(List.of(List.of(a), List.of(a, b)));
        stubAccept(repeat);

        PhysicalHashAggregate<?> agg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(agg.child(0)).thenReturn(repeat);
        Mockito.when(agg.getAggPhase()).thenReturn(org.apache.doris.nereids.trees.plans.AggPhase.GLOBAL);
        Mockito.when(agg.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(agg.getOutputExpressions()).thenReturn(List.of());
        stubAccept(agg);

        String sql = new SPMPlan2SQLBuilder().toSQL(agg);
        Assertions.assertTrue(sql.contains("GROUPING SETS((a), (a, b))"),
                "GROUPING SETS must be reconstructed: " + sql);
    }

    // ==================== ASSERT_ROWS (PhysicalAssertNumRows) ====================

    @Test
    public void testDecompileAssertNumRows() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(a));

        PhysicalAssertNumRows<?> assertNumRows = Mockito.mock(PhysicalAssertNumRows.class);
        Mockito.when(assertNumRows.child(0)).thenReturn(scan);
        stubAccept(assertNumRows);

        String sql = new SPMPlan2SQLBuilder().toSQL(assertNumRows);
        Assertions.assertTrue(sql.startsWith("ASSERT_ROWS ("),
                "assert-num-rows must be decompiled as ASSERT_ROWS: " + sql);
    }

    // ==================== test helpers ====================

    /**
     * Builds a PhysicalOlapScan mock and stubs accept() to route to
     * SPMPlan2SQLBuilder.visitPhysicalOlapScan.
     */
    private PhysicalOlapScan mockScan(String tableName, List<SlotReference> outputs) {
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(List.copyOf(outputs));
        // default scan state: no scan parameters, no partition selection, no sample,
        // no user-pinned partition list and no user-pinned tablet list
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(List.of());
        Mockito.when(scan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of());
        Mockito.when(scan.getManuallySpecifiedTabletIds()).thenReturn(List.of());
        stubAccept(scan);
        return scan;
    }

    /**
     * Builds a PhysicalFilter mock.
     */
    private PhysicalFilter<?> mockFilter(Expression predicate, Plan child) {
        PhysicalFilter<?> filter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(filter.getPredicate()).thenReturn(predicate);
        Mockito.when(filter.child(0)).thenReturn(child);
        stubAccept(filter);
        return filter;
    }

    /**
     * Builds a PhysicalProject mock.
     */
    private PhysicalProject<?> mockProject(List<SlotReference> projects, Plan child) {
        PhysicalProject<?> project = Mockito.mock(PhysicalProject.class);
        Mockito.when(project.getProjects()).thenReturn(List.copyOf(projects));
        Mockito.when(project.child(0)).thenReturn(child);
        stubAccept(project);
        return project;
    }

    /**
     * Builds a PhysicalHashJoin mock.
     */
    private PhysicalHashJoin<?, ?> mockJoin(Plan left, Plan right, Expression onPredicate) {
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(onPredicate));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getJoinType()).thenReturn(JoinType.INNER_JOIN);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);
        return join;
    }

    /**
     * Builds a PhysicalGenerate (LATERAL VIEW) mock: one generator, one output column
     * qualified with "t" (the SQL alias of the lateral view).
     */
    private static PhysicalGenerate<?> mockGenerate(Plan child, Function generator) {
        PhysicalGenerate<?> generate = Mockito.mock(PhysicalGenerate.class);
        Mockito.when(generate.child(0)).thenReturn(child);
        Mockito.when(generate.getGenerators()).thenReturn(List.of(generator));
        Mockito.when(generate.getGeneratorOutput()).thenReturn(List.of(
                (Slot) new SlotReference("x", IntegerType.INSTANCE, true, List.of("t"))));
        Mockito.when(generate.getConjuncts()).thenReturn(List.of());
        stubAccept(generate);
        return generate;
    }

    /**
     * Routes the accept() of a mock to the matching visit method of SPMPlan2SQLBuilder so
     * the test walks the real dispatch + recursion logic instead of hand-written traversal.
     */
    private static void stubAccept(Plan mock) {
        Mockito.doAnswer(invocation -> {
            PlanVisitor<?, ?> visitor = invocation.getArgument(0);
            Plan self = (Plan) invocation.getMock();
            if (visitor instanceof SPMPlan2SQLBuilder) {
                SPMPlan2SQLBuilder builder = (SPMPlan2SQLBuilder) visitor;
                return dispatch(builder, self);
            }
            return visitor.visit(self, null);
        }).when(mock).accept(Mockito.any(), Mockito.any());
    }

    /**
     * Dispatches to the matching visit method of SPMPlan2SQLBuilder by physical node type.
     */
    private static Object dispatch(SPMPlan2SQLBuilder builder, Plan plan) {
        if (plan instanceof PhysicalHashJoin) {
            return builder.visitPhysicalHashJoin((PhysicalHashJoin<?, ?>) plan, null);
        }
        if (plan instanceof PhysicalUnion) {
            return builder.visitPhysicalUnion((PhysicalUnion) plan, null);
        }
        if (plan instanceof PhysicalExcept) {
            return builder.visitPhysicalExcept((PhysicalExcept) plan, null);
        }
        if (plan instanceof PhysicalIntersect) {
            return builder.visitPhysicalIntersect((PhysicalIntersect) plan, null);
        }
        if (plan instanceof PhysicalLimit) {
            return builder.visitPhysicalLimit((PhysicalLimit<? extends Plan>) plan, null);
        }
        if (plan instanceof PhysicalStorageLayerAggregate) {
            return builder.visitPhysicalStorageLayerAggregate(
                    (PhysicalStorageLayerAggregate) plan, null);
        }
        if (plan instanceof PhysicalGenerate) {
            return builder.visitPhysicalGenerate((PhysicalGenerate<? extends Plan>) plan, null);
        }
        if (plan instanceof PhysicalFilter) {
            return builder.visitPhysicalFilter((PhysicalFilter<?>) plan, null);
        }
        if (plan instanceof PhysicalProject) {
            return builder.visitPhysicalProject((PhysicalProject<?>) plan, null);
        }
        if (plan instanceof PhysicalOlapScan) {
            return builder.visitPhysicalRelation((PhysicalOlapScan) plan, null);
        }
        if (plan instanceof PhysicalHashAggregate) {
            return builder.visitPhysicalHashAggregate((PhysicalHashAggregate<?>) plan, null);
        }
        if (plan instanceof PhysicalRepeat) {
            return builder.visitPhysicalRepeat((PhysicalRepeat<?>) plan, null);
        }
        if (plan instanceof PhysicalAssertNumRows) {
            return builder.visitPhysicalAssertNumRows((PhysicalAssertNumRows<?>) plan, null);
        }
        if (plan instanceof PhysicalCTEAnchor) {
            return builder.visitPhysicalCTEAnchor(
                    (PhysicalCTEAnchor<? extends Plan, ? extends Plan>) plan, null);
        }
        if (plan instanceof PhysicalCTEProducer) {
            return builder.visitPhysicalCTEProducer((PhysicalCTEProducer<? extends Plan>) plan, null);
        }
        if (plan instanceof PhysicalCTEConsumer) {
            return builder.visitPhysicalCTEConsumer((PhysicalCTEConsumer) plan, null);
        }
        if (plan instanceof PhysicalRecursiveUnion) {
            return builder.visitPhysicalRecursiveUnion((PhysicalRecursiveUnion<?, ?>) plan, null);
        }
        if (plan instanceof PhysicalRecursiveUnionAnchor) {
            return builder.visitPhysicalRecursiveUnionAnchor((PhysicalRecursiveUnionAnchor<?>) plan, null);
        }
        if (plan instanceof PhysicalRecursiveUnionProducer) {
            return builder.visitPhysicalRecursiveUnionProducer((PhysicalRecursiveUnionProducer<?>) plan, null);
        }
        if (plan instanceof PhysicalWorkTableReference) {
            return builder.visitPhysicalWorkTableReference((PhysicalWorkTableReference) plan, null);
        }
        if (plan instanceof PhysicalOneRowRelation) {
            return builder.visitPhysicalOneRowRelation((PhysicalOneRowRelation) plan, null);
        }
        return builder.visit(plan, null);
    }

    // ==================== Recursive CTE decompile (M4) ====================

    /**
     * Decompiles a recursive-CTE physical plan:
     *
     *     PhysicalRecursiveUnion(cte, UNION ALL)
     *     ├── anchor: PhysicalRecursiveUnionAnchor -> OneRowRelation
     *     │     SELECT CAST(_spm_const_var(2) AS BIGINT)
     *     └── recursive: PhysicalRecursiveUnionProducer
     *           └── Project (n + CAST(_spm_const_var(4) ...))
     *                 └── Filter (n < CAST(_spm_const_var(3) ...))
     *                       └── WorkTableReference(cte)
     *
     * The decompiled SQL must be a self-contained WITH RECURSIVE subquery with the
     * placeholder ids preserved (anchor 2, recursive filter 3, recursive projection 4)
     * and the recursive member referencing the CTE by name.
     */
    @Test
    public void testRecursiveCteDecompile() {
        SlotReference nAnchor = new SlotReference("n", BigIntType.INSTANCE);
        SlotReference nOut = new SlotReference("n", BigIntType.INSTANCE);
        SlotReference nRec = new SlotReference("n", BigIntType.INSTANCE);

        // anchor branch: OneRowRelation SELECT CAST(_spm_const_var(2) AS BIGINT) AS n
        Expression anchorProj = new Alias(
                new Cast(new SpmConstVar(2L, new IntegerLiteral(1)), BigIntType.INSTANCE), "n");
        PhysicalOneRowRelation oneRow = Mockito.mock(PhysicalOneRowRelation.class);
        Mockito.when(oneRow.getProjects()).thenReturn(List.of((NamedExpression) anchorProj));
        stubAccept(oneRow);
        PhysicalRecursiveUnionAnchor<?> anchorSentinel = Mockito.mock(PhysicalRecursiveUnionAnchor.class);
        Mockito.when(anchorSentinel.child()).thenReturn(oneRow);
        stubAccept(anchorSentinel);

        // recursive branch: WorkTableReference -> Filter(n < _spm_const_var(3)) ->
        // Project(n + _spm_const_var(4))
        PhysicalWorkTableReference workTable = Mockito.mock(PhysicalWorkTableReference.class);
        Mockito.when(workTable.getOutput()).thenReturn(List.of((Slot) nRec));
        Mockito.when(workTable.getTableName()).thenReturn("cte");
        Mockito.when(workTable.getNameParts()).thenReturn(List.of("cte"));
        stubAccept(workTable);
        PhysicalFilter<?> recFilter = Mockito.mock(PhysicalFilter.class);
        Mockito.when(recFilter.getPredicate()).thenReturn(new LessThan(nRec,
                new Cast(new SpmConstVar(3L, new IntegerLiteral(5)), BigIntType.INSTANCE)));
        Mockito.when(recFilter.child(0)).thenReturn(workTable);
        stubAccept(recFilter);
        Expression recProj = new Alias(new Nullable(new Add(nRec,
                new Cast(new SpmConstVar(4L, new IntegerLiteral(1)), BigIntType.INSTANCE))), "n1");
        PhysicalProject<?> recProject = Mockito.mock(PhysicalProject.class);
        Mockito.when(recProject.getProjects()).thenReturn(List.of((NamedExpression) recProj));
        Mockito.when(recProject.child(0)).thenReturn(recFilter);
        stubAccept(recProject);
        PhysicalRecursiveUnionProducer<?> recSentinel = Mockito.mock(PhysicalRecursiveUnionProducer.class);
        Mockito.when(recSentinel.child()).thenReturn(recProject);
        stubAccept(recSentinel);

        // the recursive CTE node
        PhysicalRecursiveUnion<?, ?> recUnion = Mockito.mock(PhysicalRecursiveUnion.class);
        Mockito.when(recUnion.getCteName()).thenReturn("cte");
        Mockito.when(recUnion.isUnionAll()).thenReturn(true);
        Mockito.when(recUnion.getRegularChildOutput(0)).thenReturn(List.of((SlotReference) nAnchor));
        Mockito.when(recUnion.getOutput()).thenReturn(List.of((Slot) nOut));
        Mockito.when(recUnion.child(0)).thenReturn(anchorSentinel);
        Mockito.when(recUnion.child(1)).thenReturn(recSentinel);
        stubAccept(recUnion);

        String sql = new SPMPlan2SQLBuilder().toSQL(recUnion);
        Assertions.assertTrue(sql.contains("WITH RECURSIVE cte(n)"),
                "frozen SQL must carry WITH RECURSIVE + column list: " + sql);
        Assertions.assertTrue(sql.contains("UNION ALL"), "anchor and recursive must be unioned: " + sql);
        Assertions.assertTrue(sql.contains("_spm_const_var(2)"),
                "anchor placeholder id must survive decompile: " + sql);
        Assertions.assertTrue(sql.contains("_spm_const_var(3)"),
                "recursive filter placeholder id must survive decompile: " + sql);
        Assertions.assertTrue(sql.contains("_spm_const_var(4)"),
                "recursive projection placeholder id must survive decompile: " + sql);
        Assertions.assertTrue(sql.contains("FROM cte"),
                "recursive member must reference the CTE by name: " + sql);
        Assertions.assertTrue(sql.contains("SELECT n FROM cte"),
                "the outer reference must select the CTE columns: " + sql);
        Assertions.assertFalse(sql.contains("_spm_const_var(2, "),
                "no placeholder value may leak into the frozen SQL: " + sql);
    }

    // ==================== non-recursive CTE decompile (WITH) ====================

    /**
     * A single-consumer CTE must be decompiled as a shared WITH definition plus a
     * reference, not inlined:
     *
     *     PhysicalCTEAnchor
     *     ├── PhysicalCTEProducer(cteId) -> PhysicalOlapScan(t1) [a, b]
     *     └── PhysicalProject [a]
     *           └── PhysicalCTEConsumer(cteId) [a]
     *
     * The CTE body must appear exactly once (in the WITH clause) and every consumer
     * must reference the definition by its generated alias.
     */
    @Test
    public void testDecompileNonRecursiveCte() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference consumerA = new SlotReference("a", IntegerType.INSTANCE);
        CTEId cteId = new CTEId(1);

        PhysicalOlapScan body = mockScan("t1", List.of(a, b));
        PhysicalCTEProducer<?> producer = Mockito.mock(PhysicalCTEProducer.class);
        Mockito.when(producer.getCteId()).thenReturn(cteId);
        Mockito.when(producer.child(0)).thenReturn(body);
        stubAccept(producer);

        PhysicalCTEConsumer consumer = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer.getCteId()).thenReturn(cteId);
        Mockito.when(consumer.getOutput()).thenReturn(List.of((Slot) consumerA));
        Mockito.when(consumer.getProducerSlot(Mockito.any(Slot.class))).thenReturn(a);
        stubAccept(consumer);

        PhysicalProject<?> project = mockProject(List.of(consumerA), consumer);
        PhysicalCTEAnchor<?, ?> anchor = Mockito.mock(PhysicalCTEAnchor.class);
        Mockito.when(anchor.child(0)).thenReturn(producer);
        Mockito.when(anchor.child(1)).thenReturn(project);
        Mockito.when(anchor.getOutput()).thenReturn(List.of((Slot) consumerA));
        stubAccept(anchor);

        String sql = new SPMPlan2SQLBuilder().toSQL(anchor);
        Assertions.assertTrue(sql.startsWith("WITH t_0 AS ("),
                "the CTE must lead the statement as a WITH definition: " + sql);
        Assertions.assertTrue(sql.contains("FROM t_0"),
                "consumers must reference the CTE alias: " + sql);
        Assertions.assertEquals(1, countOccurrences(sql, "FROM t1"),
                "the CTE body must appear exactly once (no inline copy): " + sql);
    }

    /**
     * A CTE with two consumers must still be emitted once: a single WITH definition
     * referenced by both consumers (each reference keeps its own relation alias, so a
     * self join of the CTE stays unambiguous):
     *
     *     PhysicalCTEAnchor
     *     ├── PhysicalCTEProducer(cteId) -> PhysicalOlapScan(t1) [a, b]
     *     └── PhysicalHashJoin [a = b]
     *           ├── PhysicalCTEConsumer(cteId) [a]
     *           └── PhysicalCTEConsumer(cteId) [b]
     */
    @Test
    public void testDecompileMultiConsumerCte() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        SlotReference consumerLeft = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference consumerRight = new SlotReference("b", IntegerType.INSTANCE);
        CTEId cteId = new CTEId(2);

        PhysicalOlapScan body = mockScan("t1", List.of(a, b));
        PhysicalCTEProducer<?> producer = Mockito.mock(PhysicalCTEProducer.class);
        Mockito.when(producer.getCteId()).thenReturn(cteId);
        Mockito.when(producer.child(0)).thenReturn(body);
        stubAccept(producer);

        PhysicalCTEConsumer consumer1 = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer1.getCteId()).thenReturn(cteId);
        Mockito.when(consumer1.getOutput()).thenReturn(List.of((Slot) consumerLeft));
        Mockito.when(consumer1.getProducerSlot(Mockito.any(Slot.class))).thenReturn(a);
        stubAccept(consumer1);

        PhysicalCTEConsumer consumer2 = Mockito.mock(PhysicalCTEConsumer.class);
        Mockito.when(consumer2.getCteId()).thenReturn(cteId);
        Mockito.when(consumer2.getOutput()).thenReturn(List.of((Slot) consumerRight));
        Mockito.when(consumer2.getProducerSlot(Mockito.any(Slot.class))).thenReturn(b);
        stubAccept(consumer2);

        PhysicalHashJoin<?, ?> join = mockJoin(consumer1, consumer2, new EqualTo(consumerLeft, consumerRight));
        PhysicalCTEAnchor<?, ?> anchor = Mockito.mock(PhysicalCTEAnchor.class);
        Mockito.when(anchor.child(0)).thenReturn(producer);
        Mockito.when(anchor.child(1)).thenReturn(join);
        Mockito.when(anchor.getOutput()).thenReturn(List.of((Slot) consumerLeft, (Slot) consumerRight));
        stubAccept(anchor);

        String sql = new SPMPlan2SQLBuilder().toSQL(anchor);
        Assertions.assertEquals(1, countOccurrences(sql, "t_0 AS ("),
                "the CTE body must be defined exactly once: " + sql);
        Assertions.assertEquals(1, countOccurrences(sql, "FROM t1"),
                "the CTE body must appear exactly once (no inline copy): " + sql);
        Assertions.assertEquals(2, countOccurrences(sql, "FROM t_0"),
                "both consumers must reference the shared definition: " + sql);
    }

    // ==================== constant UNION branches ====================

    /**
     * MergeOneRowRelationIntoUnion MOVES a constant one-row branch out of children()
     * into PhysicalUnion.constantExprsList. It must be emitted as a positional SELECT
     * branch: emitting only the regular children silently dropped the row at replay
     * (SELECT 1 UNION ALL SELECT x FROM t WHERE y = ? lost the SELECT 1 row).
     */
    @Test
    public void testUnionConstantBranchIsEmitted() {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference setOutput = new SlotReference("k1", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(x));

        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(Qualifier.ALL);
        Mockito.when(union.children()).thenReturn(List.of(scan));
        Mockito.when(union.getRegularChildrenOutputs()).thenReturn(List.of(List.of(x)));
        Mockito.when(union.getOutput()).thenReturn(List.of(setOutput));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of(
                List.of((NamedExpression) new Alias(new IntegerLiteral(1), "k1"))));
        stubAccept(union);

        String sql = new SPMPlan2SQLBuilder().toSQL(union);
        Assertions.assertTrue(sql.contains("UNION ALL"), sql);
        Assertions.assertTrue(sql.contains("1 AS k1"),
                "the constant one-row branch must survive the freeze: " + sql);
        Assertions.assertTrue(sql.contains("x AS k1"),
                "the regular branch is projected under the set output names: " + sql);
    }

    @Test
    public void testUnionConstantOnlyBranchIsEmitted() {
        SlotReference setOutput = new SlotReference("k1", IntegerType.INSTANCE);
        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(Qualifier.ALL);
        Mockito.when(union.children()).thenReturn(List.of());
        Mockito.when(union.getRegularChildrenOutputs()).thenReturn(List.of());
        Mockito.when(union.getOutput()).thenReturn(List.of(setOutput));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of(
                List.of((NamedExpression) new Alias(new IntegerLiteral(1), "k1"))));
        stubAccept(union);

        String sql = new SPMPlan2SQLBuilder().toSQL(union);
        Assertions.assertTrue(sql.contains("SELECT 1 AS k1"),
                "a constant-only UNION must not render an empty body: " + sql);
        Assertions.assertFalse(sql.contains("UNION ALL"),
                "there is no regular branch to join: " + sql);
    }

    @Test
    public void testUnionConstantArityMismatchRejected() {
        SlotReference setOutput = new SlotReference("k1", IntegerType.INSTANCE);
        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(Qualifier.ALL);
        Mockito.when(union.children()).thenReturn(List.of());
        Mockito.when(union.getRegularChildrenOutputs()).thenReturn(List.of());
        Mockito.when(union.getOutput()).thenReturn(List.of(setOutput));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of(List.of(
                (NamedExpression) new Alias(new IntegerLiteral(1), "k1"),
                (NamedExpression) new Alias(new IntegerLiteral(2), "k2"))));
        stubAccept(union);

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new SPMPlan2SQLBuilder().toSQL(union),
                "a constant row that does not match the set arity must fail the decompile");
    }

    // ==================== set-operation quantifier ====================

    /**
     * The keyword comes from the PHYSICAL qualifier: the parser maps an omitted
     * quantifier (and an explicit DISTINCT) to Qualifier.DISTINCT, so the decompiler must
     * not emit ALL for it. A DISTINCT union frozen as UNION ALL returns duplicate rows at
     * replay (and EXCEPT ALL / INTERSECT ALL lose their multiplicity when ALL is
     * dropped).
     */
    @Test
    public void testUnionQuantifierIsPreserved() {
        String allSql = new SPMPlan2SQLBuilder().toSQL(mockUnion(Qualifier.ALL));
        Assertions.assertTrue(allSql.contains(" UNION ALL "), allSql);

        String distinctSql = new SPMPlan2SQLBuilder().toSQL(mockUnion(Qualifier.DISTINCT));
        Assertions.assertFalse(distinctSql.contains("UNION ALL"),
                "a DISTINCT union must not be frozen as UNION ALL: " + distinctSql);
        Assertions.assertTrue(distinctSql.contains(" UNION "), distinctSql);
    }

    @Test
    public void testExceptQuantifierIsPreserved() {
        String distinctSql = new SPMPlan2SQLBuilder().toSQL(mockExcept(Qualifier.DISTINCT));
        Assertions.assertTrue(distinctSql.contains(" EXCEPT "), distinctSql);
        Assertions.assertFalse(distinctSql.contains("EXCEPT ALL"), distinctSql);

        String allSql = new SPMPlan2SQLBuilder().toSQL(mockExcept(Qualifier.ALL));
        Assertions.assertTrue(allSql.contains(" EXCEPT ALL "),
                "EXCEPT ALL must keep its ALL, otherwise multiplicity is lost: " + allSql);
    }

    @Test
    public void testIntersectQuantifierIsPreserved() {
        String distinctSql = new SPMPlan2SQLBuilder().toSQL(mockIntersect(Qualifier.DISTINCT));
        Assertions.assertTrue(distinctSql.contains(" INTERSECT "), distinctSql);
        Assertions.assertFalse(distinctSql.contains("INTERSECT ALL"), distinctSql);

        String allSql = new SPMPlan2SQLBuilder().toSQL(mockIntersect(Qualifier.ALL));
        Assertions.assertTrue(allSql.contains(" INTERSECT ALL "),
                "INTERSECT ALL must keep its ALL, otherwise multiplicity is lost: " + allSql);
    }

    private PhysicalUnion mockUnion(Qualifier qualifier) {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        PhysicalOlapScan left = mockScan("t1", List.of(x));
        PhysicalOlapScan right = mockScan("t2", List.of(x));
        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(qualifier);
        Mockito.when(union.children()).thenReturn(List.of(left, right));
        Mockito.when(union.getRegularChildrenOutputs())
                .thenReturn(List.of(List.of(x), List.of(x)));
        Mockito.when(union.getOutput()).thenReturn(List.of(
                new SlotReference("k1", IntegerType.INSTANCE)));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of());
        stubAccept(union);
        return union;
    }

    private PhysicalExcept mockExcept(Qualifier qualifier) {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        PhysicalOlapScan left = mockScan("t1", List.of(x));
        PhysicalOlapScan right = mockScan("t2", List.of(x));
        PhysicalExcept except = Mockito.mock(PhysicalExcept.class);
        Mockito.when(except.getQualifier()).thenReturn(qualifier);
        Mockito.when(except.children()).thenReturn(List.of(left, right));
        Mockito.when(except.getRegularChildrenOutputs())
                .thenReturn(List.of(List.of(x), List.of(x)));
        Mockito.when(except.getOutput()).thenReturn(List.of(
                new SlotReference("k1", IntegerType.INSTANCE)));
        stubAccept(except);
        return except;
    }

    private PhysicalIntersect mockIntersect(Qualifier qualifier) {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        PhysicalOlapScan left = mockScan("t1", List.of(x));
        PhysicalOlapScan right = mockScan("t2", List.of(x));
        PhysicalIntersect intersect = Mockito.mock(PhysicalIntersect.class);
        Mockito.when(intersect.getQualifier()).thenReturn(qualifier);
        Mockito.when(intersect.children()).thenReturn(List.of(left, right));
        Mockito.when(intersect.getRegularChildrenOutputs())
                .thenReturn(List.of(List.of(x), List.of(x)));
        Mockito.when(intersect.getOutput()).thenReturn(List.of(
                new SlotReference("k1", IntegerType.INSTANCE)));
        stubAccept(intersect);
        return intersect;
    }

    // ==================== FROM-less projection aliases (PhysicalOneRowRelation) ====================

    @Test
    public void testOneRowRelationEmitsExplicitAlias() {
        PhysicalOneRowRelation oneRow = Mockito.mock(PhysicalOneRowRelation.class);
        Mockito.when(oneRow.getProjects()).thenReturn(List.of(
                (NamedExpression) new Alias(new IntegerLiteral(1), "a")));
        stubAccept(oneRow);

        String sql = new SPMPlan2SQLBuilder().toSQL(oneRow);
        Assertions.assertTrue(sql.contains("1 AS a"),
                "SELECT 1 AS a must freeze with its result-header alias: " + sql);
    }

    @Test
    public void testOneRowRelationKeepsParserFallbackName() {
        // Alias(expr) is the parser's name-from-child fallback (the expression TEXT,
        // not an identifier): it must not be emitted as a quoted SQL alias
        PhysicalOneRowRelation oneRow = Mockito.mock(PhysicalOneRowRelation.class);
        Mockito.when(oneRow.getProjects()).thenReturn(List.of(
                (NamedExpression) new Alias(new IntegerLiteral(1))));
        stubAccept(oneRow);

        String sql = new SPMPlan2SQLBuilder().toSQL(oneRow);
        Assertions.assertFalse(sql.contains(" AS "),
                "a name-from-child alias is not identifier-safe and must not be emitted: " + sql);
    }

    // ==================== slot remapping in composite expressions ====================

    @Test
    public void testBitNotRendersRemappedColumn() {
        SQLRelation relation = new SQLRelation();
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        relation.registerRef(a.getExprId(), "c_5");
        Assertions.assertEquals("~(c_5)",
                new SPMExprSqlBuilder().print(new BitNot(a), relation),
                "a unary ~ child must go through the ExprId -> column mapping");
    }

    @Test
    public void testGenericFallbackRejectsRemappedSlots() {
        SQLRelation relation = new SQLRelation();
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        relation.registerRef(a.getExprId(), "c_5");
        SPMExprSqlBuilder builder = new SPMExprSqlBuilder();

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMExprSqlBuilder.ensureNoRemappedSlots(new BitNot(a), relation),
                "a remapped slot must never fall through to the raw toSql() path");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> builder.visit((Expression) new BitNot(a), relation),
                "the generic fallback must reject a stale column instead of freezing it");

        // a registered reference that still names the column is renderable
        SQLRelation qualified = new SQLRelation();
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        qualified.registerRef(b.getExprId(), "t_2.b");
        Assertions.assertDoesNotThrow(
                () -> SPMExprSqlBuilder.ensureNoRemappedSlots(new BitNot(b), qualified),
                "a qualified reference still names the column and stays renderable");
        Assertions.assertNotNull(builder.visit((Expression) new BitNot(b), qualified));
    }

    // ==================== DISTINCT restore for non-count merges ====================

    /**
     * SUM(DISTINCT x) mixed with a plain aggregate: SplitAggMultiPhase clears isDistinct
     * on the final DISTINCT_GLOBAL function because the eliminated lower stage
     * deduplicates its input. The decompiler folds that stage away and must restore
     * DISTINCT for EVERY aggregate consuming the dedup buffer (not only count); an
     * aggregate riding along the same stage stays plain.
     */
    @Test
    public void testDistinctMergeRestoresDistinctForSum() {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference y = new SlotReference("y", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(x, y));

        Alias localOutput = new Alias(new AggregateExpression(new Sum(x),
                new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER)), "m");

        PhysicalHashAggregate<?> local = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(local.child(0)).thenReturn(scan);
        Mockito.when(local.getAggPhase()).thenReturn(AggPhase.DISTINCT_LOCAL);
        Mockito.when(local.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(local.getOutputExpressions()).thenReturn(List.of(localOutput));
        stubAccept(local);

        SlotReference bufferSlot = new SlotReference(
                localOutput.getExprId(), "m", IntegerType.INSTANCE, true, List.of());
        PhysicalHashAggregate<?> finalAgg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(finalAgg.child(0)).thenReturn(local);
        Mockito.when(finalAgg.getAggPhase()).thenReturn(AggPhase.DISTINCT_GLOBAL);
        Mockito.when(finalAgg.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(finalAgg.getOutputExpressions()).thenReturn(List.of(
                (NamedExpression) new Alias(new AggregateExpression(new Sum(bufferSlot),
                        new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                        bufferSlot), "s"),
                (NamedExpression) new Alias(new AggregateExpression(new Max(y),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT)), "mx")));
        stubAccept(finalAgg);

        String sql = new SPMPlan2SQLBuilder().toSQL(finalAgg);
        Assertions.assertTrue(sql.contains("sum(DISTINCT x)"),
                "SUM(DISTINCT x) must keep its DISTINCT through the stage fold: " + sql);
        Assertions.assertTrue(sql.contains("max(y)"),
                "an aggregate riding along the same stage stays plain: " + sql);
        Assertions.assertFalse(sql.contains("max(DISTINCT"),
                "DISTINCT must not be invented for the riding aggregate: " + sql);
    }

    // ==================== external file scan modifiers ====================

    @Test
    public void testFileScanWithoutModifiersDecompiles() {
        PhysicalFileScan scan = mockFileScan();
        SQLRelation relation = new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null);
        Assertions.assertNotNull(relation);
    }

    @Test
    public void testFileScanModifiersAreRendered() {
        // TABLESAMPLE: the sample (and its REPEATABLE seed) must survive the freeze -
        // dropping it would replay over the full table
        PhysicalFileScan sampled = mockFileScan();
        Mockito.when(sampled.getTableSample())
                .thenReturn(Optional.of(new TableSample(10, true, -1)));
        Assertions.assertEquals("ext_t TABLESAMPLE(10 PERCENT)",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(sampled, null).getFrom(),
                "a sampled file scan must freeze its sample");

        PhysicalFileScan sampledRows = mockFileScan();
        Mockito.when(sampledRows.getTableSample())
                .thenReturn(Optional.of(new TableSample(1000, false, 5)));
        Assertions.assertEquals("ext_t TABLESAMPLE(1000 ROWS) REPEATABLE 5",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(sampledRows, null).getFrom(),
                "a ROWS sample with a REPEATABLE seed must survive as well");

        // FOR VERSION AS OF / FOR TIME AS OF: a time-travel read must stay pinned
        PhysicalFileScan versioned = mockFileScan();
        Mockito.when(versioned.getTableSnapshot()).thenReturn(Optional.of(
                new TableSnapshot("123", TableSnapshot.VersionType.VERSION)));
        Assertions.assertEquals("ext_t FOR VERSION AS OF 123",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(versioned, null).getFrom(),
                "a numeric version is emitted as a version literal");

        PhysicalFileScan versionedString = mockFileScan();
        Mockito.when(versionedString.getTableSnapshot()).thenReturn(Optional.of(
                new TableSnapshot("v2", TableSnapshot.VersionType.VERSION)));
        Assertions.assertEquals("ext_t FOR VERSION AS OF 'v2'",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(versionedString, null).getFrom(),
                "a non-numeric version is emitted as a string literal");

        PhysicalFileScan timed = mockFileScan();
        Mockito.when(timed.getTableSnapshot()).thenReturn(Optional.of(
                TableSnapshot.timeOf("2024-01-02 03:04:05")));
        Assertions.assertEquals("ext_t FOR TIME AS OF '2024-01-02 03:04:05'",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(timed, null).getFrom(),
                "a time-travel read keeps its timestamp");

        // @paramType(...) read parameters, map and identifier-list form
        PhysicalFileScan incremental = mockFileScan();
        Mockito.when(incremental.getScanParams()).thenReturn(Optional.of(
                new TableScanParams("incr", Map.of("branch", "main"), List.of())));
        Assertions.assertEquals("ext_t @incr(branch = 'main')",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(incremental, null).getFrom(),
                "scan parameters must survive the freeze in map form");

        PhysicalFileScan listed = mockFileScan();
        Mockito.when(listed.getScanParams()).thenReturn(Optional.of(
                new TableScanParams("snapshot", Map.of(), List.of("a", "b"))));
        Assertions.assertEquals("ext_t @snapshot(a, b)",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(listed, null).getFrom(),
                "scan parameters must survive the freeze in list form");

        // partition pruning state has a predicate-derivable origin: replay re-prunes, so
        // it is not a modifier and the scan decompiles as the bare table
        PhysicalFileScan pruned = mockFileScan();
        Mockito.when(pruned.getSelectedPartitions())
                .thenReturn(Mockito.mock(LogicalFileScan.SelectedPartitions.class));
        Assertions.assertEquals("ext_t",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(pruned, null).getFrom(),
                "file-scan partition pruning is re-derived by replay and is not a modifier");
    }

    @Test
    public void testOlapScanModifiersAreRendered() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(a));
        Partition first = Mockito.mock(Partition.class);
        Partition second = Mockito.mock(Partition.class);
        Partition third = Mockito.mock(Partition.class);
        Mockito.when(first.getName()).thenReturn("p1");
        Mockito.when(second.getName()).thenReturn("p2");
        Mockito.when(third.getName()).thenReturn("p3");
        OlapTable table = scan.getTable();
        Mockito.when(table.getPartition(1L)).thenReturn(first);
        Mockito.when(table.getPartition(2L)).thenReturn(second);
        Mockito.when(table.getPartition(3L)).thenReturn(third);

        // a user-pinned partition list is frozen from the MANUAL provenance (ids sorted,
        // so the text is deterministic whatever order the user / optimizer produced)
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of(2L, 1L));
        Assertions.assertEquals("t1 PARTITION(p1, p2)",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null).getFrom(),
                "a user partition pin must be frozen");

        // a pin that happens to cover every CURRENT partition still freezes: after ADD
        // PARTITION the same pinned query would otherwise replay over the new partition
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of(1L, 2L, 3L));
        Assertions.assertEquals("t1 PARTITION(p1, p2, p3)",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null).getFrom(),
                "a full-cardinality pin must not be dropped");

        // partition pruning also shrinks selectedPartitionIds; without a user pin no
        // clause is emitted (the replayed SQL re-derives the same selection)
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of());
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(List.of(1L));
        Assertions.assertEquals("t1",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null).getFrom(),
                "a pruned-but-unpinned scan must not freeze a partition pin");

        // a temporaray partition pin keeps its namespace
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of(1L));
        Mockito.when(table.isTemporaryPartition(1L)).thenReturn(true);
        Assertions.assertEquals("t1 TEMPORARY PARTITION(p1)",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null).getFrom(),
                "a TEMPORARY partition pin must not bind the formal namespace");
        Mockito.when(table.isTemporaryPartition(1L)).thenReturn(false);

        // a user-pinned TABLET list is frozen too (without it a replayed scan reads
        // every tablet of the selected partitions)
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of());
        Mockito.when(scan.getManuallySpecifiedTabletIds()).thenReturn(List.of(20L, 10L));
        Assertions.assertEquals("t1 TABLET(10, 20)",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null).getFrom(),
                "a user TABLET pin must be frozen");
        Mockito.when(scan.getManuallySpecifiedTabletIds()).thenReturn(List.of());

        // TABLESAMPLE on olap
        PhysicalOlapScan sampled = mockScan("t1", List.of(a));
        Mockito.when(sampled.getTableSample())
                .thenReturn(Optional.of(new TableSample(20, true, 9)));
        Assertions.assertEquals("t1 TABLESAMPLE(20 PERCENT) REPEATABLE 9",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(sampled, null).getFrom(),
                "an olap sample must survive the freeze");

        // binlog read parameters
        PhysicalOlapScan binlog = mockScan("t1", List.of(a));
        Mockito.when(binlog.getScanParams()).thenReturn(Optional.of(
                new TableScanParams("incr", Map.of("branch", "main"), List.of())));
        Assertions.assertEquals("t1 @incr(branch = 'main')",
                new SPMPlan2SQLBuilder().visitPhysicalRelation(binlog, null).getFrom(),
                "olap scan parameters must survive the freeze");
    }

    private static PhysicalFileScan mockFileScan() {
        PhysicalFileScan scan = Mockito.mock(PhysicalFileScan.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("ext_t");
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getTableSnapshot()).thenReturn(Optional.empty());
        Mockito.when(scan.getTableSample()).thenReturn(Optional.empty());
        return scan;
    }

    @Test
    public void testLazyMaterializeFileScanDecompilesAsScan() {
        // the lazy wrapper must not fail the decompile: it subclasses PhysicalFileScan,
        // so the plain scan (including its scan modifiers) is the faithful rendering
        PhysicalLazyMaterializeFileScan scan = Mockito.mock(PhysicalLazyMaterializeFileScan.class);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("ext_t");
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getTableSnapshot()).thenReturn(Optional.empty());
        Mockito.when(scan.getTableSample()).thenReturn(Optional.of(new TableSample(10, true, -1)));
        Assertions.assertEquals("ext_t TABLESAMPLE(10 PERCENT)",
                new SPMPlan2SQLBuilder().visitPhysicalLazyMaterializeFileScan(scan, null).getFrom(),
                "a lazily materialized file scan must decompile as its scan, modifiers included");
    }

    // ==================== identifier quoting ====================

    @Test
    public void testSpecialCharacterColumnIsQuoted() {
        SlotReference dashed = new SlotReference("a-b", IntegerType.INSTANCE);
        SlotReference plain = new SlotReference("a", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(dashed, plain));

        SQLRelation relation = new SPMPlan2SQLBuilder().visitPhysicalRelation(scan, null);
        Assertions.assertEquals("`a-b`", relation.getColumnNames().get(dashed.getExprId()),
                "a special-character column must be registered as a quoted identifier");
        Assertions.assertEquals("a", relation.getColumnNames().get(plain.getExprId()),
                "a plain column stays verbatim");

        PhysicalProject project = mockProject(List.of(dashed), scan);
        String sql = new SPMPlan2SQLBuilder().toSQL(project);
        Assertions.assertTrue(sql.contains("`a-b`"),
                "the frozen projection must keep the identifier quoted (no subtraction): " + sql);
    }

    // ==================== LATERAL VIEW (PhysicalGenerate) ====================

    /**
     * A wrapped Generate child (derived table with WHERE / LIMIT) must keep its COMPLETE
     * query block inside the lateral-view input: attaching the LATERAL VIEW to the bare
     * FROM fragment leaves the child's clauses on the OUTER relation, where they apply
     * AFTER the explode - LIMIT would then limit the exploded rows and frozen replay
     * could return rows the captured plan filtered out.
     */
    @Test
    public void testGenerateKeepsWrappedChildQueryBlock() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference arr = new SlotReference("arr", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k, arr));
        PhysicalFilter filter = mockFilter(new GreaterThan(k, new IntegerLiteral(0)), scan);
        PhysicalLimit<?> limit = Mockito.mock(PhysicalLimit.class);
        Mockito.when(limit.child(0)).thenReturn(filter);
        Mockito.when(limit.getLimit()).thenReturn(1L);
        Mockito.when(limit.getOffset()).thenReturn(0L);
        stubAccept(limit);
        PhysicalGenerate<?> generate = mockGenerate(limit, new Explode(arr));

        String sql = new SPMPlan2SQLBuilder().toSQL(generate);
        Assertions.assertTrue(sql.contains(
                        "(SELECT * FROM t1 WHERE (k > 0) LIMIT 1) t_0 LATERAL VIEW"),
                "the child's WHERE + LIMIT must stay inside the lateral-view input: " + sql);
        Assertions.assertTrue(sql.indexOf("WHERE") < sql.indexOf("LATERAL VIEW"),
                "no child clause may be rendered after the lateral view: " + sql);
        Assertions.assertFalse(sql.endsWith("LIMIT 1"),
                "the LIMIT must not apply to the exploded rows: " + sql);
    }

    @Test
    public void testGenerateOverPlainScanStaysInline() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference arr = new SlotReference("arr", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k, arr));
        PhysicalGenerate<?> generate = mockGenerate(scan, new Explode(arr));

        String sql = new SPMPlan2SQLBuilder().toSQL(generate);
        Assertions.assertTrue(sql.contains("FROM t1 LATERAL VIEW"),
                "a plain scan child stays inline: " + sql);
        Assertions.assertFalse(sql.contains("(SELECT * FROM t1)"),
                "a plain scan child needs no subquery wrapper: " + sql);
    }

    /**
     * The storage-layer aggregate shortcut is transparent: AggregateStrategies keeps the
     * enclosing aggregate (or the constant-only project) on top with expressions written
     * over the wrapped scan's slots, so decompiling the wrapped relation publishes exactly
     * those slots and the enclosing operators render the same SQL aggregation
     * (count(*) / min(x) / ...).
     */
    @Test
    public void testStorageLayerAggregateDecompilesWrappedRelation() {
        // the shortcut is transparent: AggregateStrategies keeps the enclosing
        // aggregate / constant project on top with expressions written over the wrapped
        // scan's slots, so the frozen text re-plans the same aggregation (count(*) /
        // min(x) / ...) against the same table instead of dropping it
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(a));
        PhysicalStorageLayerAggregate storageAgg =
                Mockito.mock(PhysicalStorageLayerAggregate.class);
        Mockito.when(storageAgg.getRelation()).thenReturn(scan);
        stubAccept(storageAgg);
        String sql = new SPMPlan2SQLBuilder().toSQL(storageAgg);
        Assertions.assertTrue(sql.contains("FROM t1"),
                "a storage-layer aggregate pushdown must decompile as its wrapped relation: " + sql);
    }

    /**
     * MergeGenerates can fold two independent stacked generators into ONE node; the
     * executor rolls several table functions over each child row (cartesian), so one
     * LATERAL VIEW per generator is the faithful rendering of the merged node.
     */
    @Test
    public void testMultiGeneratorRendersOneLateralViewPerGenerator() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k));
        PhysicalGenerate<?> generate = Mockito.mock(PhysicalGenerate.class);
        Mockito.when(generate.child(0)).thenReturn(scan);
        Mockito.when(generate.getGenerators()).thenReturn(List.of(
                (Function) new Explode(new IntegerLiteral(1)),
                (Function) new Explode(new IntegerLiteral(2))));
        Mockito.when(generate.getGeneratorOutput()).thenReturn(List.of(
                (Slot) new SlotReference("x", IntegerType.INSTANCE, true, List.of("g1")),
                (Slot) new SlotReference("y", IntegerType.INSTANCE, true, List.of("g2"))));
        Mockito.when(generate.getConjuncts()).thenReturn(List.of());
        stubAccept(generate);

        String sql = new SPMPlan2SQLBuilder().toSQL(generate);
        Assertions.assertEquals(2, countOccurrences(sql, "LATERAL VIEW"),
                "a merged multi-generator node must render one LATERAL VIEW per generator: " + sql);
        Assertions.assertTrue(sql.contains("LATERAL VIEW explode(1) g1 AS x"),
                "the first generator keeps its own alias and column name: " + sql);
        Assertions.assertTrue(sql.contains("LATERAL VIEW explode(2) g2 AS y"),
                "the second generator keeps its own alias and column name: " + sql);
    }

    /** Counts the occurrences of a literal fragment in a string. */
    private static int countOccurrences(String sql, String fragment) {
        int count = 0;
        int index = sql.indexOf(fragment);
        while (index >= 0) {
            count++;
            index = sql.indexOf(fragment, index + fragment.length());
        }
        return count;
    }
}
