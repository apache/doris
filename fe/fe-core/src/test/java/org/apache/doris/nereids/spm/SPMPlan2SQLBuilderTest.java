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
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
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
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.generator.Explode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdaf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
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
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionAnchor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRecursiveUnionProducer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalWorkTableReference;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
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

    /**
     * Two occurrences of one table whose FROM texts DIFFER (each carries its
     * own scan pin) are still a SELF join. The FROM-text comparison missed them, so the
     * frozen text carried the same table twice WITHOUT aliases - the analyzer rejects it
     * on replay ("Not unique table/alias: 't1'"), i.e. a pinned self-join baseline could
     * never be replayed. Both sides are now wrapped like any other self join.
     */
    @Test
    public void testPinnedSelfJoinForcesWrap() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockPinnedScan("t1", List.of(a), "p1");
        PhysicalOlapScan right = mockPinnedScan("t1", List.of(b), "p2");
        PhysicalHashJoin join = mockJoin(left, right, new EqualTo(a, b));

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertFalse(sql.contains("t1 PARTITION(p1) INNER JOIN t1 PARTITION(p2)"),
                "the pinned self join must not inline both occurrences: " + sql);
        Assertions.assertTrue(sql.contains("(SELECT * FROM t1 PARTITION(p1)) t_0"), sql);
        Assertions.assertTrue(sql.contains("(SELECT * FROM t1 PARTITION(p2)) t_1"), sql);
    }

    /**
     * When the column names collide, EVERY column is referenced through its
     * side's qualifier - which a composite FROM (a scan carrying a pin) cannot provide.
     * The wrapper alias is now allocated BEFORE the FROM fragments are materialized; the
     * old order emitted the pinned scans inline while the references used the alias
     * ensureQualifierAlias() allocated afterwards ("Unknown table 't_0'" on replay).
     */
    @Test
    public void testPinnedConflictJoinWrapsBothSides() {
        SlotReference a1 = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference a2 = new SlotReference("a", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockPinnedScan("t1", List.of(a1), "p1");
        PhysicalOlapScan right = mockPinnedScan("t2", List.of(a2), "p1");
        PhysicalHashJoin join = mockJoin(left, right, new EqualTo(a1, a2));

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("(SELECT * FROM t1 PARTITION(p1)) t_0"), sql);
        Assertions.assertTrue(sql.contains("(SELECT * FROM t2 PARTITION(p1)) t_1"), sql);
        Assertions.assertTrue(sql.contains("t_0.a") && sql.contains("t_1.a"),
                "the conflicting references must use the wrapper aliases of the FROM: " + sql);
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
        Assertions.assertTrue(sql.contains("b > 1"),
                "the OTHER conjunct must be carried INTO the subquery filter - finding the"
                        + " key in the hash conjuncts used to skip the whole other loop and"
                        + " silently drop it (with both keys 2 the original anti join keeps"
                        + " the row while the replay dropped it): " + sql);
        Assertions.assertFalse(sql.contains("NULL_AWARE"), sql);
    }

    /**
     * A residual LEFT NULL_AWARE ANTI JOIN whose LEFT side is a
     * pinned-partition scan (a COMPOSITE FROM that needs a wrapper) and whose sides share a
     * column NAME. The probe must be qualified with the alias of the WRAPPED left relation,
     * and the outer FROM must expose that same alias: leftSql used to be captured BEFORE
     * ensureQualifierAlias() wrapped the relation, so the frozen SQL referenced t_N.id
     * without any t_N in the FROM and could not analyze on replay.
     */
    @Test
    public void testDecompileNullAwareAntiWrapsThePinnedLeftScanWithTheProbeAlias() {
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference rightId = new SlotReference("id", IntegerType.INSTANCE);
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);

        PhysicalOlapScan left = mockPinnedScan("t1", List.of(id), "p1");
        PhysicalOlapScan right = mockScan("t2", List.of(rightId, k));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts()).thenReturn(List.of(new EqualTo(id, rightId)));
        Mockito.when(join.getOtherJoinConjuncts())
                .thenReturn(List.of(new GreaterThan(k, new IntegerLiteral(1))));
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(java.util.Optional.empty());
        Mockito.when(join.getJoinType()).thenReturn(JoinType.NULL_AWARE_LEFT_ANTI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(false);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        Assertions.assertTrue(sql.contains("NOT IN (SELECT"),
                "the residual conjunct forces the NOT IN subquery rewrite: " + sql);
        java.util.regex.Matcher wrapped = java.util.regex.Pattern
                .compile("FROM \\(SELECT \\* FROM t1 PARTITION\\(p1\\)\\) (t_\\d+)")
                .matcher(sql);
        Assertions.assertTrue(wrapped.find(),
                "the outer FROM must be the WRAPPED pinned scan: " + sql);
        Assertions.assertTrue(sql.contains("(" + wrapped.group(1) + ".id) NOT IN (SELECT"),
                "the probe must use the alias visible in the outer FROM ("
                        + wrapped.group(1) + "): " + sql);
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

    // ==================== nested TopN / dotted table names under an outer consumer ====================

    /**
     * Filter -> TopN -> Scan: the TopN's ORDER BY / LIMIT are folded onto the scan
     * relation, and the outer filter EMBEDS that relation. Returning its bare FROM
     * silently dropped both clauses - the flat "... FROM t1 WHERE id = 1" returns id 1
     * for rows 0,1, while "ORDER BY id LIMIT 1" THEN a filter id=1 returns NO row.
     */
    @Test
    public void testFilterOverTopNKeepsOrderByAndLimitInsideSubquery() {
        SlotReference id = new SlotReference("id", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(id));
        PhysicalTopN<?> topN = mockTopN(scan, List.of(new OrderKey(id, true, true)), 1L);
        PhysicalFilter<?> filter = mockFilter(new EqualTo(id, new IntegerLiteral(1)), topN);

        String sql = new SPMPlan2SQLBuilder().toSQL(filter);
        int limitIndex = sql.indexOf("LIMIT 1");
        int whereIndex = sql.indexOf("WHERE");
        Assertions.assertTrue(limitIndex > 0,
                "the folded TopN LIMIT must survive the embedding: " + sql);
        Assertions.assertTrue(whereIndex > limitIndex,
                "the LIMIT must stay INSIDE the wrapped subquery, before the filter's"
                        + " WHERE (a flat FROM re-applies the filter BEFORE the slice): " + sql);
        Assertions.assertTrue(sql.contains("ORDER BY id ASC NULLS FIRST"),
                "the ORDER BY must be kept as well: " + sql);
    }

    /**
     * A table whose name is ONE quoted component containing a dot (`t.a` under
     * enable_unicode_name_support): the flattened getNameWithFullQualifiers() would be
     * split on every dot and render FOUR identifiers instead of the intended three-part
     * name with `t.a` quoted as ONE component - a frozen baseline carrying that text
     * fails re-analysis after a reload, and a frozen row has no raw fallback tree.
     */
    @Test
    public void testDottedTableComponentIsQuotedSeparately() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("t.a");
        Mockito.when(table.getFullQualifiers())
                .thenReturn(List.of("internal", "spm_db", "t.a"));
        Mockito.when(table.getDatabase())
                .thenReturn(Mockito.mock(org.apache.doris.catalog.DatabaseIf.class));
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(List.copyOf(List.of(k)));
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(List.of());
        Mockito.when(scan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of());
        Mockito.when(scan.getManuallySpecifiedTabletIds()).thenReturn(List.of());
        stubAccept(scan);

        String sql = new SPMPlan2SQLBuilder().toSQL(scan);
        Assertions.assertTrue(sql.contains("internal.spm_db.`t.a`"),
                "the dotted component must stay ONE quoted identifier: " + sql);
        Assertions.assertFalse(sql.contains("internal.spm_db.t.a"),
                "the flattened four-part render is not the intended name: " + sql);
    }

    // ==================== GROUPING SETS (PhysicalRepeat) ====================

    @Test
    public void testSessionVarGuardedExpressionRejectsFreezing() {
        // an alias-UDF body computed under the DEFINITION's session variables carries a
        // SessionVarGuardExpr; the SQL text cannot express the guard, so freezing must
        // be rejected instead of silently replanning under the caller's variables
        org.apache.doris.nereids.trees.expressions.Expression guarded =
                new org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr(
                        new org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral(1),
                        java.util.Collections.singletonMap("enable_decimal256", "1"));
        org.apache.doris.nereids.trees.plans.logical.LogicalPlan plan =
                Mockito.mock(org.apache.doris.nereids.trees.plans.logical.LogicalPlan.class);
        // doReturn (Object-typed) sidesteps the wildcard capture of getExpressions()
        Mockito.doReturn(java.util.List.of(guarded)).when(plan).getExpressions();
        Mockito.doReturn(java.util.List.of()).when(plan).children();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMPlan2SQLBuilder.rejectSessionVarGuardedExpressions(plan));
    }

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

    // ==================== user names that collide with execution-internal markers ====================

    /**
     * A user UDAF whose own name starts with "partial_" is NOT an internal
     * execution stage. The physical plan keeps the USER'S function in every stage - the
     * "partial_" text is only a RENDERING of a buffer aggregate mode - so the name test
     * classified this one-phase GLOBAL aggregate as an intermediate stage, discarded the
     * aggregate and froze "SELECT * FROM t" (wrong cardinality AND columns on every later
     * baseline hit).
     */
    @Test
    public void testUserAggregateNamedPartialIsNotAnInternalStage() {
        SlotReference v = new SlotReference("v", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(v));

        // a REAL user UDAF instance whose own name starts with the internal prefix
        JavaUdaf userFn = new JavaUdaf("partial_myagg", 1L, "test_db", null,
                FunctionSignature.ret(IntegerType.INSTANCE).args(IntegerType.INSTANCE),
                null, org.apache.doris.catalog.Function.NullableMode.ALWAYS_NULLABLE,
                FunctionVolatility.IMMUTABLE, null, null, null, null, null, null, null, null,
                null, null, false, null, false, -1L, v);
        NamedExpression output = new Alias(new AggregateExpression(userFn,
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT)), "c");

        PhysicalHashAggregate<?> agg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(agg.child(0)).thenReturn(scan);
        Mockito.when(agg.getAggPhase()).thenReturn(AggPhase.GLOBAL);
        Mockito.when(agg.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(agg.getOutputExpressions()).thenReturn(List.of(output));
        stubAccept(agg);

        String sql = new SPMPlan2SQLBuilder().toSQL(agg);
        Assertions.assertTrue(sql.contains("partial_myagg(v)"),
                "the user aggregate must survive the decompile under its OWN name: " + sql);
        // A UDAF carries its DATABASE (JavaUdaf#getDbName) and the frozen SQL
        // must keep the qualifier - freezing db1.f(v) as f(v) let FunctionRegistry resolve
        // the replay under ANOTHER current database to a same-signature db2.f with a
        // different implementation.
        Assertions.assertTrue(sql.contains("test_db.partial_myagg(v)"),
                "the UDAF database qualifier must survive the freeze: " + sql);
    }

    /**
     * A quoted user column named count is a DATA argument, not the
     * count-star buffer. The buffer slot is decided by PROVENANCE - a slot the child
     * relation does not export - while this column is registered by its scan. The name
     * test alone froze count(`count()`) as count(*), so every later baseline hit
     * counted ROWS where the column is NULL (the review example returned 3 instead of 2).
     */
    @Test
    public void testQuotedCountColumnStaysAnAggregateArgument() {
        SlotReference countCol = new SlotReference("count()", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(countCol));
        NamedExpression output = new Alias(new AggregateExpression(new Count(countCol),
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT), countCol), "c");

        PhysicalHashAggregate<?> agg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(agg.child(0)).thenReturn(scan);
        Mockito.when(agg.getAggPhase()).thenReturn(AggPhase.GLOBAL);
        Mockito.when(agg.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(agg.getOutputExpressions()).thenReturn(List.of(output));
        stubAccept(agg);

        String sql = new SPMPlan2SQLBuilder().toSQL(agg);
        Assertions.assertTrue(sql.contains("count(`count()`)"),
                "the quoted column must stay the aggregate argument: " + sql);
        Assertions.assertFalse(sql.contains("count(*)"),
                "a user column named count() must not collapse into a star: " + sql);
    }

    /**
     * (the other side of the same provenance rule): an EXECUTION-ONLY buffer
     * slot - named after the count-star SQL but exported by NO relation - still collapses
     * into count(*), so the fix does not leak count(partial_count(*)) into the
     * frozen SQL.
     */
    @Test
    public void testExecutionOnlyCountBufferStillCollapsesIntoStar() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k));
        SlotReference buffer = new SlotReference("partial_count(*)", IntegerType.INSTANCE);
        NamedExpression output = new Alias(new AggregateExpression(new Count(),
                new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT), buffer), "c");

        PhysicalHashAggregate<?> agg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(agg.child(0)).thenReturn(scan);
        Mockito.when(agg.getAggPhase()).thenReturn(AggPhase.GLOBAL);
        Mockito.when(agg.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(agg.getOutputExpressions()).thenReturn(List.of(output));
        stubAccept(agg);

        String sql = new SPMPlan2SQLBuilder().toSQL(agg);
        Assertions.assertTrue(sql.contains("count(*)"),
                "the unexported execution buffer must still collapse into count(*): " + sql);
    }

    /**
     * A legal user column named GROUPING_ID must flow through projections.
     * Only the synthetic ROLLUP marker is dropped, told apart by provenance (no relation
     * exports it) rather than by the name: the name test removed the column from the
     * frozen child SELECT while the outer projection still referenced it, so every replay
     * failed with "Unknown column 'GROUPING_ID' in 'table list'".
     */
    @Test
    public void testUserColumnNamedGroupingIdIsExported() {
        SlotReference groupId = new SlotReference("GROUPING_ID", IntegerType.INSTANCE);
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k, groupId));
        PhysicalProject<?> project = mockProjectExprs(List.of(
                (NamedExpression) groupId,
                (NamedExpression) new Alias(new Add(k, new IntegerLiteral(1)), "kk")), scan);

        String sql = new SPMPlan2SQLBuilder().toSQL(project);
        Assertions.assertTrue(sql.contains("GROUPING_ID"),
                "a user column named GROUPING_ID must stay in the SELECT list: " + sql);
    }

    // ==================== ASSERT_ROWS (PhysicalAssertNumRows) ====================

    @Test
    public void testDecompileAssertNumRows() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(a));

        PhysicalAssertNumRows<?> assertNumRows = Mockito.mock(PhysicalAssertNumRows.class);
        Mockito.when(assertNumRows.child(0)).thenReturn(scan);
        stubAccept(assertNumRows);

        // The Nereids grammar has NO ASSERT_ROWS relation production, so rendering one
        // produced frozen texts that cannot be re-parsed - after a restart such a baseline
        // had no plan tree to fall back to and silently stopped applying. The node must
        // therefore be REJECTED, which makes the freeze keep the user planSql text.
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> new SPMPlan2SQLBuilder().toSQL(assertNumRows),
                "ASSERT_ROWS has no SQL representation and must fail the decompile");
    }

    // ==================== the exported alias of a wrapped join operand ====================

    /**
     * A joined operand whose clauses force ONE MORE wrapper (a TopN / Limit over a set
     * operation: the relation carries fromCarriesAlias AND its own ORDER BY / LIMIT block)
     * must expose the FINAL wrapper alias to the join references. Building the ON clause
     * through ensureQualifierAlias() before that wrap referenced the INNER set alias
     * (t_0), which afterwards only names the derived table INSIDE the FROM text - the
     * frozen SQL could not bind after reload.
     */
    @Test
    public void testJoinOverWrappedSetOperandUsesExposedAlias() {
        SlotReference setOutput = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalUnion union = mockUnionExporting(setOutput);
        PhysicalTopN<?> wrapped = mockTopN(union,
                List.of(new OrderKey(setOutput, true, true)), 1);
        SlotReference rightKey = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan right = mockScan("t9", List.of(rightKey));

        PhysicalHashJoin<?, ?> join = mockJoin(wrapped, right,
                new EqualTo(setOutput, rightKey));
        Mockito.when(join.getOutput()).thenReturn(List.of(setOutput, rightKey));

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        String leftOperand = sql.substring(sql.indexOf(" FROM ") + 6, sql.indexOf(" INNER JOIN"))
                .trim();
        List<String> aliases = tableAliases(leftOperand);
        Assertions.assertTrue(aliases.size() >= 2,
                "the set operand must be wrapped one level deeper: " + sql);
        String inner = aliases.get(0);
        String exposed = aliases.get(aliases.size() - 1);
        Assertions.assertNotEquals(inner, exposed, sql);
        String on = sql.substring(sql.indexOf(" ON ") + 4);
        Assertions.assertTrue(on.contains(exposed + "."),
                "the ON clause must reference the EXPOSED alias " + exposed + ": " + sql);
        Assertions.assertFalse(on.contains(inner + "."),
                "the ON clause must not reference the hidden inner alias " + inner + ": " + sql);
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(sql),
                "the frozen join text must re-parse: " + sql);
    }

    /**
     * Same for the NULL-AWARE anti path: the NOT IN predicate qualifies the preserved
     * (left) columns, so the qualifier must be allocated AFTER the wrapped operand's
     * final alias exists.
     */
    @Test
    public void testNullAwareAntiOverWrappedLeftUsesExposedAlias() {
        SlotReference setOutput = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalUnion union = mockUnionExporting(setOutput);
        PhysicalTopN<?> wrapped = mockTopN(union,
                List.of(new OrderKey(setOutput, true, true)), 1);
        SlotReference rightKey = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan right = mockScan("t9", List.of(rightKey));

        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getJoinType()).thenReturn(JoinType.NULL_AWARE_LEFT_ANTI_JOIN);
        Mockito.when(join.getHashJoinConjuncts())
                .thenReturn(List.of((Expression) new EqualTo(setOutput, rightKey)));
        Mockito.when(join.getOtherJoinConjuncts())
                .thenReturn(List.of((Expression) new GreaterThan(rightKey, new IntegerLiteral(5))));
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(Optional.empty());
        Mockito.when(join.left()).thenReturn(wrapped);
        Mockito.when(join.right()).thenReturn(right);
        Mockito.when(join.getOutput()).thenReturn(List.of(setOutput));
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        String leftOperand = sql.substring(sql.indexOf(" FROM ") + 6, sql.indexOf(" WHERE "))
                .trim();
        List<String> aliases = tableAliases(leftOperand);
        Assertions.assertTrue(aliases.size() >= 2,
                "the preserved set operand must be wrapped: " + sql);
        String inner = aliases.get(0);
        String exposed = aliases.get(aliases.size() - 1);
        String where = sql.substring(sql.indexOf(" WHERE "));
        Assertions.assertTrue(where.contains(exposed + ".k"),
                "the NOT IN probe must use the EXPOSED alias " + exposed + ": " + sql);
        Assertions.assertFalse(where.contains(inner + ".k"),
                "the NOT IN probe must not use the hidden inner alias " + inner + ": " + sql);
        Assertions.assertTrue(where.contains("NOT IN"), sql);
    }

    // ==================== generated c_N vs preserved output names ====================

    /**
     * The c_N counter only keeps GENERATED names apart. A preserved source column named
     * c_1 (here the group-by key) plus an unaliased SUM(v) used to emit
     * "GROUP BY c_1 ... sum(v) AS c_1": two ExprIds registered under one visible name, so
     * an enclosing projection / result sink read an AMBIGUOUS c_1 from the derived
     * relation. The generated name must skip the in-scope output names.
     */
    @Test
    public void testGeneratedAggregateAliasAvoidsSourceColumnName() {
        SlotReference groupKey = new SlotReference("c_1", IntegerType.INSTANCE);
        SlotReference v = new SlotReference("v", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(groupKey, v));

        PhysicalHashAggregate<?> agg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(agg.child(0)).thenReturn(scan);
        Mockito.when(agg.getAggPhase()).thenReturn(AggPhase.GLOBAL);
        Mockito.when(agg.getGroupByExpressions()).thenReturn(List.of(groupKey));
        Mockito.when(agg.getOutputExpressions()).thenReturn(List.of(
                (NamedExpression) groupKey,
                (NamedExpression) new Alias(new AggregateExpression(new Sum(v),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT)), "sum")));
        stubAccept(agg);

        String sql = new SPMPlan2SQLBuilder().toSQL(agg);
        Assertions.assertEquals(2, countOccurrences(sql, "c_1"),
                "c_1 may only appear as the source column (SELECT list + GROUP BY): " + sql);
        Assertions.assertFalse(sql.contains(" AS c_1"),
                "the generated aggregate alias must not shadow the source column: " + sql);
        Assertions.assertTrue(sql.contains("sum(v) AS c_"),
                "the aggregate still needs a generated reference: " + sql);
    }

    /**
     * A MARK_SLOT name is generated, so a preserved input column literally named c_1
     * would collide: "MARK_SLOT c_1" plus the exported c_1 make the upper filter /
     * projection ambiguous after reload.
     */
    @Test
    public void testMarkSlotNameAvoidsPreservedColumnName() {
        SlotReference preserved = new SlotReference("c_1", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        MarkJoinSlotReference markSlot = new MarkJoinSlotReference("m");

        PhysicalOlapScan left = mockScan("t1", List.of(preserved));
        PhysicalOlapScan right = mockScan("t2", List.of(b));
        PhysicalHashJoin<?, ?> join = Mockito.mock(PhysicalHashJoin.class);
        Mockito.when(join.getHashJoinConjuncts())
                .thenReturn(List.of((Expression) new EqualTo(preserved, b)));
        Mockito.when(join.getOtherJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinConjuncts()).thenReturn(List.of());
        Mockito.when(join.getMarkJoinSlotReference()).thenReturn(Optional.of(markSlot));
        Mockito.when(join.getJoinType()).thenReturn(JoinType.LEFT_SEMI_JOIN);
        Mockito.when(join.isMarkJoin()).thenReturn(true);
        Mockito.when(join.left()).thenReturn(left);
        Mockito.when(join.right()).thenReturn(right);
        stubAccept(join);

        String sql = new SPMPlan2SQLBuilder().toSQL(join);
        int idx = sql.indexOf("MARK_SLOT ");
        Assertions.assertTrue(idx > 0, sql);
        String markName = sql.substring(idx + "MARK_SLOT ".length()).split("\\s")[0];
        Assertions.assertNotEquals("c_1", markName,
                "the generated MARK_SLOT name must not collide with the preserved c_1: " + sql);
        Assertions.assertEquals(1, countOccurrences(sql, "c_1"),
                "the preserved column stays the only c_1: " + sql);
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
     * Builds a PhysicalOlapScan mock carrying ONE user-pinned partition: its FROM
     * fragment becomes "t1 PARTITION(pN)" instead of the bare table name (a composite
     * FROM fragment that is not usable as a column qualifier).
     */
    private PhysicalOlapScan mockPinnedScan(String tableName, List<SlotReference> outputs,
            String partitionName) {
        PhysicalOlapScan scan = Mockito.mock(PhysicalOlapScan.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getOutput()).thenReturn(List.copyOf(outputs));
        Mockito.when(scan.getScanParams()).thenReturn(Optional.empty());
        Mockito.when(scan.getSelectedPartitionIds()).thenReturn(List.of(7L));
        Mockito.when(scan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(scan.getManuallySpecifiedPartitions()).thenReturn(List.of(7L));
        Mockito.when(scan.getManuallySpecifiedTabletIds()).thenReturn(List.of());
        Partition partition = Mockito.mock(Partition.class);
        Mockito.when(partition.getName()).thenReturn(partitionName);
        Mockito.when(table.getPartition(7L)).thenReturn(partition);
        Mockito.when(table.isTemporaryPartition(7L)).thenReturn(false);
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
     * Builds a PhysicalTopN mock (one semantic TopN stage over its child).
     */
    private PhysicalTopN<?> mockTopN(Plan child, List<OrderKey> orderKeys, long limit) {
        PhysicalTopN<?> topN = Mockito.mock(PhysicalTopN.class);
        Mockito.when(topN.child(0)).thenReturn(child);
        Mockito.when(topN.getOrderKeys()).thenReturn(List.copyOf(orderKeys));
        Mockito.when(topN.getLimit()).thenReturn(limit);
        Mockito.when(topN.getOffset()).thenReturn(0L);
        stubAccept(topN);
        return topN;
    }

    /**
     * Builds a PhysicalQuickSort mock (a top-level ORDER BY WITHOUT the LIMIT).
     */
    private PhysicalQuickSort<?> mockQuickSort(Plan child, List<OrderKey> orderKeys) {
        PhysicalQuickSort<?> sort = Mockito.mock(PhysicalQuickSort.class);
        Mockito.when(sort.child(0)).thenReturn(child);
        Mockito.when(sort.getOrderKeys()).thenReturn(List.copyOf(orderKeys));
        stubAccept(sort);
        return sort;
    }

    /**
     * Builds a PhysicalWindow mock over the child with the given window expressions.
     */
    private org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> mockWindow(
            Plan child, List<NamedExpression> windowExprs) {
        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalWindow.class);
        Mockito.when(window.child(0)).thenReturn(child);
        Mockito.when(window.getWindowExpressions()).thenReturn(List.copyOf(windowExprs));
        // the live-column analysis starts from the ROOT output: without it the window
        // subtree would be pruned to nothing
        List<org.apache.doris.nereids.trees.expressions.Slot> output =
                new java.util.ArrayList<>(child.getOutput());
        for (NamedExpression windowExpr : windowExprs) {
            output.add(new org.apache.doris.nereids.trees.expressions.SlotReference(
                    windowExpr.getExprId(), windowExpr.getName(), windowExpr.getDataType(),
                    true, List.of("t")));
        }
        Mockito.when(window.getOutput()).thenReturn(output);
        stubAccept(window);
        return window;
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
     * Builds a PhysicalProject mock whose items are arbitrary named expressions
     * (e.g. computed aliases that are not SlotReferences).
     */
    private PhysicalProject<?> mockProjectExprs(List<NamedExpression> projects, Plan child) {
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
        if (plan instanceof PhysicalTopN) {
            return builder.visitPhysicalTopN((PhysicalTopN<? extends Plan>) plan, null);
        }
        if (plan instanceof org.apache.doris.nereids.trees.plans.physical.PhysicalWindow) {
            return builder.visitPhysicalWindow(
                    (org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?>) plan, null);
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
        if (plan instanceof PhysicalEmptyRelation) {
            return builder.visitPhysicalEmptyRelation((PhysicalEmptyRelation) plan, null);
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
        if (plan instanceof PhysicalQuickSort) {
            return builder.visitPhysicalQuickSort((PhysicalQuickSort<? extends Plan>) plan, null);
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

    /**
     * Constant-only UNION with DUPLICATE output names (including a case-variant pair:
     * identifiers are case-insensitive): every constant branch must alias each
     * duplicated output to the UNIQUE positional reference the set registered -
     * emitting `AS x` twice left the result sink's c_ExprId references
     * pointing at nonexistent columns and the frozen SQL failed re-analysis after
     * reload.
     */
    @Test
    public void testConstantUnionDuplicateNamesUseRegisteredPositionalRefs() {
        SlotReference first = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference second = new SlotReference("X", IntegerType.INSTANCE);
        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(Qualifier.ALL);
        Mockito.when(union.children()).thenReturn(List.of());
        Mockito.when(union.getRegularChildrenOutputs()).thenReturn(List.of());
        Mockito.when(union.getOutput()).thenReturn(List.of(first, second));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of(
                List.of((NamedExpression) new Alias(new IntegerLiteral(1), "x"),
                        (NamedExpression) new Alias(new IntegerLiteral(2), "x")),
                List.of((NamedExpression) new Alias(new IntegerLiteral(3), "x"),
                        (NamedExpression) new Alias(new IntegerLiteral(4), "x"))));
        stubAccept(union);

        String sql = new SPMPlan2SQLBuilder().toSQL(union);
        Assertions.assertEquals(4, sql.split(" AS c_", -1).length - 1,
                "both constant rows must alias BOTH duplicated outputs (x and the"
                        + " case-variant X) to their positional refs: " + sql);
        Assertions.assertFalse(sql.contains(" AS x"), sql);
        Assertions.assertFalse(sql.contains(" AS X"), sql);
    }

    /**
     * COMPUTED duplicate output aliases (k + 1 AS x, v + 1 AS x for two distinct
     * ExprIds) must be repaired like pass-through columns: leaving both as x made a
     * parent select x, x from a derived relation with two x columns and replay failed
     * as ambiguous after reload.
     */
    @Test
    public void testComputedDuplicateOutputNamesAreRenamed() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference v = new SlotReference("v", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k, v));
        Alias first = new Alias(new Add(k, new IntegerLiteral(1)), "x");
        Alias second = new Alias(new Add(v, new IntegerLiteral(1)), "x");
        PhysicalProject<?> project = mockProjectExprs(List.of(first, second), scan);

        String sql = new SPMPlan2SQLBuilder().toSQL(project);
        Assertions.assertEquals(1, sql.split(" AS x", -1).length - 1,
                "only the LATER duplicate keeps the shared alias: " + sql);
        Assertions.assertTrue(sql.contains(" AS c_"),
                "the earlier computed item must be renamed to a unique reference: " + sql);
    }

    /**
     * A generator output colliding with a live input column (SELECT t.x, lv.x FROM t
     * LATERAL VIEW explode(t.arr) lv AS x) must get a DISTINCT visible name: registering
     * both as bare x made the parent emit SELECT x, x ... which fails binding as
     * ambiguous after reload.
     */
    @Test
    public void testGenerateOutputCollidingWithInputColumnIsRenamed() {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference arr = new SlotReference("arr", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(x, arr));
        PhysicalGenerate<?> generate = mockGenerate(scan, new Explode(arr));

        SQLRelation relation = new SPMPlan2SQLBuilder().visitPhysicalGenerate(generate, null);
        Assertions.assertTrue(relation.getFrom().contains(" AS x_"),
                "the colliding generator output must be renamed: " + relation.getFrom());
        Assertions.assertTrue(relation.getColumnNames().containsValue("x"),
                "the input column keeps its name");
        Assertions.assertTrue(relation.getColumnNames().containsValue("x_"),
                "the generator slot is registered under the unique name");
    }

    /**
     * A window OUTPUT aliased like a live input column (row_number() ... AS x over a
     * relation exporting x) must be repaired like the intermediate Project: both used
     * to export bare x and the parent's SELECT x, x failed as ambiguous after reload.
     */
    @Test
    public void testWindowOutputCollidingWithInputColumnIsRepaired() {
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(x, k));

        org.apache.doris.nereids.trees.expressions.WindowExpression win =
                new org.apache.doris.nereids.trees.expressions.WindowExpression(
                        new org.apache.doris.nereids.trees.expressions.functions.window.RowNumber(),
                        List.of(), List.of());
        Alias windowAlias = new Alias(win, "x");

        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window =
                mockWindow(scan, List.of(windowAlias));
        String sql = new SPMPlan2SQLBuilder().toSQL(window);
        Assertions.assertEquals(1, sql.split(" AS x", -1).length - 1,
                "only the window output keeps the shared alias: " + sql);
        Assertions.assertTrue(sql.contains(" AS c_") || sql.contains("x AS c"),
                "the earlier input pass-through must be renamed: " + sql);
    }

    /**
     * #3: a QUOTED live column (a legal name like "a b") must be passed through the
     * Window SELECT. A scan registers it as `a b`, a parameterized filter keeps that
     * reference without a SELECT list, and the raw space / dot test dropped the slot: the
     * frozen SQL then referenced a column the derived table never exported and binding
     * failed after a reload.
     */
    @Test
    public void testWindowPassesThroughQuotedLiveColumn() {
        SlotReference quoted = new SlotReference("a b", IntegerType.INSTANCE);
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(quoted, k));
        PhysicalFilter<?> filter = mockFilter(new GreaterThan(k, new IntegerLiteral(100)), scan);

        org.apache.doris.nereids.trees.expressions.WindowExpression win =
                new org.apache.doris.nereids.trees.expressions.WindowExpression(
                        new org.apache.doris.nereids.trees.expressions.functions.window.RowNumber(),
                        List.of(), List.of());
        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window =
                mockWindow(filter, List.of((NamedExpression) new Alias(win, "rn")));

        SQLRelation relation = new SPMPlan2SQLBuilder().visitPhysicalWindow(window, null);
        Assertions.assertTrue(relation.getSelects().stream()
                        .anyMatch(p -> p.value().contains("`a b`")),
                "the quoted live column must be exported by the window SELECT: "
                        + relation.toSQL());
        Assertions.assertNotNull(new NereidsParser().parseSingle(relation.toSQL()),
                "the decompiled fragment must re-parse: " + relation.toSQL());
    }

    /**
     * A legal column named `a as b` (it CONTAINS the alias token). The inner
     * window exports it as a quoted pass-through item, an UPPER window consumes the
     * child's SELECT list - and the naive lastIndexOf(" as ") matched INSIDE the
     * backticks, registering "b`" as the reference so the next frozen layer no longer
     * parsed.
     */
    @Test
    public void testWindowPassesThroughQuotedNameContainingAsToken() {
        SlotReference tricky = new SlotReference("a as b", IntegerType.INSTANCE);
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(tricky, k));
        PhysicalFilter<?> filter = mockFilter(new GreaterThan(k, new IntegerLiteral(100)), scan);

        org.apache.doris.nereids.trees.expressions.WindowExpression win1 =
                new org.apache.doris.nereids.trees.expressions.WindowExpression(
                        new org.apache.doris.nereids.trees.expressions.functions.window.RowNumber(),
                        List.of(), List.of());
        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window1 =
                mockWindow(filter, List.of((NamedExpression) new Alias(win1, "rn")));
        org.apache.doris.nereids.trees.expressions.WindowExpression win2 =
                new org.apache.doris.nereids.trees.expressions.WindowExpression(
                        new org.apache.doris.nereids.trees.expressions.functions.window.RowNumber(),
                        List.of(), List.of());
        org.apache.doris.nereids.trees.plans.physical.PhysicalWindow<?> window2 =
                mockWindow(window1, List.of((NamedExpression) new Alias(win2, "rn2")));

        SQLRelation relation = new SPMPlan2SQLBuilder().visitPhysicalWindow(window2, null);
        Assertions.assertTrue(relation.getSelects().stream()
                        .anyMatch(p -> p.value().contains("`a as b`")),
                "the quoted name must survive the pass-through: " + relation.toSQL());
        Assertions.assertFalse(relation.getSelects().stream()
                        .anyMatch(p -> p.value().trim().startsWith("b`")),
                "the reference must not be cut out of the quoted name: " + relation.toSQL());
        Assertions.assertNotNull(new NereidsParser().parseSingle(relation.toSQL()),
                "the decompiled fragment must re-parse: " + relation.toSQL());
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

    // ==================== set operands are query terms, not aliased relations ====================

    /**
     * R11: set operands must be rendered as UNALIASED query terms. branch.newAlias() used
     * to emit "(SELECT ...) t_n" on either side of UNION / EXCEPT / INTERSECT, which
     * DorisParser rejects - a parenthesized query is accepted there, a trailing alias is
     * NOT (aliases are legal only in relation position under FROM). Creation never
     * re-parses the decompiled text, so the invalid fragment was invisible in memory; a
     * refresh / restart could not rebuild the persisted frozen baseline at all. The alias
     * is now allocated on the COMPLETED set relation only, and the text must re-parse.
     */
    @Test
    public void testSetOperandsAreUnaliasedAndReparse() {
        for (String keyword : List.of("UNION ALL", "EXCEPT ALL", "INTERSECT ALL")) {
            String sql;
            if ("UNION ALL".equals(keyword)) {
                sql = new SPMPlan2SQLBuilder().toSQL(mockUnion(Qualifier.ALL));
            } else if ("EXCEPT ALL".equals(keyword)) {
                sql = new SPMPlan2SQLBuilder().toSQL(mockExcept(Qualifier.ALL));
            } else {
                sql = new SPMPlan2SQLBuilder().toSQL(mockIntersect(Qualifier.ALL));
            }
            Assertions.assertTrue(sql.contains(" " + keyword + " "),
                    keyword + " must appear: " + sql);
            // the operand BEFORE the keyword must be a bare parenthesized query term:
            // ") t_N <keyword>" is the illegal aliased-operand shape
            Assertions.assertFalse(java.util.regex.Pattern
                            .compile("(?s)\\)\\s+t_\\d+\\s+" + keyword).matcher(sql).find(),
                    "a set operand must carry NO trailing alias: " + sql);
            Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(sql),
                    "the frozen set text must re-parse (refresh / restart path): " + sql);
        }
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

    /**
     * Builds a UNION ALL over two scans whose EXPORTED column is the given slot (so a
     * join against a same-named column takes the qualification path). The slot instance
     * is SHARED with the caller: the join references it by ExprId, so a different
     * instance would look up nothing.
     */
    private PhysicalUnion mockUnionExporting(SlotReference output) {
        SlotReference leftX = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference rightX = new SlotReference("x", IntegerType.INSTANCE);
        PhysicalOlapScan left = mockScan("t1", List.of(leftX));
        PhysicalOlapScan right = mockScan("t2", List.of(rightX));
        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(Qualifier.ALL);
        Mockito.when(union.children()).thenReturn(List.of(left, right));
        Mockito.when(union.getRegularChildrenOutputs())
                .thenReturn(List.of(List.of(leftX), List.of(rightX)));
        Mockito.when(union.getOutput()).thenReturn(List.of(output));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of());
        stubAccept(union);
        return union;
    }

    /** All t_N alias tokens of a FROM fragment, in order of appearance. */
    private static List<String> tableAliases(String fragment) {
        List<String> aliases = new ArrayList<>();
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("t_\\d+")
                .matcher(fragment);
        while (matcher.find()) {
            aliases.add(matcher.group());
        }
        return aliases;
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

        // toSql() prints only the BARE slot name. A QUALIFIED registered reference (the
        // collision-renaming case) still names the column, but freezing "~(b)" while the
        // relation exported "t_2.b" produces text that no longer resolves - and a
        // persisted frozen row has no plan-tree fallback.
        SQLRelation qualified = new SQLRelation();
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        qualified.registerRef(b.getExprId(), "t_2.b");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMExprSqlBuilder.ensureNoRemappedSlots(new BitNot(b), qualified),
                "toSql() cannot reproduce a qualified reference, so the fallback must be"
                        + " rejected");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> builder.visit((Expression) new BitNot(b), qualified));

        // a QUOTED reference for a special-character column would be re-parsed as an
        // expression ("a-b" is a subtraction)
        SQLRelation quoted = new SQLRelation();
        SlotReference dashed = new SlotReference("a-b", IntegerType.INSTANCE);
        quoted.registerRef(dashed.getExprId(), "`a-b`");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMExprSqlBuilder.ensureNoRemappedSlots(new BitNot(dashed), quoted),
                "toSql() would emit a-b which re-parses as a subtraction");

        // only a byte-identical registered reference is renderable through the fallback
        SQLRelation plain = new SQLRelation();
        SlotReference c = new SlotReference("c", IntegerType.INSTANCE);
        plain.registerRef(c.getExprId(), "c");
        Assertions.assertDoesNotThrow(
                () -> SPMExprSqlBuilder.ensureNoRemappedSlots(new BitNot(c), plain),
                "a reference identical to the slot name stays renderable");
        Assertions.assertNotNull(builder.visit((Expression) new BitNot(c), plain));
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

    /**
     * The GROUPED distinct shape. SELECT k, SUM(DISTINCT x), SUM(y)
     * GROUP BY k is split into a final DISTINCT_GLOBAL sum(x) (isDistinct CLEARED)
     * above a dedup stage grouping by (k, x) - and this final SUM consumes the key x
     * DIRECTLY (INPUT_TO_RESULT, no lower partial buffer), so the buffer provenance the
     * test above exercises has nothing to mark. The keys the eliminated dedup stage groups
     * by are the remaining evidence: without restoring them the frozen SQL summed the
     * key's multiplicity (4 instead of 2 for two x = 2 rows).
     */
    @Test
    public void testGroupedDistinctKeepsDistinctWithAMixedAggregate() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference x = new SlotReference("x", IntegerType.INSTANCE);
        SlotReference y = new SlotReference("y", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k, x, y));

        // the eliminated dedup stage: GROUP BY (k, x), one partial buffer for the PLAIN sum
        Alias plainBuffer = new Alias(new AggregateExpression(new Sum(y),
                new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER)), "buf");
        PhysicalHashAggregate<?> dedup = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(dedup.child(0)).thenReturn(scan);
        Mockito.when(dedup.getAggPhase()).thenReturn(AggPhase.GLOBAL);
        Mockito.when(dedup.getGroupByExpressions()).thenReturn(List.of(k, x));
        Mockito.when(dedup.getOutputExpressions()).thenReturn(List.of(k, x, plainBuffer));
        stubAccept(dedup);

        SlotReference bufferSlot = new SlotReference(
                plainBuffer.getExprId(), "buf", IntegerType.INSTANCE, true, List.of());
        PhysicalHashAggregate<?> finalAgg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(finalAgg.child(0)).thenReturn(dedup);
        Mockito.when(finalAgg.getAggPhase()).thenReturn(AggPhase.DISTINCT_GLOBAL);
        Mockito.when(finalAgg.getGroupByExpressions()).thenReturn(List.of(k));
        Mockito.when(finalAgg.getOutputExpressions()).thenReturn(List.of(
                k,
                (NamedExpression) new Alias(new AggregateExpression(new Sum(x),
                        new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT)),
                        "sx"),
                (NamedExpression) new Alias(new AggregateExpression(new Sum(bufferSlot),
                        new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                        bufferSlot), "sy")));
        stubAccept(finalAgg);

        String sql = new SPMPlan2SQLBuilder().toSQL(finalAgg);
        Assertions.assertTrue(sql.contains("sum(DISTINCT x)"),
                "the key-consuming SUM of a DISTINCT_GLOBAL stage must restore DISTINCT: "
                        + sql);
        Assertions.assertTrue(sql.contains("sum(y)"),
                "the riding aggregate keeps its plain form: " + sql);
        Assertions.assertFalse(sql.contains("sum(DISTINCT y)"), sql);
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

        // Semantic string values are decoded before rendering: a backslash must be DOUBLED
        // (the DEFAULT sql_mode treats it as an escape introducer - release\next would
        // reparse as release + newline + ext) and a quote doubled. The SPM re-parses pin
        // the DEFAULT mode, so the round trip is exact no matter which mode the session
        // carried.
        PhysicalFileScan semantic = mockFileScan();
        Mockito.when(semantic.getScanParams()).thenReturn(Optional.of(
                new TableScanParams("options",
                        Map.of("path", "release\\next", "quote", "a'b"), List.of())));
        String semanticFrom = new SPMPlan2SQLBuilder().visitPhysicalRelation(semantic, null).getFrom();
        Assertions.assertTrue(semanticFrom.contains("'release\\\\next'"),
                "a backslash must be doubled so the value cannot change meaning: " + semanticFrom);
        Assertions.assertTrue(semanticFrom.contains("'a''b'"),
                "a quote must stay doubled: " + semanticFrom);
        for (long sessionMode : new long[] {SqlModeHelper.MODE_DEFAULT,
                SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
            Map<String, String> decoded = SqlModeHelper.withSqlMode(sessionMode, () -> {
                LogicalPlan parsed = (LogicalPlan) SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT,
                        () -> new NereidsParser().parseSingle("SELECT * FROM " + semanticFrom));
                final UnboundRelation[] found = new UnboundRelation[1];
                SPMPlanTreeSupport.<RuntimeException>walkPlans(parsed, (Plan node) -> {
                    if (found[0] == null && node instanceof UnboundRelation) {
                        found[0] = (UnboundRelation) node;
                    }
                });
                return found[0].getScanParams().getMapParams();
            });
            Assertions.assertEquals("release\\next", decoded.get("path"),
                    "the pinned re-parse must decode the value back (session mode " + sessionMode + ")");
            Assertions.assertEquals("a'b", decoded.get("quote"));
        }

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

    // ==================== typed empty relation ====================

    /**
     * A pruned empty branch must keep every output column's DECLARED type: emitting INT 1
     * for all columns makes a reanalyzed placeholder-bearing UNION widen the branch (INT +
     * DATEV2 -> DATETIMEV2) and changes the result metadata.
     */
    @Test
    public void testEmptyRelationKeepsDeclaredTypes() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference d = new SlotReference("d", DateV2Type.INSTANCE);
        PhysicalEmptyRelation empty = Mockito.mock(PhysicalEmptyRelation.class);
        Mockito.doReturn(List.of((NamedExpression) a, (NamedExpression) d))
                .when(empty).getProjects();
        stubAccept(empty);

        String sql = new SPMPlan2SQLBuilder().toSQL(empty);
        Assertions.assertTrue(sql.contains("CAST(NULL AS INT)"),
                "the INT column keeps its type: " + sql);
        Assertions.assertTrue(sql.contains("CAST(NULL AS DATEV2)"),
                "the DATEV2 column keeps its type: " + sql);
        Assertions.assertTrue(sql.contains("FALSE"), "the branch stays filtered out: " + sql);
    }

    // ==================== LIKE ... ESCAPE ====================

    /** A three-argument LIKE keeps its ESCAPE child (dropping it changes the match). */
    @Test
    public void testLikeEscapeIsRendered() {
        SlotReference s = new SlotReference("s", org.apache.doris.nereids.types.VarcharType.SYSTEM_DEFAULT);
        Like like = new Like(s, new StringLiteral("a!%"), new StringLiteral("!"));
        SQLRelation relation = new SQLRelation();
        relation.registerRef(s.getExprId(), "s");

        String rendered = new SPMExprSqlBuilder().print(like, relation);
        Assertions.assertTrue(rendered.contains("LIKE"), rendered);
        Assertions.assertTrue(rendered.contains("ESCAPE '!'"),
                "the escape character must survive the decompile: " + rendered);
    }

    // ==================== explicit target types ====================

    /**
     * An explicit CAST keeps the type's SQL form (toSql), not its diagnostic toString: a
     * STRUCT frozen as a STRUCT with StructField[...] entries cannot be re-parsed after a reload.
     */
    @Test
    public void testExplicitStructCastUsesSqlTypeForm() {
        SlotReference s = new SlotReference("s", IntegerType.INSTANCE);
        StructType structType = new StructType(List.of(
                new StructField("a", IntegerType.INSTANCE, true, "")));
        Cast cast = new Cast(s, structType, true);
        SQLRelation relation = new SQLRelation();
        relation.registerRef(s.getExprId(), "s");

        String rendered = new SPMExprSqlBuilder().print(cast, relation);
        Assertions.assertTrue(rendered.contains("STRUCT<a:INT>"),
                "the grammar-parseable STRUCT form must be used: " + rendered);
        Assertions.assertFalse(rendered.contains("StructField"),
                "the diagnostic toString form must not leak: " + rendered);
    }

    // ==================== user-defined function qualifier ====================

    /**
     * A bound user-defined function keeps its database qualifier: a frozen db1.f(k) must
     * not resolve to db2.f(k) when the replayed SQL runs under another default database.
     */
    @Test
    public void testUserDefinedFunctionKeepsDbQualifier() {
        JavaUdf udf = Mockito.mock(JavaUdf.class);
        Mockito.when(udf.getDbName()).thenReturn("db1");
        Mockito.when(udf.getName()).thenReturn("f");
        Assertions.assertEquals("db1.f", SPMExprSqlBuilder.functionName(udf),
                "a bound UDF must render with its creation database");

        Mockito.when(udf.getDbName()).thenReturn("");
        Assertions.assertEquals("f", SPMExprSqlBuilder.functionName(udf),
                "an empty qualifier renders the bare name");

        // EVERY name component must be quoted independently: a database named my-db
        // used to be emitted verbatim, so `my-db`.f(k) came out as my-db.f(k) and the
        // frozen SQL re-parsed as the SUBTRACTION my - db.f(k) (analysis failure on
        // replay, or - worse - silently resolving a different column list)
        Mockito.when(udf.getDbName()).thenReturn("my-db");
        Assertions.assertEquals("`my-db`.f", SPMExprSqlBuilder.functionName(udf),
                "a database component that is not a plain identifier must be quoted");
        Mockito.when(udf.getName()).thenReturn("my-fn");
        Assertions.assertEquals("`my-db`.`my-fn`", SPMExprSqlBuilder.functionName(udf),
                "the function name component is quoted independently as well");

        // A GLOBAL UDF has NO database qualifier, and its registered name is
        // not necessarily a plain identifier (my-fn) - emitting it bare made the frozen
        // text re-parse as a SUBTRACTION (there is no parameterized-tree fallback for a
        // persisted frozen row, so the baseline could never replay)
        Mockito.when(udf.getDbName()).thenReturn("");
        Assertions.assertEquals("`my-fn`", SPMExprSqlBuilder.functionName(udf),
                "an unqualified GLOBAL UDF name must still be quoted when needed");

        // A user function NAMED like the SPM placeholder marker would be
        // indistinguishable from a genuine marker in the frozen text - the freeze must
        // refuse it instead of storing a text the replay either mis-substitutes or
        // rejects as an unresolved marker
        Mockito.when(udf.getName()).thenReturn("_spm_const_var");
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMExprSqlBuilder.functionName(udf),
                "the marker-name collision must be refused at freeze time");
    }

    // ==================== the final relabel wrapper keeps a top-level LIMIT ====================

    /**
     * The ResultSink wrapper is a PURE output relabelling (no filtering, no
     * aggregation), so the child's TOP-LEVEL ORDER BY / LIMIT pair belongs on the
     * wrapper: left INSIDE the derived table the LIMIT was unreachable for the
     * positional mergeLimits of the user query's Limit node (a matched LIMIT 200 query
     * kept the captured 100-row cap), and a derived-table ORDER BY without its LIMIT is
     * only a hint the optimizer may drop (the replay would then return an arbitrary
     * unordered LIMIT slice).
     */
    @Test
    public void testSinkRelabelWrapperHoistsTopLevelOrderByWithLimit() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k));
        PhysicalProject<?> project = mockProject(List.of(k), scan);
        PhysicalTopN<?> topN = mockTopN(project,
                List.of(new OrderKey(k, true, true)), 100);

        Slot output = Mockito.mock(Slot.class);
        Mockito.when(output.getExprId()).thenReturn(k.getExprId());
        Mockito.when(output.getName()).thenReturn("k1");
        org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink sink =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink.class);
        Mockito.when(sink.child(0)).thenReturn(topN);
        Mockito.when(sink.getOutput()).thenReturn(List.of(output));

        SQLRelation relation = new SPMPlan2SQLBuilder().visitPhysicalSink(sink, null);
        String sql = relation.toSQL();
        Assertions.assertTrue(sql.trim().endsWith("LIMIT 100"),
                "the top-level LIMIT must stay reachable at the statement tail: " + sql);
        Assertions.assertTrue(sql.contains("ORDER BY k ASC NULLS FIRST LIMIT 100"),
                "the ORDER BY must move WITH its LIMIT - separated from the LIMIT it is"
                        + " only a droppable hint and the replay returns an unordered"
                        + " slice: " + sql);
        Assertions.assertFalse(sql.contains("(SELECT k FROM t1 ORDER BY")
                        || sql.contains("(SELECT k FROM t1 LIMIT"),
                "neither clause may stay buried inside the relabel wrapper: " + sql);

        // and the frozen text must re-parse with a Limit DIRECTLY under the result sink -
        // that is exactly the node mergeLimits adopts the user's limit value from
        LogicalPlan frozen = (LogicalPlan) new NereidsParser().parseSingle(sql);
        boolean rootIsLimit = frozen
                instanceof org.apache.doris.nereids.trees.plans.logical.LogicalLimit
                || (!frozen.children().isEmpty() && frozen.child(0)
                instanceof org.apache.doris.nereids.trees.plans.logical.LogicalLimit);
        Assertions.assertTrue(rootIsLimit,
                "mergeLimits can only adopt the user's limit when the frozen root IS a"
                        + " Limit: " + frozen.getClass().getSimpleName() + " / " + sql);
    }

    /**
     * The same hoist must happen WITHOUT a LIMIT. "SELECT a FROM t ORDER BY b + 1"
     * arrives as a Sort under the relabel wrapper, and an ORDER BY left inside the
     * derived table is re-planned as a droppable hint - the replay then returned
     * unordered rows although the user explicitly asked for an order.
     */
    @Test
    public void testSinkRelabelWrapperHoistsOrderByWithoutLimit() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k));
        PhysicalProject<?> project = mockProject(List.of(k), scan);
        PhysicalQuickSort<?> sort = mockQuickSort(project,
                List.of(new OrderKey(k, true, true)));

        Slot output = Mockito.mock(Slot.class);
        Mockito.when(output.getExprId()).thenReturn(k.getExprId());
        Mockito.when(output.getName()).thenReturn("k1");
        org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink sink =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink.class);
        Mockito.when(sink.child(0)).thenReturn(sort);
        Mockito.when(sink.getOutput()).thenReturn(List.of(output));

        String sql = new SPMPlan2SQLBuilder().visitPhysicalSink(sink, null).toSQL();
        Assertions.assertTrue(sql.trim().endsWith("NULLS FIRST"),
                "the top-level ORDER BY must stay at the statement tail: " + sql);
        Assertions.assertTrue(sql.indexOf("ORDER BY") > sql.lastIndexOf(")"),
                "the ORDER BY must not stay buried inside the relabel wrapper: " + sql);
    }

    /**
     * #15: a (final) Project over Sort must keep the user's ORDER BY at the OUTER query.
     * NormalizeSort makes "SELECT a FROM t ORDER BY b + 1" a Project over Sort; inlining
     * the child relation buried the clause one level down (droppable hint).
     */
    @Test
    public void testProjectOverSortHoistsTopLevelOrderBy() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k));
        PhysicalProject<?> inner = mockProject(List.of(k), scan);
        PhysicalQuickSort<?> sort = mockQuickSort(inner, List.of(new OrderKey(k, true, true)));
        PhysicalProject<?> top = mockProject(List.of(k), sort);

        String sql = new SPMPlan2SQLBuilder().toSQL(top);
        Assertions.assertTrue(sql.trim().endsWith("NULLS FIRST"),
                "the projection must keep the user's ORDER BY at the outer query: " + sql);
        Assertions.assertTrue(sql.indexOf("ORDER BY") > sql.lastIndexOf(")"),
                "the ORDER BY must not stay inside the derived table of the projection: "
                        + sql);
    }

    /**
     * #15: a (final) Project over TopN must keep BOTH clauses outside. The buried cap was
     * invisible to mergeLimitNodes positional merge (the frozen root was a Project), so a
     * matching "LIMIT 20" could not replace the captured one and the query kept the old
     * cap.
     */
    @Test
    public void testProjectOverTopNHoistsOrderByAndLimit() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k));
        PhysicalProject<?> inner = mockProject(List.of(k), scan);
        PhysicalTopN<?> topN = mockTopN(inner, List.of(new OrderKey(k, true, true)), 20);
        PhysicalProject<?> top = mockProject(List.of(k), topN);

        String sql = new SPMPlan2SQLBuilder().toSQL(top);
        Assertions.assertTrue(sql.trim().endsWith("LIMIT 20"),
                "the cap must stay reachable at the statement tail: " + sql);
        Assertions.assertTrue(sql.indexOf("ORDER BY") > sql.lastIndexOf(")"),
                "both clauses must move OUT of the derived table: " + sql);

        // the frozen root must carry a Limit so a matching user LIMIT replaces the value
        LogicalPlan frozen = (LogicalPlan) new NereidsParser().parseSingle(sql);
        boolean rootCarriesLimit = frozen
                instanceof org.apache.doris.nereids.trees.plans.logical.LogicalLimit
                || frozen instanceof org.apache.doris.nereids.trees.plans.logical.LogicalTopN
                || (!frozen.children().isEmpty() && (frozen.child(0)
                instanceof org.apache.doris.nereids.trees.plans.logical.LogicalLimit
                || frozen.child(0)
                instanceof org.apache.doris.nereids.trees.plans.logical.LogicalTopN));
        Assertions.assertTrue(rootCarriesLimit,
                "mergeLimits can only adopt the user's limit when the frozen root (or its"
                        + " direct child) IS a Limit: "
                        + frozen.getClass().getSimpleName() + " / " + sql);
    }

    /**
     * A fully QUOTED column name is a resolvable ORDER BY reference - the
     * hoist must not leave the user's sort buried inside a derived table, where a no-LIMIT
     * sort is a droppable hint at replay and the rows come back unordered.
     */
    @Test
    public void testProjectOverSortHoistsQuotedOrderByKey() {
        SlotReference col = new SlotReference("a b", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(col));
        PhysicalProject<?> inner = mockProject(List.of(col), scan);
        PhysicalQuickSort<?> sort = mockQuickSort(inner, List.of(new OrderKey(col, true, true)));
        PhysicalProject<?> top = mockProject(List.of(col), sort);

        String sql = new SPMPlan2SQLBuilder().toSQL(top);
        Assertions.assertTrue(sql.indexOf("ORDER BY") > sql.lastIndexOf(")"),
                "the quoted ORDER BY key must hoist out of the derived table: " + sql);
        Assertions.assertNotNull(new NereidsParser().parseSingle(sql),
                "the frozen text must re-parse: " + sql);
    }

    /**
     * Hoisting the child's "ORDER BY b" (the BASE column) above a wrapper
     * that EXPORTS an alias named b would rebind the sort to that alias: the original
     * sorts by the base column, the replay would sort by the alias' expression. The
     * key is shadowed, so the clause must keep its own query block.
     */
    @Test
    public void testHoistedOrderByKeyIsNeverCapturedByAnOutputAlias() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(a, b));
        PhysicalTopN<?> topN = mockTopN(scan, List.of(new OrderKey(b, true, true)), 1);
        // the wrapper exports `b` COMPUTED from a - hoisting "ORDER BY b" would bind
        // the sort to this alias instead of the base column
        PhysicalProject<?> top = mockProjectExprs(
                List.of(new Alias(new Cast(a, IntegerType.INSTANCE), "b")), topN);

        String sql = new SPMPlan2SQLBuilder().toSQL(top);
        Assertions.assertNotNull(new NereidsParser().parseSingle(sql),
                "the frozen text must re-parse: " + sql);
        Assertions.assertTrue(sql.indexOf("ORDER BY") < sql.lastIndexOf(")"),
                "the sort key must keep its own query block instead of being captured by"
                        + " the b alias: " + sql);
    }

    /**
     * control: without a shadowing alias the hoist still happens - the
     * clause lands at the statement tail where the base column it names stays
     * resolvable.
     */
    @Test
    public void testHoistStillHappensWhenNoAliasShadowsTheKey() {
        SlotReference a = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference b = new SlotReference("b", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(a, b));
        PhysicalTopN<?> topN = mockTopN(scan, List.of(new OrderKey(b, true, true)), 2);
        PhysicalProject<?> top = mockProjectExprs(
                List.of(new Alias(new Cast(a, IntegerType.INSTANCE), "s")), topN);

        String sql = new SPMPlan2SQLBuilder().toSQL(top);
        Assertions.assertNotNull(new NereidsParser().parseSingle(sql),
                "the frozen text must re-parse: " + sql);
        Assertions.assertTrue(sql.indexOf("ORDER BY b") > sql.lastIndexOf(")"),
                "the unshadowed key hoists to the statement tail: " + sql);
        Assertions.assertTrue(sql.contains("LIMIT 2"), sql);
    }

    /**
     * An already-exported QUOTED sort key (a-b) must be recognised through
     * the backticks - appending it a second time exposed two identical columns in the
     * derived table and made every outer reference ambiguous.
     */
    @Test
    public void testExportedQuotedSortKeyIsNotAppendedTwice() {
        SlotReference dashed = new SlotReference("a-b", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(dashed));
        PhysicalProject<?> inner = mockProject(List.of(dashed), scan);
        PhysicalQuickSort<?> sort = mockQuickSort(inner,
                List.of(new OrderKey(dashed, true, true)));
        PhysicalProject<?> top = mockProject(List.of(dashed), sort);

        Slot output = Mockito.mock(Slot.class);
        Mockito.when(output.getExprId()).thenReturn(dashed.getExprId());
        Mockito.when(output.getName()).thenReturn("a-b");
        org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink sink =
                Mockito.mock(org.apache.doris.nereids.trees.plans.physical.PhysicalResultSink.class);
        Mockito.when(sink.child(0)).thenReturn(top);
        Mockito.when(sink.getOutput()).thenReturn(List.of(output));

        String sql = new SPMPlan2SQLBuilder().visitPhysicalSink(sink, null).toSQL();
        Assertions.assertFalse(sql.contains("`a-b`, `a-b`"),
                "the quoted sort key is already exported and must not be appended again:"
                        + " two identical columns make every outer reference ambiguous: " + sql);
        Assertions.assertNotNull(new NereidsParser().parseSingle(sql),
                "the frozen text must re-parse: " + sql);
    }

    /**
     * #2: a RESERVED word is a legal QUOTED alias but not an unquoted one: freezing
     * "sum(v) AS from" produced text the parser rejects, and a persisted frozen row has
     * no plan-tree fallback - the baseline could never replay.
     */
    @Test
    public void testReservedWordAliasIsQuoted() {
        SlotReference v = new SlotReference("v", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(v));
        PhysicalHashAggregate<?> agg = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(agg.child(0)).thenReturn(scan);
        Mockito.when(agg.getAggPhase()).thenReturn(AggPhase.GLOBAL);
        Mockito.when(agg.getGroupByExpressions()).thenReturn(List.of());
        Mockito.when(agg.getOutputExpressions()).thenReturn(List.of(
                (NamedExpression) new Alias(new AggregateExpression(new Sum(v),
                        new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT)), "from")));
        stubAccept(agg);

        String sql = new SPMPlan2SQLBuilder().toSQL(agg);
        Assertions.assertTrue(sql.contains("AS `from`"),
                "a reserved word alias must freeze quoted: " + sql);
        Assertions.assertNotNull(new NereidsParser().parseSingle(sql),
                "the frozen text must re-parse: a persisted row has no fallback: " + sql);
    }

    /**
     * #13: two SELECT items may collide in ANY case (a computed alias "x" next to a
     * computed alias "X"). Doris resolves column names case-insensitively, so exporting
     * both made every upper reference ambiguous after reload.
     */
    @Test
    public void testCaseInsensitiveDuplicateOutputNamesAreRenamed() {
        SlotReference k = new SlotReference("k", IntegerType.INSTANCE);
        SlotReference v = new SlotReference("v", IntegerType.INSTANCE);
        PhysicalOlapScan scan = mockScan("t1", List.of(k, v));
        Alias first = new Alias(new Add(k, new IntegerLiteral(1)), "x");
        Alias second = new Alias(new Add(v, new IntegerLiteral(2)), "X");
        PhysicalProject<?> project = mockProjectExprs(List.of(first, second), scan);

        String sql = new SPMPlan2SQLBuilder().toSQL(project);
        Assertions.assertEquals(1, sql.split(" AS X", -1).length - 1,
                "only the LATER case-variant keeps its alias: " + sql);
        Assertions.assertTrue(sql.contains(" AS c_"),
                "the earlier item must be renamed when the names collide only by case: "
                        + sql);
    }

    /**
     * #9: positional aliases of a SET must be unique across ALL its outputs. A duplicate
     * output whose ExprId is 1 generated c_1 while another output was ALREADY named c_1 -
     * the registered names were (c_1, c_2, c_1), and both the branches and the result sink
     * referenced an ambiguous c_1 after the frozen SQL was re-parsed.
     */
    @Test
    public void testSetPositionalAliasAvoidsLiveOutputName() {
        SlotReference live = new SlotReference("c_1", IntegerType.INSTANCE);
        SlotReference first = new SlotReference(new org.apache.doris.nereids.trees.expressions.ExprId(1),
                "x", IntegerType.INSTANCE, true, List.of());
        SlotReference second = new SlotReference(new org.apache.doris.nereids.trees.expressions.ExprId(2),
                "X", IntegerType.INSTANCE, true, List.of());
        PhysicalUnion union = Mockito.mock(PhysicalUnion.class);
        Mockito.when(union.getQualifier()).thenReturn(Qualifier.ALL);
        Mockito.when(union.children()).thenReturn(List.of());
        Mockito.when(union.getRegularChildrenOutputs()).thenReturn(List.of());
        Mockito.when(union.getOutput()).thenReturn(List.of(live, first, second));
        Mockito.when(union.getConstantExprsList()).thenReturn(List.of(List.of(
                (NamedExpression) new Alias(new IntegerLiteral(1), "c_1"),
                (NamedExpression) new Alias(new IntegerLiteral(2), "x"),
                (NamedExpression) new Alias(new IntegerLiteral(3), "X"))));
        stubAccept(union);

        String sql = new SPMPlan2SQLBuilder().toSQL(union);
        Assertions.assertTrue(sql.contains(" AS c_1_"),
                "the colliding positional alias must be made unique: " + sql);
        java.util.regex.Matcher names = java.util.regex.Pattern
                .compile(" AS ([A-Za-z_][A-Za-z0-9_]*)").matcher(sql);
        java.util.List<String> emitted = new java.util.ArrayList<>();
        while (names.find()) {
            String name = names.group(1).toLowerCase(java.util.Locale.ROOT);
            Assertions.assertFalse(emitted.contains(name),
                    "every set output name must be unique, saw " + name + " twice: " + sql);
            emitted.add(name);
        }
    }
}
