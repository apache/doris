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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.mtmv.ivm.IvmAggMeta;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggTarget;
import org.apache.doris.mtmv.ivm.IvmAggMeta.AggType;
import org.apache.doris.mtmv.ivm.IvmAggMeta.StateKey;
import org.apache.doris.mtmv.ivm.IvmNormalizeResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class IvmNormalizeMtmvTest {

    // DUP_KEYS table — row-id = UuidNumeric(), non-deterministic
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testGateDisabledKeepsPlanUnchanged() {
        Plan result = new IvmNormalizeMtmv().rewriteRoot(scan, newJobContext(false));
        Assertions.assertSame(scan, result);
    }

    @Test
    void testScanInjectsRowIdAtIndexZero() {
        JobContext jobContext = newJobContext(true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(scan, jobContext);

        // scan is wrapped in a project
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertSame(scan, project.child());

        // first output is the row-id alias
        List<? extends Slot> outputs = project.getOutput();
        Assertions.assertEquals(scan.getOutput().size() + 1, outputs.size());
        Slot rowIdSlot = outputs.get(0);
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, rowIdSlot.getName());

        // row-id expression is UuidNumeric for DUP_KEYS
        Alias rowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertInstanceOf(UuidNumeric.class, rowIdAlias.child());

        // IvmNormalizeResult records non-deterministic for DUP_KEYS
        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        Assertions.assertEquals(1, normalizeResult.getRowIdDeterminism().size());
        Assertions.assertFalse(normalizeResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testProjectOnScanPropagatesRowId() {
        Slot slot = scan.getOutput().get(0);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(slot), scan);

        Plan result = new IvmNormalizeMtmv().rewriteRoot(project, newJobContext(true));

        // outer project has row-id at index 0
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> outer = (LogicalProject<?>) result;
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, outer.getOutput().get(0).getName());
        // child is the scan-wrapping project
        Assertions.assertInstanceOf(LogicalProject.class, outer.child());
        Assertions.assertSame(scan, ((LogicalProject<?>) outer.child()).child());
    }

    @Test
    void testProjectReplacesRowIdPlaceholderAndKeepsExprId() {
        Alias placeholder = new Alias(new NullLiteral(LargeIntType.INSTANCE), Column.IVM_ROW_ID_COL);
        ExprId placeholderExprId = placeholder.getExprId();
        Slot slot = scan.getOutput().get(0);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(placeholder, slot), scan);

        Plan result = new IvmNormalizeMtmv().rewriteRoot(project, newJobContext(true));

        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> rewrittenProject = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(Alias.class, rewrittenProject.getProjects().get(0));
        Alias rewrittenRowId = (Alias) rewrittenProject.getProjects().get(0);
        Assertions.assertEquals(placeholderExprId, rewrittenRowId.getExprId());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, rewrittenRowId.getName());
        Assertions.assertInstanceOf(Slot.class, rewrittenRowId.child());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, ((Slot) rewrittenRowId.child()).getName());
    }

    @Test
    void testSinkWithPlaceholderChildReplacesRowIdAndPreservesExprId() {
        // Simulate what BindSink produces for an incremental MTMV full-refresh:
        // a project child with user columns + a NULL placeholder for the IVM row-id at the end.
        Slot k1Slot = scan.getOutput().get(0);
        Alias rowIdPlaceholder = new Alias(new NullLiteral(LargeIntType.INSTANCE), Column.IVM_ROW_ID_COL);
        ExprId placeholderExprId = rowIdPlaceholder.getExprId();
        LogicalProject<Plan> projectWithPlaceholder = new LogicalProject<>(
                ImmutableList.of(k1Slot, rowIdPlaceholder), scan);
        LogicalOlapTableSink<Plan> sink = new LogicalOlapTableSink<>(
                new Database(),
                scan.getTable(),
                ImmutableList.of(scan.getTable().getBaseSchema().get(0)),
                new ArrayList<>(),
                ImmutableList.of(k1Slot, rowIdPlaceholder.toSlot()),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.NONE,
                projectWithPlaceholder);

        Plan result = new IvmNormalizeMtmv().rewriteRoot(sink, newJobContextForRoot(sink, true));

        Assertions.assertInstanceOf(LogicalOlapTableSink.class, result);
        LogicalOlapTableSink<?> rewrittenSink = (LogicalOlapTableSink<?>) result;
        // child is a project with the placeholder replaced
        Assertions.assertInstanceOf(LogicalProject.class, rewrittenSink.child());
        LogicalProject<?> rewrittenProject = (LogicalProject<?>) rewrittenSink.child();
        // non-IVM column unchanged at index 0
        Assertions.assertEquals(k1Slot.getName(), rewrittenProject.getProjects().get(0).getName());
        // IVM placeholder at index 1 is now an Alias wrapping the real row-id scan slot
        Assertions.assertInstanceOf(Alias.class, rewrittenProject.getProjects().get(1));
        Alias rewrittenRowId = (Alias) rewrittenProject.getProjects().get(1);
        Assertions.assertEquals(placeholderExprId, rewrittenRowId.getExprId());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, rewrittenRowId.getName());
        Assertions.assertInstanceOf(Slot.class, rewrittenRowId.child());
        // sink outputExprs updated via withChildAndUpdateOutput — row-id slot at index 1
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, rewrittenSink.getOutputExprs().get(1).getName());
    }

    @Test
    void testLogicalOlapTableSinkKeepsSinkShapeAndNormalizesChild() {
        Slot slot = scan.getOutput().get(0);
        LogicalOlapTableSink<Plan> sink = new LogicalOlapTableSink<>(
                new Database(),
                scan.getTable(),
                ImmutableList.of(scan.getTable().getBaseSchema().get(0)),
                new ArrayList<>(),
                ImmutableList.of(slot),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.NONE,
                scan);

        Plan result = new IvmNormalizeMtmv().rewriteRoot(sink, newJobContextForRoot(sink, true));

        Assertions.assertInstanceOf(LogicalOlapTableSink.class, result);
        LogicalOlapTableSink<?> rewrittenSink = (LogicalOlapTableSink<?>) result;
        Assertions.assertEquals(ImmutableList.of(slot.getName()),
                rewrittenSink.getCols().stream().map(org.apache.doris.catalog.Column::getName)
                        .collect(Collectors.toList()));
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, rewrittenSink.getOutputExprs().get(0).getName());
        Assertions.assertInstanceOf(LogicalProject.class, rewrittenSink.child());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, rewrittenSink.child().getOutput().get(0).getName());
    }

    @Test
    void testMowTableRowIdIsDeterministic() {
        OlapTable mowTable = PlanConstructor.newOlapTable(10, "mow", 0, KeysType.UNIQUE_KEYS);
        TableProperty tableProperty = new TableProperty(new java.util.HashMap<>());
        tableProperty.setEnableUniqueKeyMergeOnWrite(true);
        mowTable.setTableProperty(tableProperty);
        LogicalOlapScan mowScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), mowTable, ImmutableList.of("db"));

        JobContext jobContext = newJobContextForScan(mowScan, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(mowScan, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, result.getOutput().get(0).getName());
        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        Assertions.assertTrue(normalizeResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testMorTableThrows() {
        // UNIQUE_KEYS without MOW (MOR) is not supported
        OlapTable morTable = PlanConstructor.newOlapTable(11, "mor", 0, KeysType.UNIQUE_KEYS);
        LogicalOlapScan morScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), morTable, ImmutableList.of("db"));

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(morScan, newJobContextForScan(morScan, true)));
    }

    @Test
    void testAggKeyTableThrows() {
        OlapTable aggTable = PlanConstructor.newOlapTable(12, "agg", 0, KeysType.AGG_KEYS);
        LogicalOlapScan aggScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), aggTable, ImmutableList.of("db"));

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(aggScan, newJobContextForScan(aggScan, true)));
    }

    @Test
    void testUnsupportedPlanNodeThrows() {
        LogicalSort<Plan> sort = new LogicalSort<>(ImmutableList.of(), scan);

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(sort, newJobContext(true)));
    }

    @Test
    void testUnsupportedNodeAsChildThrows() {
        Slot slot = scan.getOutput().get(0);
        LogicalSort<Plan> sort = new LogicalSort<>(ImmutableList.of(), scan);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(slot), sort);

        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(project, newJobContext(true)));
    }

    @Test
    void testNormalizedPlanStoredInIvmNormalizeResult() {
        JobContext jobContext = newJobContext(true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(scan, jobContext);

        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        Assertions.assertNotNull(normalizeResult.getNormalizedPlan());
        Assertions.assertSame(result, normalizeResult.getNormalizedPlan());
    }

    @Test
    void testIdempotencyGuardSkipsSecondRewrite() {
        JobContext jobContext = newJobContext(true);
        Plan firstResult = new IvmNormalizeMtmv().rewriteRoot(scan, jobContext);
        // Second rewrite on the same CascadesContext should return root unchanged
        Plan secondResult = new IvmNormalizeMtmv().rewriteRoot(firstResult, jobContext);
        Assertions.assertSame(firstResult, secondResult);
    }

    // ======================== Aggregate tests ========================

    /**
     * Builds a normalized aggregate: Aggregate(groupBy=[idSlot], output=[idSlot, Alias(Sum(nameSlot))])
     * over the DUP_KEYS scan. This is the shape NormalizeAggregate produces.
     */
    private LogicalAggregate<Plan> buildGroupedAgg() {
        // scan has: id (INT), name (STRING)
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias sumAlias = new Alias(new Sum(nameSlot), "sum_name");
        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot, sumAlias);
        return new LogicalAggregate<>(groupBy, outputs, true, java.util.Optional.empty(), scan);
    }

    /**
     * Builds a scalar aggregate (no GROUP BY): Aggregate(groupBy=[], output=[Alias(Count())])
     */
    private LogicalAggregate<Plan> buildScalarAgg() {
        Alias countAlias = new Alias(new Count(), "cnt");
        List<Expression> groupBy = ImmutableList.of();
        List<NamedExpression> outputs = ImmutableList.of(countAlias);
        return new LogicalAggregate<>(groupBy, outputs, true, java.util.Optional.empty(), scan);
    }

    @Test
    void testGroupedAggInjectsRowIdAndHiddenColumns() {
        LogicalAggregate<Plan> agg = buildGroupedAgg();
        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        // Result is a Project wrapping the modified Aggregate
        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(LogicalAggregate.class, topProject.child());

        // Top project outputs: [row_id, id, sum_name, __DORIS_IVM_AGG_COUNT_COL__, hidden_0_SUM, hidden_0_COUNT]
        List<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, outputNames.get(0));
        Assertions.assertEquals("id", outputNames.get(1));
        Assertions.assertEquals("sum_name", outputNames.get(2));
        Assertions.assertEquals(Column.IVM_AGG_COUNT_COL, outputNames.get(3));
        Assertions.assertEquals(IvmUtil.ivmAggHiddenColumnName(0, "SUM"), outputNames.get(4));
        Assertions.assertEquals(IvmUtil.ivmAggHiddenColumnName(0, "COUNT"), outputNames.get(5));

        // row-id expression is hash(id) via Cast(MurmurHash364)
        Alias rowIdAlias = (Alias) topProject.getProjects().get(0);
        Assertions.assertInstanceOf(
                org.apache.doris.nereids.trees.expressions.Cast.class, rowIdAlias.child());

        // IvmNormalizeResult has aggMeta
        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        IvmAggMeta aggMeta = normalizeResult.getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertFalse(aggMeta.isScalarAgg());
        Assertions.assertEquals(1, aggMeta.getGroupKeySlots().size());
        Assertions.assertEquals("id", aggMeta.getGroupKeySlots().get(0).getName());
        Assertions.assertEquals(Column.IVM_AGG_COUNT_COL, aggMeta.getGroupCountSlot().getName());

        // One agg target: SUM
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        AggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(0, target.getOrdinal());
        Assertions.assertEquals(AggType.SUM, target.getAggType());
        Assertions.assertEquals("sum_name", target.getVisibleSlot().getName());
        Assertions.assertNotNull(target.getHiddenStateSlot(StateKey.SUM));
        Assertions.assertNotNull(target.getHiddenStateSlot(StateKey.COUNT));

        // Row-id determinism: grouped agg → deterministic
        Assertions.assertTrue(normalizeResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testScalarAggRowIdIsZeroConstant() {
        LogicalAggregate<Plan> agg = buildScalarAgg();
        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> topProject = (LogicalProject<?>) result;

        // row-id is LargeIntLiteral(0) for scalar agg
        Alias rowIdAlias = (Alias) topProject.getProjects().get(0);
        Assertions.assertInstanceOf(LargeIntLiteral.class, rowIdAlias.child());
        Assertions.assertEquals(BigInteger.ZERO, ((LargeIntLiteral) rowIdAlias.child()).getValue());

        // IvmAggMeta: scalar, no group keys
        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        IvmAggMeta aggMeta = normalizeResult.getAggMeta();
        Assertions.assertTrue(aggMeta.isScalarAgg());
        Assertions.assertTrue(aggMeta.getGroupKeySlots().isEmpty());
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        Assertions.assertEquals(AggType.COUNT_STAR, aggMeta.getAggTargets().get(0).getAggType());

        // Row-id determinism: scalar agg → non-deterministic
        Assertions.assertFalse(normalizeResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testMultipleAggFunctionsProduceCorrectHiddenColumns() {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        // SELECT id, COUNT(*), SUM(name), AVG(name) GROUP BY id
        Alias countStarAlias = new Alias(new Count(), "cnt");
        Alias sumAlias = new Alias(new Sum(nameSlot), "s");
        Alias avgAlias = new Alias(new Avg(nameSlot), "a");

        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(
                idSlot, countStarAlias, sumAlias, avgAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        IvmAggMeta aggMeta = normalizeResult.getAggMeta();
        Assertions.assertEquals(3, aggMeta.getAggTargets().size());

        // ordinal 0: COUNT_STAR → hidden: COUNT
        AggTarget t0 = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(AggType.COUNT_STAR, t0.getAggType());
        Assertions.assertEquals(1, t0.getHiddenStateSlots().size());
        Assertions.assertNotNull(t0.getHiddenStateSlot(StateKey.COUNT));

        // ordinal 1: SUM → hidden: SUM, COUNT
        AggTarget t1 = aggMeta.getAggTargets().get(1);
        Assertions.assertEquals(AggType.SUM, t1.getAggType());
        Assertions.assertEquals(2, t1.getHiddenStateSlots().size());

        // ordinal 2: AVG → hidden: SUM, COUNT
        AggTarget t2 = aggMeta.getAggTargets().get(2);
        Assertions.assertEquals(AggType.AVG, t2.getAggType());
        Assertions.assertEquals(2, t2.getHiddenStateSlots().size());

        // Verify hidden column naming in the project output
        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        // Global group count
        Assertions.assertTrue(outputNames.contains(Column.IVM_AGG_COUNT_COL));
        // Per-agg hidden columns
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "COUNT")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(1, "SUM")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(1, "COUNT")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(2, "SUM")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(2, "COUNT")));
    }

    @Test
    void testCountExprProducesCountExprType() {
        Slot nameSlot = scan.getOutput().get(1);
        Alias countExprAlias = new Alias(new Count(nameSlot), "cnt_name");
        List<NamedExpression> outputs = ImmutableList.of(countExprAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(), outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmNormalizeResult().get().getAggMeta();
        Assertions.assertEquals(AggType.COUNT_EXPR, aggMeta.getAggTargets().get(0).getAggType());
    }

    @Test
    void testAggUnderFilterThrows() {
        LogicalAggregate<Plan> agg = buildGroupedAgg();
        LogicalFilter<Plan> filter = new LogicalFilter<>(
                ImmutableSet.of(BooleanLiteral.TRUE), agg);

        Assertions.assertThrows(AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(filter, newJobContextForRoot(filter, true)));
    }

    @Test
    void testDistinctAggThrows() {
        Slot nameSlot = scan.getOutput().get(1);
        Alias distinctCount = new Alias(new Count(true, nameSlot), "cnt_distinct");
        List<NamedExpression> outputs = ImmutableList.of(distinctCount);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(), outputs, true, java.util.Optional.empty(), scan);

        Assertions.assertThrows(AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(agg, newJobContextForRoot(agg, true)));
    }

    @Test
    void testUnsupportedAggFunctionThrows() {
        Slot nameSlot = scan.getOutput().get(1);
        Alias anyValAlias = new Alias(new AnyValue(nameSlot), "av");
        List<NamedExpression> outputs = ImmutableList.of(anyValAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(), outputs, true, java.util.Optional.empty(), scan);

        Assertions.assertThrows(AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(agg, newJobContextForRoot(agg, true)));
    }

    @Test
    void testMinAggProducesMinAndCountHiddenColumns() {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias minAlias = new Alias(new Min(nameSlot), "mn");
        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot, minAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        // Normalization should succeed and produce hidden MIN + COUNT columns
        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmNormalizeResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        AggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(AggType.MIN, target.getAggType());
        Assertions.assertNotNull(target.getHiddenStateSlot(StateKey.MIN));
        Assertions.assertNotNull(target.getHiddenStateSlot(StateKey.COUNT));

        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "MIN")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "COUNT")));
    }

    @Test
    void testMaxAggProducesMaxAndCountHiddenColumns() {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias maxAlias = new Alias(new Max(nameSlot), "mx");
        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot, maxAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmNormalizeResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        AggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(AggType.MAX, target.getAggType());
        Assertions.assertNotNull(target.getHiddenStateSlot(StateKey.MAX));
        Assertions.assertNotNull(target.getHiddenStateSlot(StateKey.COUNT));

        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "MAX")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "COUNT")));
    }

    @Test
    void testBareGroupByWithoutAggFunctionsThrows() {
        Slot idSlot = scan.getOutput().get(0);
        // GROUP BY id with no aggregate functions
        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        Assertions.assertThrows(AnalysisException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(agg, newJobContextForRoot(agg, true)));
    }

    @Test
    void testExpressionAggArgumentAccepted() {
        // SUM(id + name) should be accepted — expression args are no longer rejected
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Expression addExpr = new org.apache.doris.nereids.trees.expressions.Add(idSlot, nameSlot);
        Alias sumAlias = new Alias(new Sum(addExpr), "sum_expr");

        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot, sumAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        // Normalization should succeed
        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmNormalizeResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());

        AggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(AggType.SUM, target.getAggType());
        // exprArgs should contain the Add expression, not a Slot
        Assertions.assertEquals(1, target.getExprArgs().size());
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Add.class,
                target.getExprArgs().get(0));
    }

    @Test
    void testExpressionAggArgumentForMinMaxAccepted() {
        // MIN(id + name) and MAX(id + name) should be accepted
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Expression mulExpr = new org.apache.doris.nereids.trees.expressions.Multiply(idSlot, nameSlot);
        Alias minAlias = new Alias(new Min(mulExpr), "min_expr");
        Alias maxAlias = new Alias(new Max(mulExpr), "max_expr");

        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot, minAlias, maxAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmNormalizeResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(2, aggMeta.getAggTargets().size());

        AggTarget minTarget = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(AggType.MIN, minTarget.getAggType());
        Assertions.assertEquals(1, minTarget.getExprArgs().size());
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Multiply.class,
                minTarget.getExprArgs().get(0));

        AggTarget maxTarget = aggMeta.getAggTargets().get(1);
        Assertions.assertEquals(AggType.MAX, maxTarget.getAggType());
        Assertions.assertEquals(1, maxTarget.getExprArgs().size());
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Multiply.class,
                maxTarget.getExprArgs().get(0));
    }

    private JobContext newJobContext(boolean enableIvmNormalRewrite) {
        return newJobContextForScan(scan, enableIvmNormalRewrite);
    }

    private JobContext newJobContextForScan(LogicalOlapScan rootScan, boolean enableIvmNormalRewrite) {
        return newJobContextForRoot(rootScan, enableIvmNormalRewrite);
    }

    private JobContext newJobContextForRoot(Plan root, boolean enableIvmNormalRewrite) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIvmNormalRewrite(enableIvmNormalRewrite);
        connectContext.setSessionVariable(sessionVariable);
        StatementContext statementContext = new StatementContext(connectContext, null);
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, root, PhysicalProperties.ANY);
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }
}
