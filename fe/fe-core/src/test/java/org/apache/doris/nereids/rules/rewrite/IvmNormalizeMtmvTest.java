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

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.mtmv.ivm.agg.IvmAggFunctionKind;
import org.apache.doris.mtmv.ivm.agg.IvmAggMeta;
import org.apache.doris.mtmv.ivm.agg.IvmAggStateKey;
import org.apache.doris.mtmv.ivm.agg.IvmAggTarget;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapFromString;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.expressions.functions.scalar.UuidNumeric;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.LargeIntType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class IvmNormalizeMtmvTest {

    // DUP_KEYS table — row-id = UuidNumeric(), non-deterministic
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @BeforeEach
    void setUp() {
        enableBinlog(scan.getTable());
    }

    @Test
    void testGateDisabledKeepsPlanUnchanged() {
        Plan result = new IvmNormalizeMtmv().rewriteRoot(scan, newJobContext(false));
        Assertions.assertSame(scan, result);
    }

    @Test
    void testIvmRewriteContextEnablesNormalizeWithoutSessionVariable() {
        JobContext jobContext = newJobContextForRoot(scan, false, Collections.emptySet(),
                Optional.of(new IvmRewriteContext(IvmRewriteContext.Mode.CREATE, null, false, false)));
        Plan result = new IvmNormalizeMtmv().rewriteRoot(scan, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().orElseThrow();
        Assertions.assertTrue(rewriteResult.isNormalizeRewritten());
        Assertions.assertSame(result, rewriteResult.getNormalizedPlan());
        Assertions.assertNotNull(rewriteResult.getPlanSignature());
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

        // IvmRewriteResult records non-deterministic for DUP_KEYS
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        Assertions.assertEquals(1, rewriteResult.getRowIdDeterminism().size());
        Assertions.assertFalse(rewriteResult.getRowIdDeterminism().values().iterator().next());
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
        enableBinlog(mowTable);
        LogicalOlapScan mowScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), mowTable, ImmutableList.of("db"));

        JobContext jobContext = newJobContextForScan(mowScan, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(mowScan, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, result.getOutput().get(0).getName());
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        Assertions.assertTrue(rewriteResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testMorTableThrows() {
        // UNIQUE_KEYS without MOW (MOR) is not supported
        OlapTable morTable = PlanConstructor.newOlapTable(11, "mor", 0, KeysType.UNIQUE_KEYS);
        enableBinlog(morTable);
        LogicalOlapScan morScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), morTable, ImmutableList.of("db"));

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> new IvmNormalizeMtmv().rewriteRoot(morScan, newJobContextForScan(morScan, true)));
    }

    @Test
    void testAggKeyTableThrows() {
        OlapTable aggTable = PlanConstructor.newOlapTable(12, "agg", 0, KeysType.AGG_KEYS);
        enableBinlog(aggTable);
        LogicalOlapScan aggScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), aggTable, ImmutableList.of("db"));

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> new IvmNormalizeMtmv().rewriteRoot(aggScan, newJobContextForScan(aggScan, true)));
    }

    @Test
    void testExcludedAggKeyTableUsesDeterministicRowIdHashOnAggKeys() {
        OlapTable aggTable = newAggKeyOlapTableWithBoundDb(12, "agg", "test");
        LogicalOlapScan aggScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), aggTable, ImmutableList.of("test"));

        JobContext jobContext = newJobContextForRoot(aggScan, true,
                Collections.singleton(new TableNameInfo("internal", "test", "agg")));
        Plan result = new IvmNormalizeMtmv().rewriteRoot(aggScan, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(0));
        Alias rowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertInstanceOf(MurmurHash3128.class, rowIdAlias.child());
        Assertions.assertEquals(
                IvmUtil.buildRowIdHash(ImmutableList.of(aggScan.getOutput().get(0))).toSql(),
                rowIdAlias.child().toSql());
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        Assertions.assertTrue(rewriteResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testExcludedAggKeyRowIdDoesNotIncludeValueColumns() {
        OlapTable aggTable = newAggKeyOlapTableWithBoundDb(14, "agg_value_check", "test");
        LogicalOlapScan aggScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), aggTable, ImmutableList.of("test"));

        Plan result = new IvmNormalizeMtmv().rewriteRoot(aggScan, newJobContextForRoot(aggScan, true,
                Collections.singleton(new TableNameInfo("internal", "test", "agg_value_check"))));

        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Assertions.assertInstanceOf(Alias.class, project.getProjects().get(0));
        Alias rowIdAlias = (Alias) project.getProjects().get(0);
        Assertions.assertNotEquals(
                IvmUtil.buildRowIdHash(aggScan.getOutput()).toSql(),
                rowIdAlias.child().toSql());
    }

    @Test
    void testExcludedMowTableUsesDeterministicRowId() {
        OlapTable mowTable = newOlapTableWithBoundDb(13, "excluded_mow", KeysType.UNIQUE_KEYS, "test");
        TableProperty tableProperty = new TableProperty(new java.util.HashMap<>());
        tableProperty.setEnableUniqueKeyMergeOnWrite(true);
        mowTable.setTableProperty(tableProperty);
        LogicalOlapScan mowScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), mowTable, ImmutableList.of("test"));

        JobContext jobContext = newJobContextForRoot(mowScan, true,
                Collections.singleton(new TableNameInfo("internal", "test", "excluded_mow")));
        Plan result = new IvmNormalizeMtmv().rewriteRoot(mowScan, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> project = (LogicalProject<?>) result;
        Alias rowIdAlias = (Alias) project.getProjects().get(0);
        // Excluded MOW table should still compute deterministic row-id from unique key hash
        Assertions.assertInstanceOf(MurmurHash3128.class, rowIdAlias.child());
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        Assertions.assertTrue(rewriteResult.getRowIdDeterminism().values().iterator().next());
    }

    @Test
    void testBaseTableWithoutBinlogThrowsIvmException() {
        OlapTable noBinlogTable = PlanConstructor.newOlapTable(15, "no_binlog", 0, KeysType.DUP_KEYS);
        LogicalOlapScan noBinlogScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), noBinlogTable, ImmutableList.of("db"));

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(noBinlogScan,
                        newJobContextForRoot(noBinlogScan, true, Collections.emptySet())));
        Assertions.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("no_binlog"));
    }

    @Test
    void testBaseTableWithCcrBinlogThrowsIvmException() {
        OlapTable ccrBinlogTable = PlanConstructor.newOlapTable(17, "ccr_binlog", 0, KeysType.DUP_KEYS);
        ccrBinlogTable.getBinlogConfig().setEnable(true);
        LogicalOlapScan ccrBinlogScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), ccrBinlogTable, ImmutableList.of("db"));

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> new IvmNormalizeMtmv().rewriteRoot(ccrBinlogScan,
                        newJobContextForRoot(ccrBinlogScan, true, Collections.emptySet())));
        Assertions.assertEquals(IvmFailureReason.BINLOG_NOT_ENABLED, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("row binlog is not enabled"));
    }

    @Test
    void testExcludedTableWithoutBinlogDoesNotThrowBinlogException() {
        OlapTable aggTable = newAggKeyOlapTableWithBoundDb(16, "excluded_no_binlog", "test");
        LogicalOlapScan aggScan = new LogicalOlapScan(
                PlanConstructor.getNextRelationId(), aggTable, ImmutableList.of("test"));

        Plan result = new IvmNormalizeMtmv().rewriteRoot(aggScan, newJobContextForRoot(aggScan, true,
                Collections.singleton(new TableNameInfo("internal", "test", "excluded_no_binlog"))));

        Assertions.assertInstanceOf(LogicalProject.class, result);
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, result.getOutput().get(0).getName());
    }

    @Test
    void testUnsupportedPlanNodeThrows() {
        LogicalSort<Plan> sort = new LogicalSort<>(ImmutableList.of(), scan);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> new IvmNormalizeMtmv().rewriteRoot(sort, newJobContext(true)));
    }

    @Test
    void testUnsupportedNodeAsChildThrows() {
        Slot slot = scan.getOutput().get(0);
        LogicalSort<Plan> sort = new LogicalSort<>(ImmutableList.of(), scan);
        LogicalProject<?> project = new LogicalProject<>(ImmutableList.of(slot), sort);

        assertIvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                () -> new IvmNormalizeMtmv().rewriteRoot(project, newJobContext(true)));
    }

    @Test
    void testNormalizedPlanStoredInIvmRewriteResult() {
        JobContext jobContext = newJobContext(true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(scan, jobContext);

        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        Assertions.assertNotNull(rewriteResult.getNormalizedPlan());
        Assertions.assertSame(result, rewriteResult.getNormalizedPlan());
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

        // Top project outputs: [row_id, id, sum_name, __DORIS_IVM_AGG_COUNT_COL__, hidden_0_COUNT]
        // SUM no longer has hidden SUM column (visible stores the sum); only hidden COUNT remains
        List<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toList());
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, outputNames.get(0));
        Assertions.assertEquals("id", outputNames.get(1));
        Assertions.assertEquals("sum_name", outputNames.get(2));
        Assertions.assertEquals(Column.IVM_AGG_COUNT_COL, outputNames.get(3));
        Assertions.assertEquals(IvmUtil.ivmAggHiddenColumnName(0, "COUNT"), outputNames.get(4));
        Assertions.assertEquals(5, outputNames.size());

        // row-id expression is a 128-bit hash over the group key.
        Alias rowIdAlias = (Alias) topProject.getProjects().get(0);
        Assertions.assertInstanceOf(MurmurHash3128.class, rowIdAlias.child());

        // IvmRewriteResult has aggMeta
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        IvmAggMeta aggMeta = rewriteResult.getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertFalse(aggMeta.isScalarAgg());
        Assertions.assertEquals(1, aggMeta.getGroupKeySlots().size());
        Assertions.assertEquals("id", aggMeta.getGroupKeySlots().get(0).getName());
        Assertions.assertEquals(Column.IVM_AGG_COUNT_COL, aggMeta.getGroupCountSlot().getName());

        // One agg target: SUM
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        IvmAggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(0, target.getOrdinal());
        Assertions.assertEquals(IvmAggFunctionKind.SUM, target.getFunctionKind());
        Assertions.assertEquals("sum_name", target.getVisibleSlot().getName());
        Assertions.assertFalse(target.getHiddenStateSlots().containsKey(IvmAggStateKey.SUM));
        Assertions.assertNotNull(target.getHiddenStateSlot(IvmAggStateKey.COUNT));

        // Row-id determinism: grouped agg → deterministic
        Assertions.assertTrue(rewriteResult.isDeterministic(rowIdAlias.toSlot()));
    }

    @Test
    void testRepeatUnderAggregateDoesNotOwnRowId() {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        LogicalRepeat<Plan> repeat = new LogicalRepeat<>(
                ImmutableList.of(ImmutableList.of(idSlot, nameSlot), ImmutableList.of(idSlot), ImmutableList.of()),
                ImmutableList.of(idSlot, nameSlot),
                RepeatType.GROUPING_SETS,
                scan);
        LogicalProject<Plan> project = new LogicalProject<>(ImmutableList.of(idSlot, nameSlot), repeat);
        Alias countAlias = new Alias(new Count(), "cnt");
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(idSlot, nameSlot),
                ImmutableList.of(idSlot, nameSlot, countAlias),
                true,
                java.util.Optional.of(repeat),
                project);

        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, newJobContextForRoot(agg, true));

        Assertions.assertInstanceOf(LogicalProject.class, result);
        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, topProject.getOutput().get(0).getName());
        Assertions.assertInstanceOf(LogicalAggregate.class, topProject.child());
        LogicalAggregate<?> normalizedAgg = (LogicalAggregate<?>) topProject.child();
        Assertions.assertTrue(normalizedAgg.getSourceRepeat().isPresent());
        LogicalRepeat<?> normalizedRepeat = normalizedAgg.getSourceRepeat().get();
        Assertions.assertFalse(normalizedRepeat.getOutput().stream()
                .anyMatch(slot -> Column.IVM_ROW_ID_COL.equals(slot.getName())));
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
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        IvmAggMeta aggMeta = rewriteResult.getAggMeta();
        Assertions.assertTrue(aggMeta.isScalarAgg());
        Assertions.assertTrue(aggMeta.getGroupKeySlots().isEmpty());
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        Assertions.assertEquals(IvmAggFunctionKind.COUNT, aggMeta.getAggTargets().get(0).getFunctionKind());
        Assertions.assertTrue(aggMeta.getAggTargets().get(0).isCountStar());
        Assertions.assertTrue(aggMeta.getAggTargets().get(0).getExprArgs().isEmpty());

        // Row-id determinism: scalar agg → non-deterministic
        Assertions.assertFalse(rewriteResult.getRowIdDeterminism().values().iterator().next());
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

        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        IvmAggMeta aggMeta = rewriteResult.getAggMeta();
        Assertions.assertEquals(3, aggMeta.getAggTargets().size());

        // ordinal 0: COUNT(*) → no hidden columns
        IvmAggTarget t0 = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(IvmAggFunctionKind.COUNT, t0.getFunctionKind());
        Assertions.assertTrue(t0.isCountStar());
        Assertions.assertEquals(0, t0.getHiddenStateSlots().size());

        // ordinal 1: SUM → hidden: COUNT only (no hidden SUM)
        IvmAggTarget t1 = aggMeta.getAggTargets().get(1);
        Assertions.assertEquals(IvmAggFunctionKind.SUM, t1.getFunctionKind());
        Assertions.assertEquals(1, t1.getHiddenStateSlots().size());
        Assertions.assertNotNull(t1.getHiddenStateSlot(IvmAggStateKey.COUNT));
        Assertions.assertFalse(t1.getHiddenStateSlots().containsKey(IvmAggStateKey.SUM));

        // ordinal 2: AVG → hidden: SUM, COUNT (both needed)
        IvmAggTarget t2 = aggMeta.getAggTargets().get(2);
        Assertions.assertEquals(IvmAggFunctionKind.AVG, t2.getFunctionKind());
        Assertions.assertEquals(2, t2.getHiddenStateSlots().size());

        // Verify hidden column naming in the project output
        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        // Global group count
        Assertions.assertTrue(outputNames.contains(Column.IVM_AGG_COUNT_COL));
        // Per-agg hidden columns: COUNT(*) has none, SUM has only COUNT, AVG has SUM+COUNT
        Assertions.assertFalse(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "COUNT")));
        Assertions.assertFalse(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(1, "SUM")));
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

        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertEquals(IvmAggFunctionKind.COUNT, aggMeta.getAggTargets().get(0).getFunctionKind());
        Assertions.assertFalse(aggMeta.getAggTargets().get(0).isCountStar());
        Assertions.assertEquals(1, aggMeta.getAggTargets().get(0).getExprArgs().size());
    }

    @Test
    void testAggUnderFilterThrows() {
        LogicalAggregate<Plan> agg = buildGroupedAgg();
        LogicalFilter<Plan> filter = new LogicalFilter<>(
                ImmutableSet.of(BooleanLiteral.TRUE), agg);

        assertIvmException(IvmFailureReason.AGG_UNSUPPORTED,
                () -> new IvmNormalizeMtmv().rewriteRoot(filter, newJobContextForRoot(filter, true)));
    }

    @Test
    void testDistinctAggThrows() {
        Slot nameSlot = scan.getOutput().get(1);
        Alias distinctCount = new Alias(new Count(true, nameSlot), "cnt_distinct");
        List<NamedExpression> outputs = ImmutableList.of(distinctCount);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(), outputs, true, java.util.Optional.empty(), scan);

        assertIvmException(IvmFailureReason.AGG_UNSUPPORTED,
                () -> new IvmNormalizeMtmv().rewriteRoot(agg, newJobContextForRoot(agg, true)));
    }

    @Test
    void testUnsupportedAggFunctionThrows() {
        Slot nameSlot = scan.getOutput().get(1);
        Alias anyValAlias = new Alias(new AnyValue(nameSlot), "av");
        List<NamedExpression> outputs = ImmutableList.of(anyValAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                ImmutableList.of(), outputs, true, java.util.Optional.empty(), scan);

        assertIvmException(IvmFailureReason.AGG_UNSUPPORTED,
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

        // Normalization should succeed; no hidden MIN column — only hidden COUNT
        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        IvmAggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(IvmAggFunctionKind.MIN, target.getFunctionKind());
        Assertions.assertNotNull(target.getHiddenStateSlot(IvmAggStateKey.COUNT));

        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        Assertions.assertFalse(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "MIN")));
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
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());
        IvmAggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(IvmAggFunctionKind.MAX, target.getFunctionKind());
        Assertions.assertNotNull(target.getHiddenStateSlot(IvmAggStateKey.COUNT));

        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        Assertions.assertFalse(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "MAX")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "COUNT")));
    }

    @Test
    void testBitmapAggProducesBitmapHiddenColumns() {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Expression bitmapExpr = new BitmapFromString(nameSlot);
        Alias bitmapUnionAlias = new Alias(new BitmapUnion(bitmapExpr), "bu");
        Alias bitmapUnionCountAlias = new Alias(new BitmapUnionCount(bitmapExpr), "buc");
        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot, bitmapUnionAlias, bitmapUnionCountAlias);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(2, aggMeta.getAggTargets().size());

        IvmAggTarget unionTarget = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(IvmAggFunctionKind.BITMAP_UNION, unionTarget.getFunctionKind());
        Assertions.assertTrue(unionTarget.getHiddenStateSlots().isEmpty());

        IvmAggTarget countTarget = aggMeta.getAggTargets().get(1);
        Assertions.assertEquals(IvmAggFunctionKind.BITMAP_UNION_COUNT, countTarget.getFunctionKind());
        Assertions.assertEquals(1, countTarget.getHiddenStateSlots().size());
        Assertions.assertNotNull(countTarget.getHiddenStateSlot(IvmAggStateKey.BITMAP_UNION));

        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        Assertions.assertFalse(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(0, "BITMAP_UNION")));
        Assertions.assertTrue(outputNames.contains(IvmUtil.ivmAggHiddenColumnName(1, "BITMAP_UNION")));
    }

    @Test
    void testBareGroupByWithoutAggFunctionsSucceeds() {
        Slot idSlot = scan.getOutput().get(0);
        // GROUP BY id with no aggregate functions — should produce only group-level count
        List<Expression> groupBy = ImmutableList.of(idSlot);
        List<NamedExpression> outputs = ImmutableList.of(idSlot);
        LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                groupBy, outputs, true, java.util.Optional.empty(), scan);

        JobContext jobContext = newJobContextForRoot(agg, true);
        Plan result = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);

        // Normalization should succeed with zero agg targets
        Assertions.assertInstanceOf(LogicalProject.class, result);
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(0, aggMeta.getAggTargets().size());
        Assertions.assertNotNull(aggMeta.getGroupCountSlot());
        Assertions.assertFalse(aggMeta.isScalarAgg());

        // Output should contain: row_id, group key (id), hidden group count
        LogicalProject<?> topProject = (LogicalProject<?>) result;
        Set<String> outputNames = topProject.getOutput().stream()
                .map(Slot::getName).collect(Collectors.toSet());
        Assertions.assertTrue(outputNames.contains(Column.IVM_ROW_ID_COL));
        Assertions.assertTrue(outputNames.contains(Column.IVM_AGG_COUNT_COL));
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
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(1, aggMeta.getAggTargets().size());

        IvmAggTarget target = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(IvmAggFunctionKind.SUM, target.getFunctionKind());
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
        IvmAggMeta aggMeta = jobContext.getCascadesContext().getIvmRewriteResult().get().getAggMeta();
        Assertions.assertNotNull(aggMeta);
        Assertions.assertEquals(2, aggMeta.getAggTargets().size());

        IvmAggTarget minTarget = aggMeta.getAggTargets().get(0);
        Assertions.assertEquals(IvmAggFunctionKind.MIN, minTarget.getFunctionKind());
        Assertions.assertEquals(1, minTarget.getExprArgs().size());
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Multiply.class,
                minTarget.getExprArgs().get(0));

        IvmAggTarget maxTarget = aggMeta.getAggTargets().get(1);
        Assertions.assertEquals(IvmAggFunctionKind.MAX, maxTarget.getFunctionKind());
        Assertions.assertEquals(1, maxTarget.getExprArgs().size());
        Assertions.assertInstanceOf(org.apache.doris.nereids.trees.expressions.Multiply.class,
                maxTarget.getExprArgs().get(0));
    }

    private JobContext newJobContext(boolean enableIvmNormalRewrite) {
        return newJobContextForScan(scan, enableIvmNormalRewrite);
    }

    private void assertIvmException(IvmFailureReason failureReason, Executable executable) {
        IvmException exception = Assertions.assertThrows(IvmException.class, executable);
        Assertions.assertEquals(failureReason, exception.getFailureReason());
    }

    private JobContext newJobContextForScan(LogicalOlapScan rootScan, boolean enableIvmNormalRewrite) {
        return newJobContextForRoot(rootScan, enableIvmNormalRewrite);
    }

    private JobContext newJobContextForRoot(Plan root, boolean enableIvmNormalRewrite) {
        return newJobContextForRoot(root, enableIvmNormalRewrite, Collections.emptySet(), Optional.empty());
    }

    private JobContext newJobContextForRoot(Plan root, boolean enableIvmNormalRewrite,
            Set<TableNameInfo> excludedTriggerTables) {
        return newJobContextForRoot(root, enableIvmNormalRewrite, excludedTriggerTables, Optional.empty());
    }

    private JobContext newJobContextForRoot(Plan root, boolean enableIvmNormalRewrite,
            Set<TableNameInfo> excludedTriggerTables, Optional<IvmRewriteContext> ivmRewriteContext) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIvmNormalRewrite(enableIvmNormalRewrite);
        connectContext.setSessionVariable(sessionVariable);
        StatementContext statementContext = new StatementContext(connectContext, null);
        statementContext.setExcludedTriggerTables(excludedTriggerTables);
        statementContext.setIvmRewriteContext(ivmRewriteContext);
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, root, PhysicalProperties.ANY);
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }

    private OlapTable newOlapTableWithBoundDb(long tableId, String tableName, KeysType keysType, String dbName) {
        Database database = new Database(1L, dbName);
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                new Column("name", Type.STRING, true, AggregateType.NONE, "", ""));
        HashDistributionInfo distributionInfo = new HashDistributionInfo(3, ImmutableList.of(columns.get(0)));
        OlapTable table = new OlapTable(tableId, tableName, columns, keysType,
                new PartitionInfo(), distributionInfo) {
            @Override
            public Database getDatabase() {
                return database;
            }
        };
        table.setIndexMeta(-1, tableName, table.getFullSchema(), 0, 0, (short) 0,
                TStorageType.COLUMN, keysType);
        table.setQualifiedDbName(dbName);
        enableBinlog(table);
        return table;
    }

    private OlapTable newAggKeyOlapTableWithBoundDb(long tableId, String tableName, String dbName) {
        Database database = new Database(1L, dbName);
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                new Column("value_sum", Type.INT, false, AggregateType.SUM, "0", ""));
        HashDistributionInfo distributionInfo = new HashDistributionInfo(3, ImmutableList.of(columns.get(0)));
        OlapTable table = new OlapTable(tableId, tableName, columns, KeysType.AGG_KEYS,
                new PartitionInfo(), distributionInfo) {
            @Override
            public Database getDatabase() {
                return database;
            }
        };
        table.setIndexMeta(-1, tableName, table.getFullSchema(), 0, 0, (short) 0,
                TStorageType.COLUMN, KeysType.AGG_KEYS);
        table.setQualifiedDbName(dbName);
        return table;
    }

    private void enableBinlog(OlapTable table) {
        table.getBinlogConfig().setEnable(true);
        table.getBinlogConfig().setBinlogFormat(BinlogConfig.BinlogFormat.ROW);
    }
}
