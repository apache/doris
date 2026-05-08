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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTable.OlapTableState;
import org.apache.doris.catalog.OlapTableFactory;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.catalog.Type;
import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.IvmNormalizeMtmv;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

abstract class IvmDeltaTestBase {

    protected LogicalOlapScan buildScan() {
        OlapTable table = PlanConstructor.newOlapTable(0, "t1", 0);
        table.setQualifiedDbName("test_db");
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        return (LogicalOlapScan) scan.withIsDelta(true);
    }

    /** Builds a scan with isDelta=false for the given table id and name (for delta plan generator tests). */
    protected LogicalOlapScan buildScanForTable(long tableId, String tableName) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, tableName, 0);
        table.setQualifiedDbName("test_db");
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
    }

    /**
     * Builds a scan over a DUP_KEYS table that contains an additional {@code binlog_op}
     * column (TinyInt). Schema: id (INT key), name (STRING key), binlog_op (TINYINT).
     */
    protected LogicalOlapScan buildScanWithOpColumn() {
        List<Column> columns = ImmutableList.of(
                new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                new Column("name", Type.STRING, true, AggregateType.NONE, "", ""),
                new Column(Column.IVM_MOCK_BINLOG_OPERATION_COL, Type.TINYINT, false, AggregateType.NONE, "0", ""));
        OlapTable table = new OlapTable(1L, "t_op", columns,
                KeysType.DUP_KEYS, new SinglePartitionInfo(),
                new RandomDistributionInfo(3));
        table.setIndexMeta(-1, "t_op", table.getFullSchema(),
                0, 0, (short) 0, TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setQualifiedDbName("test_db");
        LogicalOlapScan scan = new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
        return (LogicalOlapScan) scan.withIsDelta(true);
    }

    protected LogicalResultSink<LogicalProject<LogicalOlapScan>> buildScanPlan(LogicalOlapScan scan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(exprs, scan);
        return new LogicalResultSink<>(exprs, project);
    }

    protected LogicalResultSink<LogicalProject<LogicalProject<LogicalOlapScan>>> buildProjectScanPlan(LogicalOlapScan scan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        LogicalProject<LogicalOlapScan> innerProject = new LogicalProject<>(exprs, scan);
        LogicalProject<LogicalProject<LogicalOlapScan>> outerProject = new LogicalProject<>(exprs, innerProject);
        return new LogicalResultSink<>(exprs, outerProject);
    }

    protected LogicalResultSink<LogicalProject<LogicalFilter<LogicalOlapScan>>> buildFilterScanPlan(
            LogicalOlapScan scan, Expression predicate) {
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(predicate), scan);
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(scan.getOutput());
        LogicalProject<LogicalFilter<LogicalOlapScan>> project = new LogicalProject<>(exprs, filter);
        return new LogicalResultSink<>(exprs, project);
    }

    protected ConnectContext newConnectContext() {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setEnableIvmNormalRewrite(true);
        connectContext.setSessionVariable(sessionVariable);
        StatementContext statementContext = new StatementContext(connectContext, null);
        connectContext.setStatementContext(statementContext);
        return connectContext;
    }

    protected JobContext newJobContextForRoot(Plan root, ConnectContext connectContext) {
        CascadesContext cascadesContext = CascadesContext.initContext(
                connectContext.getStatementContext(), root, PhysicalProperties.ANY);
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }

    protected PlanBundle normalizeAggPlan(LogicalAggregate<LogicalOlapScan> agg) {
        ConnectContext connectContext = newConnectContext();
        JobContext jobContext = newJobContextForRoot(agg, connectContext);
        Plan normalizedPlan = new IvmNormalizeMtmv().rewriteRoot(agg, jobContext);
        IvmNormalizeResult normalizeResult = jobContext.getCascadesContext().getIvmNormalizeResult().get();
        return new PlanBundle(connectContext, normalizedPlan, normalizeResult);
    }

    protected MTMV buildMtmvFromPlan(List<Slot> output) {
        List<Column> schema = output.stream().map(this::toColumn).collect(Collectors.toList());
        schema.add(new Column(Column.DELETE_SIGN, ScalarType.createType(PrimitiveType.BIGINT), false,
                AggregateType.NONE, false, "", false, -1));

        MTMV mtmv = (MTMV) new OlapTableFactory().init(TableType.MATERIALIZED_VIEW, false)
                .withTableId(1L)
                .withTableName("test_mv")
                .withSchema(schema)
                .withKeysType(KeysType.UNIQUE_KEYS)
                .withPartitionInfo(new SinglePartitionInfo())
                .withDistributionInfo(new RandomDistributionInfo(1))
                .withQuerySql("select 1")
                .withMvProperties(new HashMap<>())
                .build();
        mtmv.setQualifiedDbName("test_db");
        mtmv.setState(OlapTableState.NORMAL);
        mtmv.setBaseIndexId(1L);
        mtmv.setIndexMeta(1L, "test_mv", schema, 0, 0, (short) 0, TStorageType.COLUMN, KeysType.UNIQUE_KEYS);
        mtmv.rebuildFullSchema();
        mtmv.setRefreshInfo((MTMVRefreshInfo) null);
        mtmv.setStatus(new MTMVStatus());
        mtmv.setJobInfo(new MTMVJobInfo("job1"));
        mtmv.setMvProperties(new HashMap<>());
        mtmv.setRelation(new MTMVRelation(Sets.newHashSet(), Sets.newHashSet(), Sets.newHashSet(),
                Sets.newHashSet(), Sets.newHashSet()));
        mtmv.setMvPartitionInfo(new MTMVPartitionInfo());
        mtmv.setRefreshSnapshot(new MTMVRefreshSnapshot());
        TableProperty tableProperty = new TableProperty(new HashMap<>());
        tableProperty.setEnableUniqueKeyMergeOnWrite(true);
        mtmv.setTableProperty(tableProperty);
        return mtmv;
    }

    protected Column toColumn(Slot slot) {
        Type type = slot.getDataType().toCatalogDataType();
        boolean isVisible = !(Column.IVM_ROW_ID_COL.equals(slot.getName()) || IvmUtil.isIvmHiddenColumn(slot.getName()));
        return new Column(slot.getName(), type, false, AggregateType.NONE, slot.nullable(), "",
                isVisible, Column.COLUMN_UNIQUE_ID_INIT_VALUE);
    }

    protected LogicalAggregate<LogicalOlapScan> buildGroupedAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Alias countAlias = new Alias(new Count(), "cnt");
        Alias sumAlias = new Alias(new Sum(idSlot), "sum_id");
        Alias avgAlias = new Alias(new Avg(idSlot), "avg_id");
        return new LogicalAggregate<>(ImmutableList.of(idSlot),
                ImmutableList.of(idSlot, countAlias, sumAlias, avgAlias),
                true, Optional.empty(), scan);
    }

    protected LogicalAggregate<LogicalOlapScan> buildScalarAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Alias countAlias = new Alias(new Count(), "cnt");
        Alias sumAlias = new Alias(new Sum(idSlot), "sum_id");
        return new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(countAlias, sumAlias),
                true, Optional.empty(), scan);
    }

    protected LogicalAggregate<LogicalOlapScan> buildCountExprAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias countAlias = new Alias(new Count(nameSlot), "cnt_name");
        return new LogicalAggregate<>(ImmutableList.of(idSlot), ImmutableList.of(idSlot, countAlias),
                true, Optional.empty(), scan);
    }

    protected LogicalAggregate<LogicalOlapScan> buildMaxAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias maxAlias = new Alias(new Max(nameSlot), "mx");
        return new LogicalAggregate<>(ImmutableList.of(idSlot), ImmutableList.of(idSlot, maxAlias),
                true, Optional.empty(), scan);
    }

    protected LogicalAggregate<LogicalOlapScan> buildMinAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias minAlias = new Alias(new Min(nameSlot), "mn");
        return new LogicalAggregate<>(ImmutableList.of(idSlot), ImmutableList.of(idSlot, minAlias),
                true, Optional.empty(), scan);
    }

    /** Scalar MIN — no group-by keys. */
    protected LogicalAggregate<LogicalOlapScan> buildScalarMinAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Alias minAlias = new Alias(new Min(idSlot), "mn");
        return new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(minAlias),
                true, Optional.empty(), scan);
    }

    /** Scalar MAX — no group-by keys. */
    protected LogicalAggregate<LogicalOlapScan> buildScalarMaxAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Alias maxAlias = new Alias(new Max(idSlot), "mx");
        return new LogicalAggregate<>(ImmutableList.of(), ImmutableList.of(maxAlias),
                true, Optional.empty(), scan);
    }

    /** Combined MIN + MAX on same column with group-by. */
    protected LogicalAggregate<LogicalOlapScan> buildMinMaxAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias minAlias = new Alias(new Min(nameSlot), "mn");
        Alias maxAlias = new Alias(new Max(nameSlot), "mx");
        return new LogicalAggregate<>(ImmutableList.of(idSlot),
                ImmutableList.of(idSlot, minAlias, maxAlias),
                true, Optional.empty(), scan);
    }

    /** GROUP BY k1, k2 (composite keys) with SUM + COUNT. */
    protected LogicalAggregate<LogicalOlapScan> buildCompositeGroupAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Alias countAlias = new Alias(new Count(), "cnt");
        Alias sumAlias = new Alias(new Sum(idSlot), "sum_id");
        return new LogicalAggregate<>(ImmutableList.of(idSlot, nameSlot),
                ImmutableList.of(idSlot, nameSlot, countAlias, sumAlias),
                true, Optional.empty(), scan);
    }

    /** SUM(id + name) — expression (non-Slot) aggregate argument. */
    protected LogicalAggregate<LogicalOlapScan> buildExprAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Expression addExpr = new org.apache.doris.nereids.trees.expressions.Add(idSlot, nameSlot);
        Alias sumAlias = new Alias(new Sum(addExpr), "sum_expr");
        Alias cntAlias = new Alias(new Count(), "cnt");
        return new LogicalAggregate<>(ImmutableList.of(idSlot),
                ImmutableList.of(idSlot, sumAlias, cntAlias),
                true, Optional.empty(), scan);
    }

    /** MIN(id + name), MAX(id + name) — expression args for MIN/MAX. */
    protected LogicalAggregate<LogicalOlapScan> buildExprMinMaxAgg(LogicalOlapScan scan) {
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        Expression addExpr = new org.apache.doris.nereids.trees.expressions.Add(idSlot, nameSlot);
        Alias minAlias = new Alias(new Min(addExpr), "mn_expr");
        Alias maxAlias = new Alias(new Max(addExpr), "mx_expr");
        return new LogicalAggregate<>(ImmutableList.of(idSlot),
                ImmutableList.of(idSlot, minAlias, maxAlias),
                true, Optional.empty(), scan);
    }

    protected UnboundTableSink<?> getSink(InsertIntoTableCommand command) {
        Assertions.assertInstanceOf(UnboundTableSink.class, command.getLogicalQuery());
        return (UnboundTableSink<?>) command.getLogicalQuery();
    }

    protected static final class PlanBundle {
        protected final ConnectContext connectContext;
        protected final Plan normalizedPlan;
        protected final IvmNormalizeResult normalizeResult;

        protected PlanBundle(ConnectContext connectContext, Plan normalizedPlan, IvmNormalizeResult normalizeResult) {
            this.connectContext = connectContext;
            this.normalizedPlan = normalizedPlan;
            this.normalizeResult = normalizeResult;
        }
    }
}
