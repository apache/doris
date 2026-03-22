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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionItem;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.ExchangeNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class UpdateMvByPartitionCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        createTable("create table test.ivm_base (\n"
                + "  id int,\n"
                + "  value int\n"
                + ") duplicate key(id)\n"
                + "distributed by hash(id) buckets 1\n"
                + "properties('replication_num' = '1');");
        createMvByNereids("create materialized view test.ivm_mv\n"
                + "build deferred refresh incremental on manual\n"
                + "distributed by random buckets 1\n"
                + "properties('replication_num' = '1')\n"
                + "as select id, value from test.ivm_base;");
    }

    @Test
    void testFirstPartWithoutLowerBound() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)),
                ImmutableList.of(column));
        Range<PartitionKey> range1 = Range.lessThan(upper);
        RangePartitionItem item1 = new RangePartitionItem(range1);

        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(item1), "s");
        Assertions.assertEquals("OR[(s < 1),s IS NULL]", predicates.iterator().next().toSql());

    }

    @Test
    void testMaxMin() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey upper = PartitionKey.createPartitionKey(ImmutableList.of(PartitionValue.MAX_VALUE),
                ImmutableList.of(column));
        PartitionKey lower = PartitionKey.createPartitionKey(ImmutableList.of(new PartitionValue(1L)),
                ImmutableList.of(column));
        Range<PartitionKey> range = Range.closedOpen(lower, upper);
        RangePartitionItem rangePartitionItem = new RangePartitionItem(range);
        Set<Expression> predicates = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(rangePartitionItem),
                "s");
        Expression expr = predicates.iterator().next();
        System.out.println(expr.toSql());
        Assertions.assertEquals("(s >= 1)", expr.toSql());
    }

    @Test
    void testNull() throws AnalysisException {
        Column column = new Column("a", PrimitiveType.INT);
        PartitionKey v = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        ListPartitionItem listPartitionItem = new ListPartitionItem(ImmutableList.of(v));
        Expression expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem), "s")
                .iterator().next();
        Assertions.assertTrue(expr instanceof IsNull);

        PartitionKey v1 = PartitionKey.createListPartitionKeyWithTypes(
                ImmutableList.of(new PartitionValue("NULL", true)), ImmutableList.of(column.getType()), false);
        PartitionKey v2 = PartitionKey.createListPartitionKeyWithTypes(ImmutableList.of(new PartitionValue("1", false)),
                ImmutableList.of(column.getType()), false);
        listPartitionItem = new ListPartitionItem(ImmutableList.of(v1, v2));
        expr = UpdateMvByPartitionCommand.constructPredicates(Sets.newHashSet(listPartitionItem), "s").iterator()
                .next();
        Assertions.assertEquals("OR[s IS NULL,s IN (1)]", expr.toSql());
    }

    @Test
    void testFromUsesInsertedColumnNamesForIncrementalMtmv() throws Exception {
        MTMV mtmv = getMtmv("ivm_mv");

        UpdateMvByPartitionCommand command = newRefreshCommand(mtmv);

        Assertions.assertInstanceOf(UnboundTableSink.class, command.getLogicalQuery());
        UnboundTableSink<?> sink = (UnboundTableSink<?>) command.getLogicalQuery();
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), sink.getColNames());
    }

    @Test
    void testAnalyzeRefreshCommandBindsSinkAfterRowIdNormalization() throws Exception {
        MTMV mtmv = getMtmv("ivm_mv");
        connectContext.getSessionVariable().setEnableIvmNormalRewrite(true);

        UpdateMvByPartitionCommand command = newRefreshCommand(mtmv);
        LogicalOlapTableSink<?> sink = (LogicalOlapTableSink<?>) PlanChecker.from(connectContext,
                command.getLogicalQuery()).analyze(command.getLogicalQuery()).getPlan();

        Assertions.assertEquals(mtmv.getInsertedColumnNames(), getColumnNames(sink.getCols()));
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), getNamedExpressionNames(sink.getOutputExprs()));
        Assertions.assertEquals(mtmv.getInsertedColumnNames(), getSlotNames(sink.getTargetTableSlots()));
        Assertions.assertEquals(Column.IVM_ROW_ID_COL, sink.child().getOutput().get(0).getName());
    }

    @Test
    void testPlannerKeepsIvmRowIdAsLargeIntInSinkTupleAndOutputExprs() throws Exception {
        MTMV mtmv = getMtmv("ivm_mv");
        UpdateMvByPartitionCommand command = newRefreshCommand(mtmv);
        StatementContext statementContext = createStatementCtx("refresh materialized view test.ivm_mv");
        connectContext.getSessionVariable().setEnableIvmNormalRewrite(true);

        TestNereidsPlanner planner = new TestNereidsPlanner(statementContext);
        PhysicalPlan physicalPlan = planner.planWithLock(command.getLogicalQuery(), PhysicalProperties.ANY);
        PlanTranslatorContext translatorContext = new PlanTranslatorContext(planner.getCascadesContext());
        new PhysicalPlanTranslator(translatorContext).translatePlan(physicalPlan);
        List<PlanFragment> fragments = translatorContext.getPlanFragments();

        Assertions.assertNotNull(fragments);
        Assertions.assertFalse(fragments.isEmpty());
        PlanFragment sinkFragment = findSinkFragment(fragments);
        OlapTableSink sink = (OlapTableSink) sinkFragment.getSink();
        PlanFragment tabletSinkExprFragment = findTabletSinkExprFragment(fragments);
        List<Expr> sinkOutputExprs = tabletSinkExprFragment == null
                ? sinkFragment.getOutputExprs()
                : tabletSinkExprFragment.getOutputExprs();

        Assertions.assertEquals(Column.IVM_ROW_ID_COL,
                sink.getTupleDescriptor().getSlots().get(0).getColumn().getName());
        Assertions.assertEquals(PrimitiveType.LARGEINT,
                sink.getTupleDescriptor().getSlots().get(0).getType().getPrimitiveType());
        Assertions.assertFalse(sinkOutputExprs.isEmpty());
        Assertions.assertEquals(PrimitiveType.LARGEINT,
                sinkOutputExprs.get(0).getType().getPrimitiveType());
    }

    @Test
    void testRunRefreshCommandExecutesIncrementalMtmv() throws Exception {
        MTMV mtmv = getMtmv("ivm_mv");
        StatementContext statementContext = createStatementCtx("refresh materialized view test.ivm_mv");
        UpdateMvByPartitionCommand command = newRefreshCommand(mtmv);
        org.apache.doris.qe.StmtExecutor executor = MTMVPlanUtil.executeCommand(
                mtmv, command, statementContext, "refresh materialized view test.ivm_mv", true);

        Assertions.assertNotNull(executor);
        Assertions.assertFalse(executor.getContext().getSessionVariable().isEnableMaterializedViewRewrite());
        Assertions.assertFalse(executor.getContext().getSessionVariable().isEnableDmlMaterializedViewRewrite());
        Assertions.assertTrue(executor.getContext().getSessionVariable().isEnableIvmNormalRewrite());
    }

    private UpdateMvByPartitionCommand newRefreshCommand(MTMV mtmv) throws Exception {
        StatementContext statementContext = createStatementCtx("refresh materialized view test.ivm_mv");
        return UpdateMvByPartitionCommand.from(mtmv, Sets.newHashSet(), ImmutableMap.of(), statementContext);
    }

    private MTMV getMtmv(String mvName) throws Exception {
        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        return (MTMV) db.getTableOrAnalysisException(mvName);
    }

    private List<String> getColumnNames(List<Column> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (Column column : columns) {
            names.add(column.getName());
        }
        return names;
    }

    private List<String> getNamedExpressionNames(
            List<org.apache.doris.nereids.trees.expressions.NamedExpression> expressions) {
        List<String> names = new ArrayList<>(expressions.size());
        for (org.apache.doris.nereids.trees.expressions.NamedExpression expression : expressions) {
            names.add(expression.getName());
        }
        return names;
    }

    private List<String> getSlotNames(List<Slot> slots) {
        List<String> names = new ArrayList<>(slots.size());
        for (Slot slot : slots) {
            names.add(slot.getName());
        }
        return names;
    }

    private PlanFragment findSinkFragment(List<PlanFragment> fragments) {
        for (PlanFragment planFragment : fragments) {
            if (planFragment.getSink() instanceof OlapTableSink) {
                return planFragment;
            }
        }
        throw new AssertionError("no sink fragment for olap table sink");
    }

    private PlanFragment findTabletSinkExprFragment(List<PlanFragment> fragments) {
        for (PlanFragment planFragment : fragments) {
            if (planFragment.getPlanRoot() instanceof ExchangeNode
                    && planFragment.getDataPartition().getType()
                    == TPartitionType.OLAP_TABLE_SINK_HASH_PARTITIONED) {
                return planFragment;
            }
        }
        return null;
    }

    private static class TestNereidsPlanner extends NereidsPlanner {
        TestNereidsPlanner(StatementContext statementContext) {
            super(statementContext);
        }
    }
}
