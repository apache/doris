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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.load.DeleteHandler;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RowTtlIsVisible;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.planner.Planner;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class DeleteFromCommandTest extends TestWithFeService {
    private static final String DB = "test_row_ttl_delete_command";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB);
        connectContext.setDatabase(DEFAULT_CLUSTER_PREFIX + DB);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE " + DB + ".ttl_delete_dup (\n"
                + "  k INT NOT NULL,\n"
                + "  p INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") DUPLICATE KEY(k, p)\n"
                + "PARTITION BY RANGE(p) (\n"
                + "  PARTITION p_lt_10 VALUES LESS THAN ('10'),\n"
                + "  PARTITION p_lt_20 VALUES LESS THAN ('20')\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1',\n"
                + "  'function_column.enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day',\n"
                + "  'function_column.ttl_time_zone' = '+08:00'\n"
                + ")");
        createTable("CREATE TABLE " + DB + ".ttl_delete_mow (\n"
                + "  k INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1',\n"
                + "  'enable_unique_key_merge_on_write' = 'true',\n"
                + "  'enable_mow_light_delete' = 'true',\n"
                + "  'function_column.enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day',\n"
                + "  'function_column.ttl_time_zone' = '+08:00'\n"
                + ")");
        createTable("CREATE TABLE " + DB + ".ttl_delete_mor (\n"
                + "  k INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") UNIQUE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1',\n"
                + "  'enable_unique_key_merge_on_write' = 'false',\n"
                + "  'function_column.enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day',\n"
                + "  'function_column.ttl_time_zone' = '+08:00'\n"
                + ")");
        createTable("CREATE TABLE " + DB + ".ttl_delete_direct_legacy (\n"
                + "  k INT NOT NULL,\n"
                + "  event_time DATETIMEV2(6),\n"
                + "  v INT\n"
                + ") DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  'replication_num' = '1',\n"
                + "  'function_column.enable_row_ttl' = 'true',\n"
                + "  'function_column.ttl_col' = 'event_time',\n"
                + "  'function_column.ttl' = '1 day',\n"
                + "  'function_column.ttl_time_zone' = '+08:00'\n"
                + ")");
        createTable("CREATE TABLE " + DB + ".delete_source (\n"
                + "  k INT NOT NULL\n"
                + ") DUPLICATE KEY(k)\n"
                + "DISTRIBUTED BY HASH(k) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
    }

    @Test
    public void testDupAndMowLightDeleteUseOnlyUserPredicates() throws Exception {
        DeleteHandlerInvocation dup = runDeleteWithMockHandler(
                "DELETE FROM ttl_delete_dup WHERE p = 5");
        assertOnlyUserPredicate(dup.predicates, "p");
        Assertions.assertEquals(Collections.singletonList("p_lt_10"), dup.selectedPartitions.stream()
                .map(Partition::getName)
                .collect(Collectors.toList()));

        DeleteHandlerInvocation mow = runDeleteWithMockHandler(
                "DELETE FROM ttl_delete_mow WHERE k = 1");
        assertOnlyUserPredicate(mow.predicates, "k");
        Assertions.assertTrue(mow.table.getEnableUniqueKeyMergeOnWrite());
        Assertions.assertTrue(mow.table.getEnableMowLightDelete());
    }

    @Test
    public void testMorValuePredicateAndComplexPredicateUseFallback() throws Exception {
        FallbackInvocation mor = runDeleteExpectingFallback(
                "DELETE FROM ttl_delete_mor WHERE v = 1");
        Assertions.assertSame(mor.command.logicalQuery, mor.fallbackArguments.get(4));

        FallbackInvocation complex = runDeleteExpectingFallback(
                "DELETE FROM ttl_delete_mow WHERE abs(k) = 1");
        Assertions.assertSame(complex.command.logicalQuery, complex.fallbackArguments.get(4));
    }

    @Test
    public void testSubqueryFallbackKeepsRowTtlVisibilityFilter() throws Exception {
        FallbackInvocation invocation = runDeleteExpectingFallback(
                "DELETE FROM ttl_delete_mow WHERE k IN "
                        + "(SELECT k FROM delete_source WHERE k = 1)");
        Assertions.assertSame(invocation.command.logicalQuery, invocation.fallbackArguments.get(4));
        Assertions.assertTrue(invocation.physicalPlan.contains(RowTtlIsVisible.FUNCTION_NAME),
                invocation.physicalPlan);
        Assertions.assertTrue(invocation.physicalPlan.contains(Column.TTL_COL), invocation.physicalPlan);
    }

    @Test
    public void testSubqueryWithoutInnerFilterFallsBackBeforeRejectingTtlOnlyFilter() throws Exception {
        FallbackInvocation invocation = runDeleteExpectingFallback(
                "DELETE FROM ttl_delete_mow WHERE k IN (SELECT k FROM delete_source)");
        Assertions.assertSame(invocation.command.logicalQuery, invocation.fallbackArguments.get(4));
        Assertions.assertTrue(invocation.physicalPlan.contains(RowTtlIsVisible.FUNCTION_NAME),
                invocation.physicalPlan);
        Assertions.assertTrue(invocation.physicalPlan.contains(Column.TTL_COL), invocation.physicalPlan);
    }

    @Test
    public void testLegacyDirectDeleteIsRejectedBeforeDeleteOrFallback() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(DB);
        OlapTable table = (OlapTable) db.getTableOrDdlException("ttl_delete_direct_legacy");
        String ttlColProperty = PropertyAnalyzer.PROPERTIES_FUNCTION_COLUMN + "."
                + PropertyAnalyzer.PROPERTIES_TTL_COL;
        String sourceColumn = table.getTableProperty().getProperties().remove(ttlColProperty);
        Column ttlColumn = table.getTtlColumn();
        Assertions.assertNotNull(sourceColumn);
        Assertions.assertNotNull(ttlColumn);
        Type originalType = ttlColumn.getType();
        ttlColumn.setType(Type.BIGINT);
        try {
            Assertions.assertTrue(table.isLegacyDirectRowTtl());
            DeleteFromCommand command = parseDelete(
                    "DELETE FROM ttl_delete_direct_legacy WHERE k = 1");
            StmtExecutor executor = Mockito.mock(StmtExecutor.class);
            DeleteHandler deleteHandler = Mockito.mock(DeleteHandler.class);
            Env envSpy = envWithDeleteHandler(deleteHandler);
            try (MockedConstruction<DeleteFromUsingCommand> fallback =
                         Mockito.mockConstruction(DeleteFromUsingCommand.class);
                    MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(envSpy);
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> runDelete(command, executor,
                                "DELETE FROM ttl_delete_direct_legacy WHERE k = 1"));
                Assertions.assertTrue(exception.getMessage().contains(
                        PropertyAnalyzer.ROW_TTL_DIRECT_NOT_SUPPORTED), exception.getMessage());
                Assertions.assertTrue(fallback.constructed().isEmpty());
                Mockito.verify(deleteHandler, Mockito.never()).process(
                        Mockito.any(), Mockito.any(), Mockito.anyList(), Mockito.anyList(),
                        Mockito.any(), Mockito.anyList());
            }
        } finally {
            ttlColumn.setType(originalType);
            table.getTableProperty().getProperties().put(ttlColProperty, sourceColumn);
        }
    }

    @Test
    public void testIdentifyNereidsRowTtlVisibilityConjunct() {
        long durationMicros = 10;
        OlapTable directTable = Mockito.mock(OlapTable.class);
        Mockito.when(directTable.getRowTtlDurationMicros()).thenReturn(durationMicros);
        Column ttlColumn = new Column(Column.TTL_COL, Type.BIGINT, false,
                null, true, "row ttl", false);
        SlotReference ttlSlot = SlotReference.fromColumn(
                new ExprId(1), directTable, ttlColumn, Collections.emptyList());

        Assertions.assertTrue(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new RowTtlIsVisible(ttlSlot, new BigIntLiteral(durationMicros)), directTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new RowTtlIsVisible(ttlSlot, new BigIntLiteral(durationMicros + 1)), directTable));

        BigIntLiteral userValue = new BigIntLiteral(1);
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new RowTtlIsVisible(userValue, userValue), directTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new EqualTo(userValue, userValue), directTable));

        OlapTable temporalTable = Mockito.mock(OlapTable.class);
        Mockito.when(temporalTable.getRowTtlCol()).thenReturn("event_time");
        Mockito.when(temporalTable.getRowTtlDurationMicros()).thenReturn(durationMicros);
        Mockito.when(temporalTable.getRowTtlTimeZoneOffsetSeconds()).thenReturn(Optional.of(28_800));
        Column temporalTtlColumn = new Column(Column.TTL_COL, ScalarType.createDatetimeV2Type(6), false,
                null, true, "row ttl", false);
        SlotReference temporalTtlSlot = SlotReference.fromColumn(
                new ExprId(2), temporalTable, temporalTtlColumn, Collections.emptyList());
        Assertions.assertTrue(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new RowTtlIsVisible(temporalTtlSlot, new BigIntLiteral(durationMicros),
                        new IntegerLiteral(28_800)), temporalTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new RowTtlIsVisible(temporalTtlSlot, new BigIntLiteral(durationMicros),
                        new IntegerLiteral(0)), temporalTable));
    }

    @Test
    public void testIdentifyLegacyRowTtlVisibilityConjunct() throws Exception {
        long durationMicros = 10;
        OlapTable directTable = Mockito.mock(OlapTable.class);
        Mockito.when(directTable.getRowTtlDurationMicros()).thenReturn(durationMicros);
        Column ttlColumn = new Column(Column.TTL_COL, Type.BIGINT, false,
                null, true, "row ttl", false);
        SlotRef ttlSlot = Mockito.mock(SlotRef.class);
        Mockito.when(ttlSlot.getColumn()).thenReturn(ttlColumn);

        Assertions.assertTrue(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new FunctionCallExpr(RowTtlIsVisible.FUNCTION_NAME, Arrays.asList(
                        ttlSlot, new IntLiteral(durationMicros, Type.BIGINT)), false), directTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new FunctionCallExpr(RowTtlIsVisible.FUNCTION_NAME, Arrays.asList(
                        ttlSlot, new IntLiteral(durationMicros + 1, Type.BIGINT)), false), directTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new FunctionCallExpr(RowTtlIsVisible.FUNCTION_NAME, Arrays.asList(
                        new IntLiteral(0, Type.BIGINT), new IntLiteral(0, Type.BIGINT)), false), directTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new FunctionCallExpr("other_function", Collections.emptyList(), false), directTable));

        OlapTable temporalTable = Mockito.mock(OlapTable.class);
        Mockito.when(temporalTable.getRowTtlCol()).thenReturn("event_time");
        Mockito.when(temporalTable.getRowTtlDurationMicros()).thenReturn(durationMicros);
        Mockito.when(temporalTable.getRowTtlTimeZoneOffsetSeconds()).thenReturn(Optional.of(-18_000));
        Column temporalTtlColumn = new Column(Column.TTL_COL, ScalarType.createDatetimeV2Type(6), false,
                null, true, "row ttl", false);
        SlotRef temporalTtlSlot = Mockito.mock(SlotRef.class);
        Mockito.when(temporalTtlSlot.getColumn()).thenReturn(temporalTtlColumn);
        Assertions.assertTrue(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new FunctionCallExpr(RowTtlIsVisible.FUNCTION_NAME, Arrays.asList(
                        temporalTtlSlot, new IntLiteral(durationMicros, Type.BIGINT),
                        new IntLiteral(-18_000, Type.INT)), false), temporalTable));
        Assertions.assertFalse(DeleteFromCommand.isInjectedRowTtlVisibilityConjunct(
                new FunctionCallExpr(RowTtlIsVisible.FUNCTION_NAME, Arrays.asList(
                        temporalTtlSlot, new IntLiteral(durationMicros, Type.BIGINT),
                        new IntLiteral(0, Type.INT)), false), temporalTable));
    }

    @Test
    public void testBuildDeleteFallbackExceptionPreservesBothFailureCauses() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception("initial predicate failure");
        Exception fallbackException = new Exception("fallback execution failure");

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify the merged exception surfaces the fallback failure and keeps the initial failure.
        Assertions.assertEquals(
                "Delete fallback execution failed: fallback execution failure"
                        + ". Initial predicate check failed: initial predicate failure",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    @Test
    public void testBuildDeleteFallbackExceptionFallsBackToThrowableToString() throws Exception {
        DeleteFromCommand command = new DeleteFromCommand(Collections.emptyList(), null,
                false, Collections.emptyList(), null);
        Exception initialException = new Exception((String) null);
        Exception fallbackException = new Exception((String) null);

        AnalysisException mergedException = invokeBuildDeleteFallbackException(command,
                initialException, fallbackException);

        // Verify null messages still produce debuggable text.
        Assertions.assertEquals(
                "Delete fallback execution failed: java.lang.Exception"
                        + ". Initial predicate check failed: java.lang.Exception",
                mergedException.getMessage());
        Assertions.assertSame(fallbackException, mergedException.getCause());
        Assertions.assertEquals(1, mergedException.getSuppressed().length);
        Assertions.assertSame(initialException, mergedException.getSuppressed()[0]);
    }

    // Use reflection to validate the helper without exposing it only for tests.
    private AnalysisException invokeBuildDeleteFallbackException(DeleteFromCommand command,
            Exception initialException, Exception fallbackException)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method method = DeleteFromCommand.class.getDeclaredMethod("buildDeleteFallbackException",
                Exception.class, Exception.class);
        method.setAccessible(true);
        return (AnalysisException) method.invoke(command, initialException, fallbackException);
    }

    private DeleteHandlerInvocation runDeleteWithMockHandler(String sql) throws Exception {
        DeleteFromCommand command = parseDelete(sql);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        DeleteHandler deleteHandler = Mockito.mock(DeleteHandler.class);
        Env envSpy = envWithDeleteHandler(deleteHandler);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(envSpy);
            runDelete(command, executor, sql);
        }

        ArgumentCaptor<OlapTable> tableCaptor = ArgumentCaptor.forClass(OlapTable.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Partition>> partitionCaptor = ArgumentCaptor.forClass(List.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<Predicate>> predicateCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(deleteHandler).process(
                Mockito.any(Database.class), tableCaptor.capture(), partitionCaptor.capture(),
                predicateCaptor.capture(), Mockito.any(), Mockito.anyList());
        return new DeleteHandlerInvocation(tableCaptor.getValue(),
                new ArrayList<>(partitionCaptor.getValue()), new ArrayList<>(predicateCaptor.getValue()));
    }

    private FallbackInvocation runDeleteExpectingFallback(String sql) throws Exception {
        DeleteFromCommand command = parseDelete(sql);
        StmtExecutor executor = Mockito.mock(StmtExecutor.class);
        DeleteHandler deleteHandler = Mockito.mock(DeleteHandler.class);
        Env envSpy = envWithDeleteHandler(deleteHandler);
        AtomicReference<List<?>> fallbackArguments = new AtomicReference<>();
        try (MockedConstruction<DeleteFromUsingCommand> fallback = Mockito.mockConstruction(
                DeleteFromUsingCommand.class,
                (mock, context) -> fallbackArguments.set(new ArrayList<>(context.arguments())));
                MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(envSpy);
            runDelete(command, executor, sql);
            Assertions.assertEquals(1, fallback.constructed().size());
            Mockito.verify(fallback.constructed().get(0)).run(connectContext, executor);
        }
        Mockito.verify(deleteHandler, Mockito.never()).process(
                Mockito.any(), Mockito.any(), Mockito.anyList(), Mockito.anyList(),
                Mockito.any(), Mockito.anyList());
        ArgumentCaptor<Planner> plannerCaptor = ArgumentCaptor.forClass(Planner.class);
        Mockito.verify(executor).setPlanner(plannerCaptor.capture());
        NereidsPlanner planner = Assertions.assertInstanceOf(NereidsPlanner.class, plannerCaptor.getValue());
        return new FallbackInvocation(command, fallbackArguments.get(), planner.getPhysicalPlan().treeString());
    }

    private DeleteFromCommand parseDelete(String sql) {
        LogicalPlan parsed = new NereidsParser().parseSingle(sql);
        DeleteFromCommand command = Assertions.assertInstanceOf(DeleteFromCommand.class, parsed);
        Assertions.assertFalse(command instanceof DeleteFromUsingCommand);
        return command;
    }

    private void runDelete(DeleteFromCommand command, StmtExecutor executor, String sql) throws Exception {
        StatementContext statementContext = new StatementContext(connectContext, new OriginStatement(sql, 0));
        connectContext.setStatementContext(statementContext);
        command.run(connectContext, executor);
    }

    private Env envWithDeleteHandler(DeleteHandler deleteHandler) {
        Env envSpy = Mockito.spy(Env.getCurrentEnv());
        Mockito.doReturn(deleteHandler).when(envSpy).getDeleteHandler();
        return envSpy;
    }

    private void assertOnlyUserPredicate(List<Predicate> predicates, String expectedColumn) {
        Assertions.assertEquals(1, predicates.size());
        Predicate predicate = predicates.get(0);
        List<Expr> slots = new ArrayList<>();
        predicate.collect(SlotRef.class::isInstance, slots);
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(expectedColumn, ((SlotRef) slots.get(0)).getColumnName());
        String predicateSql = predicate.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE).toLowerCase();
        Assertions.assertFalse(predicateSql.contains(RowTtlIsVisible.FUNCTION_NAME));
        Assertions.assertFalse(predicateSql.contains(Column.DELETE_SIGN.toLowerCase()));
    }

    private static class DeleteHandlerInvocation {
        private final OlapTable table;
        private final List<Partition> selectedPartitions;
        private final List<Predicate> predicates;

        private DeleteHandlerInvocation(OlapTable table, List<Partition> selectedPartitions,
                List<Predicate> predicates) {
            this.table = table;
            this.selectedPartitions = selectedPartitions;
            this.predicates = predicates;
        }
    }

    private static class FallbackInvocation {
        private final DeleteFromCommand command;
        private final List<?> fallbackArguments;
        private final String physicalPlan;

        private FallbackInvocation(DeleteFromCommand command, List<?> fallbackArguments, String physicalPlan) {
            this.command = command;
            this.fallbackArguments = fallbackArguments;
            this.physicalPlan = physicalPlan;
        }
    }
}
