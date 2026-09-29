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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.lance.LanceExternalTable;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.mysql.MysqlCommand;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.PrepareCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LancePreparedSearchTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPrepareAndRepeatedBindingUseFreshVectorAndSnapshot(boolean useIndex) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        AtomicLong version = new AtomicLong(42);
        LanceExternalTable table = mockTable(version);
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            Pair<LogicalPlan, StatementContext> parsed = new NereidsParser().parseMultiple(
                    "select id, _distance from vector_search('table'='catalog.db.items', "
                            + "'column'='embedding', 'use_index'='" + useIndex + "', "
                            + "'query_vector'=?, 'top_k'=?, 'offset'=?, 'filter'=?)")
                    .get(0);
            List<Placeholder> parameters = parsed.second.getPlaceholders();
            Assertions.assertEquals(4, parameters.size());
            StatementContext prepare = MemoTestUtils.createStatementContext(context, "");
            prepare.setPrepareStage(true);
            VectorSearchTableValuedFunction prepared = analyze(parsed.first, prepare);
            Assertions.assertEquals(3, prepared.getTableColumns().size());
            Assertions.assertFalse(prepared.getSearchRequest().getSearchQuery().getVectorSearch().isSetQueryVector());

            LogicalPlan staticVector = new NereidsParser().parseMultiple(
                    "select * from vector_search('table'='catalog.db.items', 'column'='embedding', "
                            + "'query_vector'='[1,2]', 'top_k'=?)").get(0).first;
            Assertions.assertTrue(analyze(staticVector, prepare).getSearchRequest()
                    .getSearchQuery().getVectorSearch().isSetQueryVector());

            for (int i = 0; i < 2; i++) {
                version.set(42 + i);
                StatementContext execute = MemoTestUtils.createStatementContext(context, "");
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                        new StringLiteral(i == 0 ? "[1,2]" : "[3,4]"));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(3 + i));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(2).getPlaceholderId(), new IntegerLiteral(i));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(3).getPlaceholderId(),
                        new StringLiteral("id > " + i));
                VectorSearchTableValuedFunction function = analyze(parsed.first, execute);
                Assertions.assertEquals(42 + i, function.getMetadata().getVersion());
                Assertions.assertEquals(3 + i, function.getTopK());
                Assertions.assertEquals(i, function.getOffset());
                Assertions.assertEquals("id > " + i, new String(
                        function.getSearchRequest().getSearchFilter().getPayload(), StandardCharsets.UTF_8));
                byte[] values = function.getSearchRequest().getSearchQuery().getVectorSearch().getQueryVector().getValues();
                Assertions.assertEquals(i == 0 ? 1.0f : 3.0f,
                        ByteBuffer.wrap(values).order(ByteOrder.LITTLE_ENDIAN).getFloat());
                for (String invalidVector : new String[] {"[1]", "not-json"}) {
                    execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                            new StringLiteral(invalidVector));
                    Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                            () -> analyze(parsed.first, execute));
                }
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(), NullLiteral.INSTANCE);
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                        new StringLiteral("[1,2]"));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(0));
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(3));
                execute.getIdToPlaceholderRealExpr().put(parameters.get(2).getPlaceholderId(), new IntegerLiteral(-1));
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
                execute.getIdToPlaceholderRealExpr().remove(parameters.get(0).getPlaceholderId());
                Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                        () -> analyze(parsed.first, execute));
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testProxyPrepareUsesParsedStatementContext(boolean useIndex) throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext(null, true);
        context.setEnv(Env.getCurrentEnv());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setThreadLocalInfo();
        context.setCommand(MysqlCommand.COM_STMT_PREPARE);
        LanceExternalTable table = mockTable(new AtomicLong(42));
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            OriginStatement sql = new OriginStatement(
                    "select id, _distance from vector_search('table'='catalog.db.items', "
                            + "'column'='embedding', 'use_index'='" + useIndex + "', "
                            + "'query_vector'=?, 'top_k'=?, 'offset'=?, 'filter'=?)", 0);
            StmtExecutor proxy = new StmtExecutor(context, sql, true);
            StatementContext constructorContext = context.getStatementContext();
            // A forwarded EXECUTE reconstructs PREPARE before decoding its parameter packet.
            Deencapsulation.invoke(proxy, "parseByNereids");
            StatementContext parsedContext = context.getStatementContext();
            Assertions.assertNotSame(constructorContext, parsedContext);
            Assertions.assertTrue(parsedContext.getIdToPlaceholderRealExpr().isEmpty());
            LogicalPlanAdapter parsed = (LogicalPlanAdapter) proxy.getParsedStmt();
            PrepareCommand command = new PrepareCommand("1", parsed.getLogicalPlan(),
                    parsedContext.getPlaceholders(), sql);
            command.run(context, proxy);
            Assertions.assertNotNull(context.getPreparedStementContext("1"));
            Assertions.assertEquals(4, command.placeholderCount());
            Assertions.assertEquals(2, proxy.planPrepareStatementSlots().size());
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testMultiVectorPrepareDefersValueValidation(boolean useIndex) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        LanceExternalTable table = mockTable(new AtomicLong(42), true);
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            Pair<LogicalPlan, StatementContext> parsed = new NereidsParser().parseMultiple(
                    "select id, _distance from vector_search('table'='catalog.db.items', "
                            + "'column'='embedding', 'use_index'='" + useIndex + "', "
                            + "'query_vector'=?, 'top_k'=?)").get(0);
            StatementContext prepare = MemoTestUtils.createStatementContext(context, "");
            prepare.setPrepareStage(true);
            Assertions.assertFalse(analyze(parsed.first, prepare).getSearchRequest()
                    .getSearchQuery().getVectorSearch().isSetQueryVector());
            StatementContext execute = MemoTestUtils.createStatementContext(context, "");
            List<Placeholder> parameters = parsed.second.getPlaceholders();
            execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                    new StringLiteral("[[1,2],[3,4]]"));
            execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(3));
            Assertions.assertEquals(2, analyze(parsed.first, execute).getSearchRequest()
                    .getSearchQuery().getVectorSearch().getQueryVector().getNumVectors());
            execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(100001));
            Exception budget = Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> analyze(parsed.first, execute));
            Assertions.assertTrue(budget.getMessage().contains("candidate budget"));
            execute.getIdToPlaceholderRealExpr().put(parameters.get(1).getPlaceholderId(), new IntegerLiteral(3));
            execute.getIdToPlaceholderRealExpr().put(parameters.get(0).getPlaceholderId(),
                    new StringLiteral("[[1],[3,4]]"));
            Exception dimension = Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                    () -> analyze(parsed.first, execute));
            Assertions.assertTrue(dimension.getMessage().contains("dimension"));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    /**
     * A constant selector is re-resolved by every EXECUTE: a tag moved between two executions
     * selects its new version, and the selector itself is the same each time.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testExecuteResolvesConstantSelectorAgain(boolean useIndex) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        AtomicLong version = new AtomicLong(42);
        LanceExternalTable table = mockTable(version);
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            Pair<LogicalPlan, StatementContext> parsed = new NereidsParser().parseMultiple(
                    "select id, _distance from vector_search('table'='catalog.db.items', "
                            + "'column'='embedding', 'use_index'='" + useIndex + "', 'tag'='release', "
                            + "'query_vector'=?)").get(0);
            StatementContext prepare = MemoTestUtils.createStatementContext(context, "");
            prepare.setPrepareStage(true);
            analyze(parsed.first, prepare);
            // Each analysis resolves the selector exactly once; a second resolution within one
            // execution could select a different version.
            verifyResolvedOnceWithTag(table, "release");
            Placeholder vector = parsed.second.getPlaceholders().get(0);
            for (int i = 0; i < 2; i++) {
                version.set(42 + i);
                StatementContext execute = MemoTestUtils.createStatementContext(context, "");
                execute.getIdToPlaceholderRealExpr().put(vector.getPlaceholderId(), new StringLiteral("[1,2]"));
                Assertions.assertEquals(42 + i, analyze(parsed.first, execute).getMetadata().getVersion());
                verifyResolvedOnceWithTag(table, "release");
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"version", "timestamp", "tag", "branch"})
    public void testSelectorMustBeConstantInPreparedStatement(String selector) {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.setStatementContext(MemoTestUtils.createStatementContext(context, ""));
        try {
            Exception exception = Assertions.assertThrows(Exception.class,
                    () -> new NereidsParser().parseMultiple(
                            "select * from vector_search('table'='catalog.db.items', 'column'='embedding', "
                                    + "'query_vector'='[1,2]', '" + selector + "'=?)"));
            Assertions.assertTrue(exception.getMessage().contains(
                    "vector_search property '" + selector + "' must be constant in a prepared statement"),
                    exception.getMessage());
            // Before this was rejected, full_text_search received the placeholder as the text "?".
            for (String property : new String[] {"'" + selector + "'=?", "'top_k'=?", "?='x'"}) {
                exception = Assertions.assertThrows(Exception.class,
                        () -> new NereidsParser().parseMultiple(
                                "select * from full_text_search('table'='catalog.db.items', 'column'='body', "
                                        + "'query'='lance', " + property + ")"));
                Assertions.assertTrue(exception.getMessage().contains(
                        "full_text_search properties must be constant in a prepared statement"),
                        exception.getMessage());
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static void verifyResolvedOnceWithTag(LanceExternalTable table, String tag) {
        org.mockito.ArgumentCaptor<LanceRefSelector> selector =
                org.mockito.ArgumentCaptor.forClass(LanceRefSelector.class);
        // loadBasicMetadata delegates to loadMetadataForSearch in mockTable, so both modes count here.
        Mockito.verify(table, Mockito.times(1)).loadMetadataForSearch(selector.capture());
        Assertions.assertEquals(tag, selector.getValue().getTag().orElse(null));
        Mockito.clearInvocations(table);
    }

    private LanceExternalTable mockTable(AtomicLong version) {
        return mockTable(version, false);
    }

    private LanceExternalTable mockTable(AtomicLong version, boolean multiVector) {
        LanceExternalTable table = Mockito.mock(LanceExternalTable.class);
        Field vector = new Field("embedding", org.apache.arrow.vector.types.pojo.FieldType.nullable(
                new ArrowType.FixedSizeList(2)), Collections.singletonList(
                        Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))));
        if (multiVector) {
            vector = new Field("embedding", org.apache.arrow.vector.types.pojo.FieldType.nullable(ArrowType.List.INSTANCE),
                    Collections.singletonList(new Field("item",
                            org.apache.arrow.vector.types.pojo.FieldType.notNullable(new ArrowType.FixedSizeList(2)),
                            vector.getChildren())));
        }
        Schema schema = new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(64, true)), vector));
        Mockito.when(table.loadMetadataForSearch(Mockito.any(LanceRefSelector.class))).thenAnswer(
                invocation -> LanceTableMetadata.createSnapshotWithIndexes(
                        new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()),
                        version.get(), schema, Collections.emptyList(),
                        ImmutableMap.of("id", 0, "embedding", 1), Collections.emptyList()));
        Mockito.when(table.loadBasicMetadata(Mockito.any(LanceRefSelector.class))).thenAnswer(
                invocation -> table.loadMetadataForSearch(invocation.getArgument(0)));
        return table;
    }

    /** Both search functions accept the selector properties and hand the table what they select. */
    @org.junit.jupiter.api.Test
    public void testBothSearchFunctionsPassTheirSelectorToTheTable() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        LanceExternalTable table = mockSearchTable();
        String vector = "vector_search('table'='catalog.db.items', 'column'='embedding', 'query_vector'='[1,2]', ";
        String text = "full_text_search('table'='catalog.db.items', 'column'='body', 'query'='lance', ";
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            for (String useIndex : new String[] {"true", "false"}) {
                String prefix = vector + "'use_index'='" + useIndex + "', ";
                LanceRefSelector selector = selectorOf(table, prefix + "'VERSION'='3', 'Branch'='dev')", context);
                Assertions.assertEquals("dev", selector.getBranch().orElse(null));
                Assertions.assertEquals("3", selector.getSnapshot().get().getValue());
                selector = selectorOf(table, prefix + "'tag'='rel')", context);
                Assertions.assertEquals("rel", selector.getTag().orElse(null));
            }
            LanceRefSelector selector = selectorOf(table,
                    text + "'timestamp'='2026-09-20 12:00:00', 'branch'='main')", context);
            Assertions.assertFalse(selector.getBranch().isPresent());
            Assertions.assertEquals(org.apache.doris.analysis.TableSnapshot.VersionType.TIME,
                    selector.getSnapshot().get().getType());
            selector = selectorOf(table, text + "'version'='2', 'branch'='dev')", context);
            Assertions.assertEquals("dev", selector.getBranch().orElse(null));
            Assertions.assertEquals("2", selector.getSnapshot().get().getValue());
            Assertions.assertEquals("rel", selectorOf(table, text + "'tag'='rel')", context).getTag().orElse(null));
            Assertions.assertSame(LanceRefSelector.latest(), selectorOf(table, text + "'top_k'='3')", context));
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    /**
     * A search resolves its snapshot when it is bound, so a statement containing one must never be
     * answered from the SQL cache: a cached result would skip resolving a moving selector again.
     */
    @org.junit.jupiter.api.Test
    public void testSearchStatementsBypassSqlCache() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = MemoTestUtils.createConnectContext();
        context.getSessionVariable().setEnableSqlCache(true);
        LanceExternalTable table = mockSearchTable();
        try (MockedStatic<LanceExternalSearchTableValuedFunction> lookup = Mockito.mockStatic(
                LanceExternalSearchTableValuedFunction.class, Mockito.CALLS_REAL_METHODS)) {
            lookup.when(() -> LanceExternalSearchTableValuedFunction.findLanceExternalTable(
                    Mockito.any(TableName.class))).thenReturn(table);
            for (String tvf : new String[] {
                    "vector_search('table'='catalog.db.items', 'column'='embedding', 'query_vector'='[1,2]', "
                            + "'tag'='rel')",
                    "full_text_search('table'='catalog.db.items', 'column'='body', 'query'='lance', "
                            + "'tag'='rel')"}) {
                String sql = "select * from " + tvf;
                StatementContext statement = MemoTestUtils.createStatementContext(context, sql);
                SqlCacheContext sqlCache = statement.getSqlCacheContext().orElseThrow(
                        () -> new IllegalStateException("SQL cache is not enabled for " + sql));
                Assertions.assertFalse(sqlCache.isCannotProcessExpression(), sql);
                LogicalPlan plan = new NereidsParser().parseMultiple(sql).get(0).first;
                CascadesContext.initContext(statement, plan, PhysicalProperties.ANY).newAnalyzer().analyze();
                Assertions.assertTrue(sqlCache.isCannotProcessExpression(), sql);
            }
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    /** A table with a vector and a text column whose metadata is version 3 for every selector. */
    private static LanceExternalTable mockSearchTable() {
        Field item = Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        Field embedding = new Field("embedding", org.apache.arrow.vector.types.pojo.FieldType.nullable(
                new ArrowType.FixedSizeList(2)), Collections.singletonList(item));
        Schema schema = new Schema(Arrays.asList(Field.nullable("id", new ArrowType.Int(64, true)),
                embedding, Field.nullable("body", ArrowType.Utf8.INSTANCE)));
        LanceExternalTable table = Mockito.mock(LanceExternalTable.class);
        Mockito.when(table.loadMetadataForSearch(Mockito.any(LanceRefSelector.class))).thenAnswer(
                invocation -> LanceTableMetadata.createSnapshotWithIndexes(
                        new LanceTableAccess("s3://bucket/items.lance", Collections.emptyMap()), 3, schema,
                        Collections.emptyList(), ImmutableMap.of("id", 0, "embedding", 1, "body", 2),
                        Collections.emptyList()));
        Mockito.when(table.loadBasicMetadata(Mockito.any(LanceRefSelector.class))).thenAnswer(
                invocation -> table.loadMetadataForSearch(invocation.getArgument(0)));
        return table;
    }

    private LanceRefSelector selectorOf(LanceExternalTable table, String tvf, ConnectContext context) {
        Mockito.clearInvocations(table);
        LogicalPlan plan = new NereidsParser().parseMultiple("select * from " + tvf).get(0).first;
        CascadesContext cascades = CascadesContext.initContext(
                MemoTestUtils.createStatementContext(context, ""), plan, PhysicalProperties.ANY);
        cascades.newAnalyzer().analyze();
        org.mockito.ArgumentCaptor<LanceRefSelector> selectors =
                org.mockito.ArgumentCaptor.forClass(LanceRefSelector.class);
        Mockito.verify(table, Mockito.atLeast(0)).loadMetadataForSearch(selectors.capture());
        Mockito.verify(table, Mockito.atLeast(0)).loadBasicMetadata(selectors.capture());
        Assertions.assertFalse(selectors.getAllValues().isEmpty(), tvf);
        return selectors.getAllValues().get(0);
    }

    private VectorSearchTableValuedFunction analyze(LogicalPlan plan, StatementContext statement) {
        CascadesContext cascades = CascadesContext.initContext(statement, plan, PhysicalProperties.ANY);
        cascades.newAnalyzer().analyze();
        LogicalTVFRelation relation = cascades.getRewritePlan().<LogicalTVFRelation>collectToList(
                LogicalTVFRelation.class::isInstance).get(0);
        return (VectorSearchTableValuedFunction) relation.getFunction().getCatalogFunction();
    }
}
