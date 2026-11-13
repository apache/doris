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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundConnectorTableSink;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundInlineTable;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Default;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

/**
 * Test for InsertUtils.getFinalErrorMsg()
 */
public class InsertUtilsTest {

    private static final int MAX_TOTAL_BYTES = 512;
    private static final NereidsParser PARSER = new NereidsParser();

    private String generateString(int length) {
        return generateString(length, "X");
    }

    private String generateString(int length, String prefix) {
        StringBuilder sb = new StringBuilder(length);
        sb.append(prefix);
        for (int i = prefix.length(); i < length; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        return sb.toString();
    }

    /**
     * case1: normal
     */
    @Test
    public void testNormalCase() {
        String msg = "Insert failed";
        String firstErrorMsg = "Row format error";
        String url = "http://example.com/error_log";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains(url));
        Assertions.assertTrue(result.contains("first_error_msg:"));
        Assertions.assertTrue(result.contains("url:"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case2: Msg is too long
     */
    @Test
    public void testLongMsg() {
        String msg = generateString(600);
        String firstErrorMsg = "Short error";
        String url = "http://example.com";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains(url));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertTrue(result.indexOf(msg) == -1 || result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case3: firstErrorMsg is too long
     */
    @Test
    public void testLongFirstErrorMsg() {
        String msg = "Insert failed";
        String firstErrorMsg = generateString(600);
        String url = "http://example.com";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.contains(url));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(firstErrorMsg));
    }

    /**
     * case4: url is too long
     */
    @Test
    public void testLongUrl() {
        String msg = "Insert failed";
        String firstErrorMsg = "Row format error";
        String url = "http://example.com/" + generateString(600);

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(url));
    }

    /**
     * case5：firstErrorMsg and url are too long
     */
    @Test
    public void testBothFirstErrorMsgAndUrlTooLong() {
        String msg = "Insert failed";
        String firstErrorMsg = generateString(600);
        String url = "http://example.com/" + generateString(600);

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(firstErrorMsg));
        Assertions.assertFalse(result.contains(url));
    }

    /**
     * case6: firstErrorMsg , msg and url are too long
     */
    @Test
    public void testAllParametersTooLong() {
        String msg = generateString(600);
        String firstErrorMsg = generateString(600);
        String url = "http://example.com/" + generateString(600);

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
        Assertions.assertFalse(result.contains(msg));
        Assertions.assertFalse(result.contains(firstErrorMsg));
        Assertions.assertFalse(result.contains(url));
    }

    /**
     * case7 :  msg length == 512
     */
    @Test
    public void testMsgExactly512() {
        String msg = generateString(512);
        String firstErrorMsg = "";
        String url = "";

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case8: urlPartLen + firstErrorMsgPartLen > 512, but tempLen + firstErrorMsgPartLen still > 512
     * Should only keep firstErrorMsg and drop url
     */
    @Test
    public void testUrlAndFirstErrorMsgSumTooLong_DropUrl() {
        String msg = "Insert failed";
        // ". first_error_msg: ".length() = 19
        // We need firstErrorMsgPartLen such that:
        // ". url: please use `show load` for detail msg".length() + firstErrorMsgPartLen > 512
        // ". url: please use `show load` for detail msg".length() = 47
        // So firstErrorMsgPartLen should be > 465
        // firstErrorMsgPartLen = 19 + firstErrorMsg.length() > 465
        // firstErrorMsg.length() should be > 446
        String firstErrorMsg = generateString(470, "ERROR_MSG_");
        String url = generateString(100, "URL_");

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertFalse(result.contains(url));
        Assertions.assertFalse(result.contains("please use `show load` for detail msg"));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * case9: urlPartLen + firstErrorMsgPartLen > 512, but tempLen + firstErrorMsgPartLen <= 512
     * Should use url placeholder and keep firstErrorMsg
     */
    @Test
    public void testUrlAndFirstErrorMsgSumTooLong_UseUrlPlaceholder() {
        String msg = "Insert failed";
        // ". first_error_msg: ".length() = 19
        // ". url: ".length() = 7
        // ". url: please use `show load` for detail msg".length() = 47
        // We need: urlPartLen + firstErrorMsgPartLen > 512
        // AND: 47 + firstErrorMsgPartLen <= 512
        // So firstErrorMsgPartLen should be in range (512-urlPartLen, 465]
        // Let's make firstErrorMsg.length() = 400, so firstErrorMsgPartLen = 419
        // And url.length() = 100, so urlPartLen = 107
        // Then urlPartLen + firstErrorMsgPartLen = 526 > 512 ✓
        // And 47 + 419 = 466 <= 512 ✓
        String firstErrorMsg = generateString(400, "FIRST_ERROR_");
        String url = generateString(100, "URL_CONTENT_");

        String result = InsertUtils.getFinalErrorMsg(msg, firstErrorMsg, url);

        Assertions.assertTrue(result.contains(msg));
        Assertions.assertTrue(result.contains(firstErrorMsg));
        Assertions.assertTrue(result.contains("please use `show load` for detail msg"));
        Assertions.assertFalse(result.contains(url));
        Assertions.assertTrue(result.length() <= MAX_TOTAL_BYTES);
    }

    /**
     * normalizePlan must reject INSERT into a flipped plugin-driven view. Pre-flip the engine sink rejected
     * it (e.g. IcebergTableSink threw on isView()); post-flip the neutral write path reaches a connector sink,
     * so the guard lives in normalizePlan instead — mirroring the existing HMS-view guard.
     */
    @Test
    public void normalizePlanRejectsInsertIntoPluginView() {
        PluginDrivenExternalTable view = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(view.isView()).thenReturn(true);
        UnboundLogicalSink<?> plan = Mockito.mock(UnboundLogicalSink.class);

        // MUTATION: dropping the guard, or negating the isView() check -> the write proceeds past the guard
        // (no view rejection) -> this assertThrows finds no exception with the view message -> red.
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> InsertUtils.normalizePlan(plan, view, Optional.empty(), Optional.empty()));
        Assertions.assertTrue(e.getMessage().contains("Write data to view is not supported"), e.getMessage());
    }

    @Test
    public void normalizePlanDoesNotRejectNonViewPluginTableAtViewGuard() {
        PluginDrivenExternalTable nonView = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(nonView.isView()).thenReturn(false);
        UnboundLogicalSink<?> plan = Mockito.mock(UnboundLogicalSink.class);

        // A non-view plugin table must pass the view guard (the write proceeds; it may fail later for
        // unrelated reasons, but never with the VIEW rejection). MUTATION: making the guard ignore isView()
        // (always reject any plugin table) -> the view message surfaces here -> red.
        try {
            InsertUtils.normalizePlan(plan, nonView, Optional.empty(), Optional.empty());
        } catch (Exception e) {
            Assertions.assertFalse(String.valueOf(e.getMessage()).contains("Write data to view is not supported"),
                    "non-view plugin table must not be rejected by the view guard: " + e.getMessage());
        }
    }

    @Test
    public void connectorDefaultSqlExpandsExplicitDefault() {
        Column column = new Column("v", PrimitiveType.INT, true);
        column.setConnectorDefaultValueSql("35");

        NamedExpression expression = InsertUtils.generateDefaultExpression(column);

        Assertions.assertInstanceOf(UnboundAlias.class, expression);
        Assertions.assertEquals("35", expression.child(0).toSql(),
                "a connector SQL-only write default must not be normalized to NULL");
    }

    @Test
    public void pinConnectorWriteSchemaUsesTargetBranchAndKeepsPinnedSchema() {
        StatementContext statementContext = new StatementContext();
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getId()).thenReturn(17L);
        UnboundConnectorTableSink<?> sink = Mockito.mock(UnboundConnectorTableSink.class);
        Column branchColumn = new Column("v", PrimitiveType.INT, true);
        branchColumn.setConnectorDefaultValueSql("35");
        Optional<String> branchName = Optional.of("old_defaults");
        Mockito.when(table.resolveWriteColumns(branchName))
                .thenReturn(Optional.of(ImmutableList.of(branchColumn)));

        InsertUtils.pinConnectorWriteSchema(statementContext, table, sink, branchName);
        InsertUtils.pinConnectorWriteSchema(statementContext, table, sink, branchName);

        Assertions.assertSame(branchColumn,
                statementContext.getConnectorWriteSchema(17L).orElseThrow().get(0));
        Assertions.assertEquals("35", InsertUtils.generateDefaultExpression(branchColumn).child(0).toSql());
        Mockito.verify(table, Mockito.times(1)).resolveWriteColumns(branchName);
    }

    @Test
    public void overwriteInnerInsertRejectsAWriterSchemaReorderedAfterNormalization() {
        // INSERT OVERWRITE t VALUES (DEFAULT, DEFAULT) expands the defaults by position against [a, b]. When another
        // client moves b first before the inner insert pins the writer schema again, binding the expanded row by
        // position would write the default of a into b and the default of b into a.
        StatementContext statementContext = new StatementContext();
        PluginDrivenExternalTable table = overwriteTarget();
        Column a = writeColumn("a", 1, "1");
        Column b = writeColumn("b", 2, "2");
        Mockito.when(table.resolveWriteColumns(Optional.empty()))
                .thenReturn(Optional.of(ImmutableList.of(a, b)), Optional.of(ImmutableList.of(b, a)));
        PluginDrivenInsertCommandContext insertCtx = normalizeThenRepin(statementContext, table, "g1", "g1");

        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> InsertUtils.checkNormalizedWriteGeneration(statementContext, table, Optional.of(insertCtx)));
        Assertions.assertTrue(e.getMessage().contains(
                "Table t changed while INSERT OVERWRITE was planning its write; retry the statement"), e.getMessage());
    }

    @Test
    public void overwriteInnerInsertRejectsAnotherWriteMetadataIdentity() {
        // The columns are the same, but the generation the connector fences writes with (for Iceberg the schema
        // ID, partition spec and sort order) moved on between the two pins.
        StatementContext statementContext = new StatementContext();
        PluginDrivenExternalTable table = overwriteTarget();
        Mockito.when(table.resolveWriteColumns(Optional.empty()))
                .thenReturn(Optional.of(ImmutableList.of(writeColumn("a", 1, "1"), writeColumn("b", 2, "2"))));
        PluginDrivenInsertCommandContext insertCtx = normalizeThenRepin(statementContext, table, "g1", "g2");

        Assertions.assertThrows(AnalysisException.class,
                () -> InsertUtils.checkNormalizedWriteGeneration(statementContext, table, Optional.of(insertCtx)));
    }

    @Test
    public void overwriteInnerInsertAcceptsTheGenerationItWasNormalizedAgainst() {
        StatementContext statementContext = new StatementContext();
        PluginDrivenExternalTable table = overwriteTarget();
        Mockito.when(table.resolveWriteColumns(Optional.empty()))
                .thenReturn(Optional.of(ImmutableList.of(writeColumn("a", 1, "1"), writeColumn("b", 2, "2"))),
                        Optional.of(ImmutableList.of(writeColumn("a", 1, "1"), writeColumn("b", 2, "2"))));
        PluginDrivenInsertCommandContext insertCtx = normalizeThenRepin(statementContext, table, "g1", "g1");

        Assertions.assertDoesNotThrow(
                () -> InsertUtils.checkNormalizedWriteGeneration(statementContext, table, Optional.of(insertCtx)));
        // Any other insert normalizes its own plan against its own pin.
        Assertions.assertDoesNotThrow(() -> InsertUtils.checkNormalizedWriteGeneration(
                statementContext, table, Optional.of(new PluginDrivenInsertCommandContext())));
    }

    private static PluginDrivenInsertCommandContext normalizeThenRepin(StatementContext statementContext,
            PluginDrivenExternalTable table, String normalizedIdentity, String repinnedIdentity) {
        UnboundConnectorTableSink<?> sink = Mockito.mock(UnboundConnectorTableSink.class);
        // The overwrite pins the writer schema, normalizes its plan against it and records that generation.
        InsertUtils.pinConnectorWriteSchema(statementContext, table, sink, Optional.empty());
        statementContext.setConnectorWriteMetadataIdentity(table.getId(), normalizedIdentity);
        PluginDrivenInsertCommandContext insertCtx = new PluginDrivenInsertCommandContext();
        InsertUtils.recordNormalizedWriteGeneration(statementContext, table, insertCtx);
        // Each attempt of the inner insert drops the pins of the statement and pins the writer schema again.
        statementContext.resetConnectorStatementScope();
        InsertUtils.pinConnectorWriteSchema(statementContext, table, sink, Optional.empty());
        statementContext.setConnectorWriteMetadataIdentity(table.getId(), repinnedIdentity);
        return insertCtx;
    }

    private static PluginDrivenExternalTable overwriteTarget() {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getId()).thenReturn(17L);
        Mockito.when(table.getName()).thenReturn("t");
        return table;
    }

    private static Column writeColumn(String name, int fieldId, String defaultSql) {
        Column column = new Column(name, PrimitiveType.INT, true);
        column.setUniqueId(fieldId);
        column.setConnectorDefaultValueSql(defaultSql);
        return column;
    }

    @Test
    public void nativeDefaultStillExpandsExplicitDefault() {
        Column column = new Column("v", Type.INT, false, null, true, "7", "");

        NamedExpression expression = InsertUtils.generateDefaultExpression(column);

        Assertions.assertInstanceOf(UnboundAlias.class, expression);
        Assertions.assertEquals("7", expression.child(0).toSql());
    }

    @Test
    public void timestampNsCurrentTimestampNineDefaultRemainsDynamic() {
        Column column = new Column("ts", Type.TIMESTAMP_NS, false, null, true,
                ColumnDef.DefaultValue.CURRENT_TIMESTAMP + "(9)", "", true,
                new DefaultValueExprDef(ColumnDef.DefaultValue.NOW, 9L),
                Column.COLUMN_UNIQUE_ID_INIT_VALUE, null);

        NamedExpression expression = InsertUtils.generateDefaultExpression(column);

        UnboundFunction currentTimestamp = Assertions.assertInstanceOf(
                UnboundFunction.class, expression.child(0));
        Assertions.assertEquals(ColumnDef.DefaultValue.NOW, currentTimestamp.getName());
        Assertions.assertEquals(new TinyIntLiteral((byte) 9), currentTimestamp.child(0));
    }

    @Test
    public void nullableColumnWithoutDefaultStillExpandsToNull() {
        Column column = new Column("v", PrimitiveType.INT, true);

        NamedExpression expression = InsertUtils.generateDefaultExpression(column);

        Assertions.assertInstanceOf(NullLiteral.class, expression.child(0));
    }

    @Test
    public void explicitInvisibleColumnUsesInvisibleColumnErrorWithDataOnlyWriterSchema() {
        Column id = new Column("id", PrimitiveType.INT);
        Column rowId = new Column("_row_id", PrimitiveType.BIGINT);
        rowId.setIsVisible(false);
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(id));
        Mockito.when(table.getFullSchema(Optional.empty())).thenReturn(ImmutableList.of(id, rowId));

        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> InsertUtils.resolveExplicitConnectorWriteColumn(table, "_row_id"));

        Assertions.assertEquals(
                "Cannot specify invisible column '_row_id' in INSERT statement", error.getMessage());
    }

    @Test
    public void writeOnlyDefaultInValuesResolvesAgainstPinnedWriterSchema() {
        // insert into t (score, id) values (DEFAULT(score), 4): the read schema of a table with write-only
        // defaults has no default, so DEFAULT(score) must take the pinned writer column's default.
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        try {
            PluginDrivenExternalTable table = pinWriterSchema(ctx, true);
            Plan normalized = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("score", "id"),
                    new UnboundInlineTable(ImmutableList.of(ImmutableList.of(
                            new UnboundAlias(new Default(new UnboundSlot("score"))),
                            new UnboundAlias(new IntegerLiteral(4)))))),
                    table, Optional.empty(), Optional.empty());

            NamedExpression score = ((LogicalInlineTable) normalized.child(0)).getConstantExprsList().get(0).get(0);
            Assertions.assertEquals(TypeCoercionUtils.castUnbound(PARSER.parseExpression("9"), IntegerType.INSTANCE),
                    score.child(0));

            AnalysisException unknown = Assertions.assertThrows(AnalysisException.class,
                    () -> InsertUtils.normalizePlan(connectorSink(ImmutableList.of("id", "score"),
                            new UnboundInlineTable(ImmutableList.of(ImmutableList.of(
                                    new UnboundAlias(new IntegerLiteral(6)),
                                    new UnboundAlias(new Default(new UnboundSlot("no_such_column"))))))),
                            table, Optional.empty(), Optional.empty()));
            Assertions.assertTrue(unknown.getMessage().contains("no_such_column"), unknown.getMessage());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void writeOnlyDefaultInSelectResolvesByTargetColumnName() {
        // DEFAULT(name) in INSERT ... SELECT names the target column whatever the source is: here a staging
        // relation whose own column is also called name, and a SELECT without FROM.
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        try {
            PluginDrivenExternalTable table = pinWriterSchema(ctx, true);
            Expression writeDefault = PARSER.parseExpression("'write-name'");

            Plan fromStaging = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("id", "name"),
                    PARSER.parseSingle("select id, DEFAULT(name) from "
                            + "(select 7 as id, 'source-name' as name) staging_source").child(0)),
                    table, Optional.empty(), Optional.empty());
            LogicalProject<?> project = (LogicalProject<?>) fromStaging.child(0);
            Assertions.assertEquals(writeDefault, project.getProjects().get(1).child(0));

            Plan noFrom = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("id", "name"),
                    new UnboundOneRowRelation(new RelationId(1), ImmutableList.of(
                            new UnboundAlias(new IntegerLiteral(8)),
                            new UnboundAlias(new Default(new UnboundSlot("name")))))),
                    table, Optional.empty(), Optional.empty());
            UnboundOneRowRelation oneRow = (UnboundOneRowRelation) noFrom.child(0);
            Assertions.assertEquals(writeDefault, oneRow.getProjects().get(1).child(0));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void writeOnlyDefaultInStarReplacementResolvesByTargetColumnName() {
        // INSERT INTO t(id, name) SELECT src.* EXCEPT(score) REPLACE(DEFAULT(name) AS name) FROM src: the
        // replacement belongs to the INSERT's own select list, so DEFAULT(name) names the target column whatever
        // src.name's default is. UnboundStar keeps its replacements outside its expression children.
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        try {
            PluginDrivenExternalTable table = pinWriterSchema(ctx, true);
            Plan normalized = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("id", "name"),
                    PARSER.parseSingle("select src.* except(score) replace(DEFAULT(name) as name) from src")
                            .child(0)),
                    table, Optional.empty(), Optional.empty());
            UnboundStar star = (UnboundStar) ((LogicalProject<?>) normalized.child(0)).getProjects().get(0);
            Assertions.assertEquals(PARSER.parseExpression("'write-name'"), star.getReplacedAlias().get(0).child(0));
            Assertions.assertEquals(ImmutableList.of("src"), star.getQualifier());
            Assertions.assertEquals(ImmutableList.of(new UnboundSlot("score")), star.getExceptedSlots());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void writeOnlyDefaultInDerivedTableNamesItsOwnSource() {
        // INSERT INTO t(score, name) SELECT v, DEFAULT(name) FROM (SELECT DEFAULT(score) AS v FROM src) q: the
        // derived table is a query of its own, so its DEFAULT(score) names src.score, whose default can differ from
        // the target's 9, and is left to binding, qualified or not. DEFAULT in the INSERT's own select lists, set
        // operation branches included, still names the target column.
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        try {
            PluginDrivenExternalTable table = pinWriterSchema(ctx, true);
            Expression writeDefault = PARSER.parseExpression("'write-name'");
            for (String derived : ImmutableList.of("(select DEFAULT(score) as v from src) q",
                    "(select DEFAULT(s.score) as v from src s) q")) {
                Plan normalized = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("score", "name"),
                        PARSER.parseSingle("select v, DEFAULT(name) from " + derived).child(0)),
                        table, Optional.empty(), Optional.empty());
                LogicalProject<?> outer = (LogicalProject<?>) normalized.child(0);
                Assertions.assertEquals(writeDefault, outer.getProjects().get(1).child(0), derived);
                assertDerivedDefaultLeftToBinding(outer.child(0), derived);
            }

            Plan union = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("name"),
                    PARSER.parseSingle("select DEFAULT(name) union all "
                            + "select v from (select DEFAULT(name) as v from src) q").child(0)),
                    table, Optional.empty(), Optional.empty());
            LogicalUnion setOperation = (LogicalUnion) union.child(0);
            UnboundOneRowRelation oneRow = (UnboundOneRowRelation) setOperation.child(0);
            Assertions.assertEquals(writeDefault, oneRow.getProjects().get(0).child(0));
            assertDerivedDefaultLeftToBinding(setOperation.child(1).child(0), "union branch");
        } finally {
            ConnectContext.remove();
        }
    }

    private static void assertDerivedDefaultLeftToBinding(Plan derivedTable, String message) {
        Assertions.assertInstanceOf(LogicalSubQueryAlias.class, derivedTable, message);
        LogicalProject<?> inner = (LogicalProject<?>) derivedTable.child(0);
        Assertions.assertInstanceOf(Default.class, inner.getProjects().get(0).child(0), message);
    }

    @Test
    public void defaultReferenceIsLeftToBindingWithoutWriteOnlyDefaults() {
        // A connector whose read schema carries its defaults (hive) keeps the existing DEFAULT(column) handling.
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        try {
            PluginDrivenExternalTable table = pinWriterSchema(ctx, false);
            Plan normalized = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("id", "name"),
                    new UnboundOneRowRelation(new RelationId(1), ImmutableList.of(
                            new UnboundAlias(new IntegerLiteral(8)),
                            new UnboundAlias(new Default(new UnboundSlot("name")))))),
                    table, Optional.empty(), Optional.empty());

            UnboundOneRowRelation oneRow = (UnboundOneRowRelation) normalized.child(0);
            Assertions.assertInstanceOf(Default.class, oneRow.getProjects().get(1).child(0));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void writeOnlyDefaultIsTypedAsItsColumnBeforeBinding() {
        // bit_count(DEFAULT(bits)) on an INT column whose default is -1 must count the 32 bits of an INT, not the 8
        // of the TINYINT that -1 parses to; casting the finished value to the INT target cannot restore them.
        ConnectContext ctx = MemoTestUtils.createConnectContext();
        try {
            PluginDrivenExternalTable table = pinWriterSchema(ctx, true);
            Expression intDefault = TypeCoercionUtils.castUnbound(PARSER.parseExpression("-1"), IntegerType.INSTANCE);

            Expression bitCount = new UnboundFunction("bit_count",
                    ImmutableList.of(new Default(new UnboundSlot("bits"))));
            Plan values = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("bits"),
                    new UnboundInlineTable(ImmutableList.of(ImmutableList.of(new UnboundAlias(bitCount))))),
                    table, Optional.empty(), Optional.empty());
            Assertions.assertEquals(intDefault, bitCountArgument(
                    ((LogicalInlineTable) values.child(0)).getConstantExprsList().get(0).get(0)));

            Plan select = InsertUtils.normalizePlan(connectorSink(ImmutableList.of("bits"),
                    PARSER.parseSingle("select bit_count(DEFAULT(bits))").child(0)),
                    table, Optional.empty(), Optional.empty());
            Assertions.assertEquals(intDefault, bitCountArgument(
                    ((UnboundOneRowRelation) select.child(0)).getProjects().get(0)));
            Assertions.assertEquals(IntegerType.INSTANCE, intDefault.getDataType());
        } finally {
            ConnectContext.remove();
        }
    }

    private static Expression bitCountArgument(Expression value) {
        UnboundFunction bitCount = value.<UnboundFunction>collectFirst(UnboundFunction.class::isInstance).get();
        return bitCount.child(0);
    }

    private static PluginDrivenExternalTable pinWriterSchema(ConnectContext ctx, boolean writeOnlyDefaults) {
        PluginDrivenExternalTable table = Mockito.mock(PluginDrivenExternalTable.class);
        Mockito.when(table.getId()).thenReturn(17L);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.hasWriteOnlyDefaults()).thenReturn(writeOnlyDefaults);
        Column id = new Column("id", PrimitiveType.INT, true);
        id.setConnectorDefaultValueSql("NULL");
        Column name = new Column("name", PrimitiveType.STRING, true);
        name.setConnectorDefaultValueSql("'write-name'");
        Column score = new Column("score", PrimitiveType.INT, true);
        score.setConnectorDefaultValueSql("9");
        Column bits = new Column("bits", PrimitiveType.INT, true);
        bits.setConnectorDefaultValueSql("-1");
        MemoTestUtils.createStatementContext(ctx, "")
                .setConnectorWriteSchema(17L, ImmutableList.of(id, name, score, bits));
        return table;
    }

    private static UnboundConnectorTableSink<Plan> connectorSink(List<String> columnNames, Plan query) {
        return new UnboundConnectorTableSink<>(ImmutableList.of("ctl", "db", "t"), columnNames,
                ImmutableList.of(), ImmutableList.of(), query);
    }

    @Test
    public void nativeExplicitInvisibleColumnRemainsSupported() {
        Column id = new Column("id", PrimitiveType.INT);
        Column deleteSign = new Column("__DORIS_DELETE_SIGN__", PrimitiveType.TINYINT);
        deleteSign.setIsVisible(false);
        TableIf table = Mockito.mock(TableIf.class);
        Mockito.when(table.getBaseSchema(true)).thenReturn(ImmutableList.of(id, deleteSign));

        Assertions.assertSame(deleteSign,
                InsertUtils.resolveExplicitConnectorWriteColumn(table, "__doris_delete_sign__"));
    }
}
