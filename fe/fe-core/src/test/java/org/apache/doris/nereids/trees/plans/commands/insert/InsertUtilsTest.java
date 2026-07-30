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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergPartitionInfo;
import org.apache.doris.datasource.iceberg.IcebergSnapshot;
import org.apache.doris.datasource.iceberg.IcebergSnapshotCacheValue;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundInlineTable;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalInlineTable;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Test for InsertUtils.getFinalErrorMsg()
 */
public class InsertUtilsTest {

    private static final int MAX_TOTAL_BYTES = 512;

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testNormalizeValuesPinsTargetSnapshotBeforeExpandingDefault() {
        ConnectContext context = new ConnectContext();
        StatementContext statementContext = new StatementContext(context, null);
        context.setStatementContext(statementContext);
        context.setThreadLocalInfo();

        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(table.getName()).thenReturn("table");
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn("catalog");
        IcebergMvccSnapshot snapshot = new IcebergMvccSnapshot(new IcebergSnapshotCacheValue(
                new IcebergPartitionInfo(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap()),
                new IcebergSnapshot(2L, 2L)));
        Mockito.when(table.loadSnapshot(Optional.empty(), Optional.empty())).thenReturn(snapshot);
        Mockito.when(table.getBaseSchema(false)).thenAnswer(invocation -> {
            boolean pinned = statementContext.getSnapshot(table).isPresent();
            String defaultValue = pinned ? "2" : "1";
            return ImmutableList.of(new Column("id", Type.INT, false, null, false, defaultValue, ""));
        });

        UnboundInlineTable values = new UnboundInlineTable(ImmutableList.of(ImmutableList.of()));
        UnboundIcebergTableSink<Plan> sink = new UnboundIcebergTableSink<>(
                ImmutableList.of("catalog", "db", "table"),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                values);

        Plan normalized = InsertUtils.normalizePlan(sink, table, Optional.empty(), Optional.empty());

        LogicalInlineTable normalizedValues = (LogicalInlineTable) normalized.child(0);
        List<List<NamedExpression>> rows = normalizedValues.getConstantExprsList();
        Assertions.assertEquals("2", rows.get(0).get(0).child(0).toSql());
        Mockito.verify(table, Mockito.times(1)).loadSnapshot(Optional.empty(), Optional.empty());
    }

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
}
