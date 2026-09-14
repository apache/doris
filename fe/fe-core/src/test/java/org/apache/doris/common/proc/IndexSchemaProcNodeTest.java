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

package org.apache.doris.common.proc;

import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

public class IndexSchemaProcNodeTest {

    @Test
    public void testFetchResult() throws AnalysisException {
        List<Column> columnList = Lists.newArrayList();
        Column column1 = new Column("k1", Type.INT, true, null, true, "", "");
        Column column2 = new Column("mv_bitmap_union_v1", Type.BITMAP, false, AggregateType.BITMAP_UNION, true, "", "");
        TableNameInfo tableNameInfo = new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "db1", "t1");
        SlotRef slotRef = new SlotRef(tableNameInfo, "v1");
        FunctionCallExpr functionCallExpr = new FunctionCallExpr("to_bitmap", Lists.newArrayList(slotRef), true);
        column2.setDefineExpr(functionCallExpr);
        columnList.add(column1);
        columnList.add(column2);
        IndexSchemaProcNode indexSchemaProcNode = new IndexSchemaProcNode(columnList, null);
        ProcResult procResult = indexSchemaProcNode.fetchResult();
        Assertions.assertEquals(2, procResult.getRows().size());
        Assertions.assertTrue(procResult.getRows().get(1).contains(column2.getDisplayName()));
        Assertions.assertEquals(6, procResult.getColumnNames().size(), "The column size should be 6");
        Assertions.assertEquals(6, procResult.getRows().get(1).size(), "The row size should be 6");

    }

    @Test
    public void testCreateResultShowsNestedCommentsWhenCommentsRequested() {
        StructType structType = new StructType(
                new StructField("value", Type.INT, "nested-comment", true));
        Column column = new Column("info", structType, true, null, true, "", "top-level-comment");

        ProcResult result = IndexSchemaProcNode.createResult(
                Lists.newArrayList(column), null,
                Lists.newArrayList(IndexSchemaProcNode.COMMENT_COLUMN_TITLE));

        Assertions.assertTrue(result.getRows().get(0).get(1).contains("nested-comment"));
        Assertions.assertEquals("top-level-comment", result.getRows().get(0).get(6));
    }

    @Test
    public void testCreateResultPreservesNestedRequirednessWithAndWithoutComments() {
        StructType structType = new StructType(Lists.newArrayList(
                new StructField("required_value", Type.INT, "required-comment", false),
                new StructField("optional_value", Type.INT, "optional-comment", true)));
        Column column = new Column("info", structType, true, null, true, "", "top-level-comment");

        String typeWithComments = IndexSchemaProcNode.createResult(
                Lists.newArrayList(column), null,
                Lists.newArrayList(IndexSchemaProcNode.COMMENT_COLUMN_TITLE))
                .getRows().get(0).get(1);
        Assertions.assertTrue(typeWithComments.contains(
                "required_value:int not null comment \"required-comment\""));
        Assertions.assertTrue(typeWithComments.contains(
                "optional_value:int comment \"optional-comment\""));

        String typeWithoutComments = IndexSchemaProcNode.createResult(
                Lists.newArrayList(column), null, Lists.newArrayList())
                .getRows().get(0).get(1);
        Assertions.assertTrue(typeWithoutComments.contains("required_value:int not null"));
        Assertions.assertFalse(typeWithoutComments.contains("required-comment"));
        Assertions.assertFalse(typeWithoutComments.contains("optional-comment"));
    }

    @Test
    public void testCreateResultQuotesNestedCommentsAsSqlLiterals() {
        StructType structType = new StructType(
                new StructField("value", Type.INT, "owner's \\path", true));
        Column column = new Column("info", structType, true, null, true, "", "top-level-comment");

        try (MockedStatic<SqlModeHelper> mockedSqlMode = Mockito.mockStatic(SqlModeHelper.class)) {
            mockedSqlMode.when(SqlModeHelper::hasNoBackSlashEscapes).thenReturn(false);
            String displayedType = IndexSchemaProcNode.createResult(
                    Lists.newArrayList(column), null,
                    Lists.newArrayList(IndexSchemaProcNode.COMMENT_COLUMN_TITLE))
                    .getRows().get(0).get(1);

            Assertions.assertTrue(displayedType.contains("comment \"owner's \\\\path\""));
        }
    }
}
