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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class FullTextSearchTableValuedFunctionTest {
    @Test
    public void testParseCoverageMode() throws Exception {
        Assert.assertEquals(TFtsCoverageMode.STRICT,
                FullTextSearchTableValuedFunction.parseCoverageMode(" STRICT "));
        Assert.assertEquals(TFtsCoverageMode.INDEX_ONLY,
                FullTextSearchTableValuedFunction.parseCoverageMode("index_only"));
        Assert.assertEquals(TFtsCoverageMode.INDEX_ONLY,
                FullTextSearchTableValuedFunction.parseCoverageMode("INDEX-ONLY"));

        AnalysisException invalid = Assert.assertThrows(AnalysisException.class,
                () -> FullTextSearchTableValuedFunction.parseCoverageMode("flat"));
        Assert.assertTrue(invalid.getMessage().contains("strict or index_only"));
    }

    @Test
    public void testParseQueryTypeAndMatchOperator() throws Exception {
        Assert.assertEquals(TFtsQueryType.MATCH,
                FullTextSearchTableValuedFunction.parseQueryType(" MATCH "));
        Assert.assertEquals(TFtsQueryType.PHRASE,
                FullTextSearchTableValuedFunction.parseQueryType("phrase"));
        Assert.assertEquals(TFtsMatchOperator.OR,
                FullTextSearchTableValuedFunction.parseMatchOperator(" OR "));
        Assert.assertEquals(TFtsMatchOperator.AND,
                FullTextSearchTableValuedFunction.parseMatchOperator("and"));

        AnalysisException invalidType = Assert.assertThrows(AnalysisException.class,
                () -> FullTextSearchTableValuedFunction.parseQueryType("boolean"));
        Assert.assertTrue(invalidType.getMessage().contains("match or phrase"));

        AnalysisException invalidOperator = Assert.assertThrows(AnalysisException.class,
                () -> FullTextSearchTableValuedFunction.parseMatchOperator("xor"));
        Assert.assertTrue(invalidOperator.getMessage().contains("'or' or 'and'"));
    }

    @Test
    public void testResolveStringColumnCaseInsensitively() throws Exception {
        Field rowId = Field.notNullable("row_id", new ArrowType.Int(64, true));
        Field body = Field.nullable("Body", ArrowType.LargeUtf8.INSTANCE);
        LanceTableMetadata metadata = metadata(rowId, body);

        Assert.assertSame(body,
                FullTextSearchTableValuedFunction.findStringField(metadata, "body"));
    }

    @Test
    public void testRejectMissingNonStringAndAmbiguousColumns() {
        LanceTableMetadata metadata = metadata(
                Field.notNullable("row_id", new ArrowType.Int(64, true)),
                Field.nullable("body", ArrowType.Utf8.INSTANCE));
        AnalysisException missing = Assert.assertThrows(AnalysisException.class,
                () -> FullTextSearchTableValuedFunction.findStringField(metadata, "title"));
        Assert.assertTrue(missing.getMessage().contains("does not exist"));

        LanceTableMetadata nonString = metadata(
                Field.notNullable("body", new ArrowType.Int(64, true)));
        AnalysisException wrongType = Assert.assertThrows(AnalysisException.class,
                () -> FullTextSearchTableValuedFunction.findStringField(nonString, "body"));
        Assert.assertTrue(wrongType.getMessage().contains("must be STRING"));

        LanceTableMetadata ambiguous = metadata(
                Field.nullable("Body", ArrowType.Utf8.INSTANCE),
                Field.nullable("body", ArrowType.Utf8.INSTANCE));
        AnalysisException duplicate = Assert.assertThrows(AnalysisException.class,
                () -> FullTextSearchTableValuedFunction.findStringField(ambiguous, "BODY"));
        Assert.assertTrue(duplicate.getMessage().contains("ambiguous"));
    }

    @Test
    public void testBuildOutputColumnsAddsNullableFloatScore() throws Exception {
        LanceTableMetadata metadata = metadata(
                Field.notNullable("row_id", new ArrowType.Int(64, true)),
                Field.nullable("body", ArrowType.Utf8.INSTANCE));

        List<Column> columns = FullTextSearchTableValuedFunction.buildOutputColumns(
                metadata, FullTextSearchTableValuedFunction.SCORE_COLUMN, "full-text search");

        Assert.assertEquals(3, columns.size());
        Column score = columns.get(2);
        Assert.assertEquals(FullTextSearchTableValuedFunction.SCORE_COLUMN, score.getName());
        Assert.assertEquals(Type.FLOAT, score.getType());
        Assert.assertTrue(score.isAllowNull());
    }

    private static LanceTableMetadata metadata(Field... fields) {
        return LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance", 42, new Schema(Arrays.asList(fields)),
                Collections.emptyList(), Collections.emptyMap());
    }
}
