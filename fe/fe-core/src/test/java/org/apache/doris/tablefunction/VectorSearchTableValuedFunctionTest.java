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
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.lance.LanceTableMetadata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

public class VectorSearchTableValuedFunctionTest {
    @Test
    public void testParseQuotedMultiLevelNamespace() throws AnalysisException {
        TableName tableName = VectorSearchTableValuedFunction.parseTableName(
                "lance_catalog.`doris.analytics`.items");

        Assert.assertEquals("lance_catalog", tableName.getCtl());
        Assert.assertEquals("doris.analytics", tableName.getDb());
        Assert.assertEquals("items", tableName.getTbl());
    }

    @Test
    public void testRejectAmbiguousUnquotedMultiLevelNamespace() {
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.parseTableName(
                        "lance_catalog.doris.analytics.items"));

        Assert.assertTrue(exception.getMessage().contains("catalog.database.table"));
    }

    @Test
    public void testValidateAndEncodeSqlFilterInFrontend() throws Exception {
        Assert.assertArrayEquals("category = 'book'".getBytes(StandardCharsets.UTF_8),
                VectorSearchTableValuedFunction.validateAndEncodeSqlFilter(
                        "category = 'book'"));

        AnalysisException empty = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.validateAndEncodeSqlFilter("  "));
        Assert.assertTrue(empty.getMessage().contains("must not be empty"));

        AnalysisException nul = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.validateAndEncodeSqlFilter(
                        "category = 'book'\0 OR true"));
        Assert.assertTrue(nul.getMessage().contains("NUL"));
    }

    @Test
    public void testRejectCaseInsensitiveDuplicateOutputColumnsInFrontend() {
        Schema schema = new Schema(Arrays.asList(
                Field.nullable("Category", ArrowType.Utf8.INSTANCE),
                Field.nullable("category", ArrowType.Utf8.INSTANCE)));
        LanceTableMetadata metadata = new LanceTableMetadata(
                "s3://bucket/table.lance", 42, schema,
                Collections.emptyList(), Collections.emptyMap());

        AnalysisException duplicate = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.buildOutputColumns(metadata));

        Assert.assertTrue(duplicate.getMessage().contains("case-insensitive"));
    }

    @Test
    public void testRejectReservedGlobalRowIdPrefixInFrontend() {
        Schema schema = new Schema(Collections.singletonList(
                Field.nullable(Column.GLOBAL_ROWID_COL + "payload", ArrowType.Utf8.INSTANCE)));
        LanceTableMetadata metadata = new LanceTableMetadata(
                "s3://bucket/table.lance", 42, schema,
                Collections.emptyList(), Collections.emptyMap());

        AnalysisException reserved = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.buildOutputColumns(metadata));

        Assert.assertTrue(reserved.getMessage().contains(Column.GLOBAL_ROWID_COL));
    }
}
