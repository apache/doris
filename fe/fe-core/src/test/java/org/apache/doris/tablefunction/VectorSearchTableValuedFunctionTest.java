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
import org.apache.doris.datasource.lance.metadata.LanceTableAccess;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class VectorSearchTableValuedFunctionTest {
    private TVectorSearchOptions parseOptions(Map<String, String> params) throws Exception {
        return VectorSearchTableValuedFunction.buildVectorSearchOptions(params, true);
    }

    @Test
    public void testRejectInvalidDistanceBoundsBeforeMetadataAccess() {
        for (String value : new String[] {"NaN", "Infinity", "-Infinity", "1e100", "invalid", ""}) {
            AnalysisException error = Assert.assertThrows(AnalysisException.class,
                    () -> new VectorSearchTableValuedFunction(
                            Collections.singletonMap("distance_upper_bound", value)));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("finite FLOAT"));
        }
    }

    @Test
    public void testDistanceBoundsRoundTrip() throws Exception {
        Assert.assertFalse(VectorSearchTableValuedFunction.parseDistanceBounds(Collections.emptyMap())
                .isSetDistanceLowerBound());
        for (Map<String, String> properties : Arrays.asList(
                Collections.singletonMap("distance_lower_bound", "-0.5"),
                Collections.singletonMap("distance_upper_bound", "0"),
                ImmutableMap.of("distance_lower_bound", "0.1", "distance_upper_bound", "1.5"))) {
            TVectorSearchParams params = VectorSearchTableValuedFunction.parseDistanceBounds(properties);
            TVectorSearchParams decoded = new TVectorSearchParams();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(params));
            Assert.assertEquals(params, decoded);
            Assert.assertEquals(properties.containsKey("distance_lower_bound"), decoded.isSetDistanceLowerBound());
            Assert.assertEquals(properties.containsKey("distance_upper_bound"), decoded.isSetDistanceUpperBound());
            if (decoded.isSetDistanceLowerBound()) {
                Assert.assertEquals((double) Float.parseFloat(properties.get("distance_lower_bound")),
                        decoded.getDistanceLowerBound(), 0.0);
            }
        }
    }

    @Test
    public void testRejectEmptyOrReversedDistanceRange() {
        for (String upper : new String[] {"0", "-1", "0.00000000000000000000000000000000000000000000001"}) {
            AnalysisException error = Assert.assertThrows(AnalysisException.class,
                    () -> new VectorSearchTableValuedFunction(
                            ImmutableMap.of("distance_lower_bound", "0", "distance_upper_bound", upper)));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("must be less than"));
        }
        Assert.assertThrows(AnalysisException.class, () -> VectorSearchTableValuedFunction.parseDistanceBounds(
                ImmutableMap.of("distance_lower_bound", "1", "distance_upper_bound", "1.00000001")));
        Assert.assertThrows(AnalysisException.class, () -> VectorSearchTableValuedFunction.parseDistanceBounds(
                Collections.singletonMap("distance_lower_bound", "NaN")));
    }

    @Test
    public void testQueryParallelismOptions() throws Exception {
        Assert.assertNull(parseOptions(Collections.emptyMap()));
        Assert.assertFalse(parseOptions(Collections.singletonMap("nprobes", "4")).isSetQueryParallelism());
        for (String value : new String[] {"-1", "0", "1", "4", "2147483647"}) {
            TVectorSearchOptions options = parseOptions(Collections.singletonMap("query_parallelism", value));
            Assert.assertNotNull("query_parallelism must be serialized", options);
            TVectorSearchOptions decoded = new TVectorSearchOptions();
            new TDeserializer().deserialize(decoded, new TSerializer().serialize(options));
            Assert.assertTrue(decoded.isSetQueryParallelism());
            Assert.assertEquals(Integer.parseInt(value), decoded.getQueryParallelism());
            Assert.assertFalse(decoded.isSetNprobes());
        }
    }

    @Test
    public void testRejectInvalidQueryParallelism() {
        for (String value : new String[] {"-2", "2147483648", "1.5", "abc", ""}) {
            AnalysisException error = Assert.assertThrows(AnalysisException.class,
                    () -> parseOptions(Collections.singletonMap("query_parallelism", value)));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("query_parallelism"));
        }
    }

    @Test
    public void testParseQuotedMultiLevelNamespace() throws AnalysisException {
        TableName tableName = VectorSearchTableValuedFunction.parseTableName(
                "lance_catalog.`doris.analytics`.items");

        Assert.assertEquals("lance_catalog", tableName.getCtl());
        Assert.assertEquals("doris.analytics", tableName.getDb());
        Assert.assertEquals("items", tableName.getTbl());
    }

    @Test
    public void testBackquotedTableNameMayContainSelectorCharacters() throws Exception {
        TableName at = VectorSearchTableValuedFunction.parseTableName("c.d.`user@corp`");
        Assert.assertEquals("user@corp", at.getTbl());
        TableName forName = VectorSearchTableValuedFunction.parseTableName("c.d.`sales for version 2024`");
        Assert.assertEquals("sales for version 2024", forName.getTbl());
    }

    @Test
    public void testTableNameCannotSelectVersionTagOrBranch() {
        for (String table : new String[] {"c.d.t@tag(v1)", "c.d.t@branch(dev)", "c.d.t FOR VERSION AS OF 2"}) {
            AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                    () -> VectorSearchTableValuedFunction.parseTableName(table));
            Assert.assertTrue(exception.getMessage(),
                    exception.getMessage().contains("cannot select a version, tag or branch;"
                            + " use the 'version', 'timestamp', 'tag' or 'branch' property"));
        }
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
        LanceTableMetadata metadata = LanceTableMetadata.createBasicSnapshot(
                new LanceTableAccess("s3://bucket/table.lance", Collections.emptyMap()), 42, schema,
                Collections.emptyList());

        AnalysisException duplicate = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.buildOutputColumns(metadata));

        Assert.assertTrue(duplicate.getMessage().contains("case-insensitive"));
    }

    @Test
    public void testRejectReservedGlobalRowIdPrefixInFrontend() {
        Schema schema = new Schema(Collections.singletonList(
                Field.nullable(Column.GLOBAL_ROWID_COL + "payload", ArrowType.Utf8.INSTANCE)));
        LanceTableMetadata metadata = LanceTableMetadata.createBasicSnapshot(
                new LanceTableAccess("s3://bucket/table.lance", Collections.emptyMap()), 42, schema,
                Collections.emptyList());

        AnalysisException reserved = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.buildOutputColumns(metadata));

        Assert.assertTrue(reserved.getMessage().contains(Column.GLOBAL_ROWID_COL));
    }

    @Test
    public void testRequireLanceFieldIdAfterResolvingVectorColumn() throws Exception {
        Field vector = Field.nullable("vector", ArrowType.Utf8.INSTANCE);
        Schema schema = new Schema(Collections.singletonList(vector));
        LanceTableMetadata metadata = LanceTableMetadata.createSnapshotWithIndexes(
                new LanceTableAccess("s3://bucket/table.lance", Collections.emptyMap()), 42, schema, Collections.emptyList(),
                Collections.singletonMap("vector", 9), Collections.emptyList());

        Assert.assertEquals(9,
                VectorSearchTableValuedFunction.requireLanceFieldId(metadata, vector));

        LanceTableMetadata missingFieldId = LanceTableMetadata.createBasicSnapshot(
                new LanceTableAccess("s3://bucket/table.lance", Collections.emptyMap()), 42, schema,
                Collections.emptyList());
        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> VectorSearchTableValuedFunction.requireLanceFieldId(
                        missingFieldId, vector));
        Assert.assertTrue(exception.getMessage().contains("has no field ID"));
    }
}
