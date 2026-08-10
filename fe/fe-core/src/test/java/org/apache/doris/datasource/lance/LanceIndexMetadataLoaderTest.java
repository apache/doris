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

package org.apache.doris.datasource.lance;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexDescription;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LanceIndexMetadataLoaderTest {

    @Test
    public void testEmptyDescriptionsReturnImmutableList() {
        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                Collections.emptyList(), Collections.emptyMap());

        Assertions.assertTrue(indexes.isEmpty());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> indexes.add(new LanceLogicalIndex(
                        "idx", Collections.singletonList("column"), "BTREE", "{}")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(null, Collections.emptyMap()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(null), Collections.emptyMap()));
    }

    @Test
    public void testNormalizesIvfPqDescriptionAndDefensivelyCopiesColumns() {
        IndexDescription description = description("embedding_idx",
                Collections.singletonList(7), "IVF_PQ",
                "{\"target_partition_size\":256,\"unknown\":\"secret\","
                        + "\"metric_type\":\"cosine\",\"num_sub_vectors\":16,\"num_bits\":8}");

        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description),
                Collections.singletonMap(7, "embedding"));

        Assertions.assertEquals(1, indexes.size());
        LanceLogicalIndex index = indexes.get(0);
        Assertions.assertEquals("embedding_idx", index.getName());
        Assertions.assertEquals(Collections.singletonList("embedding"), index.getColumns());
        Assertions.assertEquals("IVF_PQ", index.getIndexType());
        Assertions.assertEquals(
                "{\"metric_type\":\"cosine\",\"num_bits\":8,\"num_sub_vectors\":16,"
                        + "\"target_partition_size\":256}",
                index.getProperties());
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> index.getColumns().add("another"));

        List<String> mutableColumns = new ArrayList<>(Collections.singletonList("first"));
        LanceLogicalIndex directlyConstructed = new LanceLogicalIndex(
                "direct", mutableColumns, "BTREE", "{}");
        mutableColumns.add("second");
        Assertions.assertEquals(Collections.singletonList("first"),
                directlyConstructed.getColumns());
    }

    @Test
    public void testSortsIndexesAndPreservesCompositeFieldOrder() {
        List<IndexDescription> descriptions = Arrays.asList(
                description("z_idx", Arrays.asList(3, 1, 2), "BTREE", null),
                description("a_idx", Collections.singletonList(1), "BITMAP", "{}"));

        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                descriptions, fieldNames("alpha", "beta", "gamma"));

        Assertions.assertEquals(Arrays.asList("a_idx", "z_idx"),
                Arrays.asList(indexes.get(0).getName(), indexes.get(1).getName()));
        Assertions.assertEquals(Arrays.asList("gamma", "alpha", "beta"),
                indexes.get(1).getColumns());
    }

    @Test
    public void testRejectsExactDuplicateNameButPreservesCaseOnlyNames() {
        List<IndexDescription> duplicates = Arrays.asList(
                description("idx", Collections.singletonList(1), "BTREE", null),
                description("idx", Collections.singletonList(2), "BITMAP", null));
        IllegalArgumentException duplicate = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        duplicates, fieldNames("alpha", "beta")));
        Assertions.assertTrue(duplicate.getMessage().contains(
                "Duplicate Lance logical index name 'idx'"));

        List<IndexDescription> caseOnlyNames = Arrays.asList(
                description("idx", Collections.singletonList(1), "BTREE", null),
                description("IDX", Collections.singletonList(2), "BITMAP", null));
        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                caseOnlyNames, fieldNames("alpha", "beta"));
        Assertions.assertEquals(Arrays.asList("IDX", "idx"),
                Arrays.asList(indexes.get(0).getName(), indexes.get(1).getName()));
    }

    @Test
    public void testRejectsUnknownNestedDuplicateNullAndEmptyFieldIds() {
        IllegalArgumentException unknown = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "unknown", Collections.singletonList(99), "BTREE", null)),
                        fieldNames("alpha")));
        Assertions.assertTrue(unknown.getMessage().contains(
                "Lance index metadata references unknown or nested field id 99"));

        IllegalArgumentException nested = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "nested", Collections.singletonList(2), "BTREE", null)),
                        Collections.singletonMap(1, "parent")));
        Assertions.assertTrue(nested.getMessage().contains(
                "Lance index metadata references unknown or nested field id 2"));

        IllegalArgumentException duplicate = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "duplicate", Arrays.asList(1, 1), "BTREE", null)),
                        fieldNames("alpha")));
        Assertions.assertTrue(duplicate.getMessage().contains("Duplicate field id 1"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "null", Arrays.asList(1, null), "BTREE", null)),
                        fieldNames("alpha")));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "empty", Collections.emptyList(), "BTREE", null)),
                        fieldNames("alpha")));
    }

    @Test
    public void testRejectsTooManyColumnsAndLogicalIndexes() {
        List<Integer> tooManyFieldIds = new ArrayList<>();
        for (int id = 1; id <= 65; ++id) {
            tooManyFieldIds.add(id);
        }
        IllegalArgumentException columns = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "many_columns", tooManyFieldIds, "BTREE", null)),
                        Collections.emptyMap()));
        Assertions.assertTrue(columns.getMessage().contains("64"));

        IndexDescription one = description(
                "idx", Collections.singletonList(1), "BTREE", null);
        IllegalArgumentException indexes = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.nCopies(10_001, one), fieldNames("alpha")));
        Assertions.assertTrue(indexes.getMessage().contains("10000"));
    }

    @Test
    public void testRejectsExternalStringsOverUtf8ByteLimit() {
        assertBoundFailure(
                description(repeat("x", 1025), Collections.singletonList(1), "BTREE", null),
                fieldNames("alpha"), "name", "1024");
        assertBoundFailure(
                description("idx", Collections.singletonList(1), repeat("x", 1025), null),
                fieldNames("alpha"), "type", "1024");
        assertBoundFailure(
                description("idx", Collections.singletonList(1), "BTREE", null),
                Collections.singletonMap(1, repeat("x", 1025)), "column", "1024");
        assertBoundFailure(
                description("idx", Collections.singletonList(1), "BTREE", repeat("x", 1025)),
                fieldNames("alpha"), "details", "1024");

        String multibyte = repeat("界", 342);
        Assertions.assertTrue(multibyte.length() < 1024);
        assertBoundFailure(
                description(multibyte, Collections.singletonList(1), "BTREE", null),
                fieldNames("alpha"), "name", "1024");

        String withinUtf8Limit = repeat("界", 341);
        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description(
                        withinUtf8Limit, Collections.singletonList(1), "BTREE", null)),
                fieldNames("alpha"));
        Assertions.assertEquals(withinUtf8Limit, indexes.get(0).getName());
    }

    @Test
    public void testRejectsAggregateColumnNameBytesOverLimit() {
        List<Integer> fieldIds = new ArrayList<>();
        Map<Integer, String> fields = new HashMap<>();
        for (int id = 1; id <= 17; ++id) {
            fieldIds.add(id);
            fields.put(id, repeat("x", 1024));
        }

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "wide", fieldIds, "BTREE", null)), fields));
        Assertions.assertTrue(exception.getMessage().contains("16384"));
    }

    @Test
    public void testNullAndBlankDetailsProduceEmptyProperties() {
        List<IndexDescription> descriptions = Arrays.asList(
                description("blank", Collections.singletonList(1), "BTREE", "  \n\t"),
                description("empty", Collections.singletonList(1), "BTREE", ""),
                description("null", Collections.singletonList(1), "BTREE", null),
                description("unicode_blank", Collections.singletonList(1), "BTREE", "\u2003"));

        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                descriptions, fieldNames("alpha"));

        Assertions.assertEquals("{}", indexes.get(0).getProperties());
        Assertions.assertEquals("{}", indexes.get(1).getProperties());
        Assertions.assertEquals("{}", indexes.get(2).getProperties());
        Assertions.assertEquals("{}", indexes.get(3).getProperties());
    }

    @Test
    public void testRejectsMalformedAndNonObjectJsonWithoutEchoingInput() {
        String malformedDetails = "{\"metric_type\":\"raw-secret\"";
        IllegalArgumentException malformed = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "malformed", Collections.singletonList(1),
                                "IVF_PQ", malformedDetails)),
                        fieldNames("embedding")));
        Assertions.assertTrue(malformed.getMessage().contains(
                "Invalid Lance index details JSON for 'malformed'"));
        Assertions.assertFalse(stackTrace(malformed).contains("raw-secret"));

        IllegalArgumentException array = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "array", Collections.singletonList(1), "BTREE", "[1,2]")),
                        fieldNames("alpha")));
        Assertions.assertTrue(array.getMessage().contains(
                "Invalid Lance index details JSON for 'array'"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "lenient", Collections.singletonList(1),
                                "BTREE", "{metric_type:cosine}")),
                        fieldNames("alpha")));
    }

    @Test
    public void testFiltersUnknownPropertiesAndSortsAllowlistedScalars() {
        String details = "{\"target_partition_size\":256,\"num_sub_vectors\":16,"
                + "\"unknown\":\"opaque-raw-details\",\"metric_type\":\"cosine\","
                + "\"hnsw_max_connections\":32,\"compression_type\":\"zstd\","
                + "\"hnsw_construction_ef\":200,\"hnsw_max_level\":true,\"num_bits\":8}";

        LanceLogicalIndex index = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description(
                        "idx", Collections.singletonList(1), "IVF_PQ", details)),
                fieldNames("embedding")).get(0);

        Assertions.assertEquals(
                "{\"compression_type\":\"zstd\",\"hnsw_construction_ef\":200,"
                        + "\"hnsw_max_connections\":32,\"hnsw_max_level\":true,"
                        + "\"metric_type\":\"cosine\",\"num_bits\":8,"
                        + "\"num_sub_vectors\":16,\"target_partition_size\":256}",
                index.getProperties());
        Assertions.assertFalse(index.getProperties().contains("opaque-raw-details"));
        Assertions.assertFalse(index.getProperties().contains("num_partitions"));

        LanceLogicalIndex nullProperty = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description(
                        "null", Collections.singletonList(1), "IVF_PQ",
                        "{\"metric_type\":null}")),
                fieldNames("embedding")).get(0);
        Assertions.assertEquals("{}", nullProperty.getProperties());
    }

    @Test
    public void testRejectsAllowlistedObjectAndArrayValues() {
        for (String details : Arrays.asList(
                "{\"metric_type\":{\"name\":\"cosine\"}}",
                "{\"num_bits\":[8]}")) {
            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> LanceIndexMetadataLoader.normalize(
                            Collections.singletonList(description(
                                    "idx", Collections.singletonList(1), "IVF_PQ", details)),
                            fieldNames("embedding")));
            Assertions.assertTrue(exception.getMessage().contains(
                    "Invalid Lance index details JSON for 'idx'"));
        }
    }

    @Test
    public void testRejectsPropertiesOverFinalJsonLimit() {
        String details = "{\"compression_type\":\"" + repeat("x", 390) + "\"}";
        Assertions.assertTrue(details.length() < 1024);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "idx", Collections.singletonList(1), "IVF_PQ", details)),
                        fieldNames("embedding")));
        Assertions.assertTrue(exception.getMessage().contains("400"));
        Assertions.assertFalse(exception.getMessage().contains(repeat("x", 390)));
    }

    @Test
    public void testCredentialSentinelsNeverAppearInInvalidJsonExceptionChain() {
        String accessKey = "SENTINEL_ACCESS_KEY";
        String secretKey = "SENTINEL_SECRET_KEY";
        String sessionToken = "SENTINEL_SESSION_TOKEN";
        String details = "{\"metric_type\":{\"access\":\"" + accessKey
                + "\",\"secret\":\"" + secretKey + "\",\"session\":\""
                + sessionToken + "\"}}";

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "idx", Collections.singletonList(1), "IVF_PQ", details)),
                        fieldNames("embedding")));
        String stackTrace = stackTrace(exception);
        Assertions.assertFalse(stackTrace.contains(accessKey));
        Assertions.assertFalse(stackTrace.contains(secretKey));
        Assertions.assertFalse(stackTrace.contains(sessionToken));
        Assertions.assertNull(exception.getCause());
    }

    private static void assertBoundFailure(IndexDescription description,
            Map<Integer, String> fields, String expectedType, String expectedLimit) {
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description), fields));
        Assertions.assertTrue(exception.getMessage().contains(expectedType));
        Assertions.assertTrue(exception.getMessage().contains(expectedLimit));
    }

    private static IndexDescription description(String name, List<Integer> fieldIds,
            String indexType, String detailsJson) {
        return new IndexDescription(name, fieldIds, "type.googleapis.com/lance.index",
                indexType, 0, Collections.emptyList(), detailsJson);
    }

    private static Map<Integer, String> fieldNames(String... names) {
        Map<Integer, String> fields = new HashMap<>();
        for (int index = 0; index < names.length; ++index) {
            fields.put(index + 1, names[index]);
        }
        return fields;
    }

    private static String repeat(String value, int count) {
        return String.join("", Collections.nCopies(count, value));
    }

    private static String stackTrace(Throwable throwable) {
        StringWriter writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }
}
