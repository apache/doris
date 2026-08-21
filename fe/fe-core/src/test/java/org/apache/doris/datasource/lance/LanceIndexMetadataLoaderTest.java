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
import org.lance.Dataset;
import org.lance.index.IndexCriteria;
import org.lance.index.IndexDescription;
import org.lance.schema.LanceField;
import org.mockito.Mockito;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testDescribeUserIndexesSkipsSystemIndexesAndDeduplicatesPhysicalEntries() {
        Dataset dataset = Mockito.mock(Dataset.class);
        IndexDescription first = description(
                "first_idx", Collections.singletonList(1), "BTREE", null);
        IndexDescription prefixedUserName = description(
                "__user_idx", Collections.singletonList(2), "BTREE", null);
        Mockito.when(dataset.listIndexes()).thenReturn(Arrays.asList(
                "__lance_frag_reuse", "first_idx", "__lance_mem_wal",
                "first_idx", "__user_idx"));
        Mockito.when(dataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenAnswer(invocation -> {
                    IndexCriteria criteria = invocation.getArgument(0);
                    String name = criteria.getHasName().orElseThrow(AssertionError::new);
                    if ("first_idx".equals(name)) {
                        return Collections.singletonList(first);
                    }
                    if ("__user_idx".equals(name)) {
                        return Collections.singletonList(prefixedUserName);
                    }
                    throw new AssertionError("Unexpected index criteria: " + name);
                });

        List<IndexDescription> descriptions =
                LanceIndexMetadataLoader.describeUserIndexes(dataset);

        Assertions.assertEquals(Arrays.asList(first, prefixedUserName), descriptions);
        Mockito.verify(dataset, Mockito.times(2))
                .describeIndices(Mockito.any(IndexCriteria.class));
        Mockito.verify(dataset, Mockito.never()).describeIndices();
    }

    @Test
    public void testDescribeUserIndexesReturnsEmptyForOnlySystemIndexes() {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.listIndexes()).thenReturn(Arrays.asList(
                "__lance_frag_reuse", "__lance_mem_wal",
                "__lance_frag_reuse"));

        Assertions.assertTrue(
                LanceIndexMetadataLoader.describeUserIndexes(dataset).isEmpty());

        Mockito.verify(dataset, Mockito.never())
                .describeIndices(Mockito.any(IndexCriteria.class));
        Mockito.verify(dataset, Mockito.never()).describeIndices();
    }

    @Test
    public void testShortMemWalNameIsNotTreatedAsSystemIndex() {
        Dataset dataset = Mockito.mock(Dataset.class);
        IndexDescription userDescription = description(
                "__mem_wal", Collections.singletonList(1), "BTREE", null);
        Mockito.when(dataset.listIndexes())
                .thenReturn(Collections.singletonList("__mem_wal"));
        Mockito.when(dataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenReturn(Collections.singletonList(userDescription));

        Assertions.assertEquals(Collections.singletonList(userDescription),
                LanceIndexMetadataLoader.describeUserIndexes(dataset));
        Mockito.verify(dataset).describeIndices(Mockito.argThat(criteria ->
                criteria.getHasName().filter("__mem_wal"::equals).isPresent()));
    }

    @Test
    public void testDescribeUserIndexesEnforcesUniqueUserIndexLimitBeforeDescribe() {
        Dataset atLimitDataset = Mockito.mock(Dataset.class);
        List<String> atLimitNames = new ArrayList<>();
        for (int index = 0; index < 256; ++index) {
            atLimitNames.add("idx_" + index);
        }
        atLimitNames.addAll(Arrays.asList(
                "__lance_frag_reuse", "__lance_mem_wal"));
        Mockito.when(atLimitDataset.listIndexes()).thenReturn(atLimitNames);
        Mockito.when(atLimitDataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenAnswer(invocation -> {
                    IndexCriteria criteria = invocation.getArgument(0);
                    String name = criteria.getHasName().orElseThrow(AssertionError::new);
                    return Collections.singletonList(description(
                            name, Collections.singletonList(1), "BTREE", null));
                });

        Assertions.assertEquals(256,
                LanceIndexMetadataLoader.describeUserIndexes(atLimitDataset).size());
        Mockito.verify(atLimitDataset, Mockito.times(256))
                .describeIndices(Mockito.any(IndexCriteria.class));

        Dataset overLimitDataset = Mockito.mock(Dataset.class);
        List<String> overLimitNames = new ArrayList<>(atLimitNames.subList(0, 256));
        overLimitNames.add("idx_256");
        Mockito.when(overLimitDataset.listIndexes()).thenReturn(overLimitNames);

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(overLimitDataset));
        Assertions.assertTrue(exception.getMessage().contains("256"));
        Mockito.verify(overLimitDataset, Mockito.never())
                .describeIndices(Mockito.any(IndexCriteria.class));
    }

    @Test
    public void testDescribeUserIndexesBoundsRawPhysicalEntriesIncludingSystemEntries() {
        Dataset atLimitDataset = Mockito.mock(Dataset.class);
        Mockito.when(atLimitDataset.listIndexes()).thenReturn(
                Collections.nCopies(16384, "__lance_frag_reuse"));
        Assertions.assertTrue(
                LanceIndexMetadataLoader.describeUserIndexes(atLimitDataset).isEmpty());

        Dataset overLimitDataset = Mockito.mock(Dataset.class);
        Mockito.when(overLimitDataset.listIndexes()).thenReturn(
                Collections.nCopies(16385, "__lance_frag_reuse"));

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(overLimitDataset));

        Assertions.assertTrue(exception.getMessage().contains("16384"));
        Mockito.verify(overLimitDataset, Mockito.never())
                .describeIndices(Mockito.any(IndexCriteria.class));
    }

    @Test
    public void testDescribeUserIndexesRejectsInvalidProviderResults() {
        Dataset nullNamesDataset = Mockito.mock(Dataset.class);
        Mockito.when(nullNamesDataset.listIndexes()).thenReturn(null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(nullNamesDataset));

        for (String invalidName : Arrays.asList(null, "")) {
            Dataset invalidNameDataset = Mockito.mock(Dataset.class);
            Mockito.when(invalidNameDataset.listIndexes())
                    .thenReturn(Collections.singletonList(invalidName));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> LanceIndexMetadataLoader.describeUserIndexes(invalidNameDataset));
            Mockito.verify(invalidNameDataset, Mockito.never())
                    .describeIndices(Mockito.any(IndexCriteria.class));
        }

        Dataset nullDescriptionsDataset = Mockito.mock(Dataset.class);
        Mockito.when(nullDescriptionsDataset.listIndexes())
                .thenReturn(Collections.singletonList("user_idx"));
        Mockito.when(nullDescriptionsDataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenReturn(null);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(nullDescriptionsDataset));

        Dataset failingDataset = Mockito.mock(Dataset.class);
        RuntimeException sdkFailure = new RuntimeException("SDK failure");
        Mockito.when(failingDataset.listIndexes())
                .thenReturn(Collections.singletonList("user_idx"));
        Mockito.when(failingDataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenThrow(sdkFailure);
        Assertions.assertSame(sdkFailure, Assertions.assertThrows(RuntimeException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(failingDataset)));
    }

    @Test
    public void testDescribeUserIndexesRequiresExactlyOneMatchingDescription() {
        IndexDescription requested = description(
                "user_idx", Collections.singletonList(1), "BTREE", null);
        for (List<IndexDescription> invalidDescriptions : Arrays.<List<IndexDescription>>asList(
                Collections.emptyList(), Arrays.asList(requested, requested))) {
            Dataset dataset = Mockito.mock(Dataset.class);
            Mockito.when(dataset.listIndexes())
                    .thenReturn(Collections.singletonList("user_idx"));
            Mockito.when(dataset.describeIndices(Mockito.any(IndexCriteria.class)))
                    .thenReturn(invalidDescriptions);

            IllegalArgumentException exception = Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> LanceIndexMetadataLoader.describeUserIndexes(dataset));
            Assertions.assertTrue(exception.getMessage().contains("exactly one"));
        }

        Dataset nullDescriptionDataset = Mockito.mock(Dataset.class);
        Mockito.when(nullDescriptionDataset.listIndexes())
                .thenReturn(Collections.singletonList("user_idx"));
        Mockito.when(nullDescriptionDataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenReturn(Collections.singletonList(null));
        IllegalArgumentException nullDescription = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(nullDescriptionDataset));
        Assertions.assertTrue(nullDescription.getMessage().contains("must not be null"));

        Dataset mismatchedNameDataset = Mockito.mock(Dataset.class);
        Mockito.when(mismatchedNameDataset.listIndexes())
                .thenReturn(Collections.singletonList("user_idx"));
        Mockito.when(mismatchedNameDataset.describeIndices(Mockito.any(IndexCriteria.class)))
                .thenReturn(Collections.singletonList(description(
                        "different_idx", Collections.singletonList(1), "BTREE", null)));
        IllegalArgumentException mismatchedName = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.describeUserIndexes(mismatchedNameDataset));
        Assertions.assertTrue(mismatchedName.getMessage().contains(
                "does not match requested name"));
    }

    @Test
    public void testNormalizesIvfPqDescriptionAndDefensivelyCopiesColumns() {
        IndexDescription description = description("embedding_idx",
                Collections.singletonList(7), "IVF_PQ",
                "{\"target_partition_size\":256,\"unknown\":\"secret\","
                        + "\"runtime_hints\":{\"secret\":\"opaque\"},"
                        + "\"compression\":{\"type\":\"pq\",\"num_sub_vectors\":16,"
                        + "\"unknown\":\"opaque\",\"num_bits\":8},"
                        + "\"metric_type\":\"cosine\","
                        + "\"hnsw\":{\"max_level\":3,\"max_connections\":32,"
                        + "\"construction_ef\":200}}");

        List<LanceLogicalIndex> indexes = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description),
                Collections.singletonMap(7, "embedding"));

        Assertions.assertEquals(1, indexes.size());
        LanceLogicalIndex index = indexes.get(0);
        Assertions.assertEquals("embedding_idx", index.getName());
        Assertions.assertEquals(Collections.singletonList("embedding"), index.getColumns());
        Assertions.assertEquals("IVF_PQ", index.getIndexType());
        Assertions.assertEquals(
                "{\"compression\":{\"num_bits\":8,\"num_sub_vectors\":16,\"type\":\"pq\"},"
                        + "\"hnsw\":{\"construction_ef\":200,\"max_connections\":32,"
                        + "\"max_level\":3},\"metric_type\":\"cosine\","
                        + "\"target_partition_size\":256}",
                index.getProperties());
        Assertions.assertFalse(index.getProperties().contains("runtime_hints"));
        Assertions.assertFalse(index.getProperties().contains("opaque"));
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
    public void testNormalizesRqCompressionRotationType() {
        LanceLogicalIndex index = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description(
                        "rq_idx", Collections.singletonList(1), "IVF_RQ",
                        "{\"compression\":{\"rotation_type\":\"matrix\","
                                + "\"type\":\"rq\",\"num_bits\":4},"
                                + "\"metric_type\":\"L2\"}")),
                fieldNames("embedding")).get(0);

        Assertions.assertEquals(
                "{\"compression\":{\"num_bits\":4,\"rotation_type\":\"matrix\","
                        + "\"type\":\"rq\"},\"metric_type\":\"L2\"}",
                index.getProperties());
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
    public void testBuildsCanonicalEscapedPathsForNestedFieldIds() {
        LanceField parent = field(1, "parent");
        LanceField childWithDot = field(2, "child.with.dot");
        LanceField grandchildWithBacktick = field(3, "tick`name");
        LanceField unicodeChild = field(4, "字段");
        Mockito.when(parent.getChildren()).thenReturn(Arrays.asList(childWithDot, unicodeChild));
        Mockito.when(childWithDot.getChildren())
                .thenReturn(Collections.singletonList(grandchildWithBacktick));

        Map<Integer, String> fieldNames = LanceIndexMetadataLoader.buildFieldNamesById(
                Collections.singletonList(parent));

        Assertions.assertEquals("parent", fieldNames.get(1));
        Assertions.assertEquals("parent.`child.with.dot`", fieldNames.get(2));
        Assertions.assertEquals(
                "parent.`child.with.dot`.`tick``name`", fieldNames.get(3));
        Assertions.assertEquals("parent.字段", fieldNames.get(4));

        LanceLogicalIndex index = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description(
                        "nested", Arrays.asList(2, 3, 4), "BTREE", null)),
                fieldNames).get(0);
        Assertions.assertEquals(Arrays.asList(
                "parent.`child.with.dot`",
                "parent.`child.with.dot`.`tick``name`",
                "parent.字段"), index.getColumns());
    }

    @Test
    public void testRejectsSchemaDepthOverLimit() {
        Map<Integer, String> atLimit = LanceIndexMetadataLoader.buildFieldNamesById(
                Collections.singletonList(nestedFieldChain(64)));
        Assertions.assertEquals(64, atLimit.size());

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.buildFieldNamesById(
                        Collections.singletonList(nestedFieldChain(65))));
        Assertions.assertTrue(exception.getMessage().contains("depth"));
        Assertions.assertTrue(exception.getMessage().contains("64"));
    }

    @Test
    public void testRejectsAggregateSchemaFieldCountOverLimit() {
        LanceField root = field(0, "root");
        LanceField repeatedChild = field(1, "child");
        AtomicInteger childId = new AtomicInteger();
        Mockito.when(repeatedChild.getId()).thenAnswer(
                invocation -> childId.incrementAndGet());

        Mockito.when(root.getChildren()).thenReturn(
                Collections.nCopies(16383, repeatedChild));
        Map<Integer, String> atLimit = LanceIndexMetadataLoader.buildFieldNamesById(
                Collections.singletonList(root));
        Assertions.assertEquals(16384, atLimit.size());

        childId.set(0);
        Mockito.when(root.getChildren()).thenReturn(
                Collections.nCopies(16384, repeatedChild));
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.buildFieldNamesById(
                        Collections.singletonList(root)));
        Assertions.assertTrue(exception.getMessage().contains("field count"));
        Assertions.assertTrue(exception.getMessage().contains("16384"));
    }

    @Test
    public void testRejectsUnknownDuplicateNullAndEmptyFieldIds() {
        IllegalArgumentException unknown = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        Collections.singletonList(description(
                                "unknown", Collections.singletonList(99), "BTREE", null)),
                        fieldNames("alpha")));
        Assertions.assertTrue(unknown.getMessage().contains(
                "Lance index metadata references unknown field id 99"));

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

        List<IndexDescription> atLimit = new ArrayList<>();
        for (int index = 0; index < 256; ++index) {
            atLimit.add(description(
                    "idx_" + index, Collections.singletonList(1), "BTREE", null));
        }
        Assertions.assertEquals(256,
                LanceIndexMetadataLoader.normalize(atLimit, fieldNames("alpha")).size());

        List<IndexDescription> overLimit = new ArrayList<>(atLimit);
        overLimit.add(description(
                "idx_over_limit", Collections.singletonList(1), "BTREE", null));
        IllegalArgumentException indexes = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(
                        overLimit, fieldNames("alpha")));
        Assertions.assertTrue(indexes.getMessage().contains("256"));
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
    public void testRejectsAggregateColumnNameBytesAcrossIndexes() {
        List<Integer> firstFieldIds = new ArrayList<>();
        List<Integer> secondFieldIds = new ArrayList<>();
        Map<Integer, String> fields = new HashMap<>();
        for (int id = 1; id <= 18; ++id) {
            if (id <= 9) {
                firstFieldIds.add(id);
            } else {
                secondFieldIds.add(id);
            }
            fields.put(id, repeat("x", 1024));
        }

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> LanceIndexMetadataLoader.normalize(Arrays.asList(
                        description("first", firstFieldIds, "BTREE", null),
                        description("second", secondFieldIds, "BTREE", null)), fields));
        Assertions.assertTrue(exception.getMessage().contains("aggregate"));
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
    public void testFiltersUnknownPropertiesAndSortsAllowlistedNestedScalars() {
        String details = "{\"target_partition_size\":256,\"num_sub_vectors\":16,"
                + "\"unknown\":\"opaque-raw-details\",\"metric_type\":\"cosine\","
                + "\"runtime_hints\":{\"secret\":\"credential\"},"
                + "\"compression\":{\"type\":\"pq\",\"num_sub_vectors\":16,"
                + "\"unknown\":\"nested-opaque\",\"num_bits\":8},"
                + "\"hnsw\":{\"max_connections\":32,\"construction_ef\":200,"
                + "\"max_level\":7,\"unknown\":false}}";

        LanceLogicalIndex index = LanceIndexMetadataLoader.normalize(
                Collections.singletonList(description(
                        "idx", Collections.singletonList(1), "IVF_PQ", details)),
                fieldNames("embedding")).get(0);

        Assertions.assertEquals(
                "{\"compression\":{\"num_bits\":8,\"num_sub_vectors\":16,\"type\":\"pq\"},"
                        + "\"hnsw\":{\"construction_ef\":200,\"max_connections\":32,"
                        + "\"max_level\":7},\"metric_type\":\"cosine\","
                        + "\"target_partition_size\":256}",
                index.getProperties());
        Assertions.assertFalse(index.getProperties().contains("opaque-raw-details"));
        Assertions.assertFalse(index.getProperties().contains("nested-opaque"));
        Assertions.assertFalse(index.getProperties().contains("credential"));
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
                "{\"compression\":{\"num_bits\":[8]}}",
                "{\"compression\":\"pq\"}",
                "{\"hnsw\":[32]}")) {
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
        String details = "{\"compression\":{\"type\":\""
                + repeat("x", 390) + "\"}}";
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

    private static LanceField field(int id, String name) {
        LanceField field = Mockito.mock(LanceField.class);
        Mockito.when(field.getId()).thenReturn(id);
        Mockito.when(field.getName()).thenReturn(name);
        Mockito.when(field.getChildren()).thenReturn(Collections.emptyList());
        return field;
    }

    private static LanceField nestedFieldChain(int depth) {
        LanceField child = null;
        for (int level = depth; level >= 1; --level) {
            LanceField parent = field(level, "level_" + level);
            if (child != null) {
                Mockito.when(parent.getChildren())
                        .thenReturn(Collections.singletonList(child));
            }
            child = parent;
        }
        return child;
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
