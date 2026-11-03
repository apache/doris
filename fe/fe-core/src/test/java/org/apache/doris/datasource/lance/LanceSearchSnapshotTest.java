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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.lance.metadata.LanceFragmentInfo;
import org.apache.doris.datasource.lance.metadata.LanceRefSelector;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.lance.source.LanceScanNode;
import org.apache.doris.datasource.lance.source.LanceSplit;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.FragmentMetadata;
import org.lance.FragmentOperation;
import org.lance.ReadOptions;
import org.lance.Ref;
import org.lance.WriteParams;
import org.lance.cleanup.CleanupPolicy;
import org.lance.index.DistanceType;
import org.lance.index.IndexParams;
import org.lance.index.IndexType;
import org.lance.index.scalar.ScalarIndexParams;
import org.lance.index.vector.VectorIndexParams;
import org.lance.schema.ColumnAlteration;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Search metadata and planning on historical versions, tags and branches of a real dataset.
 *
 * <p>Main: v1 is empty; v2 adds fragment 0 (ids 0..9); v3 fragment 1 (ids 10..19); v4 a vector
 * index over both; v5 an FTS index over both; v6 fragment 2 (ids 20..29), which neither index
 * covers. Tag {@code rel} points at main v6. Branch {@code dev} forks from v5 and appends ids
 * 100..109 as its own v6. A branch continues its parent's fragment numbering, so dev v6 and main
 * v6 both hold fragments 0, 1 and 2, with a different fragment 2. Tag {@code dev_rel} points at
 * dev v5, the fork point, which is not the branch's latest version.
 */
@Disabled("Re-enable after fixing Arrow C Data JNI compatibility: CI libstdc++ lacks CXXABI_1.3.9")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LanceSearchSnapshotTest {
    private static final String BRANCH = "dev";
    private static final Schema SCHEMA = new Schema(Arrays.asList(
            Field.nullable("id", new ArrowType.Int(64, true)),
            new Field("vec", FieldType.nullable(new ArrowType.FixedSizeList(2)), Collections.singletonList(
                    Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))),
            Field.nullable("body", ArrowType.Utf8.INSTANCE)));

    private Path warehouse;
    private String uri;
    private LanceExternalCatalog catalog;

    @BeforeAll
    public void setUp() throws Exception {
        LanceJniTestSupport.assumeJniBindingsLoadable();
        warehouse = Files.createTempDirectory("lance_search_snapshot");
        uri = warehouse.resolve("docs.lance").toString();
        try (BufferAllocator allocator = new RootAllocator()) {
            WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.0").build();
            try (Dataset created = Dataset.create(allocator, uri, SCHEMA, params)) {
                Assertions.assertEquals(1, created.version());
            }
            Assertions.assertEquals(2, append(uri, allocator, 1, 0, 10));
            Assertions.assertEquals(3, append(uri, allocator, 2, 10, 10));
            try (Dataset dataset = Dataset.open(uri, allocator)) {
                dataset.createIndex(Collections.singletonList("vec"), IndexType.IVF_FLAT, Optional.of("vec_idx"),
                        IndexParams.builder().setVectorIndexParams(VectorIndexParams.ivfFlat(2, DistanceType.L2))
                                .build(), false);
                Assertions.assertEquals(4, dataset.version());
                dataset.createIndex(Collections.singletonList("body"), IndexType.INVERTED, Optional.of("body_idx"),
                        IndexParams.builder().setScalarIndexParams(ScalarIndexParams.create("inverted",
                                "{\"base_tokenizer\":\"simple\",\"language\":\"English\",\"lower_case\":true,"
                                        + "\"stem\":false,\"remove_stop_words\":false}")).build(), false);
                Assertions.assertEquals(5, dataset.version());
            }
            Assertions.assertEquals(6, append(uri, allocator, 5, 20, 10));
            try (Dataset dataset = Dataset.open(uri, allocator)) {
                dataset.tags().create("rel", 6);
                try (Dataset branch = dataset.createBranch(BRANCH, Ref.ofMain(5))) {
                    Assertions.assertEquals(5, branch.version());
                }
                Assertions.assertEquals(6, append(uri + "/tree/" + BRANCH, allocator, 5, 100, 10));
                dataset.tags().create("dev_rel", 5, BRANCH);
            }
        }
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put(LanceExternalCatalog.WAREHOUSE, warehouse.toString());
        catalog = new LanceExternalCatalog(910, "lance_search_snapshot", null, properties, "");
    }

    @AfterAll
    public void tearDown() throws Exception {
        if (catalog != null) {
            catalog.onClose();
        }
        if (warehouse != null) {
            try (java.util.stream.Stream<Path> paths = Files.walk(warehouse)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
            }
        }
    }

    /** Appends one fragment with ids {@code first..first+count-1} and returns the committed version. */
    private static long append(String datasetUri, BufferAllocator allocator, long readVersion, long first, int count)
            throws Exception {
        WriteParams params = new WriteParams.Builder().withDataStorageVersion("2.0").build();
        try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
            BigIntVector id = (BigIntVector) root.getVector("id");
            FixedSizeListVector vec = (FixedSizeListVector) root.getVector("vec");
            VarCharVector body = (VarCharVector) root.getVector("body");
            id.allocateNew(count);
            vec.allocateNew();
            Float4Vector items = (Float4Vector) vec.getDataVector();
            items.allocateNew(count * 2);
            body.allocateNew(count);
            for (int i = 0; i < count; i++) {
                long value = first + i;
                id.set(i, value);
                vec.setNotNull(i);
                items.set(i * 2, value);
                items.set(i * 2 + 1, -value);
                body.setSafe(i, ("doc " + value + (value % 2 == 0 ? " lance" : " doris"))
                        .getBytes(StandardCharsets.UTF_8));
            }
            id.setValueCount(count);
            items.setValueCount(count * 2);
            vec.setValueCount(count);
            body.setValueCount(count);
            root.setRowCount(count);
            List<FragmentMetadata> fragments = Fragment.create(datasetUri, allocator, root, params);
            try (Dataset committed = new FragmentOperation.Append(fragments)
                    .commit(allocator, datasetUri, Optional.of(readVersion), Collections.emptyMap())) {
                return committed.version();
            }
        }
    }

    private LanceTableMetadata search(LanceRefSelector selector) {
        return catalog.loadTableMetadataForSearch("default", "docs", selector);
    }

    private static LanceRefSelector version(long version) {
        return LanceRefSelector.snapshot(Optional.of(TableSnapshot.versionOf(String.valueOf(version))));
    }

    private static Set<Long> fragments(LanceTableMetadata metadata) {
        return metadata.getFragments().stream().map(LanceFragmentInfo::getId)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static LanceScanNode node(LanceTableMetadata metadata, TExternalSearchRequest request, int fieldId) {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(Mockito.mock(TableIf.class));
        LanceExternalCatalog lanceCatalog = Mockito.mock(LanceExternalCatalog.class);
        Mockito.when(lanceCatalog.getLanceCatalogType()).thenReturn("filesystem");
        LanceExternalTable table = Mockito.mock(LanceExternalTable.class);
        Mockito.when(table.getCatalog()).thenReturn(lanceCatalog);
        return LanceScanNode.forExternalSearch(new PlanNodeId(0), desc, table, metadata, fieldId, request,
                new SessionVariable());
    }

    private static List<LanceSplit> vectorSplits(LanceTableMetadata metadata) throws UserException {
        return vectorSplits(metadata, "vec");
    }

    private static List<LanceSplit> vectorSplits(LanceTableMetadata metadata, String column) throws UserException {
        TVectorSearchParams vector = new TVectorSearchParams().setColumn(column).setTopK(5).setOffset(0);
        LanceScanNode node = node(metadata, new TExternalSearchRequest()
                .setSearchQuery(TExternalSearchQuery.vector_search(vector)),
                metadata.getLanceFieldId(column).getAsInt());
        List<LanceSplit> splits = new ArrayList<>();
        for (Split split : node.getSplits(1)) {
            splits.add((LanceSplit) split);
        }
        return splits;
    }

    private static List<LanceSplit> fullTextSplits(LanceTableMetadata metadata, TFtsCoverageMode mode)
            throws UserException {
        TFullTextSearchParams fullText = new TFullTextSearchParams().setColumn("body").setQuery("lance")
                .setTopK(5).setOffset(0).setCoverageMode(mode).setQueryType(TFtsQueryType.MATCH)
                .setMatchOperator(TFtsMatchOperator.OR).setMaxFuzzyDistance(0);
        LanceScanNode node = node(metadata, new TExternalSearchRequest()
                .setSearchQuery(TExternalSearchQuery.full_text_search(fullText)),
                metadata.getLanceFieldId("body").getAsInt());
        List<LanceSplit> splits = new ArrayList<>();
        for (Split split : node.getSplits(1)) {
            splits.add((LanceSplit) split);
        }
        return splits;
    }

    private static long indexedFragments(List<LanceSplit> splits) {
        return splits.stream().filter(split -> split.getIndexSegmentUuid().isPresent())
                .mapToLong(split -> split.getFragmentIds().size()).sum();
    }

    private static long flatFragments(List<LanceSplit> splits) {
        return splits.stream().filter(split -> !split.getIndexSegmentUuid().isPresent())
                .mapToLong(split -> split.getFragmentIds().size()).sum();
    }

    @Test
    public void testNoSelectorSearchesLatestMain() throws Exception {
        LanceTableMetadata latest = search(LanceRefSelector.latest());
        Assertions.assertEquals(6, latest.getVersion());
        Assertions.assertFalse(latest.getBranch().isPresent());
        Assertions.assertEquals(new TreeSet<>(Arrays.asList(0L, 1L, 2L)), fragments(latest));
    }

    @Test
    public void testVersionBeforeAnyIndexFallsBackToFlatVectorSearchAndRejectsFullText() throws Exception {
        LanceTableMetadata v3 = search(version(3));
        Assertions.assertEquals(3, v3.getVersion());
        Assertions.assertEquals(new TreeSet<>(Arrays.asList(0L, 1L)), fragments(v3));
        List<LanceSplit> splits = vectorSplits(v3);
        Assertions.assertEquals(0, indexedFragments(splits));
        Assertions.assertEquals(2, flatFragments(splits));
        splits.forEach(split -> Assertions.assertEquals(3, split.getVersion()));
        UserException exception = Assertions.assertThrows(UserException.class,
                () -> fullTextSplits(v3, TFtsCoverageMode.INDEX_ONLY));
        Assertions.assertTrue(exception.getMessage().contains(
                "No committed Lance FTS index exists for column 'body' at dataset version 3"),
                exception.getMessage());
    }

    @Test
    public void testVersionWithVectorIndexButNoFullTextIndex() throws Exception {
        LanceTableMetadata v4 = search(version(4));
        List<LanceSplit> splits = vectorSplits(v4);
        Assertions.assertEquals(2, indexedFragments(splits));
        Assertions.assertEquals(0, flatFragments(splits));
        Assertions.assertThrows(UserException.class, () -> fullTextSplits(v4, TFtsCoverageMode.INDEX_ONLY));
        Assertions.assertEquals(2, indexedFragments(fullTextSplits(search(version(5)), TFtsCoverageMode.STRICT)));
    }

    @Test
    public void testAppendAfterIndexingCoversPartially() throws Exception {
        LanceTableMetadata v6 = search(version(6));
        List<LanceSplit> vector = vectorSplits(v6);
        Assertions.assertEquals(2, indexedFragments(vector));
        Assertions.assertEquals(1, flatFragments(vector));
        UserException strict = Assertions.assertThrows(UserException.class,
                () -> fullTextSplits(v6, TFtsCoverageMode.STRICT));
        Assertions.assertTrue(strict.getMessage().contains("every fragment at dataset version 6 to be indexed"),
                strict.getMessage());
        List<LanceSplit> indexOnly = fullTextSplits(v6, TFtsCoverageMode.INDEX_ONLY);
        Assertions.assertEquals(2, indexedFragments(indexOnly));
        Assertions.assertEquals(0, flatFragments(indexOnly));
    }

    @Test
    public void testTagSelectsItsVersion() throws Exception {
        LanceTableMetadata tagged = search(LanceRefSelector.tag("rel"));
        Assertions.assertEquals(6, tagged.getVersion());
        Assertions.assertFalse(tagged.getBranch().isPresent());
        Assertions.assertEquals(new TreeSet<>(Arrays.asList(0L, 1L, 2L)), fragments(tagged));
    }

    @Test
    public void testBranchVersionsOverlapMainWithDifferentData() throws Exception {
        LanceTableMetadata dev = search(LanceRefSelector.branch(BRANCH, Optional.empty()));
        Assertions.assertEquals(6, dev.getVersion());
        Assertions.assertEquals(Optional.of(BRANCH), dev.getBranch());
        Assertions.assertTrue(dev.getDatasetUri().endsWith("/tree/" + BRANCH), dev.getDatasetUri());
        // Same version number and fragment ids as main v6, different rows.
        Assertions.assertEquals(fragments(search(version(6))), fragments(dev));
        Assertions.assertEquals(10, count(dev.getDatasetUri(), 6, "id >= 100"));
        Assertions.assertEquals(0, count(uri, 6, "id >= 100"));
        // Inherited indexes cover the fork point's fragments; the branch's own append is flat.
        List<LanceSplit> vector = vectorSplits(dev);
        Assertions.assertEquals(2, indexedFragments(vector));
        Assertions.assertEquals(1, flatFragments(vector));
        vector.forEach(split -> {
            Assertions.assertEquals(6, split.getVersion());
            Assertions.assertEquals(dev.getDatasetUri(), split.getDatasetUri());
        });
        UserException strict = Assertions.assertThrows(UserException.class,
                () -> fullTextSplits(dev, TFtsCoverageMode.STRICT));
        Assertions.assertTrue(strict.getMessage().contains("dataset version 6 of branch 'dev'"), strict.getMessage());

        LanceTableMetadata fork = search(LanceRefSelector.branch(BRANCH, Optional.of(TableSnapshot.versionOf("5"))));
        Assertions.assertEquals(5, fork.getVersion());
        Assertions.assertEquals(Optional.of(BRANCH), fork.getBranch());
        Assertions.assertEquals(new TreeSet<>(Arrays.asList(0L, 1L)), fragments(fork));
        Assertions.assertEquals(2, indexedFragments(fullTextSplits(fork, TFtsCoverageMode.STRICT)));
    }

    @Test
    public void testTagOnBranchSelectsTheBranch() throws Exception {
        // A tag at a version of the branch other than its latest: Lance's with_tag on the branch
        // URI fails for this (lance#9227), and Doris resolves it through the table root instead.
        LanceTableMetadata tagged = search(LanceRefSelector.tag("dev_rel"));
        Assertions.assertEquals(5, tagged.getVersion());
        Assertions.assertEquals(Optional.of(BRANCH), tagged.getBranch());
        Assertions.assertTrue(tagged.getDatasetUri().endsWith("/tree/" + BRANCH), tagged.getDatasetUri());
        Assertions.assertEquals(new TreeSet<>(Arrays.asList(0L, 1L)), fragments(tagged));
    }

    @Test
    public void testMainBranchIsTheTableRoot() throws Exception {
        LanceTableMetadata main = search(LanceRefSelector.branch(LanceRefSelector.MAIN_BRANCH,
                Optional.of(TableSnapshot.versionOf("4"))));
        Assertions.assertEquals(4, main.getVersion());
        Assertions.assertFalse(main.getBranch().isPresent());
        Assertions.assertFalse(main.getDatasetUri().contains("/tree/"), main.getDatasetUri());
    }

    @Test
    public void testTimestamps() throws Exception {
        Assertions.assertEquals(6, search(LanceRefSelector.snapshot(
                Optional.of(TableSnapshot.timeOf("2999-01-01 00:00:00")))).getVersion());
        Assertions.assertEquals(6, search(LanceRefSelector.branch(BRANCH,
                Optional.of(TableSnapshot.timeOf("2999-01-01 00:00:00")))).getVersion());
        RuntimeException early = Assertions.assertThrows(RuntimeException.class, () -> search(
                LanceRefSelector.snapshot(Optional.of(TableSnapshot.timeOf("2000-01-01 00:00:00")))));
        Assertions.assertTrue(early.getMessage().contains("has no version at or before '2000-01-01 00:00:00'"),
                early.getMessage());
    }

    @Test
    public void testMissingSnapshotsFailWithoutFallingBackToLatest() {
        RuntimeException version = Assertions.assertThrows(RuntimeException.class, () -> search(version(99)));
        Assertions.assertTrue(version.getMessage().contains("Lance version 99 of default.docs was not found"),
                version.getMessage());
        RuntimeException tag = Assertions.assertThrows(RuntimeException.class,
                () -> search(LanceRefSelector.tag("nope")));
        Assertions.assertTrue(tag.getMessage().contains("Lance tag 'nope'"), tag.getMessage());
        RuntimeException branch = Assertions.assertThrows(RuntimeException.class,
                () -> search(LanceRefSelector.branch("nope", Optional.empty())));
        Assertions.assertTrue(branch.getMessage().contains("Lance branch 'nope'"), branch.getMessage());
        RuntimeException branchVersion = Assertions.assertThrows(RuntimeException.class,
                () -> search(LanceRefSelector.branch(BRANCH, Optional.of(TableSnapshot.versionOf("99")))));
        Assertions.assertTrue(branchVersion.getMessage().contains("Lance version 99"), branchVersion.getMessage());
    }

    @Test
    public void testPlannedSnapshotSurvivesLaterCommits() throws Exception {
        String pinnedUri = warehouse.resolve("pinned.lance").toString();
        try (BufferAllocator allocator = new RootAllocator()) {
            try (Dataset created = Dataset.create(allocator, pinnedUri, SCHEMA,
                    new WriteParams.Builder().withDataStorageVersion("2.0").build())) {
                Assertions.assertEquals(1, created.version());
            }
            append(pinnedUri, allocator, 1, 0, 10);
            try (Dataset dataset = Dataset.open(pinnedUri, allocator)) {
                dataset.tags().create("moving", 2);
            }
            LanceTableMetadata planned = catalog.loadTableMetadataForSearch("default", "pinned",
                    LanceRefSelector.tag("moving"));
            Assertions.assertEquals(2, planned.getVersion());
            // A commit and a tag move after planning do not reach the planned splits.
            append(pinnedUri, allocator, 2, 10, 10);
            try (Dataset dataset = Dataset.open(pinnedUri, allocator)) {
                dataset.tags().update("moving", 3);
            }
            List<LanceSplit> splits = vectorSplits(planned);
            splits.forEach(split -> Assertions.assertEquals(2, split.getVersion()));
            Assertions.assertEquals(1, flatFragments(splits));
            // The next resolution sees the moved tag.
            Assertions.assertEquals(3, catalog.loadTableMetadataForSearch("default", "pinned",
                    LanceRefSelector.tag("moving")).getVersion());
        }
    }

    /**
     * Each version keeps its own schema: a column added later is absent from older versions, and
     * a renamed column keeps its field id, so the index built before the rename still serves it.
     */
    @Test
    public void testSchemaFollowsTheSelectedVersion() throws Exception {
        String schemaUri = warehouse.resolve("evolving.lance").toString();
        try (BufferAllocator allocator = new RootAllocator()) {
            try (Dataset created = Dataset.create(allocator, schemaUri, SCHEMA,
                    new WriteParams.Builder().withDataStorageVersion("2.0").build())) {
                Assertions.assertEquals(1, created.version());
            }
            Assertions.assertEquals(2, append(schemaUri, allocator, 1, 0, 10));
            try (Dataset dataset = Dataset.open(schemaUri, allocator)) {
                dataset.createIndex(Collections.singletonList("vec"), IndexType.IVF_FLAT, Optional.of("vec_idx"),
                        IndexParams.builder().setVectorIndexParams(VectorIndexParams.ivfFlat(2, DistanceType.L2))
                                .build(), false);
                dataset.addColumns(Collections.singletonList(Field.nullable("note", ArrowType.Utf8.INSTANCE)));
                dataset.alterColumns(Collections.singletonList(
                        new ColumnAlteration.Builder("vec").rename("embedding").build()));
                Assertions.assertEquals(5, dataset.version());
            }
        }
        LanceTableMetadata v3 = catalog.loadTableMetadataForSearch("default", "evolving", version(3));
        LanceTableMetadata v5 = catalog.loadTableMetadataForSearch("default", "evolving", version(5));
        Set<String> v3Columns = v3.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toSet());
        Set<String> v5Columns = v5.getSchema().getFields().stream().map(Field::getName).collect(Collectors.toSet());
        Assertions.assertEquals(new TreeSet<>(Arrays.asList("id", "vec", "body")), new TreeSet<>(v3Columns));
        Assertions.assertEquals(new TreeSet<>(Arrays.asList("id", "embedding", "body", "note")),
                new TreeSet<>(v5Columns));
        Assertions.assertFalse(v3.getLanceFieldId("note").isPresent());
        Assertions.assertEquals(v3.getLanceFieldId("vec").getAsInt(), v5.getLanceFieldId("embedding").getAsInt());
        Assertions.assertEquals(1, indexedFragments(vectorSplits(v3, "vec")));
        Assertions.assertEquals(1, indexedFragments(vectorSplits(v5, "embedding")));
    }

    @Test
    public void testVersionRemovedByCleanupIsNotFound() throws Exception {
        String cleanedUri = warehouse.resolve("cleaned.lance").toString();
        try (BufferAllocator allocator = new RootAllocator()) {
            try (Dataset created = Dataset.create(allocator, cleanedUri, SCHEMA,
                    new WriteParams.Builder().withDataStorageVersion("2.0").build())) {
                Assertions.assertEquals(1, created.version());
            }
            append(cleanedUri, allocator, 1, 0, 10);
            append(cleanedUri, allocator, 2, 10, 10);
            try (Dataset dataset = Dataset.open(cleanedUri, allocator)) {
                dataset.cleanupWithPolicy(CleanupPolicy.builder().withBeforeVersion(3)
                        .withDeleteUnverified(true).build());
            }
        }
        RuntimeException removed = Assertions.assertThrows(RuntimeException.class,
                () -> catalog.loadTableMetadataForSearch("default", "cleaned", version(2)));
        Assertions.assertTrue(removed.getMessage().contains("Lance version 2 of default.cleaned was not found"),
                removed.getMessage());
        Assertions.assertEquals(3, catalog.loadTableMetadataForSearch("default", "cleaned", version(3)).getVersion());
    }

    @Test
    public void testSearchExplainNamesBranch() throws Exception {
        LanceTableMetadata dev = search(LanceRefSelector.branch(BRANCH, Optional.empty()));
        TVectorSearchParams vector = new TVectorSearchParams().setColumn("vec").setTopK(5).setOffset(0);
        LanceScanNode node = node(dev, new TExternalSearchRequest()
                .setSearchQuery(TExternalSearchQuery.vector_search(vector)), dev.getLanceFieldId("vec").getAsInt());
        node.getSplits(1);
        String explain = node.getNodeExplainString("", TExplainLevel.NORMAL);
        Assertions.assertTrue(explain.contains("lanceVersion=6\n"), explain);
        Assertions.assertTrue(explain.contains("lanceBranch=dev\n"), explain);
        Assertions.assertTrue(explain.contains("lanceManagedVersioning=false\n"), explain);
    }

    private static long count(String datasetUri, long version, String filter) throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.open(allocator, datasetUri,
                        new ReadOptions.Builder().setVersion(version).build())) {
            return dataset.countRows(filter);
        }
    }
}
