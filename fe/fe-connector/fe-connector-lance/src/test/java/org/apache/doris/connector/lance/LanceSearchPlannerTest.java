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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.LanceSearchPlan.VectorIndexStatus;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TSearchVector;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TVectorElementType;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * The ranges of vector and full-text searches: one per index segment the search can use, one per fragment a vector
 * search scans exactly, and the per-range request. These are the search cases of branch-4.2's LanceScanNodeTest,
 * with #68815's distance ranges; the field-id check moved to the analysis of the search
 * ({@link LanceConnectorSearchTest}), and a non-positive version cannot reach a range ({@link LanceScanRange}).
 */
class LanceSearchPlannerTest {
    private static final String URI = "s3://bucket/table.lance";
    private static final UUID FIRST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final UUID SECOND = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    private static final int VECTOR_FIELD_ID = 9;
    private static final int TEXT_FIELD_ID = 7;
    private static final Schema VECTOR_SCHEMA = new Schema(Collections.singletonList(new Field("vector",
            FieldType.nullable(new ArrowType.FixedSizeList(2)), Collections.singletonList(
                    Field.nullable("item", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE))))));
    private static final Schema TEXT_SCHEMA = new Schema(Collections.singletonList(
            Field.nullable("body", ArrowType.Utf8.INSTANCE)));

    @Test
    void vectorSearchScansOneRangePerFragmentWeightedByItsRows() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(7, 1000, 1000),
                new LanceFragmentInfo(11, 250, 250), new LanceFragmentInfo(13, 800, 800),
                new LanceFragmentInfo(17, 100, 100));

        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, vectorSearch(5, 2), noIndexes());

        Assertions.assertEquals(VectorIndexStatus.NO_MATCH, plan.getVectorIndexStatus());
        List<ConnectorScanRange> ranges = plan.ranges();
        Assertions.assertEquals(4, ranges.size());
        assertFragmentRange(ranges.get(0), Collections.singletonList(7L), 1000, 1000);
        assertFragmentRange(ranges.get(1), Collections.singletonList(11L), 250, 1000);
        assertFragmentRange(ranges.get(2), Collections.singletonList(13L), 800, 1000);
        assertFragmentRange(ranges.get(3), Collections.singletonList(17L), 100, 1000);
        Assertions.assertEquals(4, plan.getUnindexedFragmentCount());
        Assertions.assertEquals(0, plan.getIndexSegmentCount());
    }

    @Test
    void distanceRangesOnlyUseExactIndexPaths() {
        for (IndexType type : IndexType.values()) {
            if (type.getValue() < IndexType.VECTOR.getValue()) {
                continue;
            }
            LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                    new LanceFragmentInfo(2, 7, 7));
            LanceIndexMetadata indexes = vectorIndexes(
                    vectorSegment(FIRST, "vector_idx", type, "L2", Collections.singletonList(1L)));
            for (int bounds = 0; bounds < 4; bounds++) {
                TExternalSearchRequest request = vectorRequest(5, 0);
                if ((bounds & 1) != 0) {
                    request.getSearchQuery().getVectorSearch().setDistanceLowerBound(1);
                }
                if ((bounds & 2) != 0) {
                    request.getSearchQuery().getVectorSearch().setDistanceUpperBound(4);
                }
                request.setVectorSearchOptions(new TVectorSearchOptions().setUseIndex(true).setQueryParallelism(4));

                LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, search(request, VECTOR_FIELD_ID),
                        () -> indexes);

                // Only a two-sided range over IVF_FLAT is exact on the index; any other range scans exactly.
                boolean indexed = bounds == 0 || (bounds == 3 && type == IndexType.IVF_FLAT);
                Assertions.assertEquals(indexed ? VectorIndexStatus.USED : VectorIndexStatus.DISTANCE_RANGE_FALLBACK,
                        plan.getVectorIndexStatus(), type + "/" + bounds);
                List<ConnectorScanRange> ranges = plan.ranges();
                Assertions.assertEquals(2, ranges.size());
                if (indexed) {
                    assertIndexRange(ranges.get(0), FIRST, Collections.singletonList(1L), 8, 8);
                } else {
                    assertFragmentRange(ranges.get(0), Collections.singletonList(1L), 8, 8);
                }
                assertFragmentRange(ranges.get(1), Collections.singletonList(2L), 7, 8);
            }
        }
    }

    @Test
    void vectorSearchScansOneRangePerIndexSegmentAndTheFragmentsItDoesNotCover() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7), new LanceFragmentInfo(3, 6, 6), new LanceFragmentInfo(4, 5, 5),
                new LanceFragmentInfo(5, 4, 4));
        LanceIndexMetadata indexes = vectorIndexes(
                vectorSegment(FIRST, "vector_idx", IndexType.VECTOR, "L2", Arrays.asList(1L, 2L)),
                vectorSegment(SECOND, "vector_idx", IndexType.VECTOR, "L2", Arrays.asList(3L, 4L)));

        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, vectorSearch(5, 0), () -> indexes);

        Assertions.assertEquals(VectorIndexStatus.USED, plan.getVectorIndexStatus());
        List<ConnectorScanRange> ranges = plan.ranges();
        Assertions.assertEquals(3, ranges.size());
        assertIndexRange(ranges.get(0), FIRST, Arrays.asList(1L, 2L), 15, 15);
        assertIndexRange(ranges.get(1), SECOND, Arrays.asList(3L, 4L), 11, 15);
        assertFragmentRange(ranges.get(2), Collections.singletonList(5L), 4, 15);
        Assertions.assertEquals(2, plan.getIndexSegmentCount());
        Assertions.assertEquals(4, plan.getIndexedFragmentCount());
        Assertions.assertEquals(1, plan.getUnindexedFragmentCount());
        // The segment travels as its UUID's 16 big-endian bytes.
        ByteBuffer encoded = params(ranges.get(0)).getIndexSegmentUuids().get(0).duplicate();
        Assertions.assertEquals(FIRST.getMostSignificantBits(), encoded.getLong());
        Assertions.assertEquals(FIRST.getLeastSignificantBits(), encoded.getLong());
    }

    @Test
    void useIndexFalseScansEveryFragmentWithoutReadingIndexes() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7));
        TExternalSearchRequest request = vectorRequest(5, 0);
        request.setVectorSearchOptions(new TVectorSearchOptions().setUseIndex(false));

        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, search(request, -1), () -> {
            throw new AssertionError("a search with use_index=false must not read index metadata");
        });

        Assertions.assertEquals(VectorIndexStatus.DISABLED, plan.getVectorIndexStatus());
        List<ConnectorScanRange> ranges = plan.ranges();
        Assertions.assertEquals(2, ranges.size());
        assertFragmentRange(ranges.get(0), Collections.singletonList(1L), 8, 8);
        assertFragmentRange(ranges.get(1), Collections.singletonList(2L), 7, 8);
    }

    @Test
    void metricMismatchScansTheFragments() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7));
        LanceIndexMetadata indexes = vectorIndexes(
                vectorSegment(FIRST, "vector_idx", IndexType.VECTOR, "L2", Arrays.asList(1L, 2L)));
        TExternalSearchRequest request = vectorRequest(5, 0);
        request.getSearchQuery().getVectorSearch().setMetric(TVectorMetric.COSINE);

        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, search(request, VECTOR_FIELD_ID),
                () -> indexes);

        Assertions.assertEquals(VectorIndexStatus.METRIC_MISMATCH, plan.getVectorIndexStatus());
        List<ConnectorScanRange> ranges = plan.ranges();
        Assertions.assertEquals(2, ranges.size());
        assertFragmentRange(ranges.get(0), Collections.singletonList(1L), 8, 8);
        assertFragmentRange(ranges.get(1), Collections.singletonList(2L), 7, 8);
    }

    @Test
    void anIndexWhoseDetailsNameNoMetricIsNotSearched() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8));
        LanceIndexMetadata indexes = vectorIndexes(
                vectorSegment(FIRST, "vector_idx", IndexType.VECTOR, null, Collections.singletonList(1L)));

        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, vectorSearch(5, 0), () -> indexes);

        Assertions.assertEquals(VectorIndexStatus.METRIC_MISMATCH, plan.getVectorIndexStatus());
        assertFragmentRange(plan.ranges().get(0), Collections.singletonList(1L), 8, 8);
    }

    @Test
    void selectsTheIndexBuiltWithTheSearchMetricWhateverTheListingOrder() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7));
        LanceIndexSegmentInfo l2 = vectorSegment(FIRST, "a_l2", IndexType.VECTOR, "L2", Arrays.asList(1L, 2L));
        LanceIndexSegmentInfo cosine = vectorSegment(SECOND, "z_cosine", IndexType.VECTOR, "COSINE",
                Arrays.asList(1L, 2L));
        TExternalSearchRequest request = vectorRequest(5, 0);
        request.getSearchQuery().getVectorSearch().setMetric(TVectorMetric.COSINE);

        for (LanceIndexMetadata indexes : Arrays.asList(vectorIndexes(l2, cosine), vectorIndexes(cosine, l2))) {
            List<ConnectorScanRange> ranges = LanceScanPlanner.planSearch(metadata,
                    search(request, VECTOR_FIELD_ID), () -> indexes).ranges();

            Assertions.assertEquals(1, ranges.size());
            assertIndexRange(ranges.get(0), SECOND, Arrays.asList(1L, 2L), 15, 15);
        }
    }

    @Test
    void amongIndexesOfTheSameMetricSelectsTheFirstByName() {
        UUID later = UUID.fromString("55555555-5555-5555-5555-555555555555");
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7), new LanceFragmentInfo(3, 6, 6));
        LanceIndexMetadata indexes = vectorIndexes(
                vectorSegment(later, "z_l2", IndexType.VECTOR, "L2", Arrays.asList(1L, 2L, 3L)),
                vectorSegment(FIRST, "a_l2", IndexType.VECTOR, "L2", Collections.singletonList(1L)),
                vectorSegment(SECOND, "a_l2", IndexType.VECTOR, "L2", Collections.singletonList(2L)));

        List<ConnectorScanRange> ranges = LanceScanPlanner.planSearch(metadata, vectorSearch(5, 0), () -> indexes)
                .ranges();

        Assertions.assertEquals(3, ranges.size());
        assertIndexRange(ranges.get(0), FIRST, Collections.singletonList(1L), 8, 8);
        assertIndexRange(ranges.get(1), SECOND, Collections.singletonList(2L), 7, 8);
        assertFragmentRange(ranges.get(2), Collections.singletonList(3L), 6, 8);
    }

    @Test
    void everyVectorSearchHandleNamesItsMetric() {
        // The arguments name L2 when the user names no metric, so every range and the index choice use one metric.
        TExternalSearchRequest defaultMetric = vectorRequest(5, 0);
        defaultMetric.getSearchQuery().getVectorSearch().setMetric(TVectorMetric.DEFAULT);
        TExternalSearchRequest unsetMetric = vectorRequest(5, 0);
        unsetMetric.getSearchQuery().getVectorSearch().unsetMetric();

        for (TExternalSearchRequest request : Arrays.asList(defaultMetric, unsetMetric)) {
            Assertions.assertThrows(IllegalArgumentException.class, () -> search(request, VECTOR_FIELD_ID));
        }
    }

    @Test
    void skipsAnIndexWithASegmentOfUnknownCoverage() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7));
        TExternalSearchRequest request = vectorRequest(5, 0);
        request.getSearchQuery().getVectorSearch().setMetric(TVectorMetric.COSINE);

        LanceSearchPlan alone = LanceScanPlanner.planSearch(metadata, search(request, VECTOR_FIELD_ID),
                () -> vectorIndexes(vectorSegment(FIRST, "a_incomplete", IndexType.VECTOR, "COSINE", null)));
        Assertions.assertEquals(VectorIndexStatus.UNKNOWN_COVERAGE, alone.getVectorIndexStatus());
        Assertions.assertEquals(2, alone.ranges().size());

        List<ConnectorScanRange> ranges = LanceScanPlanner.planSearch(metadata, search(request, VECTOR_FIELD_ID),
                () -> vectorIndexes(vectorSegment(FIRST, "a_incomplete", IndexType.VECTOR, "COSINE", null),
                        vectorSegment(SECOND, "z_cosine", IndexType.VECTOR, "COSINE", Arrays.asList(1L, 2L))))
                .ranges();
        Assertions.assertEquals(1, ranges.size());
        assertIndexRange(ranges.get(0), SECOND, Arrays.asList(1L, 2L), 15, 15);
    }

    @Test
    void anIndexCoveringNoFragmentOfTheVersionIsNotSearched() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8));
        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, vectorSearch(5, 0), () -> vectorIndexes(
                vectorSegment(FIRST, "vector_idx", IndexType.VECTOR, "L2", Collections.singletonList(9L))));

        Assertions.assertEquals(VectorIndexStatus.NO_VISIBLE_COVERAGE, plan.getVectorIndexStatus());
        assertFragmentRange(plan.ranges().get(0), Collections.singletonList(1L), 8, 8);
    }

    @Test
    void fullTextSearchScansOneRangePerCommittedIndexSegment() {
        LanceTableMetadata metadata = metadata(TEXT_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7), new LanceFragmentInfo(3, 6, 6));
        LanceIndexMetadata indexes = textIndexes(textSegment(FIRST, "body_fts", Arrays.asList(1L, 2L)),
                textSegment(SECOND, "body_fts", Collections.singletonList(3L)));

        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata,
                search(fullTextRequest(5, 2, TFtsCoverageMode.STRICT), TEXT_FIELD_ID), () -> indexes);

        Assertions.assertEquals(VectorIndexStatus.NOT_PLANNED, plan.getVectorIndexStatus());
        List<ConnectorScanRange> ranges = plan.ranges();
        Assertions.assertEquals(2, ranges.size());
        assertIndexRange(ranges.get(0), FIRST, Arrays.asList(1L, 2L), 15, 15);
        assertIndexRange(ranges.get(1), SECOND, Collections.singletonList(3L), 6, 15);
        Assertions.assertEquals(0, plan.getUnindexedFragmentCount());
    }

    @Test
    void fullTextCoverageModesDecideAboutUnindexedFragments() {
        LanceTableMetadata metadata = metadata(TEXT_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7), new LanceFragmentInfo(3, 6, 6));
        LanceIndexMetadata indexes = textIndexes(textSegment(FIRST, "body_fts", Arrays.asList(1L, 2L)));

        DorisConnectorException strict = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceScanPlanner.planSearch(metadata,
                        search(fullTextRequest(10, 0, TFtsCoverageMode.STRICT), TEXT_FIELD_ID), () -> indexes));
        Assertions.assertTrue(strict.getMessage().contains("1 unindexed fragments"), strict.getMessage());

        List<ConnectorScanRange> ranges = LanceScanPlanner.planSearch(metadata,
                search(fullTextRequest(10, 0, TFtsCoverageMode.INDEX_ONLY), TEXT_FIELD_ID), () -> indexes).ranges();
        Assertions.assertEquals(1, ranges.size());
        assertIndexRange(ranges.get(0), FIRST, Arrays.asList(1L, 2L), 15, 15);

        // An index covering no fragment of the version is an empty INDEX_ONLY search.
        Assertions.assertTrue(LanceScanPlanner.planSearch(metadata,
                search(fullTextRequest(10, 0, TFtsCoverageMode.INDEX_ONLY), TEXT_FIELD_ID),
                () -> textIndexes(textSegment(FIRST, "body_fts", Collections.singletonList(9L)))).ranges().isEmpty());
    }

    @Test
    void fullTextSearchNeedsOneIndexWithKnownDisjointCoverage() {
        LanceTableMetadata metadata = metadata(TEXT_SCHEMA, new LanceFragmentInfo(1, 8, 8));
        TExternalSearchRequest request = fullTextRequest(10, 0, TFtsCoverageMode.INDEX_ONLY);
        assertPlanFails(metadata, request, textIndexes(), "No committed Lance FTS index exists for column 'body'");
        assertPlanFails(metadata, request, textIndexes(textSegment(FIRST, "a_fts", Collections.singletonList(1L)),
                textSegment(SECOND, "b_fts", Collections.singletonList(1L))), "Multiple Lance FTS indexes");
        assertPlanFails(metadata, request, textIndexes(textSegment(FIRST, "body_fts", null)),
                "has no fragment coverage metadata");
        assertPlanFails(metadata, request, textIndexes(textSegment(FIRST, "body_fts", Collections.singletonList(1L)),
                textSegment(SECOND, "body_fts", Collections.singletonList(1L))),
                "is covered by multiple physical index segments");
    }

    @Test
    void eachRangeSearchesTopKPlusOffsetCandidatesAndSkipsNone() {
        TExternalSearchRequest logical = vectorRequest(5, 2);
        LanceSearchTableHandle search = search(logical, VECTOR_FIELD_ID);

        TVectorSearchParams split = search.splitRequest().getSearchQuery().getVectorSearch();

        Assertions.assertEquals(7, split.getTopK());
        Assertions.assertEquals(0, split.getOffset());
        TVectorSearchParams unchanged = search.getRequest().getSearchQuery().getVectorSearch();
        Assertions.assertEquals(5, unchanged.getTopK());
        Assertions.assertEquals(2, unchanged.getOffset());
    }

    @Test
    void multiVectorRangeKeepsTheMatrixVersionAndTheMetric() throws Exception {
        TExternalSearchRequest logical = vectorRequest(5, 2);
        TSearchVector matrix = new TSearchVector().setElementType(TVectorElementType.FLOAT32)
                .setDimension(2).setNumVectors(2).setValues(new byte[16]);
        logical.getSearchQuery().getVectorSearch().setQueryVector(matrix);

        TExternalSearchRequest split = search(logical, VECTOR_FIELD_ID).splitRequest();

        Assertions.assertEquals(1, split.getSchemaVersion());
        Assertions.assertEquals(TVectorMetric.L2, split.getSearchQuery().getVectorSearch().getMetric());
        Assertions.assertEquals(matrix, split.getSearchQuery().getVectorSearch().getQueryVector());
        Assertions.assertEquals(7, split.getSearchQuery().getVectorSearch().getTopK());
        Assertions.assertEquals(0, split.getSearchQuery().getVectorSearch().getOffset());
        TExternalSearchRequest decoded = new TExternalSearchRequest();
        new TDeserializer().deserialize(decoded, new TSerializer().serialize(split));
        Assertions.assertEquals(split, decoded);
    }

    @Test
    void fullTextRangeKeepsAPhraseQuery() {
        TFullTextSearchParams phrase = new TFullTextSearchParams().setColumn("body").setQuery("lance doris")
                .setTopK(5).setOffset(2).setQueryType(TFtsQueryType.PHRASE).setPhraseSlop(1)
                .setCoverageMode(TFtsCoverageMode.STRICT);
        LanceSearchTableHandle search = search(new TExternalSearchRequest().setSchemaVersion(1)
                .setSearchQuery(TExternalSearchQuery.full_text_search(phrase)), TEXT_FIELD_ID);

        TFullTextSearchParams split = search.splitRequest().getSearchQuery().getFullTextSearch();

        Assertions.assertEquals(7, split.getTopK());
        Assertions.assertEquals(0, split.getOffset());
        Assertions.assertEquals(TFtsQueryType.PHRASE, split.getQueryType());
        Assertions.assertEquals(1, split.getPhraseSlop());
        Assertions.assertFalse(split.isSetMatchOperator());
        Assertions.assertFalse(split.isSetMaxFuzzyDistance());
        Assertions.assertEquals(5, search.getRequest().getSearchQuery().getFullTextSearch().getTopK());
    }

    @Test
    void explainShowsWhatTheSearchSearchesAndHowItWasSplit() {
        LanceTableMetadata metadata = metadata(VECTOR_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7));
        TExternalSearchRequest request = vectorRequest(5, 0);
        request.getSearchQuery().getVectorSearch().setDistanceLowerBound(0.5f).setDistanceUpperBound(4);
        request.setVectorSearchOptions(new TVectorSearchOptions().setQueryParallelism(4));
        LanceSearchPlan plan = LanceScanPlanner.planSearch(metadata, search(request, VECTOR_FIELD_ID),
                () -> vectorIndexes(vectorSegment(FIRST, "vector_idx", IndexType.IVF_PQ, "L2",
                        Collections.singletonList(1L))));

        Assertions.assertEquals("externalSearchType=VECTOR\n"
                + "lanceVectorIndexStatus=DISTANCE_RANGE_FALLBACK\n"
                + "lanceVectorColumn=vector\n"
                + "lanceQueryParallelism=4\n"
                + "lanceDistanceRange=[0.5, 4.0)\n"
                + "lanceMetric=l2\n"
                + "lanceVersion=42\n"
                + "lanceSearchFragments=2\n"
                + "lanceSearchUnindexedFragments=2\n"
                + "lanceSearchIndexSegments=0\n"
                + "lanceSearchIndexFragments=0\n", explain(plan, request));

        LanceTableMetadata text = metadata(TEXT_SCHEMA, new LanceFragmentInfo(1, 8, 8),
                new LanceFragmentInfo(2, 7, 7));
        TExternalSearchRequest fullText = fullTextRequest(5, 0, TFtsCoverageMode.INDEX_ONLY);
        LanceSearchPlan textPlan = LanceScanPlanner.planSearch(text, search(fullText, TEXT_FIELD_ID),
                () -> textIndexes(textSegment(FIRST, "body_fts", Collections.singletonList(1L))));
        Assertions.assertEquals("externalSearchType=FULL_TEXT\n"
                + "lanceFullTextColumn=body\n"
                + "lanceFtsCoverageMode=INDEX_ONLY\n"
                + "lanceFtsQueryType=MATCH\n"
                + "lanceFtsMatchOperator=OR\n"
                + "lanceFtsMaxFuzzyDistance=0\n"
                + "lanceVersion=42\n"
                + "lanceSearchFragments=2\n"
                + "lanceSearchUnindexedFragments=1\n"
                + "lanceSearchIndexSegments=1\n"
                + "lanceSearchIndexFragments=1\n", explain(textPlan, fullText));
    }

    private static String explain(LanceSearchPlan plan, TExternalSearchRequest request) {
        Map<String, String> props = new LinkedHashMap<>();
        plan.addExplainProperties(props, request);
        Assertions.assertTrue(LanceSearchPlan.isSearch(props));
        StringBuilder output = new StringBuilder();
        LanceSearchPlan.appendExplain(output, "", props, "42");
        return output.toString();
    }

    private static void assertPlanFails(LanceTableMetadata metadata, TExternalSearchRequest request,
            LanceIndexMetadata indexes, String message) {
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> LanceScanPlanner.planSearch(metadata, search(request, TEXT_FIELD_ID), () -> indexes));
        Assertions.assertTrue(failure.getMessage().contains(message), failure.getMessage());
    }

    private static void assertFragmentRange(ConnectorScanRange range, List<Long> fragmentIds, long weight,
            long targetSplitSize) {
        TLanceFileDesc params = params(range);
        Assertions.assertEquals(URI, params.getDatasetUri());
        Assertions.assertEquals(42, params.getVersion());
        Assertions.assertEquals(fragmentIds, params.getFragmentIds());
        Assertions.assertFalse(params.isSetIndexSegmentUuids());
        // A search applies no LIMIT on a range and leaves the scalar index alone.
        Assertions.assertFalse(params.isSetLimit());
        Assertions.assertFalse(params.isSetUseScalarIndex());
        Assertions.assertEquals(weight, range.getSelfSplitWeight());
        Assertions.assertEquals(targetSplitSize, range.getTargetSplitSize());
    }

    private static void assertIndexRange(ConnectorScanRange range, UUID segment, List<Long> fragmentIds,
            long weight, long targetSplitSize) {
        TLanceFileDesc params = params(range);
        Assertions.assertEquals(fragmentIds, params.getFragmentIds());
        ByteBuffer encoded = params.getIndexSegmentUuids().get(0).duplicate();
        Assertions.assertEquals(segment, new UUID(encoded.getLong(), encoded.getLong()));
        Assertions.assertFalse(params.isSetLimit());
        Assertions.assertEquals(weight, range.getSelfSplitWeight());
        Assertions.assertEquals(targetSplitSize, range.getTargetSplitSize());
    }

    private static TLanceFileDesc params(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getLanceParams();
    }

    private static LanceTableMetadata metadata(Schema schema, LanceFragmentInfo... fragments) {
        return new LanceTableMetadata(new LanceTableAccess(URI, Collections.emptyMap(), false), 42, schema,
                Arrays.asList(fragments));
    }

    private static Supplier<LanceIndexMetadata> noIndexes() {
        return () -> vectorIndexes();
    }

    private static LanceIndexMetadata vectorIndexes(LanceIndexSegmentInfo... segments) {
        return new LanceIndexMetadata(Collections.singletonMap("vector", VECTOR_FIELD_ID), Arrays.asList(segments));
    }

    private static LanceIndexMetadata textIndexes(LanceIndexSegmentInfo... segments) {
        return new LanceIndexMetadata(Collections.singletonMap("body", TEXT_FIELD_ID), Arrays.asList(segments));
    }

    private static LanceIndexSegmentInfo vectorSegment(UUID uuid, String name, IndexType type, String metric,
            List<Long> fragments) {
        return new LanceIndexSegmentInfo(uuid, name, Collections.singletonList(VECTOR_FIELD_ID), fragments, type,
                metric);
    }

    private static LanceIndexSegmentInfo textSegment(UUID uuid, String name, List<Long> fragments) {
        return new LanceIndexSegmentInfo(uuid, name, Collections.singletonList(TEXT_FIELD_ID), fragments,
                IndexType.INVERTED);
    }

    private static LanceSearchTableHandle vectorSearch(long topK, long offset) {
        return search(vectorRequest(topK, offset), VECTOR_FIELD_ID);
    }

    private static LanceSearchTableHandle search(TExternalSearchRequest request, int fieldId) {
        return new LanceSearchTableHandle(new LanceTableHandle("db", "t").withVersion(42), request, fieldId);
    }

    private static TExternalSearchRequest vectorRequest(long topK, long offset) {
        return new TExternalSearchRequest().setSchemaVersion(1).setSearchQuery(TExternalSearchQuery.vector_search(
                new TVectorSearchParams().setColumn("vector").setTopK(topK).setOffset(offset)
                        .setMetric(TVectorMetric.L2)));
    }

    private static TExternalSearchRequest fullTextRequest(long topK, long offset, TFtsCoverageMode coverageMode) {
        return new TExternalSearchRequest().setSchemaVersion(1).setSearchQuery(TExternalSearchQuery.full_text_search(
                new TFullTextSearchParams().setColumn("body").setQuery("lance").setTopK(topK).setOffset(offset)
                        .setQueryType(TFtsQueryType.MATCH).setMatchOperator(TFtsMatchOperator.OR)
                        .setMaxFuzzyDistance(0).setCoverageMode(coverageMode)));
    }
}
