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

package org.apache.doris.datasource.lance.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.lance.LanceFragmentInfo;
import org.apache.doris.datasource.lance.LanceIndexSegmentInfo;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExternalSearchQuery;
import org.apache.doris.thrift.TExternalSearchRequest;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.lance.index.IndexType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class LanceScanNodeTest {

    @Test
    public void testFragmentRowsDetermineSplitWeights() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(7, 1000, 1000),
                        new LanceFragmentInfo(11, 250, 250),
                        new LanceFragmentInfo(13, 0, 0)),
                Collections.emptyMap());
        LanceScanNode node = newNode();
        setMetadata(node, metadata);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(3, splits.size());
        assertSplit(splits.get(0), 7, 1000, 100);
        assertSplit(splits.get(1), 11, 1000, 25);
        assertSplit(splits.get(2), 13, 1000, 1);
    }

    @Test
    public void testDeletionHeavyFragmentKeepsPhysicalScanWeight() throws Exception {
        // Both fragments read 1000 physical rows, but one has 990 tombstones so its logical
        // row count is only 10. The pinned BE legacy reader still scans all physical rows, so
        // both fragments must keep the standard weight instead of underweighting the
        // tombstone-heavy one to the minimum.
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(7, 1000, 1000),
                        new LanceFragmentInfo(11, 10, 1000)),
                Collections.emptyMap());
        LanceScanNode node = newNode();
        setMetadata(node, metadata);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(2, splits.size());
        assertSplit(splits.get(0), 7, 1000, 100);
        assertSplit(splits.get(1), 11, 1000, 100);
    }

    @Test
    public void testExternalSearchUsesFragmentSplits() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(7, 1000, 1000),
                        new LanceFragmentInfo(11, 250, 250)),
                Collections.emptyMap());
        TExternalSearchRequest request = vectorSearchRequest(5, 2);
        LanceScanNode node = newSearchNode(metadata, request);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(2, splits.size());
        assertSplit(splits.get(0), 7, 1000, 100);
        assertSplit(splits.get(1), 11, 1000, 25);
        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, splits.get(1));
        Assert.assertEquals(Collections.singletonList(11L), range.getTableFormatParams()
                .getLanceParams().getFragmentIds());
        Assert.assertEquals(42L, range.getTableFormatParams().getLanceParams().getVersion());
    }

    @Test
    public void testExternalSearchRejectsNonPositiveSnapshotVersionInFrontend() {
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                0,
                new Schema(Collections.emptyList()),
                Collections.singletonList(new LanceFragmentInfo(7, 1000, 1000)),
                Collections.emptyMap());
        LanceScanNode node = newSearchNode(metadata, vectorSearchRequest(5, 0));

        UserException exception = Assert.assertThrows(UserException.class,
                () -> node.getSplits(1));

        Assert.assertTrue(exception.getMessage().contains("fixed positive dataset version"));
    }

    @Test
    public void testExternalSearchUsesFragmentRowsForSplitWeights() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(7, 1000, 1000),
                        new LanceFragmentInfo(11, 250, 250),
                        new LanceFragmentInfo(13, 800, 800),
                        new LanceFragmentInfo(17, 100, 100)),
                Collections.emptyMap());
        LanceScanNode node = newSearchNode(metadata, vectorSearchRequest(5, 0));

        List<Split> splits = node.getSplits(3);

        Assert.assertEquals(4, splits.size());
        assertSplit(splits.get(0), 7, 1000, 100);
        assertSplit(splits.get(1), 11, 1000, 25);
        assertSplit(splits.get(2), 13, 1000, 80);
        assertSplit(splits.get(3), 17, 1000, 10);
        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, splits.get(0));
        Assert.assertEquals(Collections.singletonList(7L),
                range.getTableFormatParams().getLanceParams().getFragmentIds());
    }

    @Test
    public void testExternalSearchUsesOneSplitPerFragmentRegardlessOfBackendCount() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(1, 8, 8),
                        new LanceFragmentInfo(2, 7, 7),
                        new LanceFragmentInfo(3, 6, 6),
                        new LanceFragmentInfo(4, 5, 5)),
                Collections.emptyMap());
        LanceScanNode node = newSearchNode(metadata, vectorSearchRequest(5, 0));

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(4, splits.size());
        assertSplit(splits.get(0), 1, 8, 100);
        assertSplit(splits.get(1), 2, 8, 88);
        assertSplit(splits.get(2), 3, 8, 75);
        assertSplit(splits.get(3), 4, 8, 63);
    }

    @Test
    public void testExternalSearchUsesOneSplitPerIndexSegmentAndKeepsUnindexedFragments()
            throws Exception {
        UUID firstSegment = UUID.fromString("11111111-2222-3333-4444-555555555555");
        UUID secondSegment = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/table.lance",
                42,
                vectorSchema(),
                Arrays.asList(
                        new LanceFragmentInfo(1, 8, 8),
                        new LanceFragmentInfo(2, 7, 7),
                        new LanceFragmentInfo(3, 6, 6),
                        new LanceFragmentInfo(4, 5, 5),
                        new LanceFragmentInfo(5, 4, 4)),
                Collections.singletonMap("vector", 9),
                Arrays.asList(
                        new LanceIndexSegmentInfo(firstSegment, "vector_idx",
                                Collections.singletonList(9), Arrays.asList(1L, 2L),
                                IndexType.VECTOR, "L2"),
                        new LanceIndexSegmentInfo(secondSegment, "vector_idx",
                                Collections.singletonList(9), Arrays.asList(3L, 4L),
                                IndexType.VECTOR, "L2")),
                Collections.emptyMap());
        LanceScanNode node = newSearchNode(metadata, vectorSearchRequest(5, 0));

        List<Split> splits = node.getSplits(3);

        Assert.assertEquals(3, splits.size());
        assertIndexSplit(splits.get(0), firstSegment, Arrays.asList(1L, 2L), 15, 100);
        assertIndexSplit(splits.get(1), secondSegment, Arrays.asList(3L, 4L), 15, 74);
        assertSplit(splits.get(2), 5, 15, 27);

        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, splits.get(0));
        Assert.assertEquals(Arrays.asList(1L, 2L),
                range.getTableFormatParams().getLanceParams().getFragmentIds());
        ByteBuffer encodedUuid = range.getTableFormatParams().getLanceParams()
                .getIndexSegmentUuids().get(0).duplicate();
        Assert.assertEquals(firstSegment.getMostSignificantBits(), encodedUuid.getLong());
        Assert.assertEquals(firstSegment.getLeastSignificantBits(), encodedUuid.getLong());
    }

    @Test
    public void testExternalSearchUseIndexFalseKeepsFragmentSplits() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/table.lance",
                42,
                vectorSchema(),
                Arrays.asList(
                        new LanceFragmentInfo(1, 8, 8),
                        new LanceFragmentInfo(2, 7, 7)),
                Collections.emptyMap(),
                Collections.singletonList(
                        new LanceIndexSegmentInfo(UUID.randomUUID(), "vector_idx",
                                Collections.singletonList(9), Arrays.asList(1L, 2L),
                                IndexType.VECTOR, "L2")),
                Collections.emptyMap());
        TExternalSearchRequest request = vectorSearchRequest(5, 0);
        request.setVectorSearchOptions(new TVectorSearchOptions().setUseIndex(false));
        LanceScanNode node = newSearchNode(metadata, request);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(2, splits.size());
        assertSplit(splits.get(0), 1, 8, 100);
        assertSplit(splits.get(1), 2, 8, 88);
    }

    @Test
    public void testExternalSearchFallsBackToFragmentSplitsForMetricMismatch() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/table.lance",
                42,
                vectorSchema(),
                Arrays.asList(
                        new LanceFragmentInfo(1, 8, 8),
                        new LanceFragmentInfo(2, 7, 7)),
                Collections.singletonMap("vector", 9),
                Collections.singletonList(
                        new LanceIndexSegmentInfo(UUID.randomUUID(), "vector_idx",
                                Collections.singletonList(9), Arrays.asList(1L, 2L),
                                IndexType.VECTOR, "L2")),
                Collections.emptyMap());
        TExternalSearchRequest request = vectorSearchRequest(5, 0);
        request.getSearchQuery().getVectorSearch().setMetric(TVectorMetric.COSINE);
        LanceScanNode node = newSearchNode(metadata, request);

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(2, splits.size());
        assertSplit(splits.get(0), 1, 8, 100);
        assertSplit(splits.get(1), 2, 8, 88);
    }

    @Test
    public void testExternalSearchRejectsMissingFieldIdForIndexSegmentPlanning() {
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/table.lance",
                42,
                vectorSchema(),
                Collections.singletonList(new LanceFragmentInfo(1, 8, 8)),
                Collections.emptyMap(),
                Collections.singletonList(
                        new LanceIndexSegmentInfo(UUID.randomUUID(), "vector_idx",
                                Collections.singletonList(9), Collections.singletonList(1L),
                                IndexType.VECTOR, "L2")),
                Collections.emptyMap());
        LanceScanNode node = newSearchNode(metadata, vectorSearchRequest(5, 0));

        UserException exception = Assert.assertThrows(UserException.class,
                () -> node.getSplits(1));

        Assert.assertTrue(exception.getMessage().contains("has no field ID"));
    }

    @Test
    public void testSplitSearchRetainsTopKPlusOffsetCandidates() {
        TExternalSearchRequest logicalRequest = vectorSearchRequest(5, 2);
        LanceScanNode node = LanceScanNode.forExternalSearch(
                new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), null,
                null, -1, logicalRequest, new SessionVariable());

        TExternalSearchRequest splitRequest = node.createSplitSearchRequest();

        Assert.assertEquals(7, splitRequest.getSearchQuery().getVectorSearch().getTopK());
        Assert.assertEquals(0, splitRequest.getSearchQuery().getVectorSearch().getOffset());
        Assert.assertEquals(5, logicalRequest.getSearchQuery().getVectorSearch().getTopK());
        Assert.assertEquals(2, logicalRequest.getSearchQuery().getVectorSearch().getOffset());
    }

    @Test
    public void testLanceSplitRejectsInvalidRangeFieldsInFrontend() {
        assertInvalidSplit(() -> LanceSplit.forFragment("", 42, 1, 1),
                "Lance dataset URI must not be empty");
        assertInvalidSplit(() -> LanceSplit.forFragment("s3://bucket/table.lance", -1, 1, 1),
                "Lance dataset version must be non-negative");
        assertInvalidSplit(() -> LanceSplit.forFragment("s3://bucket/table.lance", 42, -1, 1),
                "Lance fragment id must be non-negative");
    }

    private static LanceScanNode newNode() {
        return new LanceScanNode(
                new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)),
                false,
                new SessionVariable(),
                ScanContext.EMPTY);
    }

    private static LanceScanNode newSearchNode(
            LanceTableMetadata metadata, TExternalSearchRequest request) {
        String vectorColumn = request.getSearchQuery().getVectorSearch().getColumn();
        int vectorFieldId = metadata.getLanceFieldId(vectorColumn).orElse(-1);
        return LanceScanNode.forExternalSearch(
                new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), null,
                metadata, vectorFieldId, request, new SessionVariable());
    }

    private static void setMetadata(LanceScanNode node, LanceTableMetadata metadata) throws Exception {
        java.lang.reflect.Field metadataField = LanceScanNode.class.getDeclaredField("plannedMetadata");
        metadataField.setAccessible(true);
        metadataField.set(node, metadata);
    }

    private static void assertSplit(Split split, long fragmentId, long targetRows, long weight) {
        LanceSplit lanceSplit = (LanceSplit) split;
        Assert.assertEquals(Collections.singletonList(fragmentId), lanceSplit.getFragmentIds());
        Assert.assertTrue(lanceSplit.getIndexSegmentUuids().isEmpty());
        Assert.assertEquals(targetRows, lanceSplit.getTargetSplitSize().longValue());
        Assert.assertEquals(weight, lanceSplit.getSplitWeight().getRawValue());
    }

    private static void assertIndexSplit(Split split, UUID segmentUuid, List<Long> fragmentIds,
            long targetRows, long weight) {
        LanceSplit lanceSplit = (LanceSplit) split;
        Assert.assertEquals(fragmentIds, lanceSplit.getFragmentIds());
        Assert.assertEquals(Collections.singletonList(segmentUuid), lanceSplit.getIndexSegmentUuids());
        Assert.assertEquals(targetRows, lanceSplit.getTargetSplitSize().longValue());
        Assert.assertEquals(weight, lanceSplit.getSplitWeight().getRawValue());
    }

    private static Schema vectorSchema() {
        return new Schema(Collections.singletonList(
                Field.nullable("vector", ArrowType.Utf8.INSTANCE)));
    }

    private static void assertInvalidSplit(Runnable action, String expectedMessage) {
        try {
            action.run();
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedMessage, e.getMessage());
        }
    }

    private static TExternalSearchRequest vectorSearchRequest(long topK, long offset) {
        TVectorSearchParams vector = new TVectorSearchParams()
                .setColumn("vector")
                .setTopK(topK)
                .setOffset(offset);
        return new TExternalSearchRequest()
                .setSearchQuery(TExternalSearchQuery.vector_search(vector));
    }
}
