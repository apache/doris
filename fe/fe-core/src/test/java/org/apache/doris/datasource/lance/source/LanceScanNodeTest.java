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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
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
import org.apache.doris.thrift.TFtsCoverageMode;
import org.apache.doris.thrift.TFtsMatchOperator;
import org.apache.doris.thrift.TFtsQueryType;
import org.apache.doris.thrift.TFullTextSearchParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TVectorMetric;
import org.apache.doris.thrift.TVectorSearchOptions;
import org.apache.doris.thrift.TVectorSearchParams;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.lance.index.IndexType;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class LanceScanNodeTest {

    @Test
    public void testLabelListSegmentsForAlreadyPushedArrayFilter() {
        UUID first = UUID.fromString("11111111-2222-3333-4444-555555555555");
        UUID second = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/labels.lance", 42,
                new Schema(Collections.singletonList(new Field("labels",
                        FieldType.nullable(ArrowType.List.INSTANCE),
                        Collections.singletonList(Field.nullable("item", new ArrowType.Int(64, true)))))),
                Arrays.asList(new LanceFragmentInfo(1, 10, 10), new LanceFragmentInfo(2, 20, 20),
                        new LanceFragmentInfo(3, 30, 30)),
                Collections.singletonMap("labels", 9),
                Arrays.asList(scalarSegment(first, IndexType.LABEL_LIST, Collections.singletonList(1L)),
                        scalarSegment(second, IndexType.LABEL_LIST, Collections.singletonList(2L))),
                Collections.emptyMap());
        Map<Long, LanceFragmentInfo> fragments = new LinkedHashMap<>();
        metadata.getFragments().forEach(fragment -> fragments.put(fragment.getId(), fragment));
        // This checks the planner's pushed-predicate contract. Array predicate conversion
        // remains a separate prerequisite for ordinary SQL to select a LabelList segment.
        Expr filter = new FunctionCallExpr("array_contains",
                Arrays.asList(new SlotRef(null, "labels"), new IntLiteral(42)));
        LanceScalarIndexPlanner.Plan plan =
                LanceScalarIndexPlanner.plan(metadata, Collections.singletonList(filter), fragments);
        Assert.assertNotNull(plan);
        Assert.assertEquals("key_idx", plan.indexName);
        plan.splits.addUncoveredFragments(fragments.values(), 1);
        List<Split> splits = plan.splits.buildFragmentSplits(20, fragments);
        Assert.assertEquals(3, splits.size());
        Assert.assertEquals(Collections.singletonList(first), ((LanceSplit) splits.get(0)).getIndexSegmentUuids());
        Assert.assertEquals(Collections.singletonList(second), ((LanceSplit) splits.get(1)).getIndexSegmentUuids());
        Assert.assertEquals(Collections.singletonList(3L), ((LanceSplit) splits.get(2)).getFragmentIds());
        Assert.assertFalse(((LanceSplit) splits.get(2)).hasIndexSegmentUuids());
    }

    @Test
    public void testScalarSegmentsKeepSingleOwnerAndSerializeFallback() throws Exception {
        for (IndexType type : Arrays.asList(IndexType.BTREE, IndexType.BITMAP)) {
            LanceScanNode node = newNode();
            UUID first = UUID.fromString("11111111-2222-3333-4444-555555555555");
            UUID second = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
            setMetadata(node, scalarMetadata(Arrays.asList(
                    scalarSegment(first, type, Arrays.asList(1L, 2L)),
                    scalarSegment(second, type, Collections.singletonList(3L)))));
            setPushedConjuncts(node, scalarPredicate());
            node.setLimit(5);

            // More BEs than segments must not cause repeated segment searches.
            List<Split> splits = node.getSplits(20);
            Assert.assertEquals(3, splits.size());
            Assert.assertEquals(Arrays.asList(1L, 2L), ((LanceSplit) splits.get(0)).getFragmentIds());
            Assert.assertEquals(Collections.singletonList(3L), ((LanceSplit) splits.get(1)).getFragmentIds());
            Assert.assertEquals(Collections.singletonList(4L), ((LanceSplit) splits.get(2)).getFragmentIds());
            for (int i = 0; i < splits.size(); i++) {
                TFileRangeDesc range = new TFileRangeDesc();
                node.setScanParams(range, splits.get(i));
                TLanceFileDesc params = range.getTableFormatParams().getLanceParams();
                Assert.assertEquals(42L, params.getVersion());
                Assert.assertEquals(5L, params.getLimit());
                if (i < 2) {
                    ByteBuffer uuid = params.getIndexSegmentUuids().get(0).duplicate();
                    Assert.assertEquals(i == 0 ? first : second, new UUID(uuid.getLong(), uuid.getLong()));
                    Assert.assertFalse(params.isSetUseScalarIndex());
                } else {
                    Assert.assertFalse(params.isSetIndexSegmentUuids());
                    Assert.assertTrue(params.isSetUseScalarIndex());
                    Assert.assertFalse(params.isUseScalarIndex());
                }
            }
        }
    }

    @Test
    public void testScalarSegmentPlanRejectsUnsafeCoverageAndUnsupportedTypes() throws Exception {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        List<List<LanceIndexSegmentInfo>> candidates = Arrays.asList(
                Collections.singletonList(scalarSegment(first, IndexType.BTREE, null)),
                Arrays.asList(scalarSegment(first, IndexType.BTREE, Arrays.asList(1L, 2L)),
                        scalarSegment(second, IndexType.BTREE, Arrays.asList(2L, 3L))),
                Collections.singletonList(scalarSegment(first, IndexType.INVERTED, Arrays.asList(1L, 2L))),
                Collections.singletonList(scalarSegment(first, IndexType.VECTOR, Arrays.asList(1L, 2L))));
        for (List<LanceIndexSegmentInfo> segments : candidates) {
            LanceScanNode node = newNode();
            setMetadata(node, scalarMetadata(segments));
            setPushedConjuncts(node, scalarPredicate());
            List<Split> splits = node.getSplits(20);
            Assert.assertEquals(4, splits.size());
            for (Split split : splits) {
                TFileRangeDesc range = new TFileRangeDesc();
                node.setScanParams(range, split);
                Assert.assertFalse(range.getTableFormatParams().getLanceParams().isSetIndexSegmentUuids());
                Assert.assertFalse(range.getTableFormatParams().getLanceParams().isSetUseScalarIndex());
            }
        }
    }

    @Test
    public void testScalarSegmentSelectionDoesNotDescendThroughOrOrNot() throws Exception {
        Expr predicate = scalarPredicate();
        for (Expr filter : Arrays.asList(
                new CompoundPredicate(CompoundPredicate.Operator.OR, predicate, predicate),
                new CompoundPredicate(CompoundPredicate.Operator.NOT, predicate, null))) {
            LanceScanNode node = newNode();
            setMetadata(node, scalarMetadata(Collections.singletonList(
                    scalarSegment(UUID.randomUUID(), IndexType.BTREE, Arrays.asList(1L, 2L, 3L, 4L)))));
            setPushedConjuncts(node, filter);
            Assert.assertEquals(4, node.getSplits(20).size());
            setPushedConjuncts(node, new CompoundPredicate(CompoundPredicate.Operator.AND, filter, predicate));
            List<Split> splits = node.getSplits(20);
            Assert.assertEquals(1, splits.size());
            Assert.assertTrue(((LanceSplit) splits.get(0)).hasIndexSegmentUuids());
        }
    }

    @Test
    public void testDebugFragmentGroupingBypassesScalarSegments() throws Exception {
        SessionVariable session = new SessionVariable();
        session.lanceFragmentsPerSplit = 1;
        LanceScanNode node = newNode(session);
        setMetadata(node, scalarMetadata(Collections.singletonList(
                scalarSegment(UUID.randomUUID(), IndexType.BTREE, Arrays.asList(1L, 2L, 3L, 4L)))));
        setPushedConjuncts(node, scalarPredicate());
        List<Split> splits = node.getSplits(20);
        Assert.assertEquals(4, splits.size());
        for (Split split : splits) {
            Assert.assertFalse(((LanceSplit) split).hasIndexSegmentUuids());
        }
    }

    @Test
    public void testScalarSegmentDoesNotPushLimitPastDorisResidual() throws Exception {
        LanceScanNode node = newNode();
        setMetadata(node, scalarMetadata(Collections.singletonList(
                scalarSegment(UUID.randomUUID(), IndexType.BTREE, Arrays.asList(1L, 2L, 3L, 4L)))));
        node.getConjuncts().add(scalarPredicate());
        node.getConjuncts().add(new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new FunctionCallExpr("abs", Collections.singletonList(new SlotRef(null, "key"))),
                new IntLiteral(2)));
        node.convertPredicate();
        node.setLimit(1);
        List<Split> splits = node.getSplits(20);
        Assert.assertEquals(1, splits.size());
        Assert.assertEquals(1, node.getConjuncts().size());
        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, splits.get(0));
        Assert.assertTrue(range.getTableFormatParams().getLanceParams().isSetIndexSegmentUuids());
        Assert.assertFalse(range.getTableFormatParams().getLanceParams().isSetLimit());
    }

    private static Expr scalarPredicate() {
        return new BinaryPredicate(BinaryPredicate.Operator.GE, new SlotRef(null, "key"), new IntLiteral(2));
    }

    private static LanceIndexSegmentInfo scalarSegment(UUID uuid, IndexType type, List<Long> fragments) {
        return new LanceIndexSegmentInfo(uuid, "key_idx", Collections.singletonList(9), fragments, type, null);
    }

    private static LanceTableMetadata scalarMetadata(List<LanceIndexSegmentInfo> segments) {
        return LanceTableMetadata.withIndexSegments("s3://bucket/scalar.lance", 42,
                new Schema(Collections.singletonList(Field.nullable("key", new ArrowType.Int(64, true)))),
                Arrays.asList(new LanceFragmentInfo(1, 10, 12), new LanceFragmentInfo(2, 20, 20),
                        new LanceFragmentInfo(3, 30, 30), new LanceFragmentInfo(4, 40, 40)),
                Collections.singletonMap("key", 9), segments, Collections.emptyMap());
    }

    private static void setPushedConjuncts(LanceScanNode node, Expr predicate) {
        node.getConjuncts().clear();
        node.getConjuncts().add(predicate);
        node.convertPredicate();
        Assert.assertTrue(node.getConjuncts().isEmpty());
    }

    @Test
    public void testGroupedFragmentsPreserveCoverageWeightsAndScanParams() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.lanceFragmentsPerSplit = 2;
        LanceScanNode node = newNode(sessionVariable);
        setMetadata(node, LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance", 42, new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(7, 10, 1000),
                        new LanceFragmentInfo(11, 250, 250),
                        new LanceFragmentInfo(13, 0, 0),
                        new LanceFragmentInfo(17, 499, 499),
                        new LanceFragmentInfo(23, 125, 125)),
                Collections.emptyMap()));

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(3, splits.size());
        List<List<Long>> expectedIds = Arrays.asList(Arrays.asList(7L, 11L),
                Arrays.asList(13L, 17L), Collections.singletonList(23L));
        long[] expectedRows = {1250, 500, 125};
        long[] expectedWeights = {100, 40, 10};
        for (int i = 0; i < splits.size(); i++) {
            LanceSplit split = (LanceSplit) splits.get(i);
            Assert.assertEquals(expectedIds.get(i), split.getFragmentIds());
            Assert.assertEquals(expectedRows[i], split.getSelfSplitWeight());
            Assert.assertEquals(1250L, split.getTargetSplitSize().longValue());
            Assert.assertEquals(expectedWeights[i], split.getSplitWeight().getRawValue());
            TFileRangeDesc range = new TFileRangeDesc();
            node.setScanParams(range, split);
            Assert.assertEquals(expectedIds.get(i), range.getTableFormatParams().getLanceParams().getFragmentIds());
            Assert.assertEquals(42L, range.getTableFormatParams().getLanceParams().getVersion());
            Assert.assertEquals("s3://bucket/table.lance",
                    range.getTableFormatParams().getLanceParams().getDatasetUri());
            Assert.assertFalse(range.getTableFormatParams().getLanceParams().isSetIndexSegmentUuids());
            Assert.assertFalse(range.getTableFormatParams().getLanceParams().isSetLimit());
        }

        node.setLimit(10);
        TFileRangeDesc limitedRange = new TFileRangeDesc();
        node.setScanParams(limitedRange, splits.get(0));
        Assert.assertEquals(10L, limitedRange.getTableFormatParams().getLanceParams().getLimit());
    }

    @Test
    public void testFragmentGroupLargerThanDatasetAndEmptyDataset() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.lanceFragmentsPerSplit = Integer.MAX_VALUE;
        LanceScanNode node = newNode(sessionVariable);
        setMetadata(node, LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance", 42, new Schema(Collections.emptyList()),
                Arrays.asList(new LanceFragmentInfo(7, 0, 0), new LanceFragmentInfo(11, 0, 0)),
                Collections.emptyMap()));

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(1, splits.size());
        Assert.assertEquals(Arrays.asList(7L, 11L), ((LanceSplit) splits.get(0)).getFragmentIds());
        Assert.assertEquals(2L, ((LanceSplit) splits.get(0)).getSelfSplitWeight());
        Assert.assertEquals(100L, splits.get(0).getSplitWeight().getRawValue());

        setMetadata(node, LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance", 43, new Schema(Collections.emptyList()),
                Collections.emptyList(), Collections.emptyMap()));
        Assert.assertTrue(node.getSplits(2).isEmpty());
    }

    @Test
    public void testFragmentGroupSizeValidation() {
        SessionVariable sessionVariable = new SessionVariable();
        Assert.assertEquals(0, sessionVariable.lanceFragmentsPerSplit);
        sessionVariable.checkLanceFragmentsPerSplit("0");
        sessionVariable.checkLanceFragmentsPerSplit("1");
        sessionVariable.checkLanceFragmentsPerSplit("8");
        Assert.assertThrows(InvalidParameterException.class,
                () -> sessionVariable.checkLanceFragmentsPerSplit("-1"));
    }

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
    public void testCountSplitsPinVersionAndKeepFallbackRangesDisjoint() throws Exception {
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance",
                42,
                new Schema(Collections.emptyList()),
                Arrays.asList(
                        new LanceFragmentInfo(7, 6000, 6001),
                        new LanceFragmentInfo(11, 5000, 5001),
                        new LanceFragmentInfo(13, 4000, 4001)),
                Collections.emptyMap());
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.parallelExecInstanceNum = 1;
        LanceScanNode node = newNode(sessionVariable);
        setMetadata(node, metadata);
        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.emptyList());

        List<Split> splits = node.getSplits(2);

        Assert.assertEquals(2, splits.size());
        assertCountRange(node, splits.get(0), Arrays.asList(7L, 13L), 10_000);
        assertCountRange(node, splits.get(1), Collections.singletonList(11L), 5_000);
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
    public void testFullTextSearchUsesOneSplitPerCommittedIndexSegment() throws Exception {
        UUID firstSegment = UUID.fromString("11111111-2222-3333-4444-555555555555");
        UUID secondSegment = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/table.lance",
                42,
                fullTextSchema(),
                Arrays.asList(
                        new LanceFragmentInfo(1, 8, 8),
                        new LanceFragmentInfo(2, 7, 7),
                        new LanceFragmentInfo(3, 6, 6)),
                Collections.singletonMap("body", 7),
                Arrays.asList(
                        new LanceIndexSegmentInfo(firstSegment, "body_fts",
                                Collections.singletonList(7), Arrays.asList(1L, 2L),
                                IndexType.INVERTED, null),
                        new LanceIndexSegmentInfo(secondSegment, "body_fts",
                                Collections.singletonList(7), Collections.singletonList(3L),
                                IndexType.INVERTED, null)),
                Collections.emptyMap());
        TExternalSearchRequest request = fullTextSearchRequest(
                5, 2, TFtsCoverageMode.STRICT);
        LanceScanNode node = newSearchNode(metadata, request);

        List<Split> splits = node.getSplits(8);

        Assert.assertEquals(2, splits.size());
        assertIndexSplit(splits.get(0), firstSegment, Arrays.asList(1L, 2L), 15, 100);
        assertIndexSplit(splits.get(1), secondSegment, Collections.singletonList(3L), 15, 40);

        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, splits.get(1));
        Assert.assertEquals(Collections.singletonList(3L), range.getTableFormatParams()
                .getLanceParams().getFragmentIds());
        ByteBuffer encodedUuid = range.getTableFormatParams().getLanceParams()
                .getIndexSegmentUuids().get(0).duplicate();
        Assert.assertEquals(secondSegment.getMostSignificantBits(), encodedUuid.getLong());
        Assert.assertEquals(secondSegment.getLeastSignificantBits(), encodedUuid.getLong());

        TExternalSearchRequest splitRequest = node.createSplitSearchRequest();
        Assert.assertEquals(7,
                splitRequest.getSearchQuery().getFullTextSearch().getTopK());
        Assert.assertEquals(0,
                splitRequest.getSearchQuery().getFullTextSearch().getOffset());
        Assert.assertEquals(5, request.getSearchQuery().getFullTextSearch().getTopK());
        Assert.assertEquals(2, request.getSearchQuery().getFullTextSearch().getOffset());
        Assert.assertEquals(TFtsQueryType.MATCH,
                splitRequest.getSearchQuery().getFullTextSearch().getQueryType());
        Assert.assertEquals(TFtsMatchOperator.OR,
                splitRequest.getSearchQuery().getFullTextSearch().getMatchOperator());
        Assert.assertEquals(0,
                splitRequest.getSearchQuery().getFullTextSearch().getMaxFuzzyDistance());
    }

    @Test
    public void testFullTextSplitRequestPreservesPhraseQuery() {
        TExternalSearchRequest logicalRequest = phraseSearchRequest(5, 2, 1);
        LanceScanNode node = LanceScanNode.forExternalSearch(
                new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), null,
                null, -1, logicalRequest, new SessionVariable());

        TExternalSearchRequest splitRequest = node.createSplitSearchRequest();

        TFullTextSearchParams splitFullText =
                splitRequest.getSearchQuery().getFullTextSearch();
        Assert.assertEquals(7, splitFullText.getTopK());
        Assert.assertEquals(0, splitFullText.getOffset());
        Assert.assertEquals(TFtsQueryType.PHRASE, splitFullText.getQueryType());
        Assert.assertEquals(1, splitFullText.getPhraseSlop());
        Assert.assertFalse(splitFullText.isSetMatchOperator());
        Assert.assertFalse(splitFullText.isSetMaxFuzzyDistance());

        TFullTextSearchParams logicalFullText =
                logicalRequest.getSearchQuery().getFullTextSearch();
        Assert.assertEquals(5, logicalFullText.getTopK());
        Assert.assertEquals(2, logicalFullText.getOffset());
    }

    @Test
    public void testFullTextSearchCoverageModesHandleUnindexedFragments() throws Exception {
        UUID segment = UUID.fromString("11111111-2222-3333-4444-555555555555");
        LanceTableMetadata metadata = LanceTableMetadata.withIndexSegments(
                "s3://bucket/table.lance",
                42,
                fullTextSchema(),
                Arrays.asList(
                        new LanceFragmentInfo(1, 8, 8),
                        new LanceFragmentInfo(2, 7, 7),
                        new LanceFragmentInfo(3, 6, 6)),
                Collections.singletonMap("body", 7),
                Collections.singletonList(
                        new LanceIndexSegmentInfo(segment, "body_fts",
                                Collections.singletonList(7), Arrays.asList(1L, 2L),
                                IndexType.INVERTED, null)),
                Collections.emptyMap());

        LanceScanNode strictNode = newSearchNode(metadata,
                fullTextSearchRequest(10, 0, TFtsCoverageMode.STRICT));
        UserException strictFailure = Assert.assertThrows(UserException.class,
                () -> strictNode.getSplits(2));
        Assert.assertTrue(strictFailure.getMessage().contains("1 unindexed fragments"));

        LanceScanNode indexOnlyNode = newSearchNode(metadata,
                fullTextSearchRequest(10, 0, TFtsCoverageMode.INDEX_ONLY));
        List<Split> indexOnlySplits = indexOnlyNode.getSplits(2);
        Assert.assertEquals(1, indexOnlySplits.size());
        assertIndexSplit(indexOnlySplits.get(0), segment, Arrays.asList(1L, 2L), 15, 100);
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
        assertInvalidSplit(() -> LanceSplit.forFragments("s3://bucket/table.lance", 42,
                Collections.emptyList(), 1), "Lance fragment split must contain fragments");
        assertInvalidSplit(() -> LanceSplit.forFragment("", 42, 1, 1),
                "Lance dataset URI must not be empty");
        assertInvalidSplit(() -> LanceSplit.forFragment("s3://bucket/table.lance", -1, 1, 1),
                "Lance dataset version must be non-negative");
        assertInvalidSplit(() -> LanceSplit.forFragment("s3://bucket/table.lance", 42, -1, 1),
                "Lance fragment id must be non-negative");
    }

    private static LanceScanNode newNode() {
        return newNode(new SessionVariable());
    }

    private static LanceScanNode newNode(SessionVariable sessionVariable) {
        return new LanceScanNode(
                new PlanNodeId(0),
                new TupleDescriptor(new TupleId(0)),
                false,
                sessionVariable,
                ScanContext.EMPTY);
    }

    private static LanceScanNode newSearchNode(
            LanceTableMetadata metadata, TExternalSearchRequest request) {
        String searchColumn = request.getSearchQuery().isSetVectorSearch()
                ? request.getSearchQuery().getVectorSearch().getColumn()
                : request.getSearchQuery().getFullTextSearch().getColumn();
        int searchFieldId = metadata.getLanceFieldId(searchColumn).orElse(-1);
        // Ordinary-scan grouping must not change any vector or full-text split expectations.
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.lanceFragmentsPerSplit = 8;
        return LanceScanNode.forExternalSearch(
                new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), null,
                metadata, searchFieldId, request, sessionVariable);
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

    private static void assertCountRange(
            LanceScanNode node, Split split, List<Long> fragmentIds, long rowCount) {
        LanceSplit lanceSplit = (LanceSplit) split;
        Assert.assertEquals(fragmentIds, lanceSplit.getFragmentIds());
        Assert.assertEquals(rowCount, lanceSplit.getTableLevelRowCount());

        TFileRangeDesc range = new TFileRangeDesc();
        node.setScanParams(range, split);
        Assert.assertEquals(42L, range.getTableFormatParams().getLanceParams().getVersion());
        Assert.assertEquals(fragmentIds,
                range.getTableFormatParams().getLanceParams().getFragmentIds());
        Assert.assertEquals(rowCount, range.getTableFormatParams().getTableLevelRowCount());
    }

    private static Schema vectorSchema() {
        return new Schema(Collections.singletonList(
                Field.nullable("vector", ArrowType.Utf8.INSTANCE)));
    }

    private static Schema fullTextSchema() {
        return new Schema(Collections.singletonList(
                Field.nullable("body", ArrowType.Utf8.INSTANCE)));
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

    private static TExternalSearchRequest fullTextSearchRequest(
            long topK, long offset, TFtsCoverageMode coverageMode) {
        TFullTextSearchParams fullText = new TFullTextSearchParams()
                .setColumn("body")
                .setQuery("lance")
                .setTopK(topK)
                .setOffset(offset)
                .setCoverageMode(coverageMode)
                .setQueryType(TFtsQueryType.MATCH)
                .setMatchOperator(TFtsMatchOperator.OR)
                .setMaxFuzzyDistance(0);
        return new TExternalSearchRequest()
                .setSearchQuery(TExternalSearchQuery.full_text_search(fullText));
    }

    private static TExternalSearchRequest phraseSearchRequest(long topK, long offset, int slop) {
        TFullTextSearchParams fullText = new TFullTextSearchParams()
                .setColumn("body")
                .setQuery("lance search")
                .setTopK(topK)
                .setOffset(offset)
                .setCoverageMode(TFtsCoverageMode.STRICT)
                .setQueryType(TFtsQueryType.PHRASE)
                .setPhraseSlop(slop);
        return new TExternalSearchRequest()
                .setSearchQuery(TExternalSearchQuery.full_text_search(fullText));
    }
}
