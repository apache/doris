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

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorFunctionCall;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.pushdown.ConnectorNot;
import org.apache.doris.connector.spi.pushdown.ConnectorOr;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The ranges of ordinary scans: index segments, uncovered fragments, debug grouping, weights and COUNT carriers.
 * These are the cases of branch-4.2's LanceScanNodeTest that do not search; the engine-side parts of those
 * cases (residual conjuncts, the session variable's checker) are tested where master keeps them.
 */
class LanceScanPlannerTest {
    private static final UUID FIRST = UUID.fromString("11111111-2222-3333-4444-555555555555");
    private static final UUID SECOND = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

    @Test
    void labelListSegmentsForAlreadyPushedArrayFilter() {
        // The planner's pushed-predicate contract: the converter decides what is pushed (it would not push an
        // integer needle), and the planner only plans the ranges.
        Schema schema = new Schema(Collections.singletonList(new Field("labels", FieldType.nullable(ArrowType.List.INSTANCE),
                Collections.singletonList(Field.nullable("item", new ArrowType.Int(64, true))))));
        List<LanceFragmentInfo> fragments = Arrays.asList(new LanceFragmentInfo(1, 10, 10),
                new LanceFragmentInfo(2, 20, 20), new LanceFragmentInfo(3, 30, 30));
        LanceTableMetadata metadata = metadata("s3://bucket/labels.lance", schema, fragments);
        LanceIndexMetadata indexes = new LanceIndexMetadata(Collections.singletonMap("labels", 9), Arrays.asList(
                segment(FIRST, IndexType.LABEL_LIST, Collections.singletonList(1L)),
                segment(SECOND, IndexType.LABEL_LIST, Collections.singletonList(2L))));
        ConnectorExpression filter = new ConnectorFunctionCall("array_contains", ConnectorType.of("BOOLEAN"),
                Arrays.asList(new ConnectorColumnRef("labels", ConnectorType.arrayOf(ConnectorType.of("BIGINT"))),
                        ConnectorLiteral.ofLong(42)));
        Map<Long, LanceFragmentInfo> visible = new LinkedHashMap<>();
        fragments.forEach(fragment -> visible.put(fragment.getId(), fragment));
        LanceScalarIndexPlanner.Plan plan = LanceScalarIndexPlanner.plan(metadata, indexes,
                new LancePredicateConverter(schema), Collections.singletonList(filter), visible);
        Assertions.assertNotNull(plan);
        Assertions.assertEquals("key_idx", plan.indexName);
        plan.splits.addUncoveredFragments(fragments, 1, true);
        List<ConnectorScanRange> ranges = plan.splits.buildRanges(-1);
        Assertions.assertEquals(3, ranges.size());
        Assertions.assertEquals(FIRST, lance(ranges.get(0)).getIndexSegmentUuid().get());
        Assertions.assertEquals(SECOND, lance(ranges.get(1)).getIndexSegmentUuid().get());
        Assertions.assertEquals(Collections.singletonList(3L), lance(ranges.get(2)).getFragmentIds());
        Assertions.assertFalse(lance(ranges.get(2)).getIndexSegmentUuid().isPresent());
    }

    @Test
    void scalarSegmentsKeepSingleOwnerAndSerializeFallback() {
        for (IndexType type : Arrays.asList(IndexType.BTREE, IndexType.BITMAP)) {
            List<ConnectorScanRange> ranges = plan(indexes(segment(FIRST, type, Arrays.asList(1L, 2L)),
                    segment(SECOND, type, Collections.singletonList(3L))), scalarPredicate(), 0, 5);
            // One range per segment however many backends there are, so no segment is searched twice.
            Assertions.assertEquals(3, ranges.size());
            Assertions.assertEquals(Arrays.asList(1L, 2L), lance(ranges.get(0)).getFragmentIds());
            Assertions.assertEquals(Collections.singletonList(3L), lance(ranges.get(1)).getFragmentIds());
            Assertions.assertEquals(Collections.singletonList(4L), lance(ranges.get(2)).getFragmentIds());
            for (int i = 0; i < ranges.size(); i++) {
                TLanceFileDesc params = params(ranges.get(i));
                Assertions.assertEquals(42L, params.getVersion());
                Assertions.assertEquals(5L, params.getLimit());
                if (i < 2) {
                    ByteBuffer uuid = params.getIndexSegmentUuids().get(0).duplicate();
                    Assertions.assertEquals(i == 0 ? FIRST : SECOND, new UUID(uuid.getLong(), uuid.getLong()));
                    Assertions.assertFalse(params.isSetUseScalarIndex());
                } else {
                    Assertions.assertFalse(params.isSetIndexSegmentUuids());
                    Assertions.assertTrue(params.isSetUseScalarIndex());
                    Assertions.assertFalse(params.isUseScalarIndex());
                }
            }
        }
    }

    @Test
    void scalarSegmentPlanRejectsUnsafeCoverageAndUnsupportedTypes() {
        for (LanceIndexMetadata indexes : Arrays.asList(
                indexes(segment(FIRST, IndexType.BTREE, null)),
                indexes(segment(FIRST, IndexType.BTREE, Arrays.asList(1L, 2L)),
                        segment(SECOND, IndexType.BTREE, Arrays.asList(2L, 3L))),
                indexes(segment(FIRST, IndexType.INVERTED, Arrays.asList(1L, 2L))),
                indexes(segment(FIRST, IndexType.VECTOR, Arrays.asList(1L, 2L))))) {
            assertNativeFragmentSelection(indexes, scalarPredicate());
        }
    }

    @Test
    void scalarSegmentSelectionSupportsSameColumnOrAndNot() {
        ConnectorExpression predicate = scalarPredicate();
        LanceIndexMetadata indexes = indexes(segment(FIRST, IndexType.BTREE, Arrays.asList(1L, 2L, 3L, 4L)));
        Assertions.assertEquals(1, plan(indexes, new ConnectorOr(Arrays.asList(predicate, predicate)), 0, -1).size());
        // A complement cannot drive the index: every fragment scans on its own, with the scalar index off.
        List<ConnectorScanRange> complement = plan(indexes, new ConnectorNot(predicate), 0, -1);
        Assertions.assertEquals(4, complement.size());
        for (ConnectorScanRange range : complement) {
            TLanceFileDesc params = params(range);
            Assertions.assertFalse(params.isSetIndexSegmentUuids());
            Assertions.assertTrue(params.isSetUseScalarIndex());
            Assertions.assertFalse(params.isUseScalarIndex());
        }
        for (ConnectorExpression filter : Arrays.asList(new ConnectorOr(Arrays.asList(predicate, predicate)),
                new ConnectorNot(predicate))) {
            List<ConnectorScanRange> ranges = plan(indexes, new ConnectorAnd(Arrays.asList(filter, predicate)), 0, -1);
            Assertions.assertEquals(1, ranges.size());
            Assertions.assertTrue(lance(ranges.get(0)).getIndexSegmentUuid().isPresent());
            Assertions.assertEquals(Arrays.asList(1L, 2L, 3L, 4L), lance(ranges.get(0)).getFragmentIds());
        }
    }

    @Test
    void debugFragmentGroupingBypassesScalarSegments() {
        List<ConnectorScanRange> ranges = LanceScanPlanner.plan(scalarMetadata(),
                new LancePredicateConverter(scalarMetadata().getSchema()), Collections.singletonList(scalarPredicate()),
                1, () -> {
                    throw new AssertionError("debug grouping must not read index metadata");
                }).ranges(-1);
        Assertions.assertEquals(4, ranges.size());
        for (ConnectorScanRange range : ranges) {
            Assertions.assertFalse(lance(range).getIndexSegmentUuid().isPresent());
        }
    }

    @Test
    void scalarSegmentCarriesLimitOnlyWhenGivenOne() {
        // The provider passes no limit when a conjunct stays in Doris (LanceScanPlanProviderTest
        // limitReachesEveryRangeOnlyWhenEveryConjunctIsPushed); a segment range then carries none.
        LanceIndexMetadata indexes = indexes(segment(FIRST, IndexType.BTREE, Arrays.asList(1L, 2L, 3L, 4L)));
        List<ConnectorScanRange> ranges = plan(indexes, scalarPredicate(), 0, -1);
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertTrue(params(ranges.get(0)).isSetIndexSegmentUuids());
        Assertions.assertFalse(params(ranges.get(0)).isSetLimit());
        Assertions.assertEquals(1L, params(plan(indexes, scalarPredicate(), 0, 1).get(0)).getLimit());
    }

    @Test
    void missingSegmentMetadataPreservesNativeIndexSelection() {
        ConnectorExpression keyIsOneOrTwo = new ConnectorOr(Arrays.asList(keyEquals(1), keyEquals(2)));
        List<LanceIndexSegmentInfo> segments = Collections.singletonList(
                segment(FIRST, IndexType.BTREE, Arrays.asList(1L, 2L, 3L, 4L)));
        assertNativeFragmentSelection(LanceIndexMetadata.withUnavailableFieldIds(segments), keyIsOneOrTwo);
        assertNativeFragmentSelection(new LanceIndexMetadata(Collections.singletonMap("key", 9),
                Collections.emptyList()), keyIsOneOrTwo);
    }

    @Test
    void competingLogicalIndexesPreserveNativeSelection() {
        List<LanceIndexSegmentInfo> segments = new ArrayList<>(Arrays.asList(
                new LanceIndexSegmentInfo(FIRST, "a_key_idx", Collections.singletonList(9),
                        Collections.singletonList(1L), IndexType.BTREE),
                new LanceIndexSegmentInfo(SECOND, "z_key_idx", Collections.singletonList(9),
                        Arrays.asList(1L, 2L, 3L, 4L), IndexType.BTREE)));
        ConnectorExpression keyIsOneOrTwo = new ConnectorOr(Arrays.asList(keyEquals(1), keyEquals(2)));
        assertNativeFragmentSelection(new LanceIndexMetadata(Collections.singletonMap("key", 9), segments),
                keyIsOneOrTwo);
        Collections.reverse(segments);
        assertNativeFragmentSelection(new LanceIndexMetadata(Collections.singletonMap("key", 9), segments),
                keyIsOneOrTwo);
    }

    @Test
    void groupedFragmentsPreserveCoverageWeightsAndScanParams() {
        LanceTableMetadata metadata = metadata("s3://bucket/table.lance", new Schema(Collections.emptyList()),
                Arrays.asList(new LanceFragmentInfo(7, 10, 1000), new LanceFragmentInfo(11, 250, 250),
                        new LanceFragmentInfo(13, 0, 0), new LanceFragmentInfo(17, 499, 499),
                        new LanceFragmentInfo(23, 125, 125)));
        LanceScanPlan plan = LanceScanPlanner.plan(metadata, new LancePredicateConverter(metadata.getSchema()),
                Collections.emptyList(), 2, () -> {
                    throw new AssertionError("debug grouping must not read index metadata");
                });
        List<ConnectorScanRange> ranges = plan.ranges(-1);
        Assertions.assertEquals(3, ranges.size());
        List<List<Long>> expectedIds = Arrays.asList(Arrays.asList(7L, 11L), Arrays.asList(13L, 17L),
                Collections.singletonList(23L));
        long[] expectedRows = {1250, 500, 125};
        for (int i = 0; i < ranges.size(); i++) {
            ConnectorScanRange range = ranges.get(i);
            Assertions.assertEquals(expectedIds.get(i), lance(range).getFragmentIds());
            Assertions.assertEquals(expectedRows[i], range.getSelfSplitWeight());
            Assertions.assertEquals(1250L, range.getTargetSplitSize());
            TLanceFileDesc params = params(range);
            Assertions.assertEquals(expectedIds.get(i), params.getFragmentIds());
            Assertions.assertEquals(42L, params.getVersion());
            Assertions.assertEquals("s3://bucket/table.lance", params.getDatasetUri());
            Assertions.assertFalse(params.isSetIndexSegmentUuids());
            Assertions.assertFalse(params.isSetLimit());
        }
        Assertions.assertEquals(10L, params(plan.ranges(10).get(0)).getLimit());
    }

    @Test
    void fragmentGroupLargerThanDatasetAndEmptyDataset() {
        LanceTableMetadata metadata = metadata("s3://bucket/table.lance", new Schema(Collections.emptyList()),
                Arrays.asList(new LanceFragmentInfo(7, 0, 0), new LanceFragmentInfo(11, 0, 0)));
        List<ConnectorScanRange> ranges = LanceScanPlanner.plan(metadata,
                new LancePredicateConverter(metadata.getSchema()), Collections.emptyList(), Integer.MAX_VALUE,
                () -> null).ranges(-1);
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals(Arrays.asList(7L, 11L), lance(ranges.get(0)).getFragmentIds());
        Assertions.assertEquals(2L, ranges.get(0).getSelfSplitWeight());
        LanceTableMetadata empty = metadata("s3://bucket/table.lance", new Schema(Collections.emptyList()),
                Collections.emptyList());
        Assertions.assertTrue(LanceScanPlanner.plan(empty, new LancePredicateConverter(empty.getSchema()),
                Collections.emptyList(), 2, () -> null).ranges(-1).isEmpty());
    }

    @Test
    void fragmentRowsDetermineSplitWeights() {
        List<ConnectorScanRange> ranges = plainRanges(new LanceFragmentInfo(7, 1000, 1000),
                new LanceFragmentInfo(11, 250, 250), new LanceFragmentInfo(13, 0, 0));
        Assertions.assertEquals(3, ranges.size());
        long[] expectedWeights = {1000, 250, 1};
        for (int i = 0; i < ranges.size(); i++) {
            Assertions.assertEquals(expectedWeights[i], ranges.get(i).getSelfSplitWeight());
            Assertions.assertEquals(1000L, ranges.get(i).getTargetSplitSize());
        }
    }

    @Test
    void deletionHeavyFragmentKeepsPhysicalScanWeight() {
        // Both fragments read 1000 physical rows, but one has 990 tombstones. The BE still scans every physical
        // row, so both keep the standard weight rather than the tombstone-heavy one sinking to the minimum.
        List<ConnectorScanRange> ranges = plainRanges(new LanceFragmentInfo(7, 1000, 1000),
                new LanceFragmentInfo(11, 10, 1000));
        Assertions.assertEquals(2, ranges.size());
        for (ConnectorScanRange range : ranges) {
            Assertions.assertEquals(1000L, range.getSelfSplitWeight());
            Assertions.assertEquals(1000L, range.getTargetSplitSize());
        }
    }

    @Test
    void countRangesPinVersionAndKeepFallbackRangesDisjoint() {
        // 4.2 packed these into min(fragments, instances x backends) carriers; the SPI gives a connector neither
        // number, so at 10000 rows or more every fragment is its own carrier.
        LanceTableMetadata metadata = metadata("s3://bucket/table.lance", new Schema(Collections.emptyList()),
                Arrays.asList(new LanceFragmentInfo(7, 6000, 6001), new LanceFragmentInfo(11, 5000, 5001),
                        new LanceFragmentInfo(13, 4000, 4001)));
        List<ConnectorScanRange> ranges = LanceScanPlanner.countRanges(metadata);
        Assertions.assertEquals(3, ranges.size());
        long[] ids = {7, 11, 13};
        long[] rows = {6000, 5000, 4000};
        for (int i = 0; i < ranges.size(); i++) {
            TLanceFileDesc params = params(ranges.get(i));
            Assertions.assertEquals(42L, params.getVersion());
            Assertions.assertEquals(Collections.singletonList(ids[i]), params.getFragmentIds());
            Assertions.assertEquals(rows[i], lance(ranges.get(i)).getTableLevelRowCount());
            Assertions.assertEquals(15000L, ranges.get(i).getPushDownRowCount());
        }
    }

    private static void assertNativeFragmentSelection(LanceIndexMetadata indexes, ConnectorExpression pushed) {
        List<ConnectorScanRange> ranges = plan(indexes, pushed, 0, -1);
        Assertions.assertEquals(4, ranges.size());
        for (ConnectorScanRange range : ranges) {
            TLanceFileDesc params = params(range);
            Assertions.assertFalse(params.isSetIndexSegmentUuids());
            Assertions.assertFalse(params.isSetUseScalarIndex());
        }
    }

    private static List<ConnectorScanRange> plan(LanceIndexMetadata indexes, ConnectorExpression pushed,
            int fragmentsPerSplit, long limit) {
        LanceTableMetadata metadata = scalarMetadata();
        return LanceScanPlanner.plan(metadata, new LancePredicateConverter(metadata.getSchema()),
                Collections.singletonList(pushed), fragmentsPerSplit, () -> indexes).ranges(limit);
    }

    private static List<ConnectorScanRange> plainRanges(LanceFragmentInfo... fragments) {
        LanceTableMetadata metadata = metadata("s3://bucket/table.lance", new Schema(Collections.emptyList()),
                Arrays.asList(fragments));
        return LanceScanPlanner.plan(metadata, new LancePredicateConverter(metadata.getSchema()),
                Collections.emptyList(), 0, () -> {
                    throw new AssertionError("a scan with nothing pushed must not read index metadata");
                }).ranges(-1);
    }

    private static LanceTableMetadata scalarMetadata() {
        return metadata("s3://bucket/scalar.lance",
                new Schema(Collections.singletonList(Field.nullable("key", new ArrowType.Int(64, true)))),
                Arrays.asList(new LanceFragmentInfo(1, 10, 12), new LanceFragmentInfo(2, 20, 20),
                        new LanceFragmentInfo(3, 30, 30), new LanceFragmentInfo(4, 40, 40)));
    }

    private static LanceTableMetadata metadata(String uri, Schema schema, List<LanceFragmentInfo> fragments) {
        return new LanceTableMetadata(new LanceTableAccess(uri, Collections.emptyMap()), 42, schema, fragments);
    }

    private static LanceIndexMetadata indexes(LanceIndexSegmentInfo... segments) {
        return new LanceIndexMetadata(Collections.singletonMap("key", 9), Arrays.asList(segments));
    }

    private static LanceIndexSegmentInfo segment(UUID uuid, IndexType type, List<Long> fragments) {
        return new LanceIndexSegmentInfo(uuid, "key_idx", Collections.singletonList(9), fragments, type);
    }

    private static ConnectorExpression scalarPredicate() {
        return new ConnectorComparison(ConnectorComparison.Operator.GE,
                new ConnectorColumnRef("key", ConnectorType.of("BIGINT")), ConnectorLiteral.ofLong(2));
    }

    private static ConnectorExpression keyEquals(long value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("key", ConnectorType.of("BIGINT")), ConnectorLiteral.ofLong(value));
    }

    private static LanceScanRange lance(ConnectorScanRange range) {
        return (LanceScanRange) range;
    }

    private static TLanceFileDesc params(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getLanceParams();
    }
}
