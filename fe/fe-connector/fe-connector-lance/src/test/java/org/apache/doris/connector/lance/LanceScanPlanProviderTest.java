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
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorAnd;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.index.IndexType;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Split planning and the scan-level parameters, from metadata a statement has already read. The catalog
 * client refuses to open, so any path that would read the dataset again fails the test.
 */
class LanceScanPlanProviderTest {

    private static final String URI = "s3://warehouse/lance/all_types.lance";
    private static final Schema SCHEMA = new Schema(Arrays.asList(
            Field.nullable("id", new ArrowType.Int(32, true)),
            Field.nullable("null_col", ArrowType.Null.INSTANCE)));

    private final LanceTestSession session = new LanceTestSession();
    private final LanceTableHandle handle = new LanceTableHandle("default", "all_types").withVersion(3);

    private LanceScanPlanProvider provider(Map<String, String> backendStorageProperties) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", "s3://warehouse/lance");
        LanceCatalogClient unopenable = new LanceCatalogClient(LanceCatalogProperties.of(properties),
                () -> {
                    throw new AssertionError("the statement already read this version; nothing may read it again");
                },
                () -> {
                    throw new AssertionError("the statement already read this version; nothing may read it again");
                });
        return new LanceScanPlanProvider(unopenable, "filesystem", () -> backendStorageProperties);
    }

    private void readInThisStatement(LanceTableMetadata metadata) {
        LanceStatementScope.sharedMetadata(session, handle, () -> metadata);
    }

    private static LanceTableMetadata metadata(Map<String, String> storageOptions, LanceFragmentInfo... fragments) {
        return new LanceTableMetadata(new LanceTableAccess(URI, storageOptions), 3, SCHEMA, Arrays.asList(fragments));
    }

    @Test
    void oneRangePerFragmentPinnedToTheStatementVersion() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1),
                new LanceFragmentInfo(1, 1, 1), new LanceFragmentInfo(4_294_967_295L, 2, 5)));
        List<ConnectorScanRange> ranges = provider(Collections.emptyMap()).planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build());

        Assertions.assertEquals(3, ranges.size());
        long[] expectedIds = {0, 1, 4_294_967_295L};
        long[] expectedWeights = {1, 1, 5};
        for (int i = 0; i < ranges.size(); i++) {
            ConnectorScanRange range = ranges.get(i);
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            range.populateRangeParams(formatDesc, new TFileRangeDesc());
            TLanceFileDesc lance = formatDesc.getLanceParams();
            Assertions.assertEquals(URI, lance.getDatasetUri());
            Assertions.assertEquals(3, lance.getVersion());
            // An unsigned 32-bit fragment id travels as a non-negative i64.
            Assertions.assertEquals(Collections.singletonList(expectedIds[i]), lance.getFragmentIds());
            Assertions.assertFalse(lance.isSetLimit());
            Assertions.assertFalse(lance.isSetIndexSegmentUuids());
            Assertions.assertEquals(-1, formatDesc.getTableLevelRowCount());
            Assertions.assertEquals("lance", range.getTableFormatType());
            Assertions.assertEquals(Optional.of(URI), range.getPath());
            Assertions.assertEquals(0, range.getStart());
            Assertions.assertEquals(0, range.getLength());
            Assertions.assertEquals(0, range.getFileSize());
            // Weighted by the rows a scanner reads, relative to the largest range of the plan.
            Assertions.assertEquals(expectedWeights[i], range.getSelfSplitWeight());
            Assertions.assertEquals(5, range.getTargetSplitSize());
        }
        // Consistent hashing must not collapse the ranges onto one backend.
        Assertions.assertEquals(3, ranges.stream().map(ConnectorScanRange::getSplitIdentity).distinct().count());
    }

    /** An empty fragment still costs a scanner, and a weight of 0 would not schedule it at all. */
    @Test
    void anEmptyFragmentStillWeighsOne() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(7, 0, 0)));
        ConnectorScanRange range = provider(Collections.emptyMap()).planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build()).get(0);
        Assertions.assertEquals(1, range.getSelfSplitWeight());
        Assertions.assertEquals(1, range.getTargetSplitSize());
    }

    @Test
    void versionWithoutFragmentsPlansNoRange() {
        readInThisStatement(metadata(Collections.emptyMap()));
        Assertions.assertTrue(provider(Collections.emptyMap()).planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build()).isEmpty());
    }

    @Test
    void duplicateFragmentIdsFail() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(1, 1, 1),
                new LanceFragmentInfo(1, 1, 1)));
        Assertions.assertEquals("Duplicate Lance fragment id 1 at dataset version 3",
                Assertions.assertThrows(DorisConnectorException.class, () -> provider(Collections.emptyMap())
                        .planScan(session, ConnectorScanRequest.builder(handle, Collections.emptyList()).build()))
                        .getMessage());
    }

    /** The options BE opens the dataset with travel once per scan, never in a range. */
    @Test
    void storageOptionsTravelOncePerScan() {
        Map<String, String> storageOptions = new HashMap<>();
        storageOptions.put("aws_access_key_id", "ak");
        storageOptions.put("aws_endpoint", "http://minio:9000");
        readInThisStatement(metadata(storageOptions, new LanceFragmentInfo(0, 1, 1)));
        Map<String, String> backend = Collections.singletonMap("AWS_ACCESS_KEY", "ak");
        LanceScanPlanProvider provider = provider(backend);

        Map<String, String> nodeProperties = provider.getScanNodePropertiesResult(session, handle,
                Collections.singletonList(new NamedColumnHandle("id")), Optional.empty()).getProperties();
        Assertions.assertEquals("lance", nodeProperties.get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
        Assertions.assertEquals("ak", nodeProperties.get(ScanNodePropertyKeys.LOCATION_PREFIX + "AWS_ACCESS_KEY"));
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, nodeProperties);
        Assertions.assertEquals(storageOptions, params.getLanceScanParams().getLanceStorageOptions());
        Assertions.assertFalse(params.getLanceScanParams().isSetLanceSubstraitFilter());
        Assertions.assertFalse(params.getLanceScanParams().isSetExternalSearchRequest());

        ConnectorScanRange range = provider.planScan(session,
                ConnectorScanRequest.builder(handle, Collections.emptyList()).build()).get(0);
        Assertions.assertTrue(range.getProperties().isEmpty());
    }

    @Test
    void catalogWithoutStorageOptionsSendsNoLanceScanParams() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, provider.getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), Optional.empty()).getProperties());
        Assertions.assertFalse(params.isSetLanceScanParams());
    }

    /**
     * Reading a column of one of the additional types needs the current BE Lance reader, so the scan must
     * not be scheduled while an older backend is a smooth-upgrade source; an unread one does not count.
     */
    @Test
    void onlyProjectingAnAdditionalTypeRaisesTheUpgradeFence() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        List<ConnectorColumnHandle> plain = Collections.singletonList(new NamedColumnHandle("id"));
        Assertions.assertNull(provider.getScanNodePropertiesResult(session, handle, plain, Optional.empty()).getProperties()
                .get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
        List<ConnectorColumnHandle> withNull = Arrays.asList(new NamedColumnHandle("id"),
                new NamedColumnHandle("NULL_COL"));
        Assertions.assertEquals("Additional Lance types", provider.getScanNodePropertiesResult(session, handle,
                withNull, Optional.empty()).getProperties().get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
    }

    @Test
    void explainNamesTheSnapshotAndTheGroupingButNoCredential() {
        Map<String, String> storageOptions = Collections.singletonMap("aws_secret_access_key", "sentinel-secret");
        readInThisStatement(metadata(storageOptions, new LanceFragmentInfo(0, 1, 1), new LanceFragmentInfo(1, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        StringBuilder explain = new StringBuilder();
        provider.appendExplainInfo(explain, "  ", provider.getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), Optional.empty()).getProperties());
        Assertions.assertEquals("  lanceCatalogType=filesystem\n  lanceVersion=3\n  lanceFragments=2\n"
                + "  lanceFragmentGrouping=FRAGMENT\n", explain.toString());
    }

    @Test
    void rangeRefusesWhatWouldMakeBeScanTheWholeDataset() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceScanRange(URI, 3, Collections.emptyList(), 1, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceScanRange(URI, 0, Collections.singletonList(0L), 1, 1));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new LanceScanRange(URI, 3, Collections.singletonList(-1L), 1, 1));
    }

    private static final ConnectorExpression ID_IS_5 = new ConnectorComparison(ConnectorComparison.Operator.EQ,
            new ConnectorColumnRef("id", ConnectorType.of("INT")), new ConnectorLiteral(ConnectorType.of("INT"), 5L));
    /** Lance cannot evaluate a comparison on a column of the Arrow Null type, so it stays in Doris. */
    private static final ConnectorExpression NULL_COL_IS_5 = new ConnectorComparison(
            ConnectorComparison.Operator.EQ, new ConnectorColumnRef("null_col", ConnectorType.of("INT")),
            new ConnectorLiteral(ConnectorType.of("INT"), 5L));

    /** As if this statement had already read the version's index metadata, which lists no index. */
    private LanceTableMetadata readWithoutIndexes(LanceTableMetadata metadata) {
        readInThisStatement(metadata);
        LanceStatementScope.sharedIndexMetadata(session, handle, metadata,
                () -> new LanceIndexMetadata(Collections.emptyMap(), Collections.emptyList()));
        return metadata;
    }

    private static TLanceFileDesc rangeParams(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getLanceParams();
    }

    private static long tableLevelRowCount(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getTableLevelRowCount();
    }

    private List<ConnectorScanRange> plan(LanceScanPlanProvider provider, ConnectorScanRequest.Builder request) {
        return provider.planScan(session, request.build());
    }

    private ConnectorScanRequest.Builder request() {
        return ConnectorScanRequest.builder(handle, Collections.emptyList());
    }

    private String explain(LanceScanPlanProvider provider, Optional<ConnectorExpression> filter,
            Map<String, String> engineFacts) {
        Map<String, String> nodeProperties = new HashMap<>(provider.getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), filter).getProperties());
        nodeProperties.putAll(engineFacts);
        StringBuilder explain = new StringBuilder();
        provider.appendExplainInfo(explain, "  ", nodeProperties);
        return explain.toString();
    }

    /** A pushed conjunct leaves the Doris residuals and reaches BE once, in the scan-level Substrait filter. */
    @Test
    void pushedConjunctsLeaveTheResidualsAndReachBeOncePerScan() throws Exception {
        readWithoutIndexes(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        ScanNodePropertiesResult result = provider.getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), Optional.of(new ConnectorAnd(Arrays.asList(NULL_COL_IS_5, ID_IS_5))));
        Assertions.assertTrue(result.hasConjunctTracking());
        Assertions.assertEquals(Collections.singleton(0), result.getNotPushedConjunctIndices());
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider.populateScanLevelParams(params, result.getProperties());
        byte[] filter = params.getLanceScanParams().getLanceSubstraitFilter();
        Assertions.assertArrayEquals(new LancePredicateConverter(SCHEMA)
                .convert(Collections.singletonList(ID_IS_5)).getSubstraitFilter(), filter);
        Assertions.assertFalse(params.getLanceScanParams().isSetLanceStorageOptions());
        Assertions.assertTrue(explain(provider, Optional.of(ID_IS_5), Collections.emptyMap())
                .endsWith("  lancePushdownPredicate=(id = 5)\n"));

        ScanNodePropertiesResult nothingPushed = provider.getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), Optional.of(NULL_COL_IS_5));
        Assertions.assertEquals(Collections.singleton(0), nothingPushed.getNotPushedConjunctIndices());
        TFileScanRangeParams none = new TFileScanRangeParams();
        provider.populateScanLevelParams(none, nothingPushed.getProperties());
        Assertions.assertFalse(none.isSetLanceScanParams());
    }

    /** A range may stop at the LIMIT only when Doris filters nothing after it. */
    @Test
    void limitReachesEveryRangeOnlyWhenEveryConjunctIsPushed() {
        readWithoutIndexes(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1),
                new LanceFragmentInfo(1, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        for (ConnectorScanRange range : plan(provider, request().filter(Optional.of(ID_IS_5)).limit(10))) {
            Assertions.assertEquals(10, rangeParams(range).getLimit());
        }
        for (ConnectorScanRange range : plan(provider, request().limit(10))) {
            Assertions.assertEquals(10, rangeParams(range).getLimit());
        }
        for (ConnectorScanRange range : plan(provider, request()
                .filter(Optional.of(new ConnectorAnd(Arrays.asList(ID_IS_5, NULL_COL_IS_5)))).limit(10))) {
            Assertions.assertFalse(rangeParams(range).isSetLimit());
        }
        for (ConnectorScanRange range : plan(provider, request().filter(Optional.of(ID_IS_5)))) {
            Assertions.assertFalse(rangeParams(range).isSetLimit());
        }
        Map<String, String> limitedAndAllPushed = new HashMap<>();
        limitedAndAllPushed.put(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT, "10");
        limitedAndAllPushed.put(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED, "true");
        Assertions.assertTrue(explain(provider, Optional.of(ID_IS_5), limitedAndAllPushed).contains("  lanceLimit=10\n"));
        limitedAndAllPushed.put(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED, "false");
        Assertions.assertFalse(explain(provider, Optional.of(ID_IS_5), limitedAndAllPushed).contains("lanceLimit"));
    }

    /**
     * COUNT(*) without a filter is the manifest's logical row count. The carriers keep their fragments and the
     * version, so a BE that cannot serve the count scans the same snapshot; EXPLAIN shows the plan's total.
     */
    @Test
    void countWithoutFilterIsServedFromManifestRowCounts() {
        readWithoutIndexes(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 9, 10),
                new LanceFragmentInfo(1, 8, 10)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        List<ConnectorScanRange> small = plan(provider, request().countPushdown(true));
        Assertions.assertEquals(1, small.size());
        Assertions.assertEquals(17, tableLevelRowCount(small.get(0)));
        Assertions.assertEquals(17, small.get(0).getPushDownRowCount());
        Assertions.assertEquals(Arrays.asList(0L, 1L), rangeParams(small.get(0)).getFragmentIds());
        Assertions.assertEquals(3, rangeParams(small.get(0)).getVersion());
        // Any filter makes the row count larger than the result: the scan reads and BE counts.
        for (ConnectorScanRange range : plan(provider, request().countPushdown(true).filter(Optional.of(ID_IS_5)))) {
            Assertions.assertEquals(-1, tableLevelRowCount(range));
            Assertions.assertEquals(-1, range.getPushDownRowCount());
        }
        for (ConnectorScanRange range : plan(provider, request())) {
            Assertions.assertEquals(-1, tableLevelRowCount(range));
        }

        LanceTableHandle large = new LanceTableHandle("default", "large").withVersion(3);
        LanceStatementScope.sharedMetadata(session, large, () -> metadata(Collections.emptyMap(),
                new LanceFragmentInfo(0, 6000, 6000), new LanceFragmentInfo(1, 5000, 5001)));
        List<ConnectorScanRange> carriers = provider.planScan(session,
                ConnectorScanRequest.builder(large, Collections.emptyList()).countPushdown(true).build());
        Assertions.assertEquals(2, carriers.size());
        Assertions.assertEquals(Arrays.asList(6000L, 5000L),
                Arrays.asList(tableLevelRowCount(carriers.get(0)), tableLevelRowCount(carriers.get(1))));
        for (ConnectorScanRange carrier : carriers) {
            Assertions.assertEquals(11000, carrier.getPushDownRowCount());
        }

        LanceTableHandle empty = new LanceTableHandle("default", "empty").withVersion(3);
        LanceStatementScope.sharedMetadata(session, empty, () -> metadata(Collections.emptyMap()));
        List<ConnectorScanRange> emptyCount = provider.planScan(session,
                ConnectorScanRequest.builder(empty, Collections.emptyList()).countPushdown(true).build());
        Assertions.assertEquals(1, emptyCount.size());
        Assertions.assertEquals(0, tableLevelRowCount(emptyCount.get(0)));
        Assertions.assertFalse(rangeParams(emptyCount.get(0)).isSetFragmentIds());
    }

    /** lance_fragments_per_split groups fragments exactly, in manifest order, and never plans index segments. */
    @Test
    void debugGroupingTakesFragmentsPerSplitAtATime() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1),
                new LanceFragmentInfo(1, 1, 1), new LanceFragmentInfo(2, 1, 1)));
        session.setSessionProperty(LanceScanPlanProvider.SESSION_FRAGMENTS_PER_SPLIT, "2");
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        List<ConnectorScanRange> ranges = plan(provider, request().filter(Optional.of(ID_IS_5)));
        Assertions.assertEquals(Arrays.asList(Arrays.asList(0L, 1L), Collections.singletonList(2L)),
                Arrays.asList(rangeParams(ranges.get(0)).getFragmentIds(), rangeParams(ranges.get(1)).getFragmentIds()));
        Assertions.assertFalse(rangeParams(ranges.get(0)).isSetUseScalarIndex());
        Assertions.assertTrue(explain(provider, Optional.empty(), Collections.emptyMap())
                .contains("  lanceFragmentGrouping=DEBUG\n  lanceFragmentsPerSplit=2\n"));
    }

    /**
     * A pushed predicate one BTREE index can drive gets one range per index segment; the fragments no segment
     * covers are read without the scalar index, whose segments the other ranges already search.
     */
    @Test
    void pushedPredicateReadsOneRangePerScalarIndexSegment() {
        LanceTableMetadata metadata = metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 10, 10),
                new LanceFragmentInfo(1, 10, 10), new LanceFragmentInfo(2, 10, 10));
        readInThisStatement(metadata);
        UUID first = new UUID(1, 2);
        UUID second = new UUID(3, 4);
        LanceIndexMetadata indexes = new LanceIndexMetadata(Collections.singletonMap("id", 7), Arrays.asList(
                new LanceIndexSegmentInfo(first, "id_idx", Collections.singletonList(7),
                        Collections.singletonList(0L), IndexType.BTREE),
                new LanceIndexSegmentInfo(second, "id_idx", Collections.singletonList(7),
                        Arrays.asList(1L, 9L), IndexType.BTREE)));
        LanceStatementScope.sharedIndexMetadata(session, handle, metadata, () -> indexes);
        LanceScanPlanProvider provider = provider(Collections.emptyMap());

        List<ConnectorScanRange> ranges = plan(provider, request().filter(Optional.of(ID_IS_5)));
        Assertions.assertEquals(3, ranges.size());
        ByteBuffer firstUuid = ByteBuffer.allocate(16).putLong(1).putLong(2);
        firstUuid.flip();
        Assertions.assertEquals(Collections.singletonList(firstUuid), rangeParams(ranges.get(0)).getIndexSegmentUuids());
        Assertions.assertEquals(Collections.singletonList(0L), rangeParams(ranges.get(0)).getFragmentIds());
        // Fragment 9 is not in this version; only the visible fragment 1 is assigned to the segment.
        Assertions.assertEquals(Collections.singletonList(1L), rangeParams(ranges.get(1)).getFragmentIds());
        TLanceFileDesc uncovered = rangeParams(ranges.get(2));
        Assertions.assertEquals(Collections.singletonList(2L), uncovered.getFragmentIds());
        Assertions.assertFalse(uncovered.isSetIndexSegmentUuids());
        Assertions.assertTrue(uncovered.isSetUseScalarIndex());
        Assertions.assertFalse(uncovered.isUseScalarIndex());
        Assertions.assertTrue(explain(provider, Optional.of(ID_IS_5), Collections.emptyMap()).contains(
                "  lanceFragmentGrouping=INDEX_SEGMENT\n  lanceScalarIndexScan=SEGMENT\n  lanceGroupingIndex=id_idx\n"
                        + "  lanceGroupingIndexSegments=2\n  lanceGroupingIndexedFragments=2\n"
                        + "  lanceGroupingUnindexedFragments=1\n"));

        // A complement cannot drive the index; such a filter also keeps BE from searching it on every range.
        ConnectorExpression notFive = new ConnectorComparison(ConnectorComparison.Operator.NE,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), new ConnectorLiteral(ConnectorType.of("INT"), 5L));
        List<ConnectorScanRange> complement = plan(provider, request().filter(Optional.of(notFive)));
        Assertions.assertEquals(3, complement.size());
        for (ConnectorScanRange range : complement) {
            Assertions.assertFalse(rangeParams(range).isSetIndexSegmentUuids());
            Assertions.assertFalse(rangeParams(range).isUseScalarIndex());
        }
    }

    /** Index metadata is read only for a scan that can use a segment; the client here refuses any read. */
    @Test
    void scanWithNothingPushedNeverReadsIndexMetadata() {
        readInThisStatement(metadata(Collections.emptyMap(), new LanceFragmentInfo(0, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        Assertions.assertEquals(1, plan(provider, request()).size());
        Assertions.assertEquals(1, plan(provider, request().filter(Optional.of(NULL_COL_IS_5))).size());
        Assertions.assertFalse(rangeParams(plan(provider, request()).get(0)).isSetUseScalarIndex());
        // The control: a pushed predicate does ask for the index metadata.
        Assertions.assertThrows(AssertionError.class, () -> plan(provider, request().filter(Optional.of(ID_IS_5))));
    }
}
