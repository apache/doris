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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

        Map<String, String> nodeProperties = provider.getScanNodeProperties(session, handle,
                Collections.singletonList(new NamedColumnHandle("id")), Optional.empty());
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
        provider.populateScanLevelParams(params, provider.getScanNodeProperties(session, handle,
                Collections.emptyList(), Optional.empty()));
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
        Assertions.assertNull(provider.getScanNodeProperties(session, handle, plain, Optional.empty())
                .get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
        List<ConnectorColumnHandle> withNull = Arrays.asList(new NamedColumnHandle("id"),
                new NamedColumnHandle("NULL_COL"));
        Assertions.assertEquals("Additional Lance types", provider.getScanNodeProperties(session, handle,
                withNull, Optional.empty()).get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
    }

    @Test
    void explainNamesTheSnapshotAndTheGroupingButNoCredential() {
        Map<String, String> storageOptions = Collections.singletonMap("aws_secret_access_key", "sentinel-secret");
        readInThisStatement(metadata(storageOptions, new LanceFragmentInfo(0, 1, 1), new LanceFragmentInfo(1, 1, 1)));
        LanceScanPlanProvider provider = provider(Collections.emptyMap());
        StringBuilder explain = new StringBuilder();
        provider.appendExplainInfo(explain, "  ", provider.getScanNodeProperties(session, handle,
                Collections.emptyList(), Optional.empty()));
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
}
