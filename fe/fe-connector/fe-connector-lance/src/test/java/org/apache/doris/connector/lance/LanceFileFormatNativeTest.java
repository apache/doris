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

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorFileSource;
import org.apache.doris.connector.spi.ConnectorFileSourceOps;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.BasePath;
import org.lance.Dataset;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A Lance dataset read through the file table functions, as {@code s3("uri" = ..., "format" = "lance")} hands it to
 * the plugin: the connector the provider builds for the format resolves the location to a handle pinned to the
 * newest version, serves its columns, and plans its scan with the table function's storage. Skips, loudly, where the
 * native libraries cannot load; see {@link LanceNativeTestSupport}.
 */
class LanceFileFormatNativeTest {
    private static final ConnectorExpression ROW_ID_IS_2 = new ConnectorComparison(
            ConnectorComparison.Operator.EQ, new ConnectorColumnRef("row_id", ConnectorType.of("BIGINT")),
            new ConnectorLiteral(ConnectorType.of("BIGINT"), 2L));

    @TempDir
    Path warehouse;

    private String uri;
    private Connector connector;
    private final LanceTestSession session = new LanceTestSession();

    @BeforeEach
    void copyTheVersionedDataset() throws IOException {
        LanceNativeTestSupport.requireNativeLibraries();
        uri = LanceNativeTestSupport.copyDataset(LanceNativeTestSupport.VERSIONED_ALL_TYPES, warehouse).toString();
        connector = new LanceConnectorProvider().createFileFormatConnector("lance", new LanceTestContext());
    }

    @Test
    void handlePinsTheNewestVersionAndServesItsColumns() {
        ConnectorMetadata metadata = connector.getMetadata(session);
        ConnectorTableHandle handle = resolve(metadata, session);
        Assertions.assertEquals(3, ((LanceFileTableHandle) handle).getVersion());

        ConnectorTableSchema schema = metadata.getTableSchema(session, handle);
        Assertions.assertEquals(27, schema.getColumns().size());
        Assertions.assertEquals("row_id", schema.getColumns().get(0).getName());
        Assertions.assertEquals(ConnectorType.of("BIGINT"), schema.getColumns().get(0).getType());
        Assertions.assertTrue(schema.getProperties().isEmpty());
        Assertions.assertEquals(27, metadata.getColumnHandles(session, handle).size());
    }

    @Test
    void metadataRetainsTheSnapshotsRegisteredBases() {
        Schema schema = new Schema(Collections.singletonList(Field.nullable("id", new ArrowType.Int(32, true))));
        BasePath base = new BasePath(0, Optional.of("local_blobs"), warehouse.resolve("blobs").toString(), false);
        try (BufferAllocator allocator = new RootAllocator();
                Dataset dataset = Dataset.write().allocator(allocator).schema(schema)
                        .uri(warehouse.resolve("with_bases.lance").toString())
                        .initialBases(Collections.singletonList(base)).execute();
                LanceMetadataMetrics.Timer timer = new LanceMetadataMetrics()
                        .start(LanceMetadataMetrics.Operation.METADATA_READ)) {
            Assertions.assertEquals(1, dataset.getBasePaths().size());
            // Substitute a remote root without contacting it, so only the manifest's base is rejected.
            LanceTableMetadata metadata = LanceCatalogClient.readMetadata(dataset,
                    new LanceTableAccess("s3://warehouse/with_bases.lance", Collections.emptyMap(), false), timer);
            Assertions.assertEquals(dataset.version(), metadata.getVersion());
            Assertions.assertEquals("Local Lance BLOB base paths are not supported",
                    Assertions.assertThrows(DorisConnectorException.class, metadata::validateBlobStorage)
                            .getMessage());
        }
    }

    /**
     * Every range reads the version the handle pinned, one range per fragment: in the statement that resolved the
     * handle, from what it read, and in another statement, by reading that version again.
     */
    @Test
    void scanReadsThePinnedVersionAfterANewerOneIsWritten() {
        ConnectorTableHandle handle = resolve(connector.getMetadata(session), session);
        try (Dataset writer = Dataset.open(uri)) {
            writer.updateConfig(Collections.singletonMap("doris_test_key", "written_after_the_pin"));
        }
        List<List<Long>> fragments = Arrays.asList(Arrays.asList(0L), Arrays.asList(1L), Arrays.asList(2L));
        Assertions.assertEquals(fragments, fragmentsOf(plan(session, request(handle)), 3));
        Assertions.assertEquals(fragments,
                fragmentsOf(plan(LanceTestSession.withoutStatement(), request(handle)), 3));
    }

    /**
     * The BE opens the dataset with the options of the table function that named it, and its file system reads the
     * table function's storage; EXPLAIN and the handle show neither.
     */
    @Test
    void scanCarriesTheTableFunctionsStorageAndShowsNoneOfIt() {
        ConnectorTableHandle resolved = resolve(connector.getMetadata(session), session);
        Map<String, String> options = new HashMap<>();
        options.put("aws_access_key_id", "AKIDEXAMPLE");
        options.put("aws_secret_access_key", "example-secret-value");
        // A handle on the same dataset and version with another table function's credentials: its scan reads the
        // metadata and opens the dataset with them, not with the first handle's storage.
        LanceFileTableHandle handle = new LanceFileTableHandle(uri, ((LanceFileTableHandle) resolved).getVersion(),
                options, Collections.singletonMap("AWS_SECRET_KEY", "example-secret-value"));

        ScanNodePropertiesResult result = provider().getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), Optional.empty());
        Map<String, String> properties = result.getProperties();
        Assertions.assertEquals("example-secret-value",
                properties.get(ScanNodePropertyKeys.LOCATION_PREFIX + "AWS_SECRET_KEY"));
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider().populateScanLevelParams(params, properties);
        Assertions.assertEquals(options, params.getLanceScanParams().getLanceStorageOptions());

        StringBuilder explain = new StringBuilder();
        provider().appendExplainInfo(explain, "  ", properties);
        Assertions.assertTrue(explain.toString().contains("  lanceCatalogType=tvf\n"), explain.toString());
        Assertions.assertTrue(explain.toString().contains("  lanceVersion=3\n"), explain.toString());
        Assertions.assertFalse(explain.toString().contains("example-secret-value"), explain.toString());
        Assertions.assertFalse(explain.toString().contains("AKIDEXAMPLE"), explain.toString());
        Assertions.assertFalse(handle.toString().contains("example-secret-value"), handle.toString());
    }

    /** What a catalog scan pushes, a file table function's scan pushes too: the filter, and COUNT(*). */
    @Test
    void filterAndCountArePushedAsForACatalogTable() {
        ConnectorTableHandle handle = resolve(connector.getMetadata(session), session);
        ScanNodePropertiesResult filtered = provider().getScanNodePropertiesResult(session, handle,
                Collections.emptyList(), Optional.of(ROW_ID_IS_2));
        Assertions.assertTrue(filtered.getNotPushedConjunctIndices().isEmpty());
        TFileScanRangeParams params = new TFileScanRangeParams();
        provider().populateScanLevelParams(params, filtered.getProperties());
        Assertions.assertTrue(params.getLanceScanParams().getLanceSubstraitFilter().length > 0);

        long rows = 0;
        for (ConnectorScanRange range : plan(session, request(handle).countPushdown(true))) {
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            range.populateRangeParams(formatDesc, new TFileRangeDesc());
            rows += formatDesc.getTableLevelRowCount();
        }
        Assertions.assertEquals(4, rows);
    }

    /**
     * A projection of a type the BE reader added in 4.2 (Arrow Null, Duration, JSON, bfloat16) keeps a smooth-upgrade
     * source BE out of the scan, as for a catalog table; other columns do not. The regression dataset carries them.
     */
    @Test
    void projectingAnAdditionalTypeRaisesTheUpgradeFence(@TempDir Path regressionWarehouse) throws IOException {
        String allTypes = LanceNativeTestSupport.copyDataset(
                "docker/thirdparties/docker-compose/iceberg/scripts/preinstalled_data/lance/all_types.lance",
                regressionWarehouse).toString();
        ConnectorTableHandle handle = ((ConnectorFileSourceOps) connector.getMetadata(session)).getFileTableHandle(
                session, new ConnectorFileSource("lance", allTypes, Collections.emptyList(), Collections.emptyMap()));
        Assertions.assertNull(provider().getScanNodePropertiesResult(session, handle,
                Collections.singletonList(new NamedColumnHandle("row_id")), Optional.empty())
                .getProperties().get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
        for (String column : Arrays.asList("null_col", "duration_ns_col", "json_col", "bfloat16_vector_col")) {
            Assertions.assertEquals("Additional Lance types", provider().getScanNodePropertiesResult(session, handle,
                    Collections.singletonList(new NamedColumnHandle(column)), Optional.empty())
                    .getProperties().get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS), column);
        }
    }

    @Test
    void missingDatasetFailsWithAMessageForTheUser() {
        String missing = warehouse.resolve("missing.lance").toString();
        ConnectorFileSourceOps metadata = (ConnectorFileSourceOps) connector.getMetadata(session);
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getFileTableHandle(session, new ConnectorFileSource("lance", missing,
                        Collections.emptyList(), Collections.emptyMap())));
        Assertions.assertTrue(failure.getMessage().startsWith(
                "Failed to load Lance metadata from the dataset at the uri: "), failure.getMessage());
    }

    private ConnectorTableHandle resolve(ConnectorMetadata metadata, ConnectorSession resolving) {
        return ((ConnectorFileSourceOps) metadata).getFileTableHandle(resolving,
                new ConnectorFileSource("lance", uri, Collections.emptyList(), Collections.emptyMap()));
    }

    private ConnectorScanPlanProvider provider() {
        return connector.getScanPlanProvider();
    }

    private static ConnectorScanRequest.Builder request(ConnectorTableHandle handle) {
        return ConnectorScanRequest.builder(handle, Collections.emptyList());
    }

    private List<ConnectorScanRange> plan(ConnectorSession planning, ConnectorScanRequest.Builder request) {
        return provider().planScan(planning, request.build());
    }

    private static List<List<Long>> fragmentsOf(List<ConnectorScanRange> ranges, long version) {
        List<List<Long>> fragments = new ArrayList<>();
        for (ConnectorScanRange range : ranges) {
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            range.populateRangeParams(formatDesc, new TFileRangeDesc());
            Assertions.assertEquals(version, formatDesc.getLanceParams().getVersion());
            fragments.add(formatDesc.getLanceParams().getFragmentIds());
        }
        return fragments;
    }
}
