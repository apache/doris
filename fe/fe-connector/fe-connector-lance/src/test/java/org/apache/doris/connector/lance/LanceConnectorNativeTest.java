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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The connector end to end on real datasets, through the SPI the engine calls: a filesystem catalog over a
 * local warehouse, the statement's version pin, time travel and the split plan BE receives.
 *
 * <p>Skips, loudly, where the native libraries cannot load; see {@link LanceNativeTestSupport}. The
 * versioned dataset is the one the BE Lance reader tests read: version 1 holds fragment 0, version 2 adds
 * fragment 1 and version 3 adds fragment 2, committed at 03:53:50.752131, .754294 and .755891 UTC on
 * 2026-07-20.
 */
class LanceConnectorNativeTest {

    @TempDir
    Path warehouse;

    private LanceConnector connector;
    private ConnectorMetadata metadata;
    private LanceTestSession session;
    private ConnectorTableHandle table;

    @BeforeEach
    void openCatalogOverTheVersionedDataset() throws Exception {
        LanceNativeTestSupport.requireNativeLibraries();
        LanceNativeTestSupport.copyDataset(LanceNativeTestSupport.VERSIONED_ALL_TYPES, warehouse);
        connector = open(warehouse);
        session = new LanceTestSession();
        metadata = connector.getMetadata(session);
        table = metadata.getTableHandle(session, "default", "all_types").get();
    }

    @AfterEach
    void closeCatalog() {
        if (connector != null) {
            connector.close();
        }
    }

    private static LanceConnector open(Path warehouse) {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "lance");
        properties.put("warehouse", warehouse.toString());
        return (LanceConnector) new LanceConnectorProvider().create(properties, new LanceTestContext());
    }

    /** Without a manifest table, the directory namespace finds datasets by listing the warehouse root. */
    @Test
    void theRootNamespaceIsTheDefaultDatabase() {
        Assertions.assertEquals(Collections.singletonList("default"), metadata.listDatabaseNames(session));
        Assertions.assertTrue(metadata.databaseExists(session, "default"));
        Assertions.assertFalse(metadata.databaseExists(session, "missing"));
        Assertions.assertEquals(Collections.singletonList("all_types"), metadata.listTableNames(session, "default"));
        Assertions.assertFalse(metadata.getTableHandle(session, "default", "missing").isPresent());
        Assertions.assertTrue(connector.testConnection(session).isSuccess());
    }

    @Test
    void queryPinsTheNewestVersionAndReadsItsSchema() {
        ConnectorMvccSnapshot pin = metadata.beginQuerySnapshot(session, table).get();
        Assertions.assertEquals(3, pin.getSnapshotId());
        Assertions.assertEquals(pin.getSnapshotId(), pin.getSchemaId());
        Assertions.assertTrue(pin.isSchemaRetained());

        ConnectorTableSchema schema = metadata.getTableSchema(session, table);
        Assertions.assertEquals(27, schema.getColumns().size());
        ConnectorColumn rowId = schema.getColumns().get(0);
        Assertions.assertEquals("row_id", rowId.getName());
        Assertions.assertEquals(ConnectorType.of("BIGINT"), rowId.getType());
        Assertions.assertFalse(rowId.isNullable());
        Assertions.assertEquals(ConnectorType.of("TIMESTAMPTZ", 6, 0), column(schema, "timestamp_value").getType());
        Assertions.assertEquals(ConnectorType.of("DECIMALV3", 76, 38), column(schema, "decimal_76_38").getType());
        // No table properties: SHOW CREATE TABLE prints them unmasked.
        Assertions.assertTrue(schema.getProperties().isEmpty());
        Assertions.assertEquals(27, metadata.getColumnHandles(session, table).size());
        Assertions.assertEquals(4, metadata.getTableStatistics(session, table).get().getRowCount());

        Assertions.assertEquals(Arrays.asList(Arrays.asList(0L), Arrays.asList(1L), Arrays.asList(2L)),
                fragmentsOf(plan(metadata.applySnapshot(session, table, pin)), 3));
    }

    @Test
    void implicitSchemaPinSurvivesAnExternalSchemaCommitAndAFreshSession() {
        ConnectorMvccSnapshot pin = metadata.beginQuerySnapshot(session, table).get();
        try (Dataset writer = Dataset.open(warehouse.resolve("all_types.lance").toString())) {
            writer.dropColumns(Collections.singletonList("row_id"));
        }
        LanceTestSession later = new LanceTestSession();
        ConnectorMetadata laterMetadata = connector.getMetadata(later);
        Assertions.assertEquals(26, laterMetadata.getTableSchema(later, table).getColumns().size());
        Assertions.assertTrue(pin.isSchemaRetained());
        Assertions.assertEquals(3, pin.getSchemaId());
        ConnectorTableSchema retained = laterMetadata.getTableSchema(later, table, pin);
        Assertions.assertEquals(27, retained.getColumns().size());
        Assertions.assertEquals("row_id", retained.getColumns().get(0).getName());
        Assertions.assertEquals(27, laterMetadata.getColumnHandles(later, table, pin).size());
        Assertions.assertEquals(3, ((LanceTableHandle) laterMetadata.applySnapshot(later, table, pin))
                .getVersion().getAsLong());
    }

    @Test
    void historicalStatisticsPinAnUnpinnedHandleInAFreshSession() {
        ConnectorMvccSnapshot first = metadata.resolveTimeTravel(session, table,
                ConnectorTimeTravelSpec.snapshotId("1")).get();
        LanceTestSession statisticsSession = new LanceTestSession();
        ConnectorMetadata statistics = connector.getMetadata(statisticsSession);
        Assertions.assertEquals(4, statistics.getTableStatistics(statisticsSession, table).get().getRowCount());
        Assertions.assertEquals(1, statistics.getTableStatistics(statisticsSession, table, first).get().getRowCount());
    }

    @Test
    void forVersionAsOfPlansThatVersionsFragments() {
        Assertions.assertEquals(Collections.singletonList(Arrays.asList(0L)),
                fragmentsOf(plan(pin(ConnectorTimeTravelSpec.snapshotId("1"))), 1));
        Assertions.assertEquals(Arrays.asList(Arrays.asList(0L), Arrays.asList(1L)),
                fragmentsOf(plan(pin(ConnectorTimeTravelSpec.snapshotId("2"))), 2));
        // A version is a version however the engine classified the literal.
        Assertions.assertEquals(Collections.singletonList(Arrays.asList(0L)),
                fragmentsOf(plan(pin(ConnectorTimeTravelSpec.versionRef("1"))), 1));
        ConnectorMvccSnapshot first = metadata.resolveTimeTravel(session, table,
                ConnectorTimeTravelSpec.snapshotId("1")).get();
        Assertions.assertEquals(27, metadata.getTableSchema(session, table, first).getColumns().size());
        Assertions.assertEquals(1, metadata.getTableStatistics(session,
                metadata.applySnapshot(session, table, first)).get().getRowCount());
    }

    /** FOR TIME AS OF compares commit times in full, in the session time zone. */
    @Test
    void forTimeAsOfPicksTheVersionCommittedAtOrBeforeTheTime() {
        Assertions.assertEquals(1, versionAt("2026-07-20 03:53:50.753", "UTC"));
        Assertions.assertEquals(2, versionAt("2026-07-20 03:53:50.755", "UTC"));
        Assertions.assertEquals(3, versionAt("2026-07-20 03:53:51", "UTC"));
        Assertions.assertEquals(1, versionAt("2026-07-20 11:53:50.753", "Asia/Shanghai"));
        // Version 1 was committed 131 microseconds into this millisecond, so it is after it.
        Assertions.assertEquals("Lance table default.all_types has no version at or before '2026-07-20 03:53:50.752'",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> versionAt("2026-07-20 03:53:50.752", "UTC")).getMessage());
        Assertions.assertEquals("Cannot parse Lance FOR TIME AS OF value '20260720', expected"
                        + " 'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd HH:mm:ss.SSS'",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> versionAt("20260720", "UTC")).getMessage());
    }

    @Test
    void missingVersionAndUnsupportedSelectorsFailClearly() {
        DorisConnectorException missing = Assertions.assertThrows(DorisConnectorException.class,
                () -> pin(ConnectorTimeTravelSpec.snapshotId("9")));
        Assertions.assertEquals("Lance version 9 of default.all_types was not found", missing.getMessage());
        Assertions.assertEquals("Lance FOR VERSION AS OF requires a numeric version, but was 'v1'",
                Assertions.assertThrows(DorisConnectorException.class,
                        () -> pin(ConnectorTimeTravelSpec.versionRef("v1"))).getMessage());
        Assertions.assertTrue(Assertions.assertThrows(DorisConnectorException.class,
                () -> pin(ConnectorTimeTravelSpec.tag("t1"))).getMessage().startsWith(
                "Lance tables do not support @tag yet"));
        Assertions.assertTrue(Assertions.assertThrows(DorisConnectorException.class,
                () -> pin(ConnectorTimeTravelSpec.branch("b1"))).getMessage().startsWith(
                "Lance tables do not support @branch yet"));
    }

    /** The regression dataset carries every type Lance tables map, including the additional ones. */
    @Test
    void projectingAnAdditionalTypeRaisesTheUpgradeFence(@TempDir Path regressionWarehouse) throws Exception {
        LanceNativeTestSupport.copyDataset(
                "docker/thirdparties/docker-compose/iceberg/scripts/preinstalled_data/lance/all_types.lance",
                regressionWarehouse);
        LanceConnector regression = open(regressionWarehouse);
        try {
            LanceTestSession statement = new LanceTestSession();
            ConnectorMetadata regressionMetadata = regression.getMetadata(statement);
            ConnectorTableHandle allTypes = regressionMetadata.applySnapshot(statement,
                    regressionMetadata.getTableHandle(statement, "default", "all_types").get(),
                    regressionMetadata.beginQuerySnapshot(statement,
                            regressionMetadata.getTableHandle(statement, "default", "all_types").get()).get());
            Assertions.assertNull(regression.getScanPlanProvider().getScanNodePropertiesResult(statement, allTypes,
                    Collections.singletonList(new NamedColumnHandle("row_id")), Optional.empty())
                    .getProperties().get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS));
            for (String column : Arrays.asList("null_col", "duration_ns_col", "json_col", "bfloat16_vector_col")) {
                Assertions.assertEquals("Additional Lance types", regression.getScanPlanProvider()
                        .getScanNodePropertiesResult(statement, allTypes,
                                Collections.singletonList(new NamedColumnHandle(column)), Optional.empty())
                        .getProperties().get(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS), column);
            }
        } finally {
            regression.close();
        }
    }

    private long versionAt(String time, String sessionTimeZone) {
        LanceTestSession zoned = new LanceTestSession(sessionTimeZone);
        return connector.getMetadata(zoned).resolveTimeTravel(zoned, table,
                ConnectorTimeTravelSpec.timestamp(time, false)).get().getSnapshotId();
    }

    private ConnectorTableHandle pin(ConnectorTimeTravelSpec spec) {
        return metadata.applySnapshot(session, table, metadata.resolveTimeTravel(session, table, spec).get());
    }

    private List<ConnectorScanRange> plan(ConnectorTableHandle pinned) {
        return connector.getScanPlanProvider().planScan(session,
                ConnectorScanRequest.builder(pinned, Collections.emptyList()).build());
    }

    /** The fragments each range sends BE, checking every range is pinned to {@code version}. */
    private static List<List<Long>> fragmentsOf(List<ConnectorScanRange> ranges, long version) {
        List<List<Long>> fragments = new ArrayList<>();
        for (ConnectorScanRange range : ranges) {
            TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
            range.populateRangeParams(formatDesc, new TFileRangeDesc());
            Assertions.assertEquals(version, formatDesc.getLanceParams().getVersion());
            Assertions.assertTrue(formatDesc.getLanceParams().getDatasetUri().endsWith("all_types.lance"));
            fragments.add(formatDesc.getLanceParams().getFragmentIds());
        }
        return fragments;
    }

    private static ConnectorColumn column(ConnectorTableSchema schema, String name) {
        return schema.getColumns().stream().filter(c -> c.getName().equals(name)).findFirst().get();
    }
}
