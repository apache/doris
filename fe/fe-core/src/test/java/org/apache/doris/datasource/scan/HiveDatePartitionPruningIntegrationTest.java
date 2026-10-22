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

package org.apache.doris.datasource.scan;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.hive.HiveCatalogProperties;
import org.apache.doris.connector.hive.HiveConnectorMetadata;
import org.apache.doris.connector.hive.HiveFileListingCache;
import org.apache.doris.connector.hive.HiveFileStatus;
import org.apache.doris.connector.hive.HiveReadTransactionManager;
import org.apache.doris.connector.hive.HiveScanPlanProvider;
import org.apache.doris.connector.hive.HiveTableHandle;
import org.apache.doris.connector.hive.HiveTableType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.datasource.plugin.PluginDrivenExternalTable;
import org.apache.doris.datasource.plugin.PluginDrivenSchemaCacheValue;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.PruneFileScanPartition;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Exercises real Hive filtering, typed logical pruning and the physical partition-name handoff. */
public class HiveDatePartitionPruningIntegrationTest {
    @Test
    public void testCompactAndTwoDigitDatesReachPhysicalSelection() {
        assertDateSelection(Type.DATE, new DateLiteral(2024, 1, 1), false);
        assertDateSelection(Type.DATEV2, new DateV2Literal(2024, 1, 1), false);
    }

    @Test
    public void testUnrepresentableCompactDatePreservesScanAll() {
        assertDateSelection(Type.DATE, new DateLiteral(2024, 1, 1), true);
    }

    @Test
    public void testExcessTimestampPrecisionPreservesSynchronousScanAllForEquality() {
        assertTimestampSelection(false);
    }

    @Test
    public void testExcessTimestampPrecisionPreservesSynchronousScanAllForIn() {
        assertTimestampSelection(true);
    }

    private static void assertTimestampSelection(boolean inPredicate) {
        List<String> names = List.of("d=2024-01-01 10:00:00.1234561", "d=2024-01-01 10:00:00.12345601",
                "d=2024-01-01 10:00:00.123456001", "d=2024-01-01 10:00:00.1234569",
                "d=2024-01-01 10:00:00.12345699", "d=2024-01-01 10:00:00.123456999",
                "d=2024-01-01 10:00:00.999999999", "d=2024-01-01 23:59:59.999999999",
                "d=2024-01-01 10:00:00.123456", "d=2024-01-01 10:00:00.123457",
                "d=2024-01-01 10:00:01", "d=2024-01-02 00:00:00", "d=2024-01-03 00:00:00");
        for (String text : List.of("2024-01-01 10:00:00.123456", "2024-01-01 10:00:00.123457",
                "2024-01-01 10:00:01", "2024-01-02 00:00:00")) {
            assertSelection(ScalarType.createDatetimeV2Type(6), new DateTimeV2Literal(DateTimeV2Type.of(6), text),
                    names, true, inPredicate);
        }
    }

    private static void assertDateSelection(Type type, Expression literal, boolean unrepresentableDate) {
        List<String> names = new ArrayList<>(List.of("d=20240101", "d=24-01-01", "d=2024-01-01",
                "d=20240102", "d=24-01-02"));
        if (unrepresentableDate) {
            // The legacy DATE partition-item parser rejects six-digit compact dates. The connector must
            // preserve this candidate and let the typed view become unavailable, rather than publish a subset.
            names.add("d=240101");
        }
        assertSelection(type, literal, names, unrepresentableDate, false);
    }

    @SuppressWarnings("unchecked")
    private static void assertSelection(Type type, Expression literal, List<String> names,
            boolean unrepresentable, boolean inPredicate) {
        HmsClient client = Mockito.mock(HmsClient.class);
        // Logical fallback uses -1; synchronous physical planning uses a bounded positive limit.
        Mockito.when(client.listPartitionNames(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyInt()))
                .thenReturn(names);
        Mockito.when(client.getExistingPartitionsWithStats(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyList()))
                .thenCallRealMethod();
        Mockito.when(client.getExistingPartitions(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyList()))
                .thenAnswer(invocation -> {
                    List<HmsPartitionInfo> partitions = new ArrayList<>();
                    for (String name : (List<String>) invocation.getArgument(2)) {
                        partitions.add(new HmsPartitionInfo(List.of(name.substring(2)), name,
                                null, null, null, Collections.emptyMap()));
                    }
                    return partitions;
                });
        HiveCatalogProperties properties = HiveCatalogProperties.of(Collections.singletonMap(
                HiveCatalogProperties.HIVE_METASTORE_URIS, "thrift://localhost:9083"));
        ConnectorContext connectorContext = Mockito.mock(ConnectorContext.class);
        ConnectorStorageContext storage = Mockito.mock(ConnectorStorageContext.class);
        Mockito.when(connectorContext.getStorageContext()).thenReturn(storage);
        Mockito.when(storage.normalizeStorageUri(Mockito.anyString()))
                .thenAnswer(invocation -> invocation.getArgument(0));
        HiveConnectorMetadata metadata = new HiveConnectorMetadata(client, properties, connectorContext);
        HiveTableHandle handle = new HiveTableHandle.Builder("db", "t", HiveTableType.HIVE)
                .inputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
                .serializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                .partitionKeyNames(List.of("d"))
                .partitionKeyTypes(Collections.singletonMap("d", type.getPrimitiveType().name()))
                .build();
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(session)).thenReturn(metadata);
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("hive");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Mockito.when(catalog.buildConnectorSession()).thenReturn(session);
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db");
        Column column = new Column("d", type);
        SchemaCacheValue schema = new PluginDrivenSchemaCacheValue(List.of(column), List.of(column), List.of("d"));
        PluginDrivenExternalTable table = new PluginDrivenExternalTable(31L, "t", "t", catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // Use the synthetic schema without initializing an Env-backed catalog.
            }

            @Override
            public Optional<SchemaCacheValue> getSchemaCacheValue() {
                return Optional.of(schema);
            }

            @Override
            public Optional<ConnectorTableHandle> resolveConnectorTableHandle(
                    ConnectorSession ignored, ConnectorMetadata ignoredMetadata) {
                return Optional.of(handle);
            }

            @Override
            public boolean supportsConnectorPartitionPruning() {
                return true;
            }
        };
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        StatementContext statement = new StatementContext(context, null);
        context.setStatementContext(statement);
        context.setThreadLocalInfo();
        try {
            SlotReference slot = new SlotReference("d", DataType.fromCatalogType(type));
            LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table, List.of("hive", "db"),
                    Collections.emptyList(), Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.of(List.of(slot)));
            Expression predicate = inPredicate ? new InPredicate(slot, List.of(literal)) : new EqualTo(slot, literal);
            LogicalFilter<LogicalFileScan> filter = new LogicalFilter<>(Set.of(predicate), scan);
            CascadesContext cascades = CascadesContext.initContext(statement, filter, PhysicalProperties.ANY);
            SelectedPartitions selected = Deencapsulation.invoke(new PruneFileScanPartition(),
                    "pruneExternalPartitions", table, filter, scan, cascades);
            HiveFileListingCache files = Mockito.mock(HiveFileListingCache.class);
            Mockito.when(files.listDataFiles(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyString(),
                    Mockito.anyList(), Mockito.nullable(FileSystem.class))).thenAnswer(invocation ->
                            List.of(new HiveFileStatus(invocation.getArgument(2) + "/data.parquet", 10L, 1L)));
            HiveScanPlanProvider provider = new HiveScanPlanProvider(client, properties, connectorContext,
                    new HiveReadTransactionManager(), files);
            if (unrepresentable) {
                Assertions.assertTrue(selected.isNotPruned());
                Assertions.assertFalse(PluginDrivenScanNode.shouldUseBatchMode(selected, true, true, 1));
                Assertions.assertFalse(PluginDrivenScanNode.reusedConnectorFilterResult(selected).isPresent());
                Assertions.assertNull(PluginDrivenScanNode.resolveRequiredPartitions(selected));
                Assertions.assertEquals(Set.of(predicate), filter.getConjuncts());
                // No filtered handle/name set may leak into synchronous physical planning. The real
                // provider must plan every fake partition file, including rounding and carry candidates.
                ConnectorScanRequest request = ConnectorScanRequest.builder(handle, Collections.emptyList())
                        .requiredPartitions(PluginDrivenScanNode.resolveRequiredPartitions(selected)).build();
                Assertions.assertTrue(request.getRequiredPartitions().isEmpty());
                List<ConnectorScanRange> ranges = provider.planScan(session, request);
                Assertions.assertEquals(names.size(), ranges.size());
                Set<String> plannedNames = new HashSet<>();
                for (ConnectorScanRange range : ranges) {
                    plannedNames.add(range.getPath().get().replace("/data.parquet", ""));
                }
                Assertions.assertEquals(Set.copyOf(names), plannedNames);
                return;
            }
            Set<String> expected = Set.of("d=20240101", "d=24-01-01", "d=2024-01-01");
            Assertions.assertEquals(expected, selected.selectedPartitions.keySet());
            Assertions.assertEquals(expected, Set.copyOf(PluginDrivenScanNode.resolveRequiredPartitions(selected)));
            Assertions.assertTrue(PluginDrivenScanNode.reusedConnectorFilterResult(selected).isPresent());
            HiveTableHandle filtered = (HiveTableHandle) selected.getConnectorFilterResult().get().getHandle();
            Assertions.assertEquals(expected, filtered.getPrunedPartitionsByName().keySet());
            Assertions.assertTrue(PluginDrivenScanNode.shouldUseBatchMode(selected, true, true, 1));
            ConnectorScanRequest request = ConnectorScanRequest.builder(filtered, Collections.emptyList())
                    .requiredPartitions(PluginDrivenScanNode.resolveRequiredPartitions(selected)).build();
            Set<String> plannedNames = new HashSet<>();
            for (String name : request.getRequiredPartitions()) {
                List<ConnectorScanRange> ranges = provider.planScanForPartitionBatch(session, request, List.of(name));
                Assertions.assertEquals(1, ranges.size());
                plannedNames.add(ranges.get(0).getPath().get().replace("/data.parquet", ""));
            }
            Assertions.assertEquals(expected, plannedNames);
            Mockito.verify(client, Mockito.times(1)).listPartitionNames("db", "t", -1);
            Mockito.verify(client, Mockito.times(1))
                    .getExistingPartitions(Mockito.eq("db"), Mockito.eq("t"), Mockito.anyList());
        } finally {
            try {
                statement.close();
            } finally {
                ConnectContext.remove();
                if (previous != null) {
                    previous.setThreadLocalInfo();
                }
            }
        }
    }
}
