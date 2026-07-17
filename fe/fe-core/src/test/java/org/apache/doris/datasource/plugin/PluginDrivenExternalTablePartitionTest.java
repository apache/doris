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

package org.apache.doris.datasource.plugin;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.SessionContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for {@link PluginDrivenExternalTable}'s partition-metadata overrides added by
 * FIX-PART-GATES: {@code isPartitionedTable}, {@code getPartitionColumns},
 * {@code supportInternalPartitionPruned}, {@code getNameToPartitionItems}, and the
 * {@code initSchema} partition-column extraction into {@link PluginDrivenSchemaCacheValue}.
 *
 * <p><b>Why these matter:</b> after the MaxCompute SPI cutover, partition visibility
 * (SHOW PARTITIONS / partitions() TVF) and internal partition pruning both depend on these
 * overrides. Without them a partitioned MaxCompute table reports as non-partitioned (SHOW
 * PARTITIONS throws "not a partitioned table") and large partitioned tables degrade to a
 * full scan. The tests lock: (1) partition columns sourced from the cached
 * {@code partition_columns} property; (2) {@code getNameToPartitionItems} addressing the
 * connector's raw-keyed partition values by the RAW remote column names (not the mapped
 * local names); (3) {@code supportInternalPartitionPruned} returning unconditional true (mirroring
 * legacy MaxComputeExternalTable) for BOTH partitioned and non-partitioned tables — gating it on
 * partition columns silently dropped all rows of filtered non-partitioned scans
 * (FIX-NONPART-PRUNE-DATALOSS).</p>
 */
public class PluginDrivenExternalTablePartitionTest {

    // ==================== read-back overrides (cache value constructed directly) ====================

    @Test
    public void testPartitionedTableExposesPartitionColumnsAndPruning() {
        List<Column> schema = Arrays.asList(
                new Column("year", PrimitiveType.INT),
                new Column("month", PrimitiveType.INT),
                new Column("val", PrimitiveType.INT));
        List<Column> partitionColumns = Arrays.asList(schema.get(0), schema.get(1));
        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                schema, partitionColumns, Arrays.asList("year", "month"));
        PluginDrivenExternalTable table = tableWithCacheValue(cacheValue);

        Assertions.assertTrue(table.isPartitionedTable(),
                "a table with partition columns must report isPartitionedTable()==true (SHOW PARTITIONS gate)");
        Assertions.assertEquals(partitionColumns, table.getPartitionColumns(),
                "getPartitionColumns() must return the cached partition columns");
        Assertions.assertTrue(table.supportInternalPartitionPruned(),
                "a partitioned table must opt into internal partition pruning");
    }

    @Test
    public void testNonPartitionedTableReportsNoPartitionsButStillOptsIntoPruning() {
        List<Column> schema = Collections.singletonList(new Column("val", PrimitiveType.INT));
        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                schema, Collections.emptyList(), Collections.emptyList());
        PluginDrivenExternalTable table = tableWithCacheValue(cacheValue);

        Assertions.assertFalse(table.isPartitionedTable());
        Assertions.assertTrue(table.getPartitionColumns().isEmpty());
        // WHY (FIX-NONPART-PRUNE-DATALOSS): supportInternalPartitionPruned MUST be unconditional true,
        // even for a NON-partitioned table (mirrors legacy MaxComputeExternalTable). A previous version
        // gated it on partition columns -> returned false here, which sent PruneFileScanPartition down
        // its ELSE branch (selection := SelectedPartitions(0, {}, isPruned=true)); PluginDrivenScanNode
        // then read that as "pruned to zero" and short-circuited to no splits, so a filtered query over
        // a non-partitioned table silently returned ZERO ROWS. With true, the rule's IF branch /
        // pruneExternalPartitions returns NOT_PRUNED for empty partition columns -> scan all. A mutation
        // reverting to `!getPartitionColumns().isEmpty()` (false here) makes this assertion red.
        Assertions.assertTrue(table.supportInternalPartitionPruned(),
                "a non-partitioned table must STILL opt into internal partition pruning, or filtered "
                        + "queries silently return zero rows (FIX-NONPART-PRUNE-DATALOSS)");
    }

    // ==================== getNameToPartitionItems (raw remote-name addressing) ====================

    @Test
    public void testGetNameToPartitionItemsBuildsFromConnectorByRemoteNames() {
        // Doris (local/mapped) partition column names differ from the RAW remote names, so a
        // mutation indexing the connector's raw-keyed value map by the local names would miss.
        List<Column> schema = Arrays.asList(
                new Column("year", PrimitiveType.INT),
                new Column("month", PrimitiveType.INT),
                new Column("val", PrimitiveType.INT));
        List<Column> partitionColumns = Arrays.asList(schema.get(0), schema.get(1));
        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                schema, partitionColumns, Arrays.asList("YEAR", "MONTH"));

        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("max_compute", metadata, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(handle));
        Mockito.when(metadata.listPartitions(Mockito.eq(session), Mockito.eq(handle), Mockito.any()))
                .thenReturn(Arrays.asList(
                        partition("YEAR=2024/MONTH=1", "2024", "1"),
                        partition("YEAR=2023/MONTH=2", "2023", "2")));

        PluginDrivenExternalTable table = tableWithCacheValue(cacheValue, catalog, db, "REMOTE_TBL");

        Map<String, PartitionItem> items = table.getNameToPartitionItems(Optional.empty());

        Assertions.assertEquals(2, items.size());
        assertPartition(items, "YEAR=2024/MONTH=1", "2024", "1");
        assertPartition(items, "YEAR=2023/MONTH=2", "2023", "2");
        // WHY: addressing must use the RAW remote names; if it used the local "year"/"month" the
        // raw-keyed value map lookups would return null and partition-key construction would break.
        Mockito.verify(metadata).getTableHandle(session, "REMOTE_DB", "REMOTE_TBL");
    }

    @Test
    public void testGetNameToPartitionItemsEmptyWhenNotPartitioned() {
        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                Collections.singletonList(new Column("val", PrimitiveType.INT)),
                Collections.emptyList(), Collections.emptyList());
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog(
                "max_compute", metadata, Mockito.mock(ConnectorSession.class));
        PluginDrivenExternalTable table = tableWithCacheValue(
                cacheValue, catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");

        Assertions.assertTrue(table.getNameToPartitionItems(Optional.empty()).isEmpty());
        Mockito.verifyNoInteractions(metadata);
    }

    // ==================== initSchema partition extraction (raw -> mapped bridge) ====================

    @Test
    public void testInitSchemaExtractsPartitionColumnsMappingRemoteNames() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("max_compute", metadata, session);
        ExternalDatabase<PluginDrivenExternalTable> db = mockDb("REMOTE_DB");

        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(handle));
        // Connector schema: raw remote column names; partition_columns prop lists RAW names.
        ConnectorTableSchema tableSchema = new ConnectorTableSchema(
                "REMOTE_TBL",
                Arrays.asList(
                        new ConnectorColumn("YEAR", ConnectorType.of("INT"), "", true, null),
                        new ConnectorColumn("REGION", ConnectorType.of("INT"), "", true, null),
                        new ConnectorColumn("VAL", ConnectorType.of("INT"), "", true, null)),
                "max_compute",
                Collections.singletonMap(ConnectorTableSchema.PARTITION_COLUMNS_KEY, "YEAR,REGION"));
        Mockito.when(metadata.getTableSchema(session, handle)).thenReturn(tableSchema);
        // Identifier mapping lowercases the remote names (raw "YEAR" -> mapped "year").
        Mockito.when(metadata.fromRemoteColumnName(Mockito.eq(session), Mockito.anyString(),
                        Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(inv -> ((String) inv.getArgument(3)).toLowerCase());

        PluginDrivenExternalTable table = bareTable(catalog, db, "REMOTE_TBL");
        Optional<SchemaCacheValue> result = table.initSchema();

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(result.get() instanceof PluginDrivenSchemaCacheValue);
        PluginDrivenSchemaCacheValue value = (PluginDrivenSchemaCacheValue) result.get();
        Assertions.assertEquals(Arrays.asList("year", "region", "val"), columnNames(value.getSchema()));
        // WHY: partition columns are matched after mapping raw->local; a mutation that matched by the
        // RAW name would find nothing (schema holds mapped "year"/"region") and drop the partitions.
        Assertions.assertEquals(Arrays.asList("year", "region"), columnNames(value.getPartitionColumns()),
                "partition columns must be the MAPPED Doris columns identified via fromRemoteColumnName");
        Assertions.assertEquals(Arrays.asList("YEAR", "REGION"), value.getPartitionColumnRemoteNames(),
                "remote names must be kept raw for addressing connector partition values");
    }

    @Test
    public void testInitSchemaNoPartitionsWhenPropAbsent() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        TestablePluginCatalog catalog = new TestablePluginCatalog("max_compute", metadata, session);
        Mockito.when(metadata.getTableHandle(session, "REMOTE_DB", "REMOTE_TBL"))
                .thenReturn(Optional.of(handle));
        Mockito.when(metadata.getTableSchema(session, handle)).thenReturn(new ConnectorTableSchema(
                "REMOTE_TBL",
                Collections.singletonList(new ConnectorColumn("c", ConnectorType.of("INT"), "", true, null)),
                "max_compute",
                Collections.emptyMap()));
        Mockito.when(metadata.fromRemoteColumnName(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenAnswer(inv -> inv.getArgument(3));

        PluginDrivenExternalTable table = bareTable(catalog, mockDb("REMOTE_DB"), "REMOTE_TBL");
        Optional<SchemaCacheValue> result = table.initSchema();

        Assertions.assertTrue(result.get() instanceof PluginDrivenSchemaCacheValue);
        Assertions.assertTrue(((PluginDrivenSchemaCacheValue) result.get()).getPartitionColumns().isEmpty());
    }

    // ==================== getTableProperties (SHOW CREATE TABLE source, D-046) ====================

    @Test
    public void testGetTablePropertiesStripsSchemaControlKeysButKeepsUserOptions() {
        // The connector stuffs BOTH user-facing table options (path / file.format) AND the FE-internal
        // schema-control keys (reserved, namespaced under __internal.) into one properties map.
        Map<String, String> rawProps = new LinkedHashMap<>();
        rawProps.put("path", "s3://wh/db/t");
        rawProps.put("file.format", "orc");
        rawProps.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY, "dt");
        rawProps.put(ConnectorTableSchema.PRIMARY_KEYS_KEY, "id");
        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                Collections.singletonList(new Column("id", PrimitiveType.INT)),
                Collections.emptyList(), Collections.emptyList(), rawProps);
        PluginDrivenExternalTable table = tableWithCacheValue(cacheValue);

        Map<String, String> props = table.getTableProperties();
        // WHY (D-046): SHOW CREATE TABLE's LOCATION reads "path" and PROPERTIES(...) dumps this map.
        // The user-facing options MUST survive, but the FE-internal reserved keys MUST be stripped —
        // they are emitted only so initSchema() can derive partition columns and would corrupt the
        // round-tripped DDL. MUTATION: dropping the filter -> the reserved keys leak ->
        // red; over-filtering (removing "path") -> LOCATION renders empty -> red.
        Assertions.assertEquals("s3://wh/db/t", props.get("path"));
        Assertions.assertEquals("orc", props.get("file.format"));
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.PARTITION_COLUMNS_KEY),
                "the reserved partition-columns key must not appear in SHOW CREATE PROPERTIES");
        Assertions.assertFalse(props.containsKey(ConnectorTableSchema.PRIMARY_KEYS_KEY),
                "the reserved primary-keys key must not appear in SHOW CREATE PROPERTIES");
    }

    @Test
    public void testGetTablePropertiesEmptyWhenConnectorEmitsNone() {
        // MaxCompute-style connector emits no table properties: the 3-arg cache-value ctor must
        // default to an empty map so SHOW CREATE TABLE stays comment-only (no empty LOCATION ''/
        // PROPERTIES () lines). MUTATION: defaulting to null -> NPE in getTableProperties / Env -> red.
        PluginDrivenSchemaCacheValue cacheValue = new PluginDrivenSchemaCacheValue(
                Collections.singletonList(new Column("c", PrimitiveType.INT)),
                Collections.emptyList(), Collections.emptyList());
        PluginDrivenExternalTable table = tableWithCacheValue(cacheValue);

        Assertions.assertTrue(table.getTableProperties().isEmpty(),
                "a connector emitting no properties (e.g. MaxCompute) must yield empty table properties");
    }

    // ==================== helpers ====================

    private static ConnectorPartitionInfo partition(String name, String year, String month) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("YEAR", year);
        values.put("MONTH", month);
        return new ConnectorPartitionInfo(name, values, Collections.emptyMap());
    }

    private static void assertPartition(Map<String, PartitionItem> items, String name,
            String year, String month) {
        PartitionItem item = items.get(name);
        Assertions.assertNotNull(item, "missing partition " + name);
        Assertions.assertTrue(item instanceof ListPartitionItem);
        PartitionKey key = ((ListPartitionItem) item).getItems().get(0);
        Assertions.assertEquals(year, key.getKeys().get(0).getStringValue(),
                "partition value for the first (year) column must come from the YEAR remote key");
        Assertions.assertEquals(month, key.getKeys().get(1).getStringValue(),
                "partition value for the second (month) column must come from the MONTH remote key");
    }

    private static List<String> columnNames(List<Column> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (Column c : columns) {
            names.add(c.getName());
        }
        return names;
    }

    /** Table whose schema-cache lookup returns the given value; not backed by a real connector. */
    private static PluginDrivenExternalTable tableWithCacheValue(SchemaCacheValue cacheValue) {
        return tableWithCacheValue(cacheValue,
                new TestablePluginCatalog("max_compute", Mockito.mock(ConnectorMetadata.class),
                        Mockito.mock(ConnectorSession.class)),
                mockDb("REMOTE_DB"), "REMOTE_TBL");
    }

    private static PluginDrivenExternalTable tableWithCacheValue(SchemaCacheValue cacheValue,
            PluginDrivenExternalCatalog catalog, ExternalDatabase<PluginDrivenExternalTable> db, String remoteName) {
        return new PluginDrivenExternalTable(1L, "tbl", remoteName, catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op: skip Env-backed catalog/db init
            }

            @Override
            public Optional<SchemaCacheValue> getSchemaCacheValue() {
                return Optional.of(cacheValue);
            }
        };
    }

    /** Table that drives the real initSchema(); does not stub the schema cache. */
    private static PluginDrivenExternalTable bareTable(PluginDrivenExternalCatalog catalog,
            ExternalDatabase<PluginDrivenExternalTable> db, String remoteName) {
        return new PluginDrivenExternalTable(1L, "tbl", remoteName, catalog, db) {
            @Override
            protected synchronized void makeSureInitialized() {
                // no-op
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static ExternalDatabase<PluginDrivenExternalTable> mockDb(String remoteName) {
        ExternalDatabase<PluginDrivenExternalTable> db = Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getRemoteName()).thenReturn(remoteName);
        return db;
    }

    /**
     * Minimal PluginDrivenExternalCatalog that returns a fixed connector/session without standing
     * up the Doris environment (mirrors the pattern in PluginDrivenExternalTableEngineTest).
     */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        private final Connector connector;
        private final ConnectorSession session;

        TestablePluginCatalog(String catalogType, ConnectorMetadata metadata, ConnectorSession session) {
            this(catalogType, mockConnector(metadata, session), session);
        }

        private TestablePluginCatalog(String catalogType, Connector connector, ConnectorSession session) {
            super(1L, "test-catalog", null, makeProps(catalogType), "", connector);
            this.connector = connector;
            this.session = session;
        }

        private static Connector mockConnector(ConnectorMetadata metadata, ConnectorSession session) {
            Connector c = Mockito.mock(Connector.class);
            Mockito.when(c.getMetadata(session)).thenReturn(metadata);
            return c;
        }

        @Override
        public Connector getConnector() {
            return connector;
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return session;
        }

        @Override
        protected List<String> listDatabaseNames() {
            return Collections.emptyList();
        }

        @Override
        protected List<String> listTableNamesFromRemote(SessionContext ctx, String dbName) {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
            return false;
        }

        private static Map<String, String> makeProps(String type) {
            Map<String, String> props = new HashMap<>();
            props.put("type", type);
            return props;
        }
    }
}
