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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Covers the metadata surface a fluss table is described through: the handle planning later reads,
 * the Doris schema DESCRIBE shows, the partition list pruning depends on, and the statistics.
 *
 * <p>Everything here runs against {@link RecordingFlussAdminOps} rather than a cluster, which is what
 * makes the awkward cases testable at all — a table that vanished, a coordinator that is down, a
 * partition spec whose map iterates in a different order than the partition columns. The mapping is
 * checked against a live cluster in {@link FlussMetadataClusterTest}.
 */
public class FlussConnectorMetadataTest {

    private static final TablePath LOG_TABLE = TablePath.of("db", "log_table");
    private static final TablePath PK_TABLE = TablePath.of("db", "pk_table");

    private static FlussConnectorMetadata metadata(RecordingFlussAdminOps adminOps) {
        return metadata(adminOps, FlussTypeMapping.Options.DEFAULT);
    }

    /**
     * A metadata with the lake seams wired to "there is no lake": the sibling owner never claims a handle
     * and the factory refuses to build one. A test in this class that unexpectedly took the lake path
     * therefore fails loudly instead of quietly exercising a half-built sibling.
     */
    private static FlussConnectorMetadata metadata(
            RecordingFlussAdminOps adminOps, FlussTypeMapping.Options options) {
        return new FlussConnectorMetadata(adminOps, options,
                properties -> {
                    throw new AssertionError("no lake sibling is expected in this test");
                },
                handle -> null);
    }

    /** A non-partitioned log table: two columns, one of them commented, three buckets. */
    private static RecordingFlussAdminOps withLogTable() {
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        adminOps.tableInfos.put(LOG_TABLE, FlussTestTables.builder(LOG_TABLE)
                .column("id", DataTypes.BIGINT())
                .column("payload", DataTypes.STRING(), "what happened")
                .buckets(3)
                .comment("the log")
                .build());
        return adminOps;
    }

    /** A partitioned primary-key table that is tiered into a paimon lake. */
    private static RecordingFlussAdminOps withDataLakePkTable() {
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        adminOps.tableInfos.put(PK_TABLE, FlussTestTables.builder(PK_TABLE)
                .column("dt", DataTypes.STRING())
                .column("region", DataTypes.STRING())
                .column("id", DataTypes.BIGINT())
                .column("amount", DataTypes.DECIMAL(20, 4))
                .primaryKey("dt", "region", "id")
                .partitionedBy("dt", "region")
                .buckets(2, "id")
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/tmp/lake")
                .build());
        return adminOps;
    }

    @Test
    public void listingIsPassedThroughToTheClusterUnchanged() {
        // Fluss already has database/table names in Doris's own shape, so nothing here may invent,
        // filter or re-case a name: what the cluster reports is what SHOW DATABASES / SHOW TABLES show.
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        adminOps.databases = Arrays.asList("fluss_db", "MixedCase");
        adminOps.tablesByDatabase.put("fluss_db", Arrays.asList("log_table", "pk_table"));
        adminOps.tablesByDatabase.put("empty_db", Collections.emptyList());

        FlussConnectorMetadata metadata = metadata(adminOps);

        Assertions.assertEquals(Arrays.asList("fluss_db", "MixedCase"), metadata.listDatabaseNames(null));
        Assertions.assertEquals(Arrays.asList("log_table", "pk_table"), metadata.listTableNames(null, "fluss_db"));
        Assertions.assertEquals(Collections.emptyList(), metadata.listTableNames(null, "empty_db"));
        Assertions.assertTrue(metadata.databaseExists(null, "fluss_db"));
        Assertions.assertFalse(metadata.databaseExists(null, "absent_db"));

        Assertions.assertEquals(
                Arrays.asList("listDatabases()", "listTables(fluss_db)", "listTables(empty_db)",
                        "databaseExists(fluss_db)", "databaseExists(absent_db)"),
                adminOps.calls);
    }

    @Test
    public void theHandleSnapshotsWhatSplitPlanningWillNeed() {
        // Planning must not have to re-read the table to learn which scanner family reads it, how many
        // buckets to split into, or where the lake is; and reading those fields one by one later could
        // straddle an ALTER. Everything below is therefore captured at handle time.
        FlussTableHandle handle = (FlussTableHandle) metadata(withDataLakePkTable())
                .getTableHandle(null, "db", "pk_table").orElseThrow(AssertionError::new);

        Assertions.assertEquals("db", handle.getDatabaseName());
        Assertions.assertEquals("pk_table", handle.getTableName());
        Assertions.assertTrue(handle.hasPrimaryKey());
        Assertions.assertEquals(Arrays.asList("dt", "region", "id"), handle.getPrimaryKeys());
        Assertions.assertEquals(Arrays.asList("dt", "region"), handle.getPartitionKeys());
        Assertions.assertTrue(handle.isPartitioned());
        Assertions.assertEquals(Collections.singletonList("id"), handle.getBucketKeys());
        Assertions.assertEquals(2, handle.getBucketCount());
        Assertions.assertTrue(handle.isDataLakeEnabled());
        Assertions.assertEquals("paimon", handle.getDataLakeFormat());
        // The lake catalog's own connection settings ride on the fluss table properties (the fluss
        // coordinator merges them in); the Doris catalog is told nothing but bootstrap servers, so this
        // map is the only place a union read can learn where the lake lives.
        Assertions.assertEquals("filesystem", handle.getProperties().get("table.datalake.paimon.metastore"));
        Assertions.assertEquals("/tmp/lake", handle.getProperties().get("table.datalake.paimon.warehouse"));
    }

    @Test
    public void logTableIsNotMistakenForPkOrLakeTable() {
        // The negative side of the handle: these three flags pick the scan strategy, so a default that
        // drifted to "true" would silently route a plain log table down the merge-on-read path.
        FlussTableHandle handle = (FlussTableHandle) metadata(withLogTable())
                .getTableHandle(null, "db", "log_table").orElseThrow(AssertionError::new);

        Assertions.assertFalse(handle.hasPrimaryKey());
        Assertions.assertFalse(handle.isPartitioned());
        Assertions.assertFalse(handle.isDataLakeEnabled());
        Assertions.assertNull(handle.getDataLakeFormat());
        Assertions.assertEquals(3, handle.getBucketCount());
    }

    @Test
    public void missingTableIsEmptyButUnreachableClusterIsAnError() {
        RecordingFlussAdminOps adminOps = withLogTable();
        Assertions.assertEquals(Optional.empty(), metadata(adminOps).getTableHandle(null, "db", "absent"));

        // The distinction that matters: reporting "no such table" for a cluster that is merely down
        // turns a broken catalog into an empty-looking one — the user never sees the real cause, and a
        // CREATE TABLE IF NOT EXISTS would believe the name is free.
        adminOps.failure = new IllegalStateException("coordinator unreachable");
        IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
                () -> metadata(adminOps).getTableHandle(null, "db", "log_table"));
        Assertions.assertEquals("coordinator unreachable", e.getMessage());
    }

    @Test
    public void theSchemaCarriesColumnsCommentsAndTheirTypes() {
        FlussConnectorMetadata metadata = metadata(withLogTable());
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "log_table")
                .orElseThrow(AssertionError::new);

        ConnectorTableSchema schema = metadata.getTableSchema(null, handle);

        Assertions.assertEquals("log_table", schema.getTableName());
        Assertions.assertEquals("FLUSS", schema.getTableFormatType());
        Assertions.assertEquals(Arrays.asList("id", "payload"), names(schema.getColumns()));
        Assertions.assertEquals(ConnectorType.of("BIGINT"), schema.getColumns().get(0).getType());
        // Column comments come from the fluss schema, NOT from the table's row type: fluss rebuilds that
        // row type from the schema and drops every description on the way, so a mapping that read
        // getRowType() would report every column as undocumented.
        Assertions.assertEquals("what happened", schema.getColumns().get(1).getComment());
        Assertions.assertEquals("the log", metadata.getTableComment(null, "db", "log_table"));
    }

    @Test
    public void everyColumnIsReportedNullableEvenWhenFlussSaysNotNull() {
        // Fluss marks primary-key columns NOT NULL. Propagating that would let the planner fold
        // null-rejecting predicates on this path only, while the same table read through its lake
        // sibling (served by the paimon connector, which reports everything nullable) would keep them —
        // one table, two plans. Doris external tables report nullable throughout for this reason.
        FlussConnectorMetadata metadata = metadata(withDataLakePkTable());
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "pk_table")
                .orElseThrow(AssertionError::new);

        for (ConnectorColumn column : metadata.getTableSchema(null, handle).getColumns()) {
            Assertions.assertTrue(column.isNullable(), column.getName() + " must be reported nullable");
        }
    }

    @Test
    public void partitionedTableAdvertisesItsPartitionColumns() {
        // Without this property fe-core treats the table as unpartitioned: no error, just partition
        // pruning quietly gone. The value keeps the partition-column ORDER, which is what the partition
        // names below are zipped against.
        FlussConnectorMetadata metadata = metadata(withDataLakePkTable());
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "pk_table")
                .orElseThrow(AssertionError::new);

        Map<String, String> properties = metadata.getTableSchema(null, handle).getProperties();
        Assertions.assertEquals("dt,region",
                properties.get(ConnectorTableSchema.PARTITION_COLUMNS_KEY));
        // The fluss table's own properties stay visible so SHOW CREATE TABLE can render them.
        Assertions.assertEquals("true", properties.get("table.datalake.enabled"));

        FlussConnectorMetadata logMetadata = metadata(withLogTable());
        ConnectorTableHandle logHandle = logMetadata.getTableHandle(null, "db", "log_table")
                .orElseThrow(AssertionError::new);
        Assertions.assertFalse(
                logMetadata.getTableSchema(null, logHandle).getProperties()
                        .containsKey(ConnectorTableSchema.PARTITION_COLUMNS_KEY),
                "an unpartitioned table must not claim partition columns");
    }

    @Test
    public void timestampWithLocalTimeZoneKeepsItsMarkerUnderEitherMapping() {
        // The marker is what DESCRIBE prints in Extra, and it describes the SOURCE column, so it must
        // not depend on whether enable.mapping.timestamp_tz mapped the column to TIMESTAMPTZ or to a
        // plain DATETIME. Losing it under one mapping would make the same column self-describe
        // differently depending on a catalog switch.
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        adminOps.tableInfos.put(LOG_TABLE, FlussTestTables.builder(LOG_TABLE)
                .column("event_time", DataTypes.TIMESTAMP_LTZ(6))
                .column("local_time", DataTypes.TIMESTAMP(6))
                .buckets(1)
                .build());

        for (boolean mapTimestampTz : new boolean[] {false, true}) {
            FlussConnectorMetadata metadata = metadata(
                    adminOps, new FlussTypeMapping.Options(false, mapTimestampTz));
            ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "log_table")
                    .orElseThrow(AssertionError::new);
            List<ConnectorColumn> columns = metadata.getTableSchema(null, handle).getColumns();
            Assertions.assertTrue(columns.get(0).isWithTimeZone(),
                    "TIMESTAMP_LTZ must keep the marker with timestamp_tz mapping = " + mapTimestampTz);
            Assertions.assertFalse(columns.get(1).isWithTimeZone(),
                    "a plain TIMESTAMP must not be marked as zoned");
        }
    }

    @Test
    public void columnHandlesAreKeyedByNameAndCarryTheFieldIndex() {
        // The index is the projection fluss is asked for later; keying by name is how the engine's slots
        // find their handle. A mismatch between the two would project the wrong column, not fail.
        FlussConnectorMetadata metadata = metadata(withDataLakePkTable());
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "pk_table")
                .orElseThrow(AssertionError::new);

        Map<String, ConnectorColumnHandle> handles = metadata.getColumnHandles(null, handle);

        Assertions.assertEquals(Arrays.asList("dt", "region", "id", "amount"),
                new ArrayList<>(handles.keySet()));
        Assertions.assertEquals(new FlussColumnHandle("id", 2), handles.get("id"));
        Assertions.assertEquals(3, ((FlussColumnHandle) handles.get("amount")).getFieldIndex());
    }

    @Test
    public void partitionsAreRenderedWithDorisNamesInPartitionColumnOrder() {
        // Fluss spells a partition "2026_08_02$eu"; Doris spells it "dt=2026_08_02/region=eu", which is
        // what fe-core parses back for SHOW PARTITIONS and the partition_values function. The ordered
        // values are supplied explicitly because fe-core zips them positionally against the partition
        // columns — and it does that silently, so a wrong order mis-assigns values rather than failing.
        RecordingFlussAdminOps adminOps = withDataLakePkTable();
        adminOps.partitionsByTable.put(PK_TABLE, Arrays.asList(
                partition(1L, "region", "eu", "dt", "2026_08_02"),
                partition(2L, "dt", "2026_08_03", "region", "us")));

        FlussConnectorMetadata metadata = metadata(adminOps);
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "pk_table")
                .orElseThrow(AssertionError::new);

        List<ConnectorPartitionInfo> partitions = metadata.listPartitions(null, handle, Optional.empty());
        Assertions.assertEquals(2, partitions.size());
        // The first fixture's spec deliberately lists region before dt: the rendering must follow the
        // partition COLUMNS, not whatever order the spec happens to iterate in.
        Assertions.assertEquals("dt=2026_08_02/region=eu", partitions.get(0).getPartitionName());
        Assertions.assertEquals(Arrays.asList("2026_08_02", "eu"), partitions.get(0).getOrderedPartitionValues());
        Assertions.assertEquals("2026_08_02", partitions.get(0).getPartitionValues().get("dt"));
        Assertions.assertEquals("eu", partitions.get(0).getPartitionValues().get("region"));
        Assertions.assertEquals("dt=2026_08_03/region=us", partitions.get(1).getPartitionName());

        Assertions.assertEquals(Arrays.asList("dt=2026_08_02/region=eu", "dt=2026_08_03/region=us"),
                metadata.listPartitionNames(null, handle));
    }

    @Test
    public void anUnpartitionedTableNeverAsksTheClusterForPartitions() {
        // Asking fluss for the partitions of an unpartitioned table is an error there, and the handle
        // already knows the answer is "none" — so this must be decided locally, not round-tripped.
        RecordingFlussAdminOps adminOps = withLogTable();
        FlussConnectorMetadata metadata = metadata(adminOps);
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "log_table")
                .orElseThrow(AssertionError::new);

        Assertions.assertEquals(Collections.emptyList(), metadata.listPartitions(null, handle, Optional.empty()));
        Assertions.assertEquals(Collections.emptyList(), metadata.listPartitionNames(null, handle));
        Assertions.assertFalse(adminOps.calls.stream().anyMatch(call -> call.startsWith("listPartitionInfos")),
                "no partition call should have been made, calls were: " + adminOps.calls);
    }

    @Test
    public void statisticsAreARowCountOrNothingAtAll() {
        RecordingFlussAdminOps adminOps = withLogTable();
        adminOps.statsByTable.put(LOG_TABLE, new TableStats(4200L));
        FlussConnectorMetadata metadata = metadata(adminOps);
        ConnectorTableHandle handle = metadata.getTableHandle(null, "db", "log_table")
                .orElseThrow(AssertionError::new);

        ConnectorTableStatistics stats = metadata.getTableStatistics(null, handle)
                .orElseThrow(AssertionError::new);
        Assertions.assertEquals(4200L, stats.getRowCount());
        // Fluss reports no data size; -1 (unknown) says so instead of implying zero bytes, which would
        // make the optimizer treat the table as free to scan.
        Assertions.assertEquals(-1L, stats.getDataSize());

        // A table whose statistics are off reports zero, which is not the same as "empty" — report
        // unknown so the estimate falls back instead of pinning the table at zero rows.
        adminOps.statsByTable.put(LOG_TABLE, new TableStats(0L));
        Assertions.assertEquals(Optional.empty(), metadata.getTableStatistics(null, handle));

        // Statistics are best effort: analysis and SHOW must not fail because the coordinator hiccuped.
        adminOps.failure = new IllegalStateException("coordinator unreachable");
        Assertions.assertEquals(Optional.empty(), metadata.getTableStatistics(null, handle));
    }

    @Test
    public void oneStatementReadsTheTableOnceAndTheNextStatementReadsItAgain() {
        // Four metadata questions about one table are one coordinator round trip. Beyond the cost, it is
        // what keeps a statement self-consistent: a concurrent ALTER between two of these calls would
        // otherwise build a plan from two different schema versions.
        RecordingFlussAdminOps adminOps = withLogTable();
        FlussConnectorMetadata metadata = metadata(adminOps);
        FlussTestSession session = new FlussTestSession(7L, "query-1");

        ConnectorTableHandle handle = metadata.getTableHandle(session, "db", "log_table")
                .orElseThrow(AssertionError::new);
        metadata.getTableSchema(session, handle);
        metadata.getColumnHandles(session, handle);
        metadata.getTableComment(session, "db", "log_table");

        Assertions.assertEquals(1, countCalls(adminOps, "getTableInfo"),
                "expected one fetch per statement, calls were: " + adminOps.calls);

        // A second statement must NOT reuse the first one's view — this memo is per statement, never a
        // cache, or an external ALTER would stay invisible until the FE restarts.
        FlussTestSession next = new FlussTestSession(7L, "query-2");
        metadata.getTableHandle(next, "db", "log_table");
        Assertions.assertEquals(2, countCalls(adminOps, "getTableInfo"));

        // And with no session at all (offline planning) nothing is shared: load every time.
        metadata.getTableHandle(null, "db", "log_table");
        metadata.getTableHandle(null, "db", "log_table");
        Assertions.assertEquals(4, countCalls(adminOps, "getTableInfo"));
    }

    @Test
    public void theTableDescriptorIsTheGenericFileScanShape() {
        // A fluss scan reaches the BE through the same file scan node the lake connectors use, so the
        // descriptor is the hive-shaped one they send. It has to name the right table: the BE looks the
        // descriptor up by id and reports the names in profiles.
        TTableDescriptor descriptor = metadata(withLogTable())
                .buildTableDescriptor(null, 42L, "log_table", "db", "log_table", 2, 9L);

        Assertions.assertEquals(TTableType.HIVE_TABLE, descriptor.getTableType());
        Assertions.assertEquals(42L, descriptor.getId());
        Assertions.assertEquals(2, descriptor.getNumCols());
        Assertions.assertEquals("log_table", descriptor.getTableName());
        Assertions.assertEquals("db", descriptor.getDbName());
        Assertions.assertEquals("db", descriptor.getHiveTable().getDbName());
        Assertions.assertEquals("log_table", descriptor.getHiveTable().getTableName());
    }

    @Test
    public void tableThatVanishesIsReportedMissingNotBroken() {
        // getTableHandle discriminates on fluss's own not-exists exception, so it must keep working when
        // that exception arrives from a memoized loader rather than straight from the admin call.
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        FlussTestSession session = new FlussTestSession(1L, "query-1");
        Assertions.assertEquals(Optional.empty(),
                metadata(adminOps).getTableHandle(session, "db", "gone"));
        Assertions.assertThrows(TableNotExistException.class,
                () -> adminOps.getTableInfo(TablePath.of("db", "gone")));
    }

    private static long countCalls(RecordingFlussAdminOps adminOps, String method) {
        return adminOps.calls.stream().filter(call -> call.startsWith(method + "(")).count();
    }

    private static List<String> names(List<ConnectorColumn> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (ConnectorColumn column : columns) {
            names.add(column.getName());
        }
        return names;
    }

    /** A fluss partition whose spec is given as key/value pairs, in the caller's order. */
    private static PartitionInfo partition(long partitionId, String... keyValues) {
        Map<String, String> spec = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            spec.put(keyValues[i], keyValues[i + 1]);
        }
        return new PartitionInfo(partitionId,
                ResolvedPartitionSpec.fromPartitionSpec(
                        new ArrayList<>(spec.keySet()), new PartitionSpec(spec)),
                null);
    }
}
