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

import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metadata.TableStats;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The lake gateway: {@code tbl$lake} is served by an embedded paimon sibling, and every per-handle call
 * about that table has to reach the sibling instead of fluss.
 *
 * <p>The sibling is a hand-written {@link RecordingLakeSibling} (this module bans mock frameworks) whose
 * answers are values fluss could never produce, so an assertion that finds them proves the forward really
 * happened rather than that both sides agree by coincidence.
 */
public class FlussLakeTableTest {

    private static final TablePath LAKE_TABLE = TablePath.of("db", "lake_table");
    private static final TablePath PLAIN_TABLE = TablePath.of("db", "plain_table");

    /** The sibling connectors this test's factory built, in build order. */
    private final List<RecordingLakeSibling> builtSiblings = new ArrayList<>();

    private final ConnectorSession session = new FlussTestSession(7L, "q1");

    /**
     * A fluss table with a paimon lake, exactly as a coordinator reports one: the lake switch, the format,
     * and the cluster's paimon settings injected under {@code table.datalake.paimon.}.
     */
    private RecordingFlussAdminOps withLakeTable() {
        return withLakeTable("paimon");
    }

    private RecordingFlussAdminOps withLakeTable(String lakeFormat) {
        RecordingFlussAdminOps adminOps = new RecordingFlussAdminOps();
        adminOps.tableInfos.put(LAKE_TABLE, FlussTestTables.builder(LAKE_TABLE)
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .buckets(2)
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", lakeFormat)
                .property("table.datalake.paimon.metastore", "filesystem")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
                .build());
        adminOps.tableInfos.put(PLAIN_TABLE, FlussTestTables.builder(PLAIN_TABLE)
                .column("id", DataTypes.BIGINT())
                .buckets(1)
                .build());
        return adminOps;
    }

    /**
     * Metadata whose lake seams are the real ones a connector supplies: a factory that builds (and
     * records) a sibling, and an owner resolver that answers exactly the way {@code FlussConnector}'s does
     * — by asking each built sibling whether the handle is its own.
     */
    private FlussConnectorMetadata metadata(RecordingFlussAdminOps adminOps) {
        return new FlussConnectorMetadata(adminOps, FlussTypeMapping.Options.DEFAULT,
                properties -> {
                    RecordingLakeSibling sibling = new RecordingLakeSibling(properties);
                    builtSiblings.add(sibling);
                    return sibling;
                },
                handle -> {
                    for (RecordingLakeSibling sibling : builtSiblings) {
                        if (sibling.ownsHandle(handle)) {
                            return sibling;
                        }
                    }
                    return null;
                });
    }

    private ConnectorTableHandle baseHandle(FlussConnectorMetadata metadata, TablePath tablePath) {
        return metadata.getTableHandle(session, tablePath.getDatabaseName(), tablePath.getTableName())
                .orElseThrow(AssertionError::new);
    }

    private ConnectorTableHandle lakeHandle(FlussConnectorMetadata metadata) {
        return metadata.getSysTableHandle(session, baseHandle(metadata, LAKE_TABLE), "lake")
                .orElseThrow(AssertionError::new);
    }

    @Test
    public void datalakeTableOffersBothHalvesAsSystemTables() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        // This listing is what makes fe-core resolve the names "lake_table$lake" and "lake_table$log" at
        // all. Both or neither: they are complementary halves of one table, so a table that can offer one
        // can always offer the other.
        Assertions.assertEquals(Arrays.asList("lake", "log"),
                metadata.listSupportedSysTables(session, baseHandle(metadata, LAKE_TABLE)));
    }

    @Test
    public void tableWithoutALakeOffersNoSystemTable() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        // Advertising $lake on a table that has none would offer a sub-table whose only possible outcome
        // is an error; fe-core's "no such table" is the honest answer.
        Assertions.assertTrue(
                metadata.listSupportedSysTables(session, baseHandle(metadata, PLAIN_TABLE)).isEmpty());
    }

    /**
     * {@code $log} stays on this side of the gateway. It is the SAME fluss table, only read from where the
     * lake snapshot ends, so it resolves to a fluss handle and no sibling is built at all — asking paimon
     * for a table Doris is about to read out of fluss would be a round trip for nothing.
     */
    @Test
    public void theLogHandleIsThisTableReadAsItsLogTail() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle base = baseHandle(metadata, LAKE_TABLE);

        ConnectorTableHandle handle = metadata.getSysTableHandle(session, base, "log")
                .orElseThrow(AssertionError::new);

        Assertions.assertTrue(handle instanceof FlussTableHandle);
        FlussTableHandle logHandle = (FlussTableHandle) handle;
        Assertions.assertTrue(logHandle.isLogOnly());
        // Same table, different segment: the name has to match (the scan reads it out of fluss under that
        // name) while the handle must not compare equal to the base one.
        Assertions.assertEquals(((FlussTableHandle) base).toTablePath(), logHandle.toTablePath());
        Assertions.assertNotEquals(base, logHandle);
        Assertions.assertFalse(((FlussTableHandle) base).isLogOnly(), "the base handle is unchanged");
        Assertions.assertTrue(builtSiblings.isEmpty(), "reading the log needs no paimon sibling");
    }

    /**
     * A half has no halves. Left unguarded, {@code tbl$log} would advertise a {@code $lake} of its own —
     * a name fe-core could not resolve anyway, because it splits a table name at its LAST {@code $}.
     */
    @Test
    public void theLogHandleOffersNoSystemTablesOfItsOwn() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle logHandle = metadata
                .getSysTableHandle(session, baseHandle(metadata, LAKE_TABLE), "log")
                .orElseThrow(AssertionError::new);

        Assertions.assertTrue(metadata.listSupportedSysTables(session, logHandle).isEmpty());
        Assertions.assertFalse(metadata.getSysTableHandle(session, logHandle, "lake").isPresent());
        Assertions.assertFalse(metadata.getSysTableHandle(session, logHandle, "log").isPresent());
    }

    /**
     * Fluss reports one row count, for the whole table. Handing it to {@code tbl$log} would describe the
     * small half with the big half's number — and in the direction that makes the optimizer avoid the
     * cheap scan. Unknown is a state the optimizer already has a plan for.
     */
    @Test
    public void theLogHandleReportsUnknownStatistics() {
        RecordingFlussAdminOps adminOps = withLakeTable();
        adminOps.statsByTable.put(LAKE_TABLE, new TableStats(1_000_000L));
        FlussConnectorMetadata metadata = metadata(adminOps);
        ConnectorTableHandle base = baseHandle(metadata, LAKE_TABLE);

        Assertions.assertEquals(1_000_000L,
                metadata.getTableStatistics(session, base).orElseThrow(AssertionError::new).getRowCount());
        Assertions.assertFalse(metadata.getTableStatistics(session,
                metadata.getSysTableHandle(session, base, "log").orElseThrow(AssertionError::new))
                .isPresent());
    }

    /**
     * A primary-key table's log past the lake snapshot is a change stream — inserts, updates and deletes
     * against keys the lake already holds — so "the rows not yet in the lake" is not a thing it can
     * return. Serving it anyway would answer with change records dressed as rows.
     */
    @Test
    public void primaryKeyTableRefusesTheLogSystemTable() {
        RecordingFlussAdminOps adminOps = withLakeTable();
        TablePath pkTable = TablePath.of("db", "pk_table");
        adminOps.tableInfos.put(pkTable, FlussTestTables.builder(pkTable)
                .column("id", DataTypes.BIGINT())
                .column("name", DataTypes.STRING())
                .primaryKey("id")
                .buckets(2)
                .property("table.datalake.enabled", "true")
                .property("table.datalake.format", "paimon")
                .property("table.datalake.paimon.warehouse", "/lake/warehouse")
                .build());
        FlussConnectorMetadata metadata = metadata(adminOps);
        ConnectorTableHandle base = baseHandle(metadata, pkTable);

        // Still offered, so the failure below is what the user meets rather than "no such table".
        Assertions.assertEquals(Arrays.asList("lake", "log"),
                metadata.listSupportedSysTables(session, base));
        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getSysTableHandle(session, base, "log"));
        // The message has to carry both ways out, or the user is left with a refusal and no alternative.
        Assertions.assertTrue(e.getMessage().contains("primary key"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains("pk_table"), e.getMessage());
        Assertions.assertTrue(e.getMessage().contains(FlussCatalogProperties.UNION_READ_MODE),
                e.getMessage());
    }

    @Test
    public void tableWithoutALakeRefusesTheLogSystemTable() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle base = baseHandle(metadata, PLAIN_TABLE);

        DorisConnectorException e = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getSysTableHandle(session, base, "log"));
        Assertions.assertTrue(e.getMessage().contains("table.datalake.enabled"), e.getMessage());
    }

    @Test
    public void theLakeHandleIsTheSiblingsHandleForTheSameName() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle handle = lakeHandle(metadata);

        // The lake table lives under the SAME db.table name in paimon — that is where fluss's tiering
        // service writes it. A handle of the sibling's own type is also what routes every later call.
        Assertions.assertTrue(handle instanceof RecordingLakeSibling.Handle);
        Assertions.assertEquals("db", ((RecordingLakeSibling.Handle) handle).dbName);
        Assertions.assertEquals("lake_table", ((RecordingLakeSibling.Handle) handle).tableName);
        Assertions.assertEquals(1, builtSiblings.size(), "one lake configuration, one sibling");
        Assertions.assertEquals(Collections.singletonList("getTableHandle:db.lake_table"),
                builtSiblings.get(0).calls);
    }

    @Test
    public void theSiblingIsConfiguredFromTheTablesLakeProperties() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        lakeHandle(metadata);

        Map<String, String> expected = new HashMap<>();
        expected.put("paimon.catalog.type", "filesystem");
        expected.put("warehouse", "/lake/warehouse");
        // Whole-map equality: the sibling is a real catalog and a leaked fluss property would either be
        // rejected by it or silently reinterpreted as a paimon option.
        Assertions.assertEquals(expected, builtSiblings.get(0).properties);
    }

    @Test
    public void anUnknownSystemTableNameIsNotServed() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        // Only "lake" is this connector's; asking for anything else must not build a sibling.
        Assertions.assertEquals(Optional.empty(),
                metadata.getSysTableHandle(session, baseHandle(metadata, LAKE_TABLE), "snapshots"));
        Assertions.assertTrue(builtSiblings.isEmpty());
    }

    @Test
    public void tableWithoutALakeFailsLoudOnTheLakeHandle() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        // Reachable when the table's lake is turned off between the listing and the resolution: two round
        // trips, so the gate is re-checked rather than assumed.
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getSysTableHandle(session, baseHandle(metadata, PLAIN_TABLE), "lake"));
        Assertions.assertTrue(failure.getMessage().contains("table.datalake.enabled"),
                failure.getMessage());
    }

    @Test
    public void anUnsupportedLakeFormatFailsLoud() {
        FlussConnectorMetadata metadata = metadata(withLakeTable("iceberg"));
        // The reason $lake is offered for ANY lake format: this precise message beats fe-core's generic
        // "no such table" for a user whose table is tiered to a lake this connector cannot read yet.
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getSysTableHandle(session, baseHandle(metadata, LAKE_TABLE), "lake"));
        Assertions.assertTrue(failure.getMessage().contains("iceberg"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("paimon"), failure.getMessage());
        Assertions.assertTrue(builtSiblings.isEmpty(), "an unreadable format must not build a catalog");
    }

    @Test
    public void anUntieredLakeTableFailsLoud() {
        FlussConnectorMetadata metadata = new FlussConnectorMetadata(withLakeTable(),
                FlussTypeMapping.Options.DEFAULT,
                properties -> {
                    RecordingLakeSibling sibling = new RecordingLakeSibling(properties);
                    sibling.lakeTableExists = false;
                    builtSiblings.add(sibling);
                    return sibling;
                },
                handle -> null);

        // The lake table is only created on the tiering service's first commit. Answering "no such table"
        // would send the user after a name that is in fact correct; naming tiering is the actionable form.
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getSysTableHandle(session, baseHandle(metadata, LAKE_TABLE), "lake"));
        Assertions.assertTrue(failure.getMessage().contains("tiering"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains("db.lake_table"), failure.getMessage());
    }

    @Test
    public void siblingThatDisownsItsHandleFailsLoud() {
        // Connector.ownsHandle defaults to false, so a connector that never overrode it disowns the handles
        // it just produced. Every guard on this side then silently fails open and the first cast throws a
        // ClassCastException naming two class loaders and no cause -- which is exactly what the paimon
        // connector did the first time it was used as a sibling for real. Caught where the handle is born,
        // the message names the class that has to change.
        FlussConnectorMetadata metadata = new FlussConnectorMetadata(withLakeTable(),
                FlussTypeMapping.Options.DEFAULT,
                properties -> {
                    RecordingLakeSibling sibling = new RecordingLakeSibling(properties);
                    sibling.claimsItsOwnHandles = false;
                    builtSiblings.add(sibling);
                    return sibling;
                },
                handle -> null);

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> metadata.getSysTableHandle(session, baseHandle(metadata, LAKE_TABLE), "lake"));
        Assertions.assertTrue(failure.getMessage().contains("ownsHandle"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains(RecordingLakeSibling.class.getName()),
                failure.getMessage());
    }

    @Test
    public void theSchemaOfALakeTableComesFromTheSibling() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableSchema schema = metadata.getTableSchema(session, lakeHandle(metadata));

        // Fluss's own schema for this table is (id, name) with format FLUSS; every value here is the
        // sibling's, so the forward is what produced them.
        Assertions.assertEquals(RecordingLakeSibling.SCHEMA_TABLE_NAME, schema.getTableName());
        Assertions.assertEquals(RecordingLakeSibling.FORMAT_TYPE, schema.getTableFormatType());
        Assertions.assertEquals(1, schema.getColumns().size());
        Assertions.assertEquals(RecordingLakeSibling.COLUMN_NAME, schema.getColumns().get(0).getName());
    }

    @Test
    public void theColumnHandlesOfALakeTableComeFromTheSibling() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        Assertions.assertEquals(
                Collections.singleton(RecordingLakeSibling.COLUMN_NAME),
                metadata.getColumnHandles(session, lakeHandle(metadata)).keySet());
    }

    @Test
    public void thePartitionsOfALakeTableComeFromTheSibling() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle handle = lakeHandle(metadata);

        // Fluss's own answer for this (unpartitioned) table is an empty list, so a missing guard would
        // silently report "no partitions" for a partitioned lake table and read the whole thing.
        List<ConnectorPartitionInfo> partitions =
                metadata.listPartitions(session, handle, Optional.empty());
        Assertions.assertEquals(1, partitions.size());
        Assertions.assertEquals(RecordingLakeSibling.PARTITION_NAME, partitions.get(0).getPartitionName());

        Assertions.assertEquals(
                Collections.singletonList(RecordingLakeSibling.PARTITION_NAME),
                metadata.listPartitionNames(session, handle));
        // listPartitionNames must reach the sibling's own method, not derive names from listPartitions:
        // the sibling may answer the two differently (and more cheaply).
        Assertions.assertTrue(builtSiblings.get(0).calls.contains("listPartitionNames"));
    }

    @Test
    public void theStatisticsOfALakeTableComeFromTheSibling() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        // Fluss's own path would call getTableStats on the fluss table (an unprogrammed recording call,
        // which throws), so this asserts the forward AND that fluss's remote call never happened.
        Assertions.assertEquals(RecordingLakeSibling.ROW_COUNT,
                metadata.getTableStatistics(session, lakeHandle(metadata))
                        .orElseThrow(AssertionError::new).getRowCount());
    }

    @Test
    public void systemTableQuestionsAboutALakeTableGoToTheSibling() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle handle = lakeHandle(metadata);

        // A lake handle reaching fluss's own bodies would ClassCastException, so these guards are about
        // not crashing as much as about delegating: paimon owns its table's system tables.
        Assertions.assertEquals(Collections.singletonList("snapshots"),
                metadata.listSupportedSysTables(session, handle));
        Assertions.assertEquals(Optional.empty(),
                metadata.getSysTableHandle(session, handle, "snapshots"));
        Assertions.assertTrue(metadata.isPartitionValuesSysTable(session, handle, "partitions"));
    }

    @Test
    public void flussHandleStillGetsFlussAnswers() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        // Build the sibling first, so the guards have a sibling to (wrongly) route to if they matched on
        // anything other than handle ownership.
        lakeHandle(metadata);

        ConnectorTableSchema schema = metadata.getTableSchema(session, baseHandle(metadata, LAKE_TABLE));
        Assertions.assertEquals("lake_table", schema.getTableName());
        Assertions.assertEquals("FLUSS", schema.getTableFormatType());
        Assertions.assertEquals(2, schema.getColumns().size());
    }

    @Test
    public void oneStatementBuildsOneSiblingMetadata() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle handle = lakeHandle(metadata);

        metadata.getTableSchema(session, handle);
        metadata.getColumnHandles(session, handle);
        metadata.listPartitions(session, handle, Optional.empty());
        metadata.getTableStatistics(session, handle);

        // The per-statement funnel: the sibling's metadata is built once and every forward reuses it,
        // exactly as fe-core's own funnel does for a plain connector. Rebuilding per call would re-open
        // whatever that metadata memoizes (a schema, a snapshot) and let one statement see two versions.
        Assertions.assertEquals(1, builtSiblings.get(0).metadataBuilds);
    }

    @Test
    public void twoStatementsDoNotShareTheSiblingMetadata() {
        FlussConnectorMetadata metadata = metadata(withLakeTable());
        ConnectorTableHandle handle = lakeHandle(metadata);
        metadata.getTableSchema(session, handle);

        // A different statement scope must not reuse the previous statement's metadata: the funnel is
        // keyed inside the scope, so this proves the key is not accidentally global.
        ConnectorSession other = new FlussTestSession(7L, "q2");
        metadata.getTableSchema(other, handle);

        Assertions.assertEquals(2, builtSiblings.get(0).metadataBuilds);
    }

}
