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

package org.apache.doris.fluss;

import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.VectorTable;

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reading a fluss primary-key table through the scanner, against a real cluster in this JVM.
 *
 * <p>What makes this different from the log test is that the answer is not what was written — it is
 * what the writes add up to. A key upserted twice must come back once, with the later value; a deleted
 * key must not come back at all; and none of that is visible in the change log the table stores, which
 * holds every intermediate state. This is the first line of defence for that merge: a scanner that got
 * it wrong would return extra rows that look entirely plausible one at a time.
 *
 * <p>Snapshots are <em>triggered</em>, not waited for. The cluster's periodic interval is ten minutes,
 * so a bucket has a snapshot only where a test asked for one, which is what makes "these rows came out
 * of the snapshot and those out of the log after it" a fact rather than a race.
 *
 * <p>Named {@code ...Test}, not {@code ...ITCase}: surefire's default includes do not match
 * {@code *ITCase}, so that name would leave the class silently unexecuted with a green build.
 */
public class FlussJniScannerPkTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .build();

    /** One bucket, so every fixture row lands where the test triggers its snapshot. */
    private static final int BUCKETS = 1;

    /** {@code fluss.kv_snapshot_id} for a bucket fluss has never snapshotted. */
    private static final long NO_SNAPSHOT = -1L;

    /** Fluss's "start at the beginning of the log" sentinel. */
    private static final long EARLIEST = -2L;

    private static int databaseCounter;

    private static Connection connection;
    private static Admin admin;
    private static String bootstrapServers;

    private String db;

    @BeforeAll
    public static void connectToCluster() {
        OffHeap.setTesting();
        bootstrapServers = FLUSS_CLUSTER.getBootstrapServers();
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterAll
    public static void disconnect() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @BeforeEach
    public void createDatabase() throws Exception {
        // The cluster extension drops every non-built-in database after each test.
        db = "doris_fluss_pk_test_" + (++databaseCounter);
        admin.createDatabase(db, DatabaseDescriptor.EMPTY, true).get();
    }

    // ---------------------------------------------------------------- the merged view

    /**
     * With no snapshot the whole state has to be rebuilt from the change log, and the change log is
     * exactly what must not be returned as-is: these five writes produce an insert, an update (which
     * fluss logs as two records) and a delete, and the answer is two rows.
     */
    @Test
    public void changeLogAloneStillYieldsTheMergedView() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_log_only");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"), row(3, "three"));
        upsert(tablePath, row(2, "two-updated"));
        delete(tablePath, row(3, "three"));

        Object[][] rows = readMerged(tablePath, NO_SNAPSHOT, EARLIEST,
                columns("id", "int", "name", "string"));

        Assertions.assertArrayEquals(
                new Object[][] {{1, "one"}, {2, "two-updated"}}, sortById(rows));
    }

    /**
     * The two halves have to be merged, not concatenated: a key the snapshot holds and the log updates
     * must come back once with the log's value, and a key the log deletes must not come back at all
     * even though the snapshot still has it.
     */
    @Test
    public void logAfterASnapshotOverridesAndDeletesWhatTheSnapshotHolds() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_snapshot_and_log");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"), row(3, "three"));
        long snapshotId = snapshot(tablePath);
        upsert(tablePath, row(2, "two-after-snapshot"), row(4, "four"));
        delete(tablePath, row(3, "three"));

        Object[][] rows = readMerged(tablePath, snapshotId, snapshotLogOffset(tablePath),
                columns("id", "int", "name", "string"));

        Assertions.assertArrayEquals(new Object[][] {
                {1, "one"}, {2, "two-after-snapshot"}, {4, "four"}}, sortById(rows));
    }

    /** Nothing written since the snapshot: the snapshot alone is the whole answer. */
    @Test
    public void snapshotWithNoLogTailIsReadOnItsOwn() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_snapshot_only");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"));
        long snapshotId = snapshot(tablePath);
        long logOffset = snapshotLogOffset(tablePath);

        // Start and stop meet, which is what planning produces for a bucket whose log has caught up.
        Object[][] rows = read(pkParams(tablePath, columns("id", "int", "name", "string"),
                snapshotId, logOffset, logOffset), 1024);

        Assertions.assertArrayEquals(new Object[][] {{1, "one"}, {2, "two"}}, sortById(rows));
    }

    /**
     * Rows are compared to the state at the stopping offset, not to the state now. A write that lands
     * after planning belongs to the next query; letting it through would make two scans of the same
     * plan disagree.
     */
    @Test
    public void writesAfterTheStoppingOffsetAreNotRead() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_late_write");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"));
        long stopOffset = latestOffset(tablePath);
        upsert(tablePath, row(2, "two-later"), row(3, "three"));

        Object[][] rows = read(pkParams(tablePath, columns("id", "int", "name", "string"),
                NO_SNAPSHOT, EARLIEST, stopOffset), 1024);

        Assertions.assertArrayEquals(new Object[][] {{1, "one"}, {2, "two"}}, sortById(rows));
    }

    // ---------------------------------------------------------------- values

    /**
     * Every mapped type, half of it read out of a kv snapshot and half out of the change log after it.
     * The two are stored in different row formats, and a value wrapper that only handles one of them
     * would still pass a test that read a single row from a single place.
     */
    @Test
    public void everyMappedTypeSurvivesBothHalvesOfTheMerge() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_all_types");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("c_boolean", DataTypes.BOOLEAN())
                        .column("c_tinyint", DataTypes.TINYINT())
                        .column("c_smallint", DataTypes.SMALLINT())
                        .column("c_bigint", DataTypes.BIGINT())
                        .column("c_float", DataTypes.FLOAT())
                        .column("c_double", DataTypes.DOUBLE())
                        .column("c_char", DataTypes.CHAR(5))
                        .column("c_string", DataTypes.STRING())
                        .column("c_decimal", DataTypes.DECIMAL(20, 4))
                        .column("c_date", DataTypes.DATE())
                        .column("c_timestamp", DataTypes.TIMESTAMP(6))
                        .column("c_timestamp_ltz", DataTypes.TIMESTAMP_LTZ(6))
                        .column("c_array", DataTypes.ARRAY(DataTypes.INT()))
                        .column("c_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column("c_struct", DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING())))
                        .primaryKey("id")
                        .build())
                .distributedBy(BUCKETS, "id")
                .build(), true).get();

        LocalDateTime timestamp = LocalDateTime.of(2026, 8, 3, 12, 34, 56, 123456000);
        Instant instant = Instant.parse("2026-08-03T04:34:56.123456Z");
        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("k1"), 1);
        upsert(tablePath, GenericRow.of(1, true, (byte) 1, (short) 2, 4L, 5.5f, 6.5d,
                BinaryString.fromString("abcde"), BinaryString.fromString("hello"),
                Decimal.fromBigDecimal(new BigDecimal("12345.6789"), 20, 4),
                (int) LocalDate.of(2026, 8, 3).toEpochDay(),
                TimestampNtz.fromLocalDateTime(timestamp),
                TimestampLtz.fromInstant(instant),
                new GenericArray(new int[] {10, 20}), new GenericMap(map),
                GenericRow.of(7, BinaryString.fromString("inner"))));
        long snapshotId = snapshot(tablePath);
        // Row 2 goes only into the log after the snapshot, and is all-null besides its key: a column
        // read through the wrong accessor often produces a plausible value for real data and only
        // misbehaves on null.
        upsert(tablePath, GenericRow.of(2, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null));

        Object[][] rows = readMerged(tablePath, snapshotId, snapshotLogOffset(tablePath), columns(
                "id", "int",
                "c_boolean", "boolean",
                "c_tinyint", "tinyint",
                "c_smallint", "smallint",
                "c_bigint", "bigint",
                "c_float", "float",
                "c_double", "double",
                "c_char", "char(5)",
                "c_string", "string",
                "c_decimal", "decimal(20,4)",
                "c_date", "datev2",
                "c_timestamp", "datetimev2(6)",
                "c_timestamp_ltz", "timestamptz(6)",
                "c_array", "array<int>",
                "c_map", "map<string,int>",
                "c_struct", "struct<a:int,b:string>"));

        Object[][] sorted = sortById(rows);
        Assertions.assertEquals(2, sorted.length);
        Object[] fromSnapshot = sorted[0];
        Assertions.assertEquals(1, fromSnapshot[0]);
        Assertions.assertEquals(true, fromSnapshot[1]);
        Assertions.assertEquals((byte) 1, fromSnapshot[2]);
        Assertions.assertEquals((short) 2, fromSnapshot[3]);
        Assertions.assertEquals(4L, fromSnapshot[4]);
        Assertions.assertEquals(5.5f, fromSnapshot[5]);
        Assertions.assertEquals(6.5d, fromSnapshot[6]);
        Assertions.assertEquals("abcde", fromSnapshot[7]);
        Assertions.assertEquals("hello", fromSnapshot[8]);
        Assertions.assertEquals(new BigDecimal("12345.6789"), fromSnapshot[9]);
        Assertions.assertEquals(LocalDate.of(2026, 8, 3), fromSnapshot[10]);
        Assertions.assertEquals(timestamp, fromSnapshot[11]);
        // TIMESTAMPTZ is carried as the UTC wall clock of the instant, not the session zone's.
        Assertions.assertEquals(LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC),
                fromSnapshot[12]);
        Assertions.assertEquals("[10, 20]", String.valueOf(fromSnapshot[13]));
        Assertions.assertEquals("{k1=1}", String.valueOf(fromSnapshot[14]));
        Assertions.assertEquals("{a=7, b=inner}", String.valueOf(fromSnapshot[15]));

        Object[] fromLog = sorted[1];
        Assertions.assertEquals(2, fromLog[0]);
        for (int i = 1; i < fromLog.length; i++) {
            Assertions.assertNull(fromLog[i], "column " + i + " of the all-null row came back non-null");
        }
    }

    // ---------------------------------------------------------------- projection and shape

    /**
     * The merge needs the primary key even when the query does not ask for it, so the reader adds it
     * to what it fetches and projects it back out. Getting that wrong shows up as an extra column, or
     * as columns in the fetch order rather than the requested one.
     */
    @Test
    public void projectionExcludingThePrimaryKeyKeepsTheRequestedShape() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_projection");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .primaryKey("id")
                        .build())
                .distributedBy(BUCKETS, "id")
                .build(), true).get();
        upsert(tablePath, GenericRow.of(1, 10, BinaryString.fromString("x")));
        long snapshotId = snapshot(tablePath);
        upsert(tablePath, GenericRow.of(1, 11, BinaryString.fromString("y")));

        Object[][] rows = readMerged(tablePath, snapshotId, snapshotLogOffset(tablePath),
                columns("b", "string", "a", "int"));

        Assertions.assertEquals(1, rows.length);
        Assertions.assertArrayEquals(new Object[] {"y", 11}, rows[0]);
    }

    /**
     * A query can need no column of this scanner at all — {@code select dt, count(*) ... group by dt}
     * over a partitioned table — and the row count still has to be the merged one. Counting change log
     * records instead would report four rows where the table holds two.
     */
    @Test
    public void projectingNoColumnStillCountsTheMergedRows() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_count_only");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"));
        upsert(tablePath, row(1, "one-updated"));
        delete(tablePath, row(2, "two"));
        upsert(tablePath, row(3, "three"));

        Map<String, String> params = pkParams(tablePath, columns(), NO_SNAPSHOT, EARLIEST,
                latestOffset(tablePath));
        int rows = 0;
        FlussJniScanner scanner = new FlussJniScanner(1024, params);
        try {
            scanner.open();
            while (scanner.getNextBatchMeta() != 0) {
                rows += scanner.getTable().getNumRows();
                scanner.resetTable();
            }
        } finally {
            scanner.releaseTable();
            scanner.close();
        }

        Assertions.assertEquals(2, rows);
    }

    /** More merged rows than fit in one batch: the reader has to resume where it stopped. */
    @Test
    public void mergedRowsSpanSeveralBatches() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_batches");
        createPkTable(tablePath);
        GenericRow[] first = new GenericRow[50];
        for (int i = 0; i < 50; i++) {
            first[i] = row(i, "v" + i);
        }
        upsert(tablePath, first);
        long snapshotId = snapshot(tablePath);
        GenericRow[] second = new GenericRow[50];
        for (int i = 50; i < 100; i++) {
            second[i - 50] = row(i, "v" + i);
        }
        upsert(tablePath, second);

        Object[][] rows = read(pkParams(tablePath, columns("id", "int", "name", "string"),
                snapshotId, snapshotLogOffset(tablePath), latestOffset(tablePath)), 7);

        Object[][] sorted = sortById(rows);
        Assertions.assertEquals(100, sorted.length);
        for (int i = 0; i < 100; i++) {
            Assertions.assertArrayEquals(new Object[] {i, "v" + i}, sorted[i], "row " + i);
        }
    }

    /**
     * A partitioned primary-key table is snapshotted per partition, and the scanner has to subscribe to
     * the partition's bucket rather than the table's. Subscribing without the partition reads a
     * different tablet altogether.
     */
    @Test
    public void partitionedTableIsReadThroughItsPartitionBucket() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_part");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("dt", DataTypes.STRING().copy(false))
                        .column("name", DataTypes.STRING())
                        .primaryKey("id", "dt")
                        .build())
                .partitionedBy("dt")
                .distributedBy(BUCKETS, "id")
                .build(), true).get();
        admin.createPartition(tablePath,
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_03")), true).get();
        admin.createPartition(tablePath,
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_04")), true).get();
        long partitionId = partitionId(tablePath, "2026_08_03");
        upsert(tablePath,
                GenericRow.of(1, BinaryString.fromString("2026_08_03"), BinaryString.fromString("a")),
                GenericRow.of(2, BinaryString.fromString("2026_08_04"), BinaryString.fromString("b")));
        FLUSS_CLUSTER.triggerAndWaitSnapshot(new TableBucket(tableId(tablePath), partitionId, 0));
        upsert(tablePath,
                GenericRow.of(1, BinaryString.fromString("2026_08_03"),
                        BinaryString.fromString("a-updated")));

        KvSnapshots snapshots = admin.getLatestKvSnapshots(tablePath, "2026_08_03").get();
        long stop = admin.listOffsets(tablePath, "2026_08_03", Collections.singletonList(0),
                new OffsetSpec.LatestSpec()).all().get().get(0);
        // The partition column is not read here: FE declares it and BE materializes it from the range.
        Map<String, String> params = pkParams(tablePath, columns("id", "int", "name", "string"),
                snapshots.getSnapshotId(0).orElse(NO_SNAPSHOT),
                snapshots.getLogOffset(0).orElse(EARLIEST), stop);
        params.put("fluss.partition_id", String.valueOf(partitionId));

        Object[][] rows = read(params, 1024);

        // Only this partition's row, and with the update applied.
        Assertions.assertArrayEquals(new Object[][] {{1, "a-updated"}}, sortById(rows));
    }

    /** A snapshot id pointing at a snapshot that does not exist must fail, not read an empty table. */
    @Test
    public void unknownSnapshotIdFailsRatherThanReadingNothing() throws Exception {
        TablePath tablePath = TablePath.of(db, "pk_bad_snapshot");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"));

        Map<String, String> params = pkParams(tablePath, columns("id", "int", "name", "string"),
                999L, 0L, latestOffset(tablePath));

        Assertions.assertThrows(Exception.class, () -> read(params, 1024));
    }

    // ---------------------------------------------------------------- helpers

    private void createPkTable(TablePath tablePath) throws Exception {
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT().copy(false))
                        .column("name", DataTypes.STRING())
                        .primaryKey("id")
                        .build())
                .distributedBy(BUCKETS, "id")
                .build(), true).get();
    }

    private static GenericRow row(int id, String name) {
        return GenericRow.of(id, BinaryString.fromString(name));
    }

    private static void upsert(TablePath tablePath, GenericRow... rows) throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            for (GenericRow row : rows) {
                writer.upsert(row);
            }
            writer.flush();
        }
    }

    private static void delete(TablePath tablePath, GenericRow row) throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.delete(row);
            writer.flush();
        }
    }

    /** Takes a kv snapshot of bucket 0 and returns its id. */
    private static long snapshot(TablePath tablePath) throws Exception {
        FLUSS_CLUSTER.triggerAndWaitSnapshot(new TableBucket(tableId(tablePath), 0));
        return admin.getLatestKvSnapshots(tablePath).get().getSnapshotId(0)
                .orElseThrow(() -> new AssertionError("the trigger produced no snapshot"));
    }

    private static long snapshotLogOffset(TablePath tablePath) throws Exception {
        return admin.getLatestKvSnapshots(tablePath).get().getLogOffset(0)
                .orElseThrow(() -> new AssertionError("the bucket has no snapshot"));
    }

    private static long latestOffset(TablePath tablePath) throws Exception {
        return admin.listOffsets(tablePath, Collections.singletonList(0), new OffsetSpec.LatestSpec())
                .all().get().get(0);
    }

    private static long tableId(TablePath tablePath) throws Exception {
        return admin.getTableInfo(tablePath).get().getTableId();
    }

    private static long partitionId(TablePath tablePath, String partitionName) throws Exception {
        return admin.listPartitionInfos(tablePath).get().stream()
                .filter(p -> p.getPartitionName().equals(partitionName))
                .findFirst().orElseThrow(AssertionError::new)
                .getPartitionId();
    }

    /** Reads the whole bucket up to where its log has got to, the way a planned range would. */
    private static Object[][] readMerged(TablePath tablePath, long snapshotId, long logStartOffset,
            Map<String, String> columns) throws Exception {
        return read(pkParams(tablePath, columns, snapshotId, logStartOffset, latestOffset(tablePath)),
                1024);
    }

    /** {@code name, dorisType, name, dorisType, ...} as the two params BE sends. */
    private static Map<String, String> columns(String... nameThenType) {
        List<String> names = new ArrayList<>();
        List<String> types = new ArrayList<>();
        for (int i = 0; i < nameThenType.length; i += 2) {
            names.add(nameThenType[i]);
            types.add(nameThenType[i + 1]);
        }
        Map<String, String> columns = new LinkedHashMap<>();
        columns.put("required_fields", String.join(",", names));
        columns.put("columns_types", String.join("#", types));
        return columns;
    }

    /** The merged map BE hands the scanner for a primary-key range. */
    private static Map<String, String> pkParams(TablePath tablePath, Map<String, String> columns,
            long snapshotId, long logStartOffset, long logStopOffset) {
        Map<String, String> params = new HashMap<>(columns);
        params.put("fluss.client.bootstrap.servers", bootstrapServers);
        params.put("fluss.db_name", tablePath.getDatabaseName());
        params.put("fluss.table_name", tablePath.getTableName());
        params.put("fluss.range_type", "PK_FULL");
        params.put("fluss.bucket_id", "0");
        params.put("fluss.kv_snapshot_id", String.valueOf(snapshotId));
        params.put("fluss.log_start_offset", String.valueOf(logStartOffset));
        params.put("fluss.log_stop_offset", String.valueOf(logStopOffset));
        params.put("time_zone", "UTC");
        return params;
    }

    /**
     * Drives the scanner the way BE does — batch by batch until it reports none left — and returns the
     * rows it produced. {@code getMaterializedData} hands back COLUMN-major arrays, so this transposes;
     * reading it as rows would silently compare a column against a row.
     */
    private static Object[][] read(Map<String, String> params, int batchSize) throws Exception {
        List<Object[]> allRows = new ArrayList<>();
        FlussJniScanner scanner = new FlussJniScanner(batchSize, params);
        try {
            scanner.open();
            while (scanner.getNextBatchMeta() != 0) {
                VectorTable table = scanner.getTable();
                Object[][] byColumn = table.getMaterializedData();
                int rows = table.getNumRows();
                for (int row = 0; row < rows; row++) {
                    Object[] values = new Object[byColumn.length];
                    for (int column = 0; column < byColumn.length; column++) {
                        values[column] = byColumn[column][row];
                    }
                    allRows.add(values);
                }
                scanner.resetTable();
            }
        } finally {
            scanner.releaseTable();
            scanner.close();
        }
        return allRows.toArray(new Object[0][]);
    }

    /**
     * Sorted by the first column, which every fixture here makes the primary key. The merge returns
     * rows in primary-key encoding order, which is not the natural order of the key's Java type, so
     * asserting the reader's order would be asserting fluss's key encoding.
     */
    private static Object[][] sortById(Object[][] rows) {
        Object[][] sorted = Arrays.copyOf(rows, rows.length);
        Arrays.sort(sorted, Comparator.comparingInt(row -> (Integer) row[0]));
        return sorted;
    }
}
