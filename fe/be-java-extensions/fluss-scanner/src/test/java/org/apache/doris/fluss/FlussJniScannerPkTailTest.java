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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reading the log tail of a primary-key table — the fluss half of a union read — against a real
 * cluster in this JVM.
 *
 * <p>What this range means is narrower than the whole-bucket read next door, and the difference is the
 * point of every test here: the lake holds the bucket as it stood at the offset it was tiered at, so
 * this range owes exactly what has happened since, replayed into the state it ended in. A key nobody
 * touched in the tail must NOT come back — the lake already has it, and returning it again is a
 * duplicate row. A key updated in the tail must come back once, with the final value. A key deleted in
 * the tail must come back not at all, and that is not the same as never having been written: the lake
 * row it supersedes is hidden by a key set BE's C++ side builds from these same offsets, so the row
 * disappearing is the whole intent.
 *
 * <p>Named {@code ...Test}, not {@code ...ITCase}: surefire's default includes do not match
 * {@code *ITCase}, so that name would leave the class silently unexecuted with a green build.
 */
public class FlussJniScannerPkTailTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .build();

    /** One bucket, so every fixture row lands in the bucket the test reads the tail of. */
    private static final int BUCKETS = 1;

    /** A ceiling no fixture here comes near; the one test about the ceiling sets its own. */
    private static final long NO_CEILING_IN_PRACTICE = 1_000_000L;

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
        db = "doris_fluss_tail_test_" + (++databaseCounter);
        admin.createDatabase(db, DatabaseDescriptor.EMPTY, true).get();
    }

    // ---------------------------------------------------------------- what the tail owes

    /**
     * The three things at once, on one tail: a key the tail updated comes back once with the later
     * value, a key the tail deleted does not come back, a key the tail inserted comes back — and the
     * key nobody touched does not come back at all, because it is the lake's to return.
     *
     * <p>Returning the change log as it stands would give five rows for these three keys.
     */
    @Test
    public void tailComesBackAsTheStateItEndedIn() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_mixed");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"), row(3, "three"));
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, row(1, "one-updated"));
        delete(tablePath, row(2, "two"));
        upsert(tablePath, row(4, "four"));

        Object[][] rows = read(tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, latestOffset(tablePath), NO_CEILING_IN_PRACTICE), 1024);

        Assertions.assertArrayEquals(
                new Object[][] {{1, "one-updated"}, {4, "four"}}, sortById(rows));
    }

    /**
     * A key written only before the tail belongs to the lake half. Reading from the beginning of the
     * log instead of from where the lake left off returns it here too, and the query then has it twice.
     */
    @Test
    public void rowsBeforeTheTailAreLeftToTheLake() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_start");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"));
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, row(3, "three"));

        Object[][] rows = read(tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, latestOffset(tablePath), NO_CEILING_IN_PRACTICE), 1024);

        Assertions.assertArrayEquals(new Object[][] {{3, "three"}}, sortById(rows));
    }

    /**
     * The tail ends where planning said it ends. A write that lands after that belongs to the next
     * query — and here it would also disagree with the key set the lake half was filtered by, which was
     * built from the same two offsets.
     */
    @Test
    public void writesAfterTheStoppingOffsetAreNotRead() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_late_write");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"));
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, row(2, "two"));
        long stop = latestOffset(tablePath);
        upsert(tablePath, row(3, "three"));

        Object[][] rows = read(tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, stop, NO_CEILING_IN_PRACTICE), 1024);

        Assertions.assertArrayEquals(new Object[][] {{2, "two"}}, sortById(rows));
    }

    /**
     * A tail that only deletes returns nothing, and says so: the deleted key is counted, because the
     * lake row it hides is dropped on the strength of this range having seen the key. A tail that
     * quietly returned no rows and counted nothing would look exactly like a tail that read nothing at
     * all.
     *
     * <p>The record count is asserted too — an update of an existing key is logged as a pair, a delete
     * as one record. A fluss upgrade that changes the shape of the change log should land here.
     */
    @Test
    public void keysDeletedInTheTailAreCountedRatherThanReturned() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_delete");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"));
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, row(1, "one-updated"));
        delete(tablePath, row(2, "two"));

        Map<String, String> params = tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, latestOffset(tablePath), NO_CEILING_IN_PRACTICE);
        Map<String, String> statistics = new HashMap<>();
        Object[][] rows = read(params, 1024, statistics);

        Assertions.assertArrayEquals(new Object[][] {{1, "one-updated"}}, sortById(rows));
        Assertions.assertEquals("1", statistics.get("counter:FlussJniTailTombstoneKeys"));
        Assertions.assertEquals("3", statistics.get("counter:FlussJniTailRecordsRead"),
                "an update is two change log records and a delete is one");
    }

    // ---------------------------------------------------------------- projection and shape

    /**
     * Replaying needs the primary key whether or not the query selects it, so the reader asks fluss for
     * it and does not return it. Getting that wrong shows up as an extra column — or, if the key were
     * simply not fetched, as two rows for one key.
     */
    @Test
    public void projectionExcludingThePrimaryKeyKeepsTheRequestedShape() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_projection");
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
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, GenericRow.of(1, 11, BinaryString.fromString("y")));
        upsert(tablePath, GenericRow.of(1, 12, BinaryString.fromString("z")));

        Object[][] rows = read(tailParams(tablePath, columns("b", "string", "a", "int"),
                lakeEnd, latestOffset(tablePath), NO_CEILING_IN_PRACTICE), 1024);

        Assertions.assertEquals(1, rows.length);
        Assertions.assertArrayEquals(new Object[] {"z", 12}, rows[0]);
    }

    /**
     * A query can need no column of this scanner at all — {@code select dt, count(*) ... group by dt}
     * over a partitioned table — and the count still has to be of the replayed rows. Counting change
     * log records instead reports four where the tail contributes two.
     */
    @Test
    public void projectingNoColumnStillCountsTheReplayedRows() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_count_only");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"), row(2, "two"));
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, row(1, "one-updated"));
        delete(tablePath, row(2, "two"));
        upsert(tablePath, row(3, "three"));

        Map<String, String> params = tailParams(tablePath, columns(), lakeEnd,
                latestOffset(tablePath), NO_CEILING_IN_PRACTICE);
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

    /** More surviving rows than fit in one batch: the reader has to resume where it stopped. */
    @Test
    public void replayedRowsSpanSeveralBatches() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_batches");
        createPkTable(tablePath);
        upsert(tablePath, row(0, "v0"));
        long lakeEnd = latestOffset(tablePath);
        GenericRow[] tail = new GenericRow[50];
        for (int i = 1; i <= 50; i++) {
            tail[i - 1] = row(i, "v" + i);
        }
        upsert(tablePath, tail);

        Object[][] rows = read(tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, latestOffset(tablePath), NO_CEILING_IN_PRACTICE), 7);

        Object[][] sorted = sortById(rows);
        Assertions.assertEquals(50, sorted.length);
        for (int i = 1; i <= 50; i++) {
            Assertions.assertArrayEquals(new Object[] {i, "v" + i}, sorted[i - 1], "row " + i);
        }
    }

    /**
     * A partitioned table's tail is a tail of one partition's bucket. Subscribing without the partition
     * reads a different tablet altogether, and the lake half it is joined to is this partition's.
     */
    @Test
    public void partitionedTailIsReadThroughItsPartitionBucket() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_part");
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
        long lakeEnd = partitionOffset(tablePath, "2026_08_03");
        upsert(tablePath,
                GenericRow.of(1, BinaryString.fromString("2026_08_03"),
                        BinaryString.fromString("a-updated")),
                GenericRow.of(3, BinaryString.fromString("2026_08_04"),
                        BinaryString.fromString("c")));

        // The partition column is not read here: FE declares it and BE materializes it from the range.
        Map<String, String> params = tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, partitionOffset(tablePath, "2026_08_03"), NO_CEILING_IN_PRACTICE);
        params.put("fluss.partition_id", String.valueOf(partitionId));

        Object[][] rows = read(params, 1024);

        Assertions.assertArrayEquals(new Object[][] {{1, "a-updated"}}, sortById(rows));
    }

    // ---------------------------------------------------------------- what must fail loudly

    /**
     * The tail is held in memory to be replayed, in a process shared by every query on this BE. A tail
     * that outgrew its ceiling — tiering stopped, say — has to say so rather than read on.
     */
    @Test
    public void tailBiggerThanItsCeilingIsRefused() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_ceiling");
        createPkTable(tablePath);
        upsert(tablePath, row(1, "one"));
        long lakeEnd = latestOffset(tablePath);
        upsert(tablePath, row(2, "two"), row(3, "three"), row(4, "four"));

        Map<String, String> params = tailParams(tablePath, columns("id", "int", "name", "string"),
                lakeEnd, latestOffset(tablePath), 2L);

        Exception failure = Assertions.assertThrows(Exception.class, () -> read(params, 1024));
        Assertions.assertTrue(messageChain(failure).contains("fluss.union_read.max_tail_rows"),
                "the ceiling that was hit should be named, but the failure was: "
                        + messageChain(failure));
    }

    /**
     * An empty tail is not something planning produces — a bucket whose lake has caught up contributes
     * no range at all. One arriving means the two halves were bounded by different offsets, which is a
     * duplicated or a missing row either way, and reading nothing would hide it.
     */
    @Test
    public void emptyTailRangeIsRefused() {
        Map<String, String> params = tailParams(TablePath.of(db, "tail_empty"),
                columns("id", "int"), 7L, 7L, NO_CEILING_IN_PRACTICE);

        Exception failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FlussJniScanner(1024, params));
        Assertions.assertTrue(failure.getMessage().contains("must read something"), failure.getMessage());
    }

    /**
     * A log table has no key to replay by, and its records are rows in their own right. A tail range
     * pointed at one means planning produced the wrong kind of range; replaying it by "the key" would
     * mean inventing one.
     */
    @Test
    public void tailOfALogTableIsRefused() throws Exception {
        TablePath tablePath = TablePath.of(db, "tail_log_table");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build())
                .distributedBy(BUCKETS)
                .build(), true).get();
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(row(1, "one"));
            writer.flush();
        }

        Map<String, String> params = tailParams(tablePath, columns("id", "int", "name", "string"),
                0L, latestOffset(tablePath), NO_CEILING_IN_PRACTICE);

        Exception failure = Assertions.assertThrows(Exception.class, () -> read(params, 1024));
        Assertions.assertTrue(messageChain(failure).contains("no primary key"),
                "the failure should name what is missing, but it was: " + messageChain(failure));
    }

    /** A ceiling that cannot bound anything is a configuration mistake, not a tail of size zero. */
    @Test
    public void nonPositiveCeilingIsRefused() {
        Map<String, String> params = tailParams(TablePath.of(db, "tail_bad_ceiling"),
                columns("id", "int"), 0L, 7L, 0L);

        Exception failure = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FlussJniScanner(1024, params));
        Assertions.assertTrue(failure.getMessage().contains("fluss.union.max_tail_rows"),
                failure.getMessage());
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

    /** Where bucket 0's log has got to — what planning uses to bound a tail. */
    private static long latestOffset(TablePath tablePath) throws Exception {
        return admin.listOffsets(tablePath, Collections.singletonList(0), new OffsetSpec.LatestSpec())
                .all().get().get(0);
    }

    private static long partitionOffset(TablePath tablePath, String partitionName) throws Exception {
        return admin.listOffsets(tablePath, partitionName, Collections.singletonList(0),
                new OffsetSpec.LatestSpec()).all().get().get(0);
    }

    private static long partitionId(TablePath tablePath, String partitionName) throws Exception {
        return admin.listPartitionInfos(tablePath).get().stream()
                .filter(p -> p.getPartitionName().equals(partitionName))
                .findFirst().orElseThrow(AssertionError::new)
                .getPartitionId();
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

    /**
     * The merged map BE hands the scanner for a tail range: the range's own keys, plus the scan-level
     * ceiling that only a union read of a primary-key table carries. No kv snapshot — the state before
     * the tail is the lake's, not a snapshot's.
     */
    private static Map<String, String> tailParams(TablePath tablePath, Map<String, String> columns,
            long logStartOffset, long logStopOffset, long maxTailRows) {
        Map<String, String> params = new HashMap<>(columns);
        params.put("fluss.client.bootstrap.servers", bootstrapServers);
        params.put("fluss.db_name", tablePath.getDatabaseName());
        params.put("fluss.table_name", tablePath.getTableName());
        params.put("fluss.range_type", "PK_TAIL");
        params.put("fluss.bucket_id", "0");
        params.put("fluss.log_start_offset", String.valueOf(logStartOffset));
        params.put("fluss.log_stop_offset", String.valueOf(logStopOffset));
        params.put("fluss.union.max_tail_rows", String.valueOf(maxTailRows));
        params.put("time_zone", "UTC");
        return params;
    }

    private static Object[][] read(Map<String, String> params, int batchSize) throws Exception {
        return read(params, batchSize, new HashMap<>());
    }

    /**
     * Drives the scanner the way BE does — batch by batch until it reports none left — and returns the
     * rows it produced, filling {@code statistics} with what the scanner counted.
     * {@code getMaterializedData} hands back COLUMN-major arrays, so this transposes; reading it as
     * rows would silently compare a column against a row.
     */
    private static Object[][] read(Map<String, String> params, int batchSize,
            Map<String, String> statistics) throws Exception {
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
            statistics.putAll(scanner.getStatistics());
        } finally {
            scanner.releaseTable();
            scanner.close();
        }
        return allRows.toArray(new Object[0][]);
    }

    /** Every message down the cause chain, because open() wraps what the reader threw. */
    private static String messageChain(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            messages.append(cause.getMessage()).append(" | ");
        }
        return messages.toString();
    }

    /**
     * Sorted by the first column, which every fixture here makes the primary key. Replay returns rows
     * in the order their keys were first seen in the tail, which is fluss's write order, not an order
     * this reader promises.
     */
    private static Object[][] sortById(Object[][] rows) {
        Object[][] sorted = Arrays.copyOf(rows, rows.length);
        Arrays.sort(sorted, Comparator.comparingInt(row -> (Integer) row[0]));
        return sorted;
    }
}
