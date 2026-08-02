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
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The scanner against a real fluss cluster started in this JVM.
 *
 * <p>Everything here is end to end on purpose. The whole job of this class is to turn what a fluss
 * cluster actually stores into what Doris's vector table expects, and both ends of that are other
 * people's formats: a test that fed it hand-built rows would only check the conversion against our own
 * idea of fluss's encoding. So the fixtures are written through the fluss client — which puts them in
 * the real arrow log format — and read back through the scanner into a real off-heap vector table.
 *
 * <p>Named {@code ...Test} rather than {@code ...ITCase}: surefire's default includes do not match
 * {@code *ITCase}, so that name would leave the class silently unexecuted with a green build.
 */
public class FlussJniScannerLogTest {

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER = FlussClusterExtension.builder()
            .setNumOfTabletServers(1)
            .build();

    /** One bucket, so a fixture's rows land in a known place and the offsets are the row numbers. */
    private static final int BUCKETS = 1;

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
        db = "doris_fluss_scanner_test_" + (++databaseCounter);
        admin.createDatabase(db, DatabaseDescriptor.EMPTY, true).get();
    }

    // ---------------------------------------------------------------- values

    /**
     * Every type the connector maps, written by fluss and read back through the scanner, plus an
     * all-null row. The null row is not padding: a fluss column read through the wrong accessor often
     * still produces a plausible value for real data and only misbehaves on null, and a value that is
     * null on the way in but not on the way out is the failure this catches.
     */
    @Test
    public void everyMappedTypeSurvivesTheRoundTrip() throws Exception {
        TablePath tablePath = TablePath.of(db, "all_types");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("c_boolean", DataTypes.BOOLEAN())
                        .column("c_tinyint", DataTypes.TINYINT())
                        .column("c_smallint", DataTypes.SMALLINT())
                        .column("c_int", DataTypes.INT())
                        .column("c_bigint", DataTypes.BIGINT())
                        .column("c_float", DataTypes.FLOAT())
                        .column("c_double", DataTypes.DOUBLE())
                        .column("c_char", DataTypes.CHAR(5))
                        .column("c_string", DataTypes.STRING())
                        .column("c_decimal", DataTypes.DECIMAL(20, 4))
                        .column("c_date", DataTypes.DATE())
                        .column("c_timestamp", DataTypes.TIMESTAMP(6))
                        .column("c_timestamp_ltz", DataTypes.TIMESTAMP_LTZ(6))
                        .column("c_binary", DataTypes.BINARY(4))
                        .column("c_bytes", DataTypes.BYTES())
                        .build())
                .distributedBy(BUCKETS)
                .build(), true).get();

        LocalDateTime timestamp = LocalDateTime.of(2026, 8, 2, 12, 34, 56, 123456000);
        Instant instant = Instant.parse("2026-08-02T04:34:56.123456Z");
        appendRows(tablePath,
                GenericRow.of(true, (byte) 1, (short) 2, 3, 4L, 5.5f, 6.5d,
                        BinaryString.fromString("abcde"), BinaryString.fromString("hello"),
                        Decimal.fromBigDecimal(new BigDecimal("12345.6789"), 20, 4),
                        (int) LocalDate.of(2026, 8, 2).toEpochDay(),
                        TimestampNtz.fromLocalDateTime(timestamp),
                        TimestampLtz.fromInstant(instant),
                        new byte[] {1, 2, 3, 4}, new byte[] {9, 8}),
                GenericRow.of(null, null, null, null, null, null, null, null, null, null, null,
                        null, null, null, null));

        Object[][] rows = scanAll(tablePath, columns(
                "c_boolean", "boolean",
                "c_tinyint", "tinyint",
                "c_smallint", "smallint",
                "c_int", "int",
                "c_bigint", "bigint",
                "c_float", "float",
                "c_double", "double",
                "c_char", "char(5)",
                "c_string", "string",
                "c_decimal", "decimal(20,4)",
                "c_date", "datev2",
                "c_timestamp", "datetimev2(6)",
                "c_timestamp_ltz", "timestamptz(6)",
                "c_binary", "varbinary",
                "c_bytes", "varbinary"), 0, 2, 1024);

        Assertions.assertEquals(2, rows.length);
        Object[] row = rows[0];
        Assertions.assertEquals(true, row[0]);
        Assertions.assertEquals((byte) 1, row[1]);
        Assertions.assertEquals((short) 2, row[2]);
        Assertions.assertEquals(3, row[3]);
        Assertions.assertEquals(4L, row[4]);
        Assertions.assertEquals(5.5f, row[5]);
        Assertions.assertEquals(6.5d, row[6]);
        Assertions.assertEquals("abcde", row[7]);
        Assertions.assertEquals("hello", row[8]);
        Assertions.assertEquals(new BigDecimal("12345.6789"), row[9]);
        Assertions.assertEquals(LocalDate.of(2026, 8, 2), row[10]);
        Assertions.assertEquals(timestamp, row[11]);
        // TIMESTAMPTZ is carried as the UTC wall clock of the instant, not the session zone's.
        Assertions.assertEquals(LocalDateTime.ofInstant(instant, java.time.ZoneOffset.UTC), row[12]);
        // Content of the two binary columns is asserted separately, through the STRING mapping: the
        // VARBINARY read-BACK in java-common (VectorColumn.getBytesVarbinary) decodes a StringView
        // layout that nothing writes -- appendVarbinary and BE's _fill_varbinary_column both use
        // [len:long][addr:long] -- so it cannot be used to check a value. What it does show here is
        // that the column arrived non-null, and the all-null row below that it can be null.
        Assertions.assertNotNull(row[13]);
        Assertions.assertNotNull(row[14]);

        for (int i = 0; i < row.length; i++) {
            Assertions.assertNull(rows[1][i], "column " + i + " of the all-null row came back non-null");
        }
    }

    /**
     * Fluss BYTES maps to Doris STRING unless the catalog opts into VARBINARY, and BINARY is
     * fixed-width where BYTES is not — so which fluss accessor a column needs is a property of the
     * fluss type, not of the Doris one it was mapped to. Both are read here through the STRING
     * mapping, which is also the only read-back path that can check a byte value at all (see the
     * java-common note in the round-trip test above).
     */
    @Test
    public void binaryAndBytesReadAsStringsGiveTheirBytes() throws Exception {
        TablePath tablePath = TablePath.of(db, "bytes_as_string");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("c_bytes", DataTypes.BYTES())
                        .column("c_binary", DataTypes.BINARY(2))
                        .column("c_char", DataTypes.CHAR(2))
                        .build())
                .distributedBy(BUCKETS)
                .build(), true).get();
        appendRows(tablePath, GenericRow.of(new byte[] {(byte) 'h', (byte) 'i'},
                new byte[] {(byte) 'o', (byte) 'k'}, BinaryString.fromString("ab")));

        Object[][] rows = scanAll(tablePath,
                columns("c_bytes", "string", "c_binary", "string", "c_char", "string"), 0, 1, 1024);

        Assertions.assertArrayEquals(new Object[] {"hi", "ok", "ab"}, rows[0]);
    }

    @Test
    public void nestedTypesComeBackWithTheirStructure() throws Exception {
        TablePath tablePath = TablePath.of(db, "nested_types");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("c_array", DataTypes.ARRAY(DataTypes.INT()))
                        .column("c_map", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
                        .column("c_struct", DataTypes.ROW(
                                DataTypes.FIELD("a", DataTypes.INT()),
                                DataTypes.FIELD("b", DataTypes.STRING())))
                        .build())
                .distributedBy(BUCKETS)
                .build(), true).get();

        Map<Object, Object> map = new LinkedHashMap<>();
        map.put(BinaryString.fromString("k1"), 1);
        map.put(BinaryString.fromString("k2"), 2);
        appendRows(tablePath,
                GenericRow.of(new GenericArray(new int[] {10, 20, 30}), new GenericMap(map),
                        GenericRow.of(7, BinaryString.fromString("inner"))),
                // A shorter array right after a longer one: the value wrappers are cached by position
                // and reused across rows, so a stale wrapper would surface as a phantom fourth element.
                GenericRow.of(new GenericArray(new int[] {40}),
                        new GenericMap(Collections.singletonMap(BinaryString.fromString("k3"), 3)),
                        GenericRow.of(8, BinaryString.fromString("other"))));

        Object[][] rows = scanAll(tablePath, columns(
                "c_array", "array<int>",
                "c_map", "map<string,int>",
                "c_struct", "struct<a:int,b:string>"), 0, 2, 1024);

        Assertions.assertEquals(2, rows.length);
        Assertions.assertEquals("[10, 20, 30]", String.valueOf(rows[0][0]));
        Assertions.assertEquals("{k1=1, k2=2}", String.valueOf(rows[0][1]));
        Assertions.assertEquals("{a=7, b=inner}", String.valueOf(rows[0][2]));
        Assertions.assertEquals("[40]", String.valueOf(rows[1][0]));
        Assertions.assertEquals("{k3=3}", String.valueOf(rows[1][1]));
        Assertions.assertEquals("{a=8, b=other}", String.valueOf(rows[1][2]));
    }

    // ---------------------------------------------------------------- projection and bounds

    /**
     * The projection is by name, in the order Doris asked for — which is not the table's order. Passing
     * the table's own order through instead would silently transpose columns of the same type.
     */
    @Test
    public void projectionFollowsTheRequestedNamesAndOrder() throws Exception {
        TablePath tablePath = TablePath.of(db, "projection");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .build())
                .distributedBy(BUCKETS)
                .build(), true).get();
        appendRows(tablePath, GenericRow.of(1, 2, 3));

        Object[][] rows = scanAll(tablePath, columns("c", "int", "a", "int"), 0, 1, 1024);

        Assertions.assertEquals(1, rows.length);
        Assertions.assertArrayEquals(new Object[] {3, 1}, rows[0]);
    }

    /**
     * A query can ask for no column of this scanner at all: {@code select dt, count(*) ... group by dt}
     * over a partitioned table needs only the partition column, and BE materializes that one from the
     * range. Fluss refuses an empty projection, so the scanner has to stand something in for it and
     * still report how many rows the range held — answering 0 rows would turn a populated partition
     * into an empty one.
     */
    @Test
    public void projectingNoColumnStillCountsTheRows() throws Exception {
        TablePath tablePath = TablePath.of(db, "no_projection");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 7);

        Object[][] rows = scanAll(tablePath, columns(), 0, 7, 1024);

        Assertions.assertEquals(7, rows.length);
        Assertions.assertEquals(0, rows[0].length, "a column was returned for an empty projection");
    }

    /** The range is half-open: rows at or past the stopping offset belong to nobody's scan yet. */
    @Test
    public void theStoppingOffsetBoundsTheRead() throws Exception {
        TablePath tablePath = TablePath.of(db, "bounded");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 10);

        Object[][] rows = scanAll(tablePath, columns("id", "int"), 0, 4, 1024);

        Assertions.assertEquals(4, rows.length);
        Assertions.assertArrayEquals(new Object[] {0}, rows[0]);
        Assertions.assertArrayEquals(new Object[] {3}, rows[3]);
    }

    @Test
    public void theStartingOffsetSkipsWhatCameBefore() throws Exception {
        TablePath tablePath = TablePath.of(db, "start_offset");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 10);

        Object[][] rows = scanAll(tablePath, columns("id", "int"), 6, 10, 1024);

        Assertions.assertEquals(4, rows.length);
        Assertions.assertArrayEquals(new Object[] {6}, rows[0]);
        Assertions.assertArrayEquals(new Object[] {9}, rows[3]);
    }

    /** Rows written after planning are past the stopping offset and must not leak into the scan. */
    @Test
    public void rowsWrittenAfterPlanningAreNotRead() throws Exception {
        TablePath tablePath = TablePath.of(db, "late_rows");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 5);
        // Planning would have stopped at 5; these arrive afterwards, inside the same fetched batch range.
        appendInts(tablePath, 100, 105);

        Object[][] rows = scanAll(tablePath, columns("id", "int"), 0, 5, 1024);

        Assertions.assertEquals(5, rows.length);
        Assertions.assertArrayEquals(new Object[] {4}, rows[4]);
    }

    /** Doris pulls fixed-size batches; the range's rows must arrive whole across several of them. */
    @Test
    public void rangeLargerThanOneBatchIsReadAcrossBatches() throws Exception {
        TablePath tablePath = TablePath.of(db, "batched");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 25);

        Object[][] rows = scanAll(tablePath, columns("id", "int"), 0, 25, 10);

        Assertions.assertEquals(25, rows.length);
        for (int i = 0; i < 25; i++) {
            Assertions.assertArrayEquals(new Object[] {i}, rows[i], "row " + i);
        }
    }

    /**
     * A partitioned table is addressed by partition id plus bucket. Subscribing without the partition
     * would read the wrong bucket entirely.
     */
    @Test
    public void partitionedTableIsReadThroughItsPartitionId() throws Exception {
        TablePath tablePath = TablePath.of(db, "partitioned");
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("dt", DataTypes.STRING())
                        .build())
                .partitionedBy("dt")
                .distributedBy(BUCKETS)
                .build(), true).get();
        admin.createPartition(tablePath,
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_02")), true).get();
        admin.createPartition(tablePath,
                new PartitionSpec(Collections.singletonMap("dt", "2026_08_03")), true).get();
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(GenericRow.of(1, BinaryString.fromString("2026_08_02")));
            writer.append(GenericRow.of(2, BinaryString.fromString("2026_08_03")));
            writer.flush();
        }
        long partitionId = admin.listPartitionInfos(tablePath).get().stream()
                .filter(p -> p.getPartitionName().equals("2026_08_03"))
                .findFirst().orElseThrow(AssertionError::new)
                .getPartitionId();

        Map<String, String> params = params(tablePath, columns("id", "int"), 0, 1);
        params.put("fluss.partition_id", String.valueOf(partitionId));
        Object[][] rows = runScanner(params, 1024);

        Assertions.assertEquals(1, rows.length);
        Assertions.assertArrayEquals(new Object[] {2}, rows[0]);
    }

    // ---------------------------------------------------------------- lifecycle and fail-loud

    /**
     * A fluss connection owns netty and metadata-updater threads that outlive the query if the scanner
     * leaks it — and a BE runs scanners for the life of the process.
     */
    @Test
    public void closingTheScannerReleasesItsClientThreads() throws Exception {
        TablePath tablePath = TablePath.of(db, "threads");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 3);
        int before = countClientThreads();

        for (int i = 0; i < 3; i++) {
            scanAll(tablePath, columns("id", "int"), 0, 3, 1024);
        }

        long deadline = System.currentTimeMillis() + 30_000;
        while (countClientThreads() > before && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        Assertions.assertTrue(countClientThreads() <= before,
                "fluss client threads leaked: " + before + " before, " + countClientThreads() + " after");
    }

    /** An unreadable range must name what is wrong, not hand fluss a null and fail somewhere inside. */
    @Test
    public void missingParameterIsNamed() {
        Map<String, String> params = params(TablePath.of(db, "x"), columns("id", "int"), 0, 1);
        params.remove("fluss.bucket_id");

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FlussJniScanner(1024, params).open());
        Assertions.assertTrue(e.getMessage().contains("fluss.bucket_id"), e.getMessage());
    }

    /**
     * Union ranges are planned by FE but not readable here yet. Refusing beats reading their fluss
     * half and returning it as if it were the whole table.
     */
    @Test
    public void unsupportedRangeTypeIsRefused() {
        Map<String, String> params = params(TablePath.of(db, "x"), columns("id", "int"), 0, 1);
        params.put("fluss.range_type", "UNION_PK");

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new FlussJniScanner(1024, params));
        Assertions.assertTrue(e.getMessage().contains("UNION_PK"), e.getMessage());
    }

    /** A column dropped between planning and reading must say so, not read a neighbouring column. */
    @Test
    public void columnThatIsNoLongerThereIsNamed() throws Exception {
        TablePath tablePath = TablePath.of(db, "missing_column");
        createIntTable(tablePath);
        appendInts(tablePath, 0, 1);

        Exception e = Assertions.assertThrows(Exception.class,
                () -> runScanner(params(tablePath, columns("nope", "int"), 0, 1), 1024));
        Assertions.assertTrue(rootMessage(e).contains("nope"), rootMessage(e));
    }

    // ---------------------------------------------------------------- helpers

    private static String rootMessage(Throwable e) {
        StringBuilder message = new StringBuilder();
        for (Throwable t = e; t != null; t = t.getCause()) {
            message.append(t.getMessage()).append(" | ");
        }
        return message.toString();
    }

    private static int countClientThreads() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            // Client-side only: the embedded cluster's own threads are fluss-netty-server-*.
            if (thread.getName().startsWith("fluss-netty-client")) {
                count++;
            }
        }
        return count;
    }

    private void createIntTable(TablePath tablePath) throws Exception {
        admin.createTable(tablePath, TableDescriptor.builder()
                .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                .distributedBy(BUCKETS)
                .build(), true).get();
    }

    private void appendInts(TablePath tablePath, int fromInclusive, int toExclusive) throws Exception {
        GenericRow[] rows = new GenericRow[toExclusive - fromInclusive];
        for (int i = fromInclusive; i < toExclusive; i++) {
            rows[i - fromInclusive] = GenericRow.of(i);
        }
        appendRows(tablePath, rows);
    }

    private static void appendRows(TablePath tablePath, GenericRow... rows) throws Exception {
        try (Table table = connection.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (GenericRow row : rows) {
                writer.append(row);
            }
            writer.flush();
        }
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

    /** The merged map BE hands the scanner: the scan-level properties plus this range's. */
    private static Map<String, String> params(TablePath tablePath, Map<String, String> columns,
            long startOffset, long stopOffset) {
        Map<String, String> params = new HashMap<>(columns);
        params.put("fluss.client.bootstrap.servers", bootstrapServers);
        params.put("fluss.db_name", tablePath.getDatabaseName());
        params.put("fluss.table_name", tablePath.getTableName());
        params.put("fluss.range_type", "LOG");
        params.put("fluss.bucket_id", "0");
        params.put("fluss.log_start_offset", String.valueOf(startOffset));
        params.put("fluss.log_stop_offset", String.valueOf(stopOffset));
        params.put("time_zone", "UTC");
        return params;
    }

    private static Object[][] scanAll(TablePath tablePath, Map<String, String> columns,
            long startOffset, long stopOffset, int batchSize) throws Exception {
        return runScanner(params(tablePath, columns, startOffset, stopOffset), batchSize);
    }

    /**
     * Drives the scanner the way BE does — batch by batch until it reports none left — and returns the
     * rows it produced. {@code getMaterializedData} hands back COLUMN-major arrays, so this transposes;
     * reading it as rows would silently compare a column against a row and mostly pass on square data.
     */
    private static Object[][] runScanner(Map<String, String> params, int batchSize) throws Exception {
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
}
