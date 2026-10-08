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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.FileSystemSchemaManager;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;

/** Real persisted files verify the Java result contract and the split metadata used by the gate. */
public class PaimonRustReaderCompatibilityTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testPersistedIntegerNarrowing() throws Exception {
        FileStoreTable table = table(DataTypes.INT(), Collections.emptyMap());
        commit(table, GenericRow.of(1, 383), GenericRow.of(2, -129), GenericRow.of(3, null));
        assertRead(table, true, row -> row.isNullAt(1) ? null : row.getInt(1), Arrays.asList(383, -129, null));
        table.schemaManager().commitChanges(Collections.singletonList(
                SchemaChange.updateColumnType("v", DataTypes.TINYINT(), true)));
        table = FileStoreTableFactory.create(table.fileIO(), table.location());
        assertRead(table, false, row -> row.isNullAt(1) ? null : row.getByte(1),
                Arrays.asList((byte) 127, (byte) 127, null));
    }

    @Test
    public void testPersistedStringToTemporal() throws Exception {
        for (boolean date : new boolean[] {true, false}) {
            for (boolean nested : new boolean[] {false, true}) {
                DataType sourceType = nested ? DataTypes.ROW(DataTypes.FIELD(2, "value", DataTypes.STRING()))
                        : DataTypes.STRING();
                DataType target = date ? DataTypes.DATE() : DataTypes.TIMESTAMP(3);
                FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT())
                        .column("v", sourceType), ImmutableMap.of("bucket", "-1"));
                Object value = BinaryString.fromString("42");
                commit(table, GenericRow.of(1, nested ? GenericRow.of(value) : value),
                        GenericRow.of(2, nested ? GenericRow.of((Object) null) : null));
                assertRead(table, true, row -> row.getInt(0), Arrays.asList(1, 2));
                table.schemaManager().commitChanges(Collections.singletonList(SchemaChange.updateColumnType(
                        nested ? new String[] {"v", "value"} : new String[] {"v"}, target, true)));
                table = FileStoreTableFactory.create(table.fileIO(), table.location());
                // Java accepts epoch-day/epoch-millisecond strings that Arrow's date parser rejects.
                assertRead(table, false, row -> {
                    InternalRow valueRow = nested ? row.getRow(1, 1) : row;
                    int pos = nested ? 0 : 1;
                    if (valueRow.isNullAt(pos)) {
                        return null;
                    }
                    return date ? java.time.LocalDate.ofEpochDay(valueRow.getInt(pos)).toString()
                            : valueRow.getTimestamp(pos, 3).toString();
                }, Arrays.asList(date ? "1970-02-12" : "1970-01-01T00:00:00.042", null));
            }
        }
    }

    @Test
    public void testPersistedIntegerToTimestamp() throws Exception {
        for (boolean bigint : new boolean[] {false, true}) {
            for (int precision : new int[] {0, 3, 6}) {
                for (boolean nested : new boolean[] {false, true}) {
                    DataType sourceType = bigint ? DataTypes.BIGINT() : DataTypes.INT();
                    FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT())
                            .column("v", nested ? DataTypes.ROW(DataTypes.FIELD(2, "value", sourceType)) : sourceType),
                            ImmutableMap.of("bucket", "-1"));
                    Object value = bigint ? (Object) 1700000000L : 1700000000;
                    commit(table, GenericRow.of(1, nested ? GenericRow.of(value) : value),
                            GenericRow.of(2, nested ? GenericRow.of((Object) null) : null));
                    assertRead(table, true, row -> row.getInt(0), Arrays.asList(1, 2));
                    table.schemaManager().commitChanges(Collections.singletonList(SchemaChange.updateColumnType(
                            nested ? new String[] {"v", "value"} : new String[] {"v"},
                            DataTypes.TIMESTAMP(precision), true)));
                    table = FileStoreTableFactory.create(table.fileIO(), table.location());
                    // Paimon numeric timestamps use seconds independently of the target precision.
                    assertRead(table, false, row -> {
                        InternalRow valueRow = nested ? row.getRow(1, 1) : row;
                        int pos = nested ? 0 : 1;
                        return valueRow.isNullAt(pos) ? null : valueRow.getTimestamp(pos, precision).toString();
                    }, Arrays.asList("2023-11-14T22:13:20", null));
                }
            }
        }
    }

    @Test
    public void testPersistedTimestampToDateBeforeEpoch() throws Exception {
        // Java divides toward zero; Arrow takes the preceding calendar date before the epoch.
        assertPersistedEvolution(DataTypes.TIMESTAMP(3), DataTypes.DATE(), Timestamp.fromEpochMillis(-1),
                false, row -> row.getInt(1), 0);
    }

    @Test
    public void testPersistedTimestampZoneChanges() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            // Paimon interprets NTZ/LTZ changes in the JVM zone, unlike an Arrow timezone cast.
            assertPersistedEvolution(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
                    Timestamp.fromEpochMillis(0), false, row -> row.getTimestamp(1, 3).getMillisecond(),
                    -8L * 60 * 60 * 1000);
            assertPersistedEvolution(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3), DataTypes.TIMESTAMP(3),
                    Timestamp.fromEpochMillis(0), false, row -> row.getTimestamp(1, 3).getMillisecond(),
                    8L * 60 * 60 * 1000);
        } finally {
            TimeZone.setDefault(previous);
        }
    }

    @Test
    public void testPersistedTimeToTimestamp() throws Exception {
        assertPersistedEvolution(DataTypes.TIME(3), DataTypes.TIMESTAMP(3), 1234, false,
                row -> row.getTimestamp(1, 3).toString(), "1970-01-01T00:00:01.234");
    }

    @Test
    public void testPersistedFormattedStrings() throws Exception {
        assertPersistedEvolution(DataTypes.FLOAT(), DataTypes.STRING(), 0.0001f, false,
                row -> row.getString(1).toString(), "1.0E-4");
        assertPersistedEvolution(DataTypes.DOUBLE(), DataTypes.STRING(), 1.0e20, false,
                row -> row.getString(1).toString(), "1.0E20");
        assertPersistedEvolution(DataTypes.TIMESTAMP(3), DataTypes.STRING(), Timestamp.fromEpochMillis(1230),
                false, row -> row.getString(1).toString(), "1970-01-01 00:00:01.230");
        assertPersistedEvolution(DataTypes.TIME(3), DataTypes.STRING(), 1230, false,
                row -> row.getString(1).toString(), "00:00:01.23");
    }

    @Test
    public void testPersistedStringBounds() throws Exception {
        assertPersistedEvolution(DataTypes.VARCHAR(10), DataTypes.VARCHAR(3), BinaryString.fromString("abcdef"),
                false, row -> row.getString(1).toString(), "abc");
        assertPersistedEvolution(DataTypes.CHAR(3), DataTypes.CHAR(10), BinaryString.fromString("x"),
                false, row -> row.getString(1).toString(), "x         ");
        assertPersistedEvolution(DataTypes.VARCHAR(3), DataTypes.VARCHAR(10), BinaryString.fromString("abc"),
                true, row -> row.getString(1).toString(), "abc");
    }

    @Test
    public void testPersistedRowToString() throws Exception {
        DataType rowType = DataTypes.ROW(DataTypes.FIELD(2, "a", DataTypes.INT()),
                DataTypes.FIELD(3, "b", DataTypes.STRING()));
        assertPersistedEvolution(rowType, DataTypes.STRING(), GenericRow.of(11, BinaryString.fromString("x")),
                false, row -> row.getString(1).toString(), "{11, x}");
    }

    @Test
    public void testPersistedTimestampEvolutionIntoNanosecondRange() throws Exception {
        List<String> expected = Arrays.asList("2300-01-01T00:00:00.123", "1600-01-01T00:00:00.123",
                "2024-01-01T00:00:00.123", null);
        for (int oldPrecision : new int[] {3, 6}) {
            for (boolean localZone : new boolean[] {false, true}) {
                DataType source = localZone ? DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(oldPrecision)
                        : DataTypes.TIMESTAMP(oldPrecision);
                FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT()).column("v", source),
                        ImmutableMap.of("bucket", "-1"));
                commit(table, GenericRow.of(1, Timestamp.fromLocalDateTime(LocalDateTime.parse(expected.get(0)))),
                        GenericRow.of(2, Timestamp.fromLocalDateTime(LocalDateTime.parse(expected.get(1)))),
                        GenericRow.of(3, Timestamp.fromLocalDateTime(LocalDateTime.parse(expected.get(2)))),
                        GenericRow.of(4, null));
                assertRead(table, true, row -> row.isNullAt(1) ? null
                        : row.getTimestamp(1, oldPrecision).toString(), expected);
                DataType target = localZone ? DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9) : DataTypes.TIMESTAMP(9);
                table.schemaManager().commitChanges(Collections.singletonList(
                        SchemaChange.updateColumnType("v", target, true)));
                table = FileStoreTableFactory.create(table.fileIO(), table.location());
                // Java retains the full historical range; Arrow's nanosecond cast overflows these dates to NULL.
                assertRead(table, false, row -> row.isNullAt(1) ? null : row.getTimestamp(1, 9).toString(), expected);
            }
        }
    }

    @Test
    public void testPersistedSafeEvolutionControls() throws Exception {
        assertPersistedEvolution(DataTypes.INT(), DataTypes.BIGINT(), 123, true, row -> row.getLong(1), 123L);
        assertPersistedEvolution(DataTypes.INT(), DataTypes.DECIMAL(12, 2), 123, true,
                row -> row.getDecimal(1, 12, 2).toBigDecimal().toPlainString(), "123.00");
        assertPersistedEvolution(DataTypes.DECIMAL(10, 3), DataTypes.DECIMAL(10, 2),
                Decimal.fromUnscaledLong(1235, 10, 3), true,
                row -> row.getDecimal(1, 10, 2).toBigDecimal().toPlainString(), "1.24");
        assertPersistedEvolution(DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6), Timestamp.fromEpochMillis(1234),
                true, row -> row.getTimestamp(1, 6).toString(), "1970-01-01T00:00:01.234");
    }

    private <T> void assertPersistedEvolution(DataType sourceType, DataType targetType, Object value,
            boolean rustExpected, Function<InternalRow, T> extract, T expected) throws Exception {
        FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT()).column("v", sourceType),
                ImmutableMap.of("bucket", "-1"));
        commit(table, GenericRow.of(1, value), GenericRow.of(2, null));
        assertRead(table, true, row -> row.getInt(0), Arrays.asList(1, 2));
        table.schemaManager().commitChanges(Collections.singletonList(
                SchemaChange.updateColumnType("v", targetType, true)));
        table = FileStoreTableFactory.create(table.fileIO(), table.location());
        assertRead(table, rustExpected, row -> row.isNullAt(1) ? null : extract.apply(row),
                Arrays.asList(expected, null));
    }

    @Test
    public void testPersistedFloatingToDecimalRounding() throws Exception {
        FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT())
                .column("v", DataTypes.DOUBLE()), ImmutableMap.of("bucket", "-1"));
        commit(table, GenericRow.of(1, 1.005), GenericRow.of(2, -1.005), GenericRow.of(3, 1.25),
                GenericRow.of(4, null));
        assertRead(table, true, row -> row.getInt(0), Arrays.asList(1, 2, 3, 4));
        table.schemaManager().commitChanges(Collections.singletonList(
                SchemaChange.updateColumnType("v", DataTypes.DECIMAL(3, 2), true)));
        table = FileStoreTableFactory.create(table.fileIO(), table.location());
        assertRead(table, false, row -> row.isNullAt(1) ? null
                : row.getDecimal(1, 3, 2).toBigDecimal().toPlainString(), Arrays.asList("1.01", "-1.01", "1.25", null));
    }

    @Test
    public void testPersistedUnusedAggregateDefault() throws Exception {
        for (String engine : Arrays.asList("aggregation", "partial-update")) {
            for (String function : Arrays.asList("collect", "sum")) {
                Map<String, String> options = new HashMap<>();
                options.put("merge-engine", engine);
                options.put("fields.default-aggregate-function", function);
                options.put("fields.v.aggregate-function", "max");
                Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT().notNull())
                        .column("v", DataTypes.INT()).primaryKey("id");
                boolean partial = "partial-update".equals(engine);
                if (partial) {
                    schema.column("seq", DataTypes.INT());
                    options.put("fields.seq.sequence-group", "v");
                }
                FileStoreTable table = table(schema, options);
                commit(table, partial ? GenericRow.of(1, 7, 1) : GenericRow.of(1, 7));
                // An unused function override cannot establish that historical files contain unique keys.
                assertRead(table, false, row -> row.getInt(1), Collections.singletonList(7));
            }
        }
    }

    @Test
    public void testPersistedNonIntegerNarrowing() throws Exception {
        for (DataType type : Arrays.asList(DataTypes.DOUBLE(), DataTypes.DECIMAL(10, 2))) {
            boolean floating = type.equals(DataTypes.DOUBLE());
            FileStoreTable table = table(type, Collections.emptyMap());
            commit(table, GenericRow.of(1, floating ? 383.0 : Decimal.fromUnscaledLong(38300, 10, 2)),
                    GenericRow.of(2, floating ? -129.0 : Decimal.fromUnscaledLong(-12900, 10, 2)),
                    GenericRow.of(3, floating ? 12.0 : Decimal.fromUnscaledLong(1200, 10, 2)),
                    GenericRow.of(4, null));
            assertRead(table, true, row -> row.getInt(0), Arrays.asList(1, 2, 3, 4));
            table.schemaManager().commitChanges(Collections.singletonList(
                    SchemaChange.updateColumnType("v", DataTypes.TINYINT(), true)));
            table = FileStoreTableFactory.create(table.fileIO(), table.location());
            assertRead(table, false, row -> row.isNullAt(1) ? null : row.getByte(1),
                    Arrays.asList((byte) 127, (byte) 127, (byte) 12, null));
        }
    }

    @Test
    public void testPersistedPartialUpdateDefaultAggregate() throws Exception {
        for (boolean decimal : new boolean[] {false, true}) {
            DataType type = decimal ? DataTypes.DECIMAL(2, 0) : DataTypes.TINYINT();
            for (boolean override : new boolean[] {false, true}) {
                Map<String, String> options = new HashMap<>();
                options.put("merge-engine", "partial-update");
                options.put("fields.seq.sequence-group", "v");
                options.put("fields.default-aggregate-function", "sum");
                if (override) {
                    options.put("fields.v.aggregate-function", "max");
                }
                FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT().notNull())
                        .column("v", type).column("seq", DataTypes.INT()).primaryKey("id"), options);
                commit(table, GenericRow.of(1, decimal ? Decimal.fromUnscaledLong(99, 2, 0) : (byte) 127, 1));
                // Single-file partial updates must also fall back: old writers retained repeated keys.
                assertRead(table, false, row -> decimal
                        ? row.getDecimal(1, 2, 0).toBigDecimal().toPlainString()
                        : Byte.toString(row.getByte(1)), Collections.singletonList(decimal ? "99" : "127"));
                commit(table, GenericRow.of(1, decimal ? Decimal.fromUnscaledLong(1, 2, 0) : (byte) 1, 2));
                if (decimal) {
                    commit(table, GenericRow.of(1, Decimal.fromUnscaledLong(-1, 2, 0), 3));
                }
                assertRead(table, false, row -> decimal
                        ? row.getDecimal(1, 2, 0).toBigDecimal().toPlainString()
                        : Byte.toString(row.getByte(1)), Collections.singletonList(
                                decimal ? "99" : override ? "127" : "-128"));
            }
        }
    }

    @Test
    public void testPersistedInsertOnlyOverlappingRunsFallback() throws Exception {
        FileStoreTable table = table(DataTypes.STRING(), ImmutableMap.of("read.batch-size", "1"));
        for (int run = 0; run < 3; run++) {
            List<GenericRow> rows = new ArrayList<>();
            String payload = run + String.join("", Collections.nCopies(4096, "x"));
            for (int key = 0; key < 1025; key++) {
                rows.add(GenericRow.of(key, BinaryString.fromString(payload)));
            }
            commit(table, rows.toArray(new GenericRow[0]));
        }
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        Assert.assertEquals("Overlapping runs must be read in one logical split", 1, splits.size());
        DataSplit split = (DataSplit) splits.get(0);
        Assert.assertEquals(3, split.dataFiles().size());
        split.dataFiles().forEach(file -> {
            Assert.assertEquals(1025L, file.rowCount());
            Assert.assertEquals(Long.valueOf(0), file.deleteRowCount().get());
        });
        assertRead(table, false, row -> {
            String payload = row.getString(1).toString();
            Assert.assertEquals(4097, payload.length());
            return payload.substring(0, 1);
        }, Collections.nCopies(1025, "2"));
    }

    @Test
    public void testPersistedAppendOnlyMultiFileControl() throws Exception {
        FileStoreTable table = table(Schema.newBuilder().column("id", DataTypes.INT()).column("v", DataTypes.INT()),
                ImmutableMap.of("bucket", "-1"));
        commit(table, GenericRow.of(1, 11));
        commit(table, GenericRow.of(2, 22));
        Assert.assertTrue(table.newReadBuilder().newScan().plan().splits().stream()
                .anyMatch(split -> ((DataSplit) split).dataFiles().size() > 1));
        assertRead(table, true, row -> row.getInt(1), Arrays.asList(11, 22));
    }

    @Test
    public void testPersistedIntegerSumAndProductOverflow() throws Exception {
        DataType[] types = {DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.BIGINT()};
        Object[] maximums = {(byte) 127, (short) 32767, Integer.MAX_VALUE, Long.MAX_VALUE};
        Object[] ones = {(byte) 1, (short) 1, 1, 1L};
        Object[] halves = {(byte) 64, (short) 16384, 1073741824, 4611686018427387904L};
        Object[] twos = {(byte) 2, (short) 2, 2, 2L};
        for (String function : Arrays.asList("sum", "product")) {
            for (int i = 0; i < types.length; i++) {
                FileStoreTable table = table(types[i], aggregate(function));
                commit(table, GenericRow.of(1, "sum".equals(function) ? maximums[i] : halves[i]));
                commit(table, GenericRow.of(1, "sum".equals(function) ? ones[i] : twos[i]));
                InternalRow.FieldGetter getter = InternalRow.createFieldGetter(types[i], 1);
                Object minimum = i == 0 ? (byte) -128 : i == 1 ? (short) -32768
                        : i == 2 ? Integer.MIN_VALUE : Long.MIN_VALUE;
                // Compare decimal strings to avoid Java's conditional numeric promotion.
                assertRead(table, false, row -> getter.getFieldOrNull(row).toString(),
                        Collections.singletonList(minimum.toString()));
            }
        }
    }

    @Test
    public void testPersistedCompactDecimalIntermediateOverflow() throws Exception {
        FileStoreTable table = table(DataTypes.DECIMAL(2, 0), aggregate("sum"));
        for (long value : new long[] {99, 1, -1}) {
            commit(table, GenericRow.of(1, Decimal.fromUnscaledLong(value, 2, 0)));
        }
        assertRead(table, false, row -> row.getDecimal(1, 2, 0).toBigDecimal().toPlainString(),
                Collections.singletonList("99"));
    }

    @Test
    public void testPersistedCollect() throws Exception {
        FileStoreTable table = table(DataTypes.ARRAY(DataTypes.INT()), aggregate("collect"));
        commit(table, GenericRow.of(1, new GenericArray(new Integer[] {11})));
        commit(table, GenericRow.of(1, new GenericArray(new Integer[] {22})));
        assertRead(table, false, row -> {
            List<Integer> values = Arrays.asList(row.getArray(1).getInt(0), row.getArray(1).getInt(1));
            Collections.sort(values);
            return values;
        }, Collections.singletonList(Arrays.asList(11, 22)));
    }

    @Test
    public void testPersistedAggregateInsertsAndRetractsFallback() throws Exception {
        for (RowKind kind : Arrays.asList(RowKind.DELETE, RowKind.UPDATE_BEFORE)) {
            FileStoreTable table = table(DataTypes.DOUBLE(), aggregate("sum"));
            commit(table, GenericRow.of(1, 10.0));
            assertRead(table, false, row -> row.getDouble(1), Collections.singletonList(10.0));
            commit(table, GenericRow.of(1, 3.0));
            assertRead(table, false, row -> row.getDouble(1), Collections.singletonList(13.0));
            commit(table, GenericRow.ofKind(kind, 1, 3.0));
            assertRead(table, false, row -> row.getDouble(1), Collections.singletonList(10.0));
        }
    }

    @Test
    public void testPersistedZeroAndSparseOutputMerge() throws Exception {
        for (boolean survivor : new boolean[] {false, true}) {
            FileStoreTable table = table(DataTypes.INT(), Collections.emptyMap());
            List<GenericRow> inserts = new ArrayList<>();
            List<GenericRow> deletes = new ArrayList<>();
            for (int i = 0; i < 2048; i++) {
                inserts.add(GenericRow.of(i, i));
                deletes.add(GenericRow.ofKind(RowKind.DELETE, i, i));
            }
            if (survivor) {
                inserts.add(GenericRow.of(2048, 2048));
            }
            commit(table, inserts.toArray(new GenericRow[0]));
            commit(table, deletes.toArray(new GenericRow[0]));
            assertRead(table, false, row -> row.getInt(1), survivor
                    ? Collections.singletonList(2048) : Collections.emptyList());
        }
    }

    private static Map<String, String> aggregate(String function) {
        return ImmutableMap.of("merge-engine", "aggregation", "fields.v.aggregate-function", function);
    }

    private FileStoreTable table(DataType type, Map<String, String> options) throws Exception {
        return table(Schema.newBuilder().column("id", DataTypes.INT().notNull()).column("v", type)
                .primaryKey("id"), options);
    }

    private FileStoreTable table(Schema.Builder schema, Map<String, String> options) throws Exception {
        Path path = new Path(temporaryFolder.newFolder().toURI());
        LocalFileIO fileIO = LocalFileIO.create();
        schema.option("bucket", "1").option("write-only", "true")
                .option("file.format", "parquet").option("deletion-vectors.enabled", "false")
                .option("read.batch-size", "16").option("scan.manifest.parallelism", "1");
        options.forEach(schema::option);
        new FileSystemSchemaManager(fileIO, path).createTable(schema.build());
        return FileStoreTableFactory.create(fileIO, path);
    }

    private static void commit(FileStoreTable table, GenericRow... rows) throws Exception {
        // Separate writers force persisted, uncompacted inputs instead of a write-time reduction.
        BatchWriteBuilder builder = table.newBatchWriteBuilder();
        try (BatchTableWrite write = builder.newWrite(); BatchTableCommit commit = builder.newCommit()) {
            for (GenericRow row : rows) {
                write.write(row);
            }
            commit.commit(write.prepareCommit());
        }
    }

    private static <T> void assertRead(FileStoreTable table, boolean rustExpected,
            Function<InternalRow, T> extract, List<T> expected) throws Exception {
        ReadBuilder builder = table.newReadBuilder();
        List<Split> splits = builder.newScan().plan().splits();
        Assert.assertFalse("Persisted files must reach scan planning", splits.isEmpty());
        PaimonRustReaderCapabilities capabilities = new PaimonRustReaderCapabilities(table,
                new TupleDescriptor(new TupleId(0)));
        List<T> actual = new ArrayList<>();
        for (Split split : splits) {
            Assert.assertEquals("reader eligibility for persisted input", rustExpected,
                    capabilities.canRead((DataSplit) split));
            try (RecordReader<InternalRow> reader = builder.newRead().createReader(split)) {
                reader.forEachRemaining(row -> actual.add(extract.apply(row)));
            }
        }
        Assert.assertEquals(expected, actual);
    }
}
