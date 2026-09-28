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
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    public void testPersistedAggregateRetractsAndSupportedControl() throws Exception {
        for (RowKind kind : Arrays.asList(RowKind.DELETE, RowKind.UPDATE_BEFORE)) {
            FileStoreTable table = table(DataTypes.DOUBLE(), aggregate("sum"));
            commit(table, GenericRow.of(1, 10.0));
            commit(table, GenericRow.of(1, 3.0));
            assertRead(table, true, row -> row.getDouble(1), Collections.singletonList(13.0));
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
        Path path = new Path(temporaryFolder.newFolder().toURI());
        LocalFileIO fileIO = LocalFileIO.create();
        Schema.Builder schema = Schema.newBuilder().column("id", DataTypes.INT().notNull()).column("v", type)
                .primaryKey("id").option("bucket", "1").option("write-only", "true")
                .option("file.format", "parquet").option("deletion-vectors.enabled", "false")
                .option("read.batch-size", "16").option("scan.manifest.parallelism", "1");
        options.forEach(schema::option);
        new SchemaManager(fileIO, path).createTable(schema.build());
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
