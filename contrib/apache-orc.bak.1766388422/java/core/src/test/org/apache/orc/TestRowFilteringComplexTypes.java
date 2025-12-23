/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRowFilteringComplexTypes {
    private Path workDir = new Path(System.getProperty("test.tmp.dir", "target" + File.separator + "test"
            + File.separator + "tmp"));

    private Configuration conf;
    private FileSystem fs;
    private Path testFilePath;

    private static final int ColumnBatchRows = 1024;

    @BeforeEach
    public void openFileSystem(TestInfo testInfo) throws Exception {
        conf = new Configuration();
        OrcConf.READER_USE_SELECTED.setBoolean(conf, true);
        fs = FileSystem.getLocal(conf);
        testFilePath = new Path(workDir,
            "TestRowFilteringComplexTypes." + testInfo.getTestMethod().get().getName() + ".orc");
        fs.delete(testFilePath, false);
    }

    @Test
    // Inner Struct should receive the filterContext and propagate it the the SubTypes
    public void testInnerStructRowFilter() throws Exception {
        // Set the row stride to a multiple of the batch size
        final int INDEX_STRIDE = 16 * ColumnBatchRows;
        final int NUM_BATCHES = 2;

        TypeDescription schema = TypeDescription.createStruct()
                .addField("int1", TypeDescription.createInt())
                .addField("innerStruct", TypeDescription.createStruct()
                        .addField("a", TypeDescription.createDecimal())
                        .addField("b", TypeDescription.createDecimal())
                );

        try (Writer writer = OrcFile.createWriter(testFilePath,
                OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(INDEX_STRIDE))) {
            VectorizedRowBatch batch = schema.createRowBatchV2();
            LongColumnVector col1 = (LongColumnVector) batch.cols[0];
            StructColumnVector col2 = (StructColumnVector) batch.cols[1];
            DecimalColumnVector innerCol1 = (DecimalColumnVector) col2.fields[0];
            DecimalColumnVector innerCol2 = (DecimalColumnVector) col2.fields[1];

            for (int b = 0; b < NUM_BATCHES; ++b) {
                batch.reset();
                batch.size = ColumnBatchRows;
                for (int row = 0; row < batch.size; row++) {
                    col1.vector[row] = row;
                    if ((row % 2) == 0) {
                        innerCol1.vector[row] = new HiveDecimalWritable(101 + row);
                        innerCol2.vector[row] = new HiveDecimalWritable(100 + row);
                    } else {
                        innerCol1.vector[row] = new HiveDecimalWritable(999 + row);
                        innerCol2.vector[row] = new HiveDecimalWritable(998 + row);
                    }
                }
                col1.isRepeating = false;
                writer.addRowBatch(batch);
            }
        }

        Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
        try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
                reader.options().setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
            VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
            StructColumnVector col2 = (StructColumnVector) batch.cols[1];

            int noNullCnt = 0;
            while (rows.nextBatch(batch)) {
                assertTrue(batch.selectedInUse);
                assertEquals(ColumnBatchRows / 2, batch.size);
                for (int r = 0; r < ColumnBatchRows; ++r) {
                    StringBuilder sb = new StringBuilder();
                    col2.stringifyValue(sb, r);
                    if (sb.toString().compareTo("[0, 0]") != 0) {
                        noNullCnt++;
                    }
                }
            }
            // Make sure that our filter worked
            assertEquals(NUM_BATCHES * 512, noNullCnt);
            assertEquals(0, batch.selected[0]);
            assertEquals(2, batch.selected[1]);
        }
    }


    @Test
    // Inner UNION should make use of the filterContext
    public void testInnerUnionRowFilter() throws Exception {
        // Set the row stride to a multiple of the batch size
        final int INDEX_STRIDE = 16 * ColumnBatchRows;
        final int NUM_BATCHES = 2;

        TypeDescription schema = TypeDescription.fromString(
            "struct<int1:int,innerUnion:uniontype<decimal(16,3),decimal(16,3)>>");

        try (Writer writer = OrcFile.createWriter(testFilePath,
            OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(INDEX_STRIDE))) {
            VectorizedRowBatch batch = schema.createRowBatchV2();
            LongColumnVector col1 = (LongColumnVector) batch.cols[0];
            UnionColumnVector col2 = (UnionColumnVector) batch.cols[1];
            Decimal64ColumnVector innerCol1 = (Decimal64ColumnVector) col2.fields[0];
            Decimal64ColumnVector innerCol2 = (Decimal64ColumnVector) col2.fields[1];

            for (int b = 0; b < NUM_BATCHES; ++b) {
                batch.reset();
                batch.size = ColumnBatchRows;
                for (int row = 0; row < batch.size; row++) {
                    int totalRow = ColumnBatchRows * b + row;
                    col1.vector[row] = totalRow;
                    col2.tags[row] = totalRow % 2;
                    if (col2.tags[row] == 0) {
                        innerCol1.vector[row] = totalRow * 1000;
                    } else {
                        innerCol2.vector[row] = totalRow * 3 * 1000;
                    }
                }
                col1.isRepeating = false;
                writer.addRowBatch(batch);
            }
        }

        Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
        try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
            reader.options().setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
            VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
            LongColumnVector col1 = (LongColumnVector) batch.cols[0];
            UnionColumnVector col2 = (UnionColumnVector) batch.cols[1];
            Decimal64ColumnVector innerCol1 = (Decimal64ColumnVector) col2.fields[0];
            Decimal64ColumnVector innerCol2 = (Decimal64ColumnVector) col2.fields[1];

            int previousBatchRows = 0;
            while (rows.nextBatch(batch)) {
                assertTrue(batch.selectedInUse);
                assertEquals(ColumnBatchRows / 2, batch.size);
                for (int r = 0; r < batch.size; ++r) {
                    int row = batch.selected[r];
                    int originalRow = (r + previousBatchRows) * 2;
                    String msg = "row " + originalRow;
                    assertEquals(originalRow, col1.vector[row], msg);
                    assertEquals(0, col2.tags[row], msg);
                    assertEquals(originalRow * 1000, innerCol1.vector[row], msg);
                }
                // check to make sure that we didn't read innerCol2
                for(int r = 1; r < ColumnBatchRows; r += 2) {
                    assertEquals(0, innerCol2.vector[r], "row " + r);
                }
                previousBatchRows += batch.size;
            }
        }
    }


    @Test
    // Inner MAP should NOT make use of the filterContext
    // TODO: selected rows should be combined with map offsets
    public void testInnerMapRowFilter() throws Exception {
        // Set the row stride to a multiple of the batch size
        final int INDEX_STRIDE = 16 * ColumnBatchRows;
        final int NUM_BATCHES = 2;

        TypeDescription schema = TypeDescription.createStruct()
                .addField("int1", TypeDescription.createInt())
                .addField("innerMap", TypeDescription.createMap(
                        TypeDescription.createDecimal(),
                        TypeDescription.createDecimal()
                        )
                );

        try (Writer writer = OrcFile.createWriter(testFilePath,
                OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(INDEX_STRIDE))) {
            VectorizedRowBatch batch = schema.createRowBatchV2();
            LongColumnVector col1 = (LongColumnVector) batch.cols[0];
            MapColumnVector mapCol = (MapColumnVector) batch.cols[1];
            DecimalColumnVector keyCol = (DecimalColumnVector) mapCol.keys;
            DecimalColumnVector valCol = (DecimalColumnVector) mapCol.values;

            for (int b = 0; b < NUM_BATCHES; ++b) {
                batch.reset();
                batch.size = ColumnBatchRows;
                for (int row = 0; row < batch.size; row++) {
                    col1.vector[row] = row;
                    // Insert 2 kv pairs in each row
                    for (int i = 0; i < 2; i++) {
                        keyCol.vector[i] = new HiveDecimalWritable(i);
                        valCol.vector[i] = new HiveDecimalWritable(i * 10);
                    }
                    mapCol.lengths[row] = 2;
                    mapCol.offsets[row] = 0;
                }
                col1.isRepeating = false;
                writer.addRowBatch(batch);
            }
        }

        Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
        try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
                reader.options().setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
            VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
            MapColumnVector col2 = (MapColumnVector) batch.cols[1];

            int noNullCnt = 0;
            while (rows.nextBatch(batch)) {
                assertTrue(batch.selectedInUse);
                assertEquals(ColumnBatchRows / 2, batch.size);
                for (int r = 0; r < ColumnBatchRows; ++r) {
                    StringBuilder sb = new StringBuilder();
                    col2.stringifyValue(sb, r);
                    if (sb.toString().equals("[{\"key\": 0, \"value\": 0}, {\"key\": 1, \"value\": 10}]")) {
                        noNullCnt++;
                    }
                }
            }
            // Make sure that we did NOT skip any rows
            assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
            // Even though selected Array is still used its not propagated
            assertEquals(0, batch.selected[0]);
            assertEquals(2, batch.selected[1]);
        }
    }

    @Test
    // Inner LIST should NOT make use of the filterContext
    // TODO: selected rows should be combined with list offsets
    public void testInnerListRowFilter() throws Exception {
        // Set the row stride to a multiple of the batch size
        final int INDEX_STRIDE = 16 * ColumnBatchRows;
        final int NUM_BATCHES = 2;

        TypeDescription schema = TypeDescription.createStruct()
                .addField("int1", TypeDescription.createInt())
                .addField("innerList", TypeDescription
                        .createList(TypeDescription.createDecimal())
                );

        try (Writer writer = OrcFile.createWriter(testFilePath,
                OrcFile.writerOptions(conf).setSchema(schema).rowIndexStride(INDEX_STRIDE))) {
            VectorizedRowBatch batch = schema.createRowBatchV2();
            LongColumnVector col1 = (LongColumnVector) batch.cols[0];
            ListColumnVector listCol = (ListColumnVector) batch.cols[1];
            DecimalColumnVector listValues = (DecimalColumnVector) listCol.child;

            for (int b = 0; b < NUM_BATCHES; ++b) {
                batch.reset();
                batch.size = ColumnBatchRows;
                for (int row = 0; row < batch.size; row++) {
                    col1.vector[row] = row;
                    // Insert 10 values to the interList per row
                    for (int i = 0; i < 10; i++) {
                        listValues.vector[i] = new HiveDecimalWritable(i);
                    }
                    listCol.lengths[row] = 10;
                    listCol.offsets[row] = 0;
                }
                col1.isRepeating = false;
                writer.addRowBatch(batch);
            }
        }

        Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));
        try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
                reader.options().setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
            VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
            ListColumnVector col2 = (ListColumnVector) batch.cols[1];

            int noNullCnt = 0;
            while (rows.nextBatch(batch)) {
                assertTrue(batch.selectedInUse);
                assertEquals(ColumnBatchRows / 2, batch.size);
                for (int r = 0; r < ColumnBatchRows; ++r) {
                    StringBuilder sb = new StringBuilder();
                    col2.stringifyValue(sb, r);
                    if (sb.toString().equals("[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]")) {
                        noNullCnt++;
                    }
                }
            }
            // Make sure that we did NOT skip any rows
            assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
            // Even though selected Array is still used its not propagated
            assertEquals(0, batch.selected[0]);
            assertEquals(2, batch.selected[1]);
        }
    }
}
