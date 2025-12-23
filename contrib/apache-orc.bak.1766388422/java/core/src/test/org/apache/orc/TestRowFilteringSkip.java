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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.Format;
import java.text.SimpleDateFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Types that are skipped at row-level include: Decimal, Decimal64, Double, Float, Char, VarChar, String, Boolean, Timestamp
 * For the remaining types that are not row-skipped see {@link TestRowFilteringNoSkip}
 */
public class TestRowFilteringSkip {

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
    testFilePath = new Path(workDir, "TestRowFilteringSkip." +
        testInfo.getTestMethod().get().getName() + ".orc");
    fs.delete(testFilePath, false);
  }

  public static String convertTime(long time){
    Date date = new Date(time);
    Format format = new SimpleDateFormat("yyyy-MM-d HH:mm:ss.SSS");
    return format.format(date);
  }

  // Filter all rows except: 924 and 940
  public static void intAnyRowFilter(OrcFilterContext batch) {
    // Dummy Filter implementation passing just one Batch row
    int newSize = 2;
    batch.getSelected()[0] = batch.getSelectedSize()-100;
    batch.getSelected()[1] = 940;
    batch.setSelectedInUse(true);
    batch.setSelectedSize(newSize);
  }

  // Filter all rows except the first one
  public static void intFirstRowFilter(OrcFilterContext batch) {
    int newSize = 0;
    for (int row = 0; row <batch.getSelectedSize(); ++row) {
      if (row == 0) {
        batch.getSelected()[newSize++] = row;
      }
    }
    batch.setSelectedInUse(true);
    batch.setSelectedSize(newSize);
  }

  // Filter out rows in a round-robbin fashion starting with a pass
  public static void intRoundRobbinRowFilter(OrcFilterContext batch) {
    int newSize = 0;
    int[] selected = batch.getSelected();
    for (int row = 0; row < batch.getSelectedSize(); ++row) {
      if ((row % 2) == 0) {
        selected[newSize++] = row;
      }
    }
    batch.setSelectedInUse(true);
    batch.setSelected(selected);
    batch.setSelectedSize(newSize);
  }

  static int rowCount = 0;
  public static void intCustomValueFilter(OrcFilterContext batch) {
    LongColumnVector col1 = (LongColumnVector) ((OrcFilterContextImpl) batch).getCols()[0];
    int newSize = 0;
    for (int row = 0; row <batch.getSelectedSize(); ++row) {
      long val = col1.vector[row];
      if ((val == 2) || (val == 5) || (val == 13) || (val == 29) || (val == 70)) {
        batch.getSelected()[newSize++] = row;
      }
      rowCount++;
    }
    batch.setSelectedInUse(true);
    batch.setSelectedSize(newSize);
  }

  @Test
  public void testDecimalRepeatingFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal());

    HiveDecimalWritable passDataVal = new HiveDecimalWritable("100");
    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          col2.vector[row] = passDataVal;
        }
        col1.isRepeating = true;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].compareTo(passDataVal) == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(col2.vector[0], passDataVal);
      assertEquals(col2.vector[511], nullDataVal);
      assertEquals(col2.vector[1020],  passDataVal);
      assertEquals(col2.vector[1021], nullDataVal);
    }
  }

  @Test
  public void testDecimalRoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal());

    HiveDecimalWritable failDataVal = new HiveDecimalWritable("-100");
    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = new HiveDecimalWritable(row+1);
          else
            col2.vector[row] = failDataVal;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].getHiveDecimal().longValue() > 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(col2.vector[0].getHiveDecimal().longValue(), 1);
      assertEquals(col2.vector[511], nullDataVal);
      assertEquals(col2.vector[1020].getHiveDecimal().longValue(),  1021);
      assertEquals(col2.vector[1021], nullDataVal);
    }
  }

  @Test
  public void testDecimalNullRoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal());

    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = new HiveDecimalWritable(row+1);
        }
        // Make sure we trigger the nullCount path of DecimalTreeReader
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].getHiveDecimal().longValue() > 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(col2.vector[0].getHiveDecimal().longValue(), 1);
      assertEquals(col2.vector[511], nullDataVal);
      assertEquals(col2.vector[1020].getHiveDecimal().longValue(),  1021);
      assertEquals(col2.vector[1021], nullDataVal);
    }
  }


  @Test
  public void testMultiDecimalSingleFilterCallback() throws Exception {
    /// Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal())
        .addField("decimal2", TypeDescription.createDecimal());

    HiveDecimalWritable passDataVal = new HiveDecimalWritable("12");
    HiveDecimalWritable failDataVal = new HiveDecimalWritable("100");
    HiveDecimalWritable nullDataVal = new HiveDecimalWritable("0");

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      // Write 50 batches where each batch has a single value for str.
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      DecimalColumnVector col3 = (DecimalColumnVector) batch.cols[2];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if (row == 924 || row == 940) {
            col2.vector[row] = passDataVal;
            col3.vector[row] = passDataVal;
          } else {
            col2.vector[row] = failDataVal;
            col3.vector[row] = failDataVal;
          }
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intAnyRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DecimalColumnVector col2 = (DecimalColumnVector) batch.cols[1];
      DecimalColumnVector col3 = (DecimalColumnVector) batch.cols[2];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r].compareTo(passDataVal) == 0 && col3.vector[r].compareTo(passDataVal) == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 2, noNullCnt);
      assertEquals(924, batch.selected[0]);
      assertEquals(940, batch.selected[1]);
      assertEquals(0, batch.selected[2]);
      assertEquals(col2.vector[0],  nullDataVal);
      assertEquals(col3.vector[0],  nullDataVal);
      assertEquals(col2.vector[511], nullDataVal);
      assertEquals(col3.vector[511], nullDataVal);
      assertEquals(col2.vector[924], passDataVal);
      assertEquals(col3.vector[940], passDataVal);
      assertEquals(col2.vector[1020], nullDataVal);
      assertEquals(col3.vector[1020], nullDataVal);
      assertEquals(col2.vector[1021], nullDataVal);
      assertEquals(col3.vector[1021], nullDataVal);
    }
  }

  @Test
  public void testDecimal64RoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal().withPrecision(10).withScale(2));

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = row + 1;
          else
            col2.vector[row] = -1 * row;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(col2.vector[0], 1);
      assertEquals(col2.vector[511], 0);
      assertEquals(col2.vector[1020],  1021);
      assertEquals(col2.vector[1021], 0);
    }
  }

  @Test
  public void testDecimal64NullRoundRobbinFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("decimal1", TypeDescription.createDecimal().withPrecision(10).withScale(2));

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = row + 1;
        }
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      Decimal64ColumnVector col2 = (Decimal64ColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(col2.vector[0], 1);
      assertEquals(col2.vector[511], 0);
      assertEquals(col2.vector[1020],  1021);
      assertEquals(col2.vector[1021], 0);
    }
  }

  @Test
  public void testDoubleRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("double2", TypeDescription.createDouble());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) ==0 )
            col2.vector[row] = 100;
          else
            col2.vector[row] = 999;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 100)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(100.0, col2.vector[0]);
      assertEquals(0.0, col2.vector[511]);
      assertEquals(100, col2.vector[1020]);
      assertEquals(0, col2.vector[1021]);
    }
  }

  @Test
  public void testFloatRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("float2", TypeDescription.createFloat());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) ==0 )
            col2.vector[row] = 100+row;
          else
            col2.vector[row] = 999;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      DoubleColumnVector col2 = (DoubleColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] != 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertTrue(col2.vector[0] != 999.0);
      assertEquals(0.0, col2.vector[511]);
      assertEquals(1120.0, col2.vector[1020]);
      assertEquals(0, col2.vector[1021]);
    }
  }

  @Test
  public void testCharRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("char2", TypeDescription.createChar());

    byte[] passData = ("p").getBytes(StandardCharsets.UTF_8);
    byte[] failData = ("f").getBytes(StandardCharsets.UTF_8);

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.setVal(row, passData);
          else
            col2.setVal(row, failData);
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals("p", col2.toString(0));
      assertTrue(col2.toString(511).isEmpty());
      assertEquals("p", col2.toString(1020));
      assertTrue(col2.toString(1021).isEmpty());
    }
  }

  @Test
  public void testVarCharRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("varchar2", TypeDescription.createVarchar());

    byte[] passData = ("p").getBytes(StandardCharsets.UTF_8);
    byte[] failData = ("f").getBytes(StandardCharsets.UTF_8);

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.setVal(row, passData);
          else
            col2.setVal(row, failData);
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals("p", col2.toString(0));
      assertTrue(col2.toString(511).isEmpty());
      assertEquals("p", col2.toString(1020));
      assertTrue(col2.toString(1021).isEmpty());
    }
  }

  @Test
  public void testDirectStringRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 10 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("string1", TypeDescription.createString());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      // Write 50 batches where each batch has a single value for str.
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) ==0 )
            col2.setVal(row, ("passData-" + row).getBytes(StandardCharsets.UTF_8));
          else
            col2.setVal(row, ("failData-" + row).getBytes(StandardCharsets.UTF_8));
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertTrue(col2.toString(0).startsWith("pass"));
      assertTrue(col2.toString(511).isEmpty());
      assertTrue(col2.toString(1020).startsWith("pass"));
    }
  }

  @Test
  public void testDictionaryStringRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 100 * ColumnBatchRows;
    final int NUM_BATCHES = 100;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("string1", TypeDescription.createString());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if (row % 2 ==0)
            col2.setVal(row, ("passData").getBytes(StandardCharsets.UTF_8));
          else
            col2.setVal(row, ("failData").getBytes(StandardCharsets.UTF_8));
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      BytesColumnVector col2 = (BytesColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (!col2.toString(r).isEmpty())
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertTrue(col2.toString(0).startsWith("pass"));
      assertTrue(col2.toString(511).isEmpty());
      assertTrue(col2.toString(1020).startsWith("pass"));
    }
  }

  @Test
  public void testRepeatingBooleanRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("bool2", TypeDescription.createBoolean());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          col2.vector[row] = 0;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * ColumnBatchRows, noNullCnt);
      assertFalse(col2.isRepeating);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(0, col2.vector[0]);
      assertEquals(0, col2.vector[511]);
      assertEquals(0, col2.vector[1020]);
      assertEquals(0, col2.vector[1021]);
    }
  }

  @Test
  public void testBooleanRoundRobbinRowFilterCallback() throws Exception {
    final int INDEX_STRIDE = 0;
    final int NUM_BATCHES = 10;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("bool2", TypeDescription.createBoolean());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.vector[row] = 1;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(1, col2.vector[0]);
      assertEquals(0, col2.vector[511]);
      assertEquals(1, col2.vector[1020]);
      assertEquals(0, col2.vector[1021]);
    }
  }

  @Test
  public void testBooleanAnyRowFilterCallback() throws Exception {
    final int INDEX_STRIDE = 0;
    final int NUM_BATCHES = 100;

    // ORC write some data (one PASSing row per batch)
    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("bool2", TypeDescription.createBoolean());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if (row == 924 || row == 940)
            col2.vector[row] = 1;
        }
        col1.isRepeating = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intAnyRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      LongColumnVector col2 = (LongColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.vector[r] == 1)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 2, noNullCnt);
      assertEquals(924, batch.selected[0]);
      assertEquals(940, batch.selected[1]);
      assertEquals(0, col2.vector[0]);
      assertEquals(0, col2.vector[511]);
      assertEquals(0, col2.vector[1020]);
      assertEquals(1, col2.vector[924]);
      assertEquals(1, col2.vector[940]);
    }
  }

  @Test
  public void testTimestampRoundRobbinRowFilterCallback() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription schema = TypeDescription.createStruct()
        .addField("int1", TypeDescription.createInt())
        .addField("ts2", TypeDescription.createTimestamp());

    try (Writer writer = OrcFile.createWriter(testFilePath,
        OrcFile.writerOptions(conf)
            .setSchema(schema)
            .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = schema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.set(row, Timestamp.valueOf((1900+row)+"-04-01 12:34:56.9"));
          else {
            col2.isNull[row] = true;
            col2.set(row, null);
          }
        }
        col1.isRepeating = true;
        col1.noNulls = false;
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"int1"}, TestRowFilteringSkip::intRoundRobbinRowFilter))) {
      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col2.getTime(r) == 0)
            noNullCnt ++;
        }
      }
      // Make sure that our filter worked
      assertEquals(NUM_BATCHES * 512, noNullCnt);
      assertFalse(col2.isRepeating);
      assertEquals(0, batch.selected[0]);
      assertEquals(2, batch.selected[1]);
      assertEquals(0, convertTime(col2.getTime(0)).compareTo("1900-04-1 12:34:56.900"));
      assertEquals(0, col2.getTime(511));
      assertEquals(0, convertTime(col2.getTime(1020)).compareTo("2920-04-1 12:34:56.900"));
      assertEquals(0, col2.getTime(1021));
    }
  }

  @Test
  public void testSchemaEvolutionMissingColumn() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription fileSchema = TypeDescription.createStruct()
      .addField("int1", TypeDescription.createInt())
      .addField("ts2", TypeDescription.createTimestamp());

    try (Writer writer = OrcFile.createWriter(testFilePath,
                                              OrcFile.writerOptions(conf)
                                                .setSchema(fileSchema)
                                                .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = fileSchema.createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          col1.vector[row] = row;
          if ((row % 2) == 0)
            col2.set(row, Timestamp.valueOf((1900+row)+"-04-01 12:34:56.9"));
          else {
            col2.isNull[row] = true;
            col2.set(row, null);
          }
        }
        col1.noNulls = true;
        col2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    TypeDescription readSchema = fileSchema
      .clone()
      .addField("missing", TypeDescription.createInt());
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    // Read nothing with NOT NULL filter on missing
    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"missing"}, TestRowFilteringSkip::notNullFilterMissing))) {
      VectorizedRowBatch batch = readSchema.createRowBatchV2();

      assertFalse(rows.nextBatch(batch));
    }

    // Read everything with select all filter on missing
    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"missing"}, TestRowFilteringSkip::allowAll))) {
      VectorizedRowBatch batch = readSchema
        .createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64, ColumnBatchRows);
      long rowCount = 0;
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col2 = (TimestampColumnVector) batch.cols[1];
      while (rows.nextBatch(batch)) {
        rowCount += batch.size;
        assertFalse(batch.cols[2].noNulls);
        assertTrue(batch.cols[2].isRepeating);
        assertTrue(batch.cols[2].isNull[0]);
        for (int i = 0; i < batch.size; i++) {
          assertEquals(i, col1.vector[i]);
          if (i % 2 == 0) {
            assertEquals(Timestamp.valueOf((1900+i)+"-04-01 12:34:56.9"),
                         col2.asScratchTimestamp(i));
          } else {
            assertTrue(col2.isNull[i]);
          }
        }
      }
      assertEquals(ColumnBatchRows * NUM_BATCHES, rowCount);
    }
  }

  @Test
  public void testSchemaEvolutionMissingNestedColumn() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription fileSchema = TypeDescription.createStruct()
      .addField("int1", TypeDescription.createInt())
      .addField("s2", TypeDescription.createStruct()
        .addField("ts2", TypeDescription.createTimestamp()));

    try (Writer writer = OrcFile.createWriter(testFilePath,
                                              OrcFile.writerOptions(conf)
                                                .setSchema(fileSchema)
                                                .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = fileSchema.createRowBatchV2();
      LongColumnVector int1 = (LongColumnVector) batch.cols[0];
      StructColumnVector s2 = (StructColumnVector) batch.cols[1];
      TimestampColumnVector ts2 = (TimestampColumnVector) s2.fields[0];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          int1.vector[row] = row;
          if ((row % 2) == 0)
            ts2.set(row, Timestamp.valueOf((1900+row)+"-04-01 12:34:56.9"));
          else {
            s2.isNull[row] = true;
          }
        }
        int1.noNulls = true;
        ts2.noNulls = false;
        s2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    TypeDescription readSchema = fileSchema.clone();
    readSchema
      .findSubtype("s2")
      .addField("missing", TypeDescription.createInt());
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    // Read nothing with NOT NULL filter on missing
    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"s2.missing"},
                      TestRowFilteringSkip::notNullFilterNestedMissing))) {
      VectorizedRowBatch batch = readSchema.createRowBatchV2();
      assertFalse(rows.nextBatch(batch));
    }

    // Read everything with select all filter on missing
    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"s2.missing"}, TestRowFilteringSkip::allowAll))) {
      VectorizedRowBatch batch = readSchema
        .createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64, ColumnBatchRows);
      long rowCount = 0;
      LongColumnVector int1 = (LongColumnVector) batch.cols[0];
      StructColumnVector s2 = (StructColumnVector) batch.cols[1];
      TimestampColumnVector ts2 = (TimestampColumnVector) s2.fields[0];
      while (rows.nextBatch(batch)) {
        rowCount += batch.size;
        // Validate that the missing column is null
        assertFalse(s2.fields[1].noNulls);
        assertTrue(s2.fields[1].isRepeating);
        assertTrue(s2.fields[1].isNull[0]);
        for (int i = 0; i < batch.size; i++) {
          assertEquals(i, int1.vector[i]);
          if (i % 2 == 0) {
            assertEquals(Timestamp.valueOf((1900+i)+"-04-01 12:34:56.9"),
                         ts2.asScratchTimestamp(i));
          } else {
            assertTrue(s2.isNull[i]);
            assertTrue(ts2.isNull[i]);
          }
        }
      }
      assertEquals(ColumnBatchRows * NUM_BATCHES, rowCount);
    }
  }

  @Test
  public void testSchemaEvolutionMissingAllChildren() throws Exception {
    // Set the row stride to a multiple of the batch size
    final int INDEX_STRIDE = 16 * ColumnBatchRows;
    final int NUM_BATCHES = 10;

    TypeDescription fileSchema = TypeDescription.createStruct()
      .addField("int1", TypeDescription.createInt())
      .addField("s2", TypeDescription.createStruct()
        .addField("ts2", TypeDescription.createTimestamp()));

    try (Writer writer = OrcFile.createWriter(testFilePath,
                                              OrcFile.writerOptions(conf)
                                                .setSchema(fileSchema)
                                                .rowIndexStride(INDEX_STRIDE))) {
      VectorizedRowBatch batch = fileSchema.createRowBatchV2();
      LongColumnVector int1 = (LongColumnVector) batch.cols[0];
      StructColumnVector s2 = (StructColumnVector) batch.cols[1];
      TimestampColumnVector ts2 = (TimestampColumnVector) s2.fields[0];
      for (int b=0; b < NUM_BATCHES; ++b) {
        batch.reset();
        batch.size = ColumnBatchRows;
        for (int row = 0; row < batch.size; row++) {
          int1.vector[row] = row;
          if ((row % 2) == 0)
            ts2.set(row, Timestamp.valueOf((1900+row)+"-04-01 12:34:56.9"));
          else {
            s2.isNull[row] = true;
          }
        }
        int1.noNulls = true;
        ts2.noNulls = false;
        s2.noNulls = false;
        writer.addRowBatch(batch);
      }
    }

    TypeDescription readSchema = TypeDescription.createStruct()
      .addField("int1", TypeDescription.createInt())
      .addField("s2", TypeDescription.createStruct()
        .addField("missing_other", TypeDescription.createString())
        .addField("missing", TypeDescription.createInt()));
    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    // Read nothing with NOT NULL filter on missing
    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"s2.missing"},
                      TestRowFilteringSkip::notNullFilterNestedMissing))) {
      VectorizedRowBatch batch = readSchema.createRowBatchV2();
      assertFalse(rows.nextBatch(batch));
    }

    // Read everything with select all filter on missing
    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
      reader.options()
        .schema(readSchema)
        .setRowFilter(new String[]{"s2.missing"}, TestRowFilteringSkip::allowAll))) {
      VectorizedRowBatch batch = readSchema
        .createRowBatch(TypeDescription.RowBatchVersion.USE_DECIMAL64, ColumnBatchRows);
      long rowCount = 0;
      LongColumnVector int1 = (LongColumnVector) batch.cols[0];
      StructColumnVector s2 = (StructColumnVector) batch.cols[1];
      LongColumnVector missing = (LongColumnVector) s2.fields[1];
      BytesColumnVector missingOther = (BytesColumnVector) s2.fields[0];
      while (rows.nextBatch(batch)) {
        rowCount += batch.size;
        // Validate that the missing columns are null
        assertFalse(missing.noNulls);
        assertTrue(missing.isRepeating);
        assertTrue(missing.isNull[0]);
        assertFalse(missingOther.noNulls);
        assertTrue(missingOther.isRepeating);
        assertTrue(missingOther.isNull[0]);
        // Struct column vector should still give the correct null and not null alternating
        assertFalse(s2.isRepeating);
        assertFalse(s2.noNulls);
        for (int i = 0; i < batch.size; i++) {
          assertEquals(i, int1.vector[i]);
          assertEquals(i % 2 != 0, s2.isNull[i]);
        }
      }
      assertEquals(ColumnBatchRows * NUM_BATCHES, rowCount);
    }
  }

  private static void notNullFilterMissing(OrcFilterContext batch) {
    int selIdx = 0;
    ColumnVector cv = ((OrcFilterContextImpl) batch).getCols()[2];
    if (cv.isRepeating) {
      if (!cv.isNull[0]) {
        for (int i = 0; i < batch.getSelectedSize(); i++) {
          batch.getSelected()[selIdx++] = i;
        }
      }
    } else {
      for (int i = 0; i < batch.getSelectedSize(); i++) {
        if (!((OrcFilterContextImpl) batch).getCols()[2].isNull[i]) {
          batch.getSelected()[selIdx++] = i;
        }
      }
    }
    batch.setSelectedInUse(true);
    batch.setSelectedSize(selIdx);
  }

  private static void notNullFilterNestedMissing(OrcFilterContext batch) {
    int selIdx = 0;
    StructColumnVector scv = (StructColumnVector) ((OrcFilterContextImpl) batch).getCols()[1];
    ColumnVector cv = scv.fields[1];
    if (cv.isRepeating) {
      if (!cv.isNull[0]) {
        for (int i = 0; i < batch.getSelectedSize(); i++) {
          batch.getSelected()[selIdx++] = i;
        }
      }
    } else {
      for (int i = 0; i < batch.getSelectedSize(); i++) {
        if (!cv.isNull[i]) {
          batch.getSelected()[selIdx++] = i;
        }
      }
    }
    batch.setSelectedInUse(true);
    batch.setSelectedSize(selIdx);
  }

  private static void allowAll(OrcFilterContext batch) {
    // Do nothing everything is allowed by default
  }

  @Test
  public void testCustomFileTimestampRoundRobbinRowFilterCallback() throws Exception {
    testFilePath = new Path(getClass().getClassLoader().
        getSystemResource("orc_split_elim.orc").getPath());

    Reader reader = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf));

    try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(
        reader.options()
            .setRowFilter(new String[]{"userid"}, TestRowFilteringSkip::intCustomValueFilter))) {

      VectorizedRowBatch batch = reader.getSchema().createRowBatchV2();
      LongColumnVector col1 = (LongColumnVector) batch.cols[0];
      TimestampColumnVector col5 = (TimestampColumnVector) batch.cols[4];

      // We assume that it fits in a single stripe
      assertEquals(1, reader.getStripes().size());

      int noNullCnt = 0;
      while (rows.nextBatch(batch)) {
        assertTrue(batch.selectedInUse);
        assertNotNull(batch.selected);
        // Rows are filtered so it should never be 1024
        assertTrue(batch.size != ColumnBatchRows);
        assertTrue(col1.noNulls);
        for (int r = 0; r < ColumnBatchRows; ++r) {
          if (col1.vector[r] != 100) noNullCnt ++;
        }
        // We should always select 1 row as the file is spaced as such. We could get 0 in case all
        // the rows are filtered out.
        if (batch.size == 0) {
          continue;
        }
        assertEquals(1, batch.size);
        long val = col1.vector[batch.selected[0]] ;
        // Check that we have read the valid value
        assertTrue((val == 2) || (val == 5) || (val == 13) || (val == 29) || (val == 70));
        if (val == 2) {
          assertEquals(0, col5.getTime(batch.selected[0]));
        } else {
          assertNotEquals(0, col5.getTime(batch.selected[0]));
        }

        // Check that unselected is not populated
        assertEquals(0, batch.selected[1]);
      }

      // Total rows of the file should be 25k
      assertEquals(25000, rowCount);
      // Make sure that our filter worked ( 5 rows with userId != 100)
      assertEquals(5, noNullCnt);
    }
  }
}
