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
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRowFilteringComplexTypesNulls {
  private static final Logger LOG =
    LoggerFactory.getLogger(TestRowFilteringComplexTypesNulls.class);
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                                  "target" + File.separator + "test"
                                                                  + File.separator + "tmp"));
  private static final Path filePath = new Path(workDir, "complex_null_file.orc");
  private static Configuration conf;
  private static FileSystem fs;

  private static final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("s2", TypeDescription.createStruct()
      .addField("f2", TypeDescription.createDecimal().withPrecision(20).withScale(6))
      .addField("f2f", TypeDescription.createString())
    )
    .addField("u3", TypeDescription.createUnion()
      .addUnionChild(TypeDescription.createLong())
      .addUnionChild(TypeDescription.createString())
    )
    .addField("f4", TypeDescription.createString())
    .addField("ridx", TypeDescription.createLong());
  private static final long RowCount = 4000000L;
  private static final String[] FilterColumns = new String[] {"ridx", "s2.f2", "u3.0"};
  private static final int scale = 3;

  @BeforeAll
  public static void setup() throws IOException {
    conf = new Configuration();
    fs = FileSystem.get(conf);

    LOG.info("Creating file {} with schema {}", filePath, schema);
    try (Writer writer = OrcFile.createWriter(filePath,
                                              OrcFile.writerOptions(conf)
                                                .fileSystem(fs)
                                                .overwrite(true)
                                                .rowIndexStride(8192)
                                                .setSchema(schema))) {
      Random rnd = new Random(1024);
      VectorizedRowBatch b = schema.createRowBatch();
      for (int rowIdx = 0; rowIdx < RowCount; rowIdx++) {
        long v = rnd.nextLong();
        addRow(b, rowIdx, v);
        b.size += 1;
        if (b.size == b.getMaxSize()) {
          writer.addRowBatch(b);
          b.reset();
        }
      }
      if (b.size > 0) {
        writer.addRowBatch(b);
        b.reset();
      }
    }
    LOG.info("Created file {}", filePath);
  }

  private static void addRow(VectorizedRowBatch b, long rowIdx, long rndValue) {
    //rndValue = rowIdx;
    // column f1: LONG
    ((LongColumnVector) b.cols[0]).vector[b.size] = rndValue;
    // column s2: STRUCT<fs: DECIMAL(20, 6)> alternate null values at STRUCT, no nulls on CHILD
    if (rowIdx % 2 == 0) {
      b.cols[1].noNulls = false;
      b.cols[1].isNull[b.size] = true;
    } else {
      HiveDecimalWritable d = new HiveDecimalWritable();
      d.setFromLongAndScale(rndValue, scale);
      ((DecimalColumnVector) ((StructColumnVector) b.cols[1]).fields[0]).vector[b.size] = d;
      ((BytesColumnVector) ((StructColumnVector) b.cols[1]).fields[1])
        .setVal(b.size, String.valueOf(rndValue).getBytes(StandardCharsets.UTF_8));
    }
    // column u3: UNION<LONG, STRING> repeat, NULL, LONG, STRING
    if (rowIdx % 3 == 0) {
      b.cols[2].noNulls = false;
      b.cols[2].isNull[b.size] = true;
    } else if (rowIdx % 3 == 1) {
      ((UnionColumnVector) b.cols[2]).tags[b.size] = 0;
      ((LongColumnVector) ((UnionColumnVector) b.cols[2]).fields[0]).vector[b.size] = rndValue;
    } else {
      ((UnionColumnVector) b.cols[2]).tags[b.size] = 1;
      ((BytesColumnVector) ((UnionColumnVector) b.cols[2]).fields[1])
        .setVal(b.size, String.valueOf(rndValue).getBytes(StandardCharsets.UTF_8));
    }
    // column f4: STRING
    ((BytesColumnVector) b.cols[3])
      .setVal(b.size, String.valueOf(rndValue).getBytes(StandardCharsets.UTF_8));
    // column ridx: LONG
    ((LongColumnVector) b.cols[4]).vector[b.size] = rowIdx;
  }

  private static HiveDecimalWritable getF2(VectorizedRowBatch b, int idx) {
    return ((DecimalColumnVector) ((StructColumnVector) b.cols[1]).fields[0]).vector[idx];
  }

  private static String getF2F(VectorizedRowBatch b, int idx) {
    return ((BytesColumnVector) ((StructColumnVector) b.cols[1]).fields[1]).toString(idx);
  }

  private static Object getF3(VectorizedRowBatch b, int idx) {
    UnionColumnVector v = (UnionColumnVector) b.cols[2];
    if (v.tags[idx] == 0) {
      return ((LongColumnVector) v.fields[0]).vector[idx];
    } else {
      return ((BytesColumnVector) v.fields[1]).toString(idx);
    }
  }

  @Test
  public void writeIsSuccessful() throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(RowCount, r.getNumberOfRows());
    assertTrue(r.getStripes().size() > 1);
  }

  @Test
  public void readEverything() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows()) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p >= 100);
  }

  @Test
  public void filterAllRowsStructColumn() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .setRowFilter(new String[] {"s2.f2"}, new F2InFilter(new HashSet<>(0)));
    long rowCount = 0;
    try (RecordReader rr = r.rows(options)) {
      while (rr.nextBatch(b)) {
        rowCount += b.size;
      }
    }
    FileSystem.Statistics stats = readEnd();
    assertEquals(0, rowCount);
    // We should read less than half the length of the file
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage < 50,
        String.format("Bytes read %.2f%% should be less than 50%%", readPercentage));
  }

  private long validateFilteredRecordReader(RecordReader rr, VectorizedRowBatch b)
    throws IOException {
    long rowCount = 0;
    while (rr.nextBatch(b)) {
      validateBatch(b, -1);
      rowCount += b.size;
    }
    return rowCount;
  }

  private void validateBatch(VectorizedRowBatch b, long expRowNum) {
    HiveDecimalWritable d = new HiveDecimalWritable();

    for (int i = 0; i < b.size; i++) {
      int idx;
      if (b.selectedInUse) {
        idx = b.selected[i];
      } else {
        idx = i;
      }
      long expValue = ((LongColumnVector) b.cols[0]).vector[idx];
      long rowIdx = ((LongColumnVector) b.cols[4]).vector[idx];
      //ridx
      if (expRowNum != -1) {
        assertEquals(expRowNum + i, rowIdx);
      }
      //s2
      if (rowIdx % 2 == 0) {
        assertTrue(b.cols[1].isNull[idx]);
      } else {
        d.setFromLongAndScale(expValue, scale);
        assertEquals(d, getF2(b, idx));
        assertEquals(String.valueOf(expValue), getF2F(b, idx));
      }
      //u3
      if (rowIdx % 3 == 0) {
        assertTrue(b.cols[2].isNull[idx]);
      } else if (rowIdx % 3 == 1) {
        assertEquals(expValue, getF3(b, idx));
      } else {
        assertEquals(String.valueOf(expValue), getF3(b, idx));
      }
      //f4
      BytesColumnVector sv = (BytesColumnVector) b.cols[3];
      assertEquals(String.valueOf(expValue),
                   sv.toString(idx));
    }
  }

  private double readPercentage(FileSystem.Statistics stats, long fileSize) {
    double p = stats.getBytesRead() * 100.0 / fileSize;
    LOG.info(String.format("%nFileSize: %d%nReadSize: %d%nRead %%: %.2f",
                           fileSize,
                           stats.getBytesRead(),
                           p));
    return p;
  }

  @Test
  public void readEverythingWithFilter() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(r.options()
                                    .setRowFilter(FilterColumns, new AllowAllFilter()))) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    double p = readPercentage(readEnd(), fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p >= 100);
  }

  @Test
  public void filterAlternateBatches() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .setRowFilter(FilterColumns, new AlternateFilter());
    long rowCount;
    try (RecordReader rr = r.rows(options)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    readEnd();
    assertTrue(RowCount > rowCount);
  }

  @Test
  public void filterWithSeek() throws IOException {
    readStart();
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    VectorizedRowBatch b = schema.createRowBatch();
    Reader.Options options = r.options()
      .setRowFilter(FilterColumns, new AlternateFilter());
    long seekRow;
    try (RecordReader rr = r.rows(options)) {
      // Validate the first batch
      assertTrue(rr.nextBatch(b));
      validateBatch(b, 0);
      assertEquals(b.size, rr.getRowNumber());

      // Read the next batch, will skip a batch that is filtered
      assertTrue(rr.nextBatch(b));
      validateBatch(b, 2048);
      assertEquals(2048 + 1024, rr.getRowNumber());

      // Seek forward
      seekToRow(rr, b, 4096);

      // Seek back to the filtered batch
      long bytesRead = readEnd().getBytesRead();
      seekToRow(rr, b, 1024);
      // No IO should have taken place
      assertEquals(bytesRead, readEnd().getBytesRead());

      // Seek forward to next row group, where the first batch is not filtered
      seekToRow(rr, b, 8192);

      // Seek forward to next row group but position on filtered batch
      seekToRow(rr, b, (8192 * 2) + 1024);

      // Seek forward to next stripe
      seekRow = r.getStripes().get(0).getNumberOfRows();
      seekToRow(rr, b, seekRow);

      // Seek back to previous stripe, filtered row, it should require more IO as a result of
      // stripe change
      bytesRead = readEnd().getBytesRead();
      seekToRow(rr, b, 1024);
      assertTrue(readEnd().getBytesRead() > bytesRead,
          "Change of stripe should require more IO");
    }
    FileSystem.Statistics stats = readEnd();
    double readPercentage = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertTrue(readPercentage > 130);
  }

  private void seekToRow(RecordReader rr, VectorizedRowBatch b, long row) throws IOException {
    rr.seekToRow(row);
    assertTrue(rr.nextBatch(b));
    long expRowNum;
    if ((row / b.getMaxSize()) % 2 == 0) {
      expRowNum = row;
    } else {
      // As the seek batch gets filtered
      expRowNum = row + b.getMaxSize();
    }
    validateBatch(b, expRowNum);
    assertEquals(expRowNum + b.getMaxSize(), rr.getRowNumber());
  }

  private static class F2InFilter implements Consumer<OrcFilterContext> {
    private final Set<HiveDecimal> ids;

    private F2InFilter(Set<HiveDecimal> ids) {
      this.ids = ids;
    }

    @Override
    public void accept(OrcFilterContext b) {
      int newSize = 0;
      ColumnVector[] f2Branch = b.findColumnVector("s2.f2");
      DecimalColumnVector f2 = (DecimalColumnVector) f2Branch[f2Branch.length - 1];
      for (int i = 0; i < b.getSelectedSize(); i++) {
        if (!OrcFilterContext.isNull(f2Branch, i)
            && ids.contains(f2.vector[i].getHiveDecimal())) {
          b.getSelected()[newSize] = i;
          newSize += 1;
        }
      }
      b.setSelectedInUse(true);
      b.setSelectedSize(newSize);
    }
  }

  /**
   * Fill odd batches values in a default read
   * if ridx(rowIdx) / 1024 is even then allow otherwise fail
   */
  private static class AlternateFilter implements Consumer<OrcFilterContext> {
    @Override
    public void accept(OrcFilterContext b) {
      LongColumnVector v = (LongColumnVector) ((OrcFilterContextImpl) b).getCols()[4];
      if ((v.vector[0] / 1024) % 2 == 1) {
        b.setSelectedInUse(true);
        b.setSelectedSize(0);
      }
    }
  }

  private static class AllowAllFilter implements Consumer<OrcFilterContext> {
    @Override
    public void accept(OrcFilterContext batch) {
      // do nothing every row is allowed
    }
  }

  private static void readStart() {
    FileSystem.clearStatistics();
  }

  private static FileSystem.Statistics readEnd() {
    return FileSystem.getAllStatistics().get(0);
  }
}
