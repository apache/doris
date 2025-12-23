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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMinSeekSize {
  private static final Logger LOG = LoggerFactory.getLogger(TestMinSeekSize.class);
  private static final Path workDir = new Path(System.getProperty("test.tmp.dir",
                                                                  "target" + File.separator + "test"
                                                                  + File.separator + "tmp"));
  private static final Path filePath = new Path(workDir, "min_seek_size_file.orc");
  private static Configuration conf;
  private static FileSystem fs;

  private static final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createDecimal().withPrecision(20).withScale(6))
    .addField("f3", TypeDescription.createLong())
    .addField("f4", TypeDescription.createString())
    .addField("ridx", TypeDescription.createLong());
  private static final boolean[] AlternateColumns = new boolean[] {true, true, false, true, false
    , true};
  private static final long RowCount = 16384;
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
        for (int colIdx = 0; colIdx < schema.getChildren().size() - 1; colIdx++) {
          switch (schema.getChildren().get(colIdx).getCategory()) {
            case LONG:
              ((LongColumnVector) b.cols[colIdx]).vector[b.size] = v;
              break;
            case DECIMAL:
              HiveDecimalWritable d = new HiveDecimalWritable();
              d.setFromLongAndScale(v, scale);
              ((DecimalColumnVector) b.cols[colIdx]).vector[b.size] = d;
              break;
            case STRING:
              ((BytesColumnVector) b.cols[colIdx]).setVal(b.size,
                                                          String.valueOf(v)
                                                            .getBytes(StandardCharsets.UTF_8));
              break;
            default:
              throw new IllegalArgumentException();
          }
        }
        // Populate the rowIdx
        ((LongColumnVector) b.cols[4]).vector[b.size] = rowIdx;

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

  @Test
  public void writeIsSuccessful() throws IOException {
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    assertEquals(RowCount, r.getNumberOfRows());
  }

  private long validateFilteredRecordReader(RecordReader rr, VectorizedRowBatch b)
    throws IOException {
    long rowCount = 0;
    while (rr.nextBatch(b)) {
      validateBatch(b, rowCount);
      rowCount += b.size;
    }
    return rowCount;
  }

  private void validateColumnNull(VectorizedRowBatch b, int colIdx) {
    assertFalse(b.cols[colIdx].noNulls);
    assertTrue(b.cols[colIdx].isRepeating);
    assertTrue(b.cols[colIdx].isNull[0]);
  }

  private void validateBatch(VectorizedRowBatch b, long expRowNum) {
    HiveDecimalWritable d = new HiveDecimalWritable();
    validateColumnNull(b, 1);
    validateColumnNull(b, 3);
    for (int i = 0; i < b.size; i++) {
      int rowIdx;
      if (b.selectedInUse) {
        rowIdx = b.selected[i];
      } else {
        rowIdx = i;
      }
      long expValue = ((LongColumnVector) b.cols[0]).vector[rowIdx];
      d.setFromLongAndScale(expValue, scale);
      assertEquals(expValue, ((LongColumnVector) b.cols[2]).vector[rowIdx]);
      if (expRowNum != -1) {
        assertEquals(expRowNum + i, ((LongColumnVector) b.cols[4]).vector[rowIdx]);
      }
    }
  }

  @Test
  public void readAlternateColumnsWOMinSeekSize() throws IOException {
    readStart();
    OrcConf.ORC_MIN_DISK_SEEK_SIZE.setInt(conf, 0);
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    Reader.Options opts = r.options().include(AlternateColumns);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(opts)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    FileSystem.Statistics stats = readEnd();
    double p = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    assertTrue(p < 60);
  }

  @Test
  public void readAlternateColumnsWMinSeekSize() throws IOException {
    readStart();
    OrcConf.ORC_MIN_DISK_SEEK_SIZE.setInt(conf, 1024 * 1024);
    Reader r = OrcFile.createReader(filePath, OrcFile.readerOptions(conf).filesystem(fs));
    Reader.Options opts = r.options().include(AlternateColumns);
    assertEquals(opts.minSeekSize(), 1024 * 1024);
    VectorizedRowBatch b = schema.createRowBatch();
    long rowCount;
    try (RecordReader rr = r.rows(opts)) {
      rowCount = validateFilteredRecordReader(rr, b);
    }
    FileSystem.Statistics stats = readEnd();
    double p = readPercentage(stats, fs.getFileStatus(filePath).getLen());
    assertEquals(RowCount, rowCount);
    // Read all bytes
    assertTrue(p >= 100);
  }

  private double readPercentage(FileSystem.Statistics stats, long fileSize) {
    double p = stats.getBytesRead() * 100.0 / fileSize;
    LOG.info(String.format("%nFileSize: %d%nReadSize: %d%nRead %%: %.2f",
                           fileSize,
                           stats.getBytesRead(),
                           p));
    return p;
  }

  private static void readStart() {
    FileSystem.clearStatistics();
  }

  private static FileSystem.Statistics readEnd() {
    return FileSystem.getAllStatistics().get(0);
  }
}
