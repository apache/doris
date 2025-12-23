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

package org.apache.orc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FilterTestUtil {
  private final static Logger LOG = LoggerFactory.getLogger(FilterTestUtil.class);
  public static final TypeDescription schema = TypeDescription.createStruct()
    .addField("f1", TypeDescription.createLong())
    .addField("f2", TypeDescription.createDecimal().withPrecision(20).withScale(6))
    .addField("f3", TypeDescription.createLong())
    .addField("f4", TypeDescription.createString())
    .addField("ridx", TypeDescription.createLong());
  public static final long RowCount = 4000000L;
  private static final int scale = 3;

  public static void createFile(Configuration conf, FileSystem fs, Path filePath)
    throws IOException {
    if (fs.exists(filePath)) {
      return;
    }

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

  public static void validateRow(OrcStruct row, long expId) {
    HiveDecimalWritable d = new HiveDecimalWritable();

    if (expId > 0) {
      assertEquals(expId, ((LongWritable) row.getFieldValue(4)).get());
    }
    for (int i = 0; i < row.getNumFields(); i++) {
      long expValue = ((LongWritable) row.getFieldValue(0)).get();
      d.setFromLongAndScale(expValue, scale);
      assertEquals(d, row.getFieldValue(1));
      assertEquals(expValue, ((LongWritable) row.getFieldValue(2)).get());
      assertEquals(String.valueOf(expValue),
                          row.getFieldValue(3).toString());
    }
  }

  public static void validateLimitedRow(OrcStruct row, long expId) {
    HiveDecimalWritable d = new HiveDecimalWritable();

    if (expId > 0) {
      assertEquals(expId, ((LongWritable) row.getFieldValue(4)).get());
    }
    for (int i = 0; i < row.getNumFields(); i++) {
      long expValue = ((LongWritable) row.getFieldValue(0)).get();
      d.setFromLongAndScale(expValue, scale);
      assertEquals(d, row.getFieldValue(1));
      assertEquals(expValue, ((LongWritable) row.getFieldValue(2)).get());
    }
  }

  public static void validateRow(OrcStruct row) {
    validateRow(row, -1);
  }

  public static double readPercentage(FileSystem.Statistics stats, long fileSize) {
    double p = stats.getBytesRead() * 100.0 / fileSize;
    LOG.info(String.format("FileSize: %d%nReadSize: %d%nRead %%: %.2f",
                           fileSize,
                           stats.getBytesRead(),
                           p));
    return p;
  }

  public static void readStart() {
    FileSystem.clearStatistics();
  }

  public static FileSystem.Statistics readEnd() {
    return FileSystem.getAllStatistics().get(0);
  }
}
