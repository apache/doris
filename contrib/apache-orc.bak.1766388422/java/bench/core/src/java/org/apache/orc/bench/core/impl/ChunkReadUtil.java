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

package org.apache.orc.bench.core.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class ChunkReadUtil {
  private static final int SCALE = 6;

  static void setConf(Configuration conf, int minSeekSize, double extraByteTolerance) {
    OrcConf.ORC_MIN_DISK_SEEK_SIZE.setInt(conf, minSeekSize);
    OrcConf.ORC_MIN_DISK_SEEK_SIZE_TOLERANCE.setDouble(conf, extraByteTolerance);
  }

  static long readORCFile(Path file, Configuration conf, boolean alternate)
    throws IOException {
    Reader r = OrcFile.createReader(file, OrcFile.readerOptions(conf));
    long rowCount = 0;
    VectorizedRowBatch batch = r.getSchema().createRowBatch();
    Reader.Options o = r.options();
    if (alternate) {
      o.include(includeAlternate(r.getSchema()));
    }
    RecordReader rr = r.rows(o);
    while (rr.nextBatch(batch)) {
      rowCount += batch.size;
    }
    return rowCount;
  }

  private static boolean[] includeAlternate(TypeDescription schema) {
    boolean[] includes = new boolean[schema.getMaximumId() + 1];
    for (int i = 1; i < includes.length; i += 2) {
      includes[i] = true;
    }
    includes[0] = true;
    return includes;
  }

  static long createORCFile(int colCount, int rowCount, Path file) throws IOException {
    TypeDescription schema = createSchema(colCount);
    return writeFile(schema, rowCount, file);
  }

  private static long writeFile(TypeDescription schema, int rowCount, Path path)
    throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    try (Writer writer = OrcFile.createWriter(path,
                                              OrcFile.writerOptions(conf)
                                                .fileSystem(fs)
                                                .overwrite(true)
                                                .rowIndexStride(8192)
                                                .setSchema(schema)
                                                .overwrite(true))) {
      Random rnd = new Random(1024);
      VectorizedRowBatch b = schema.createRowBatch();
      for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
        ((LongColumnVector) b.cols[0]).vector[b.size] = rowIdx;
        long v = rnd.nextLong();
        for (int colIdx = 1; colIdx < schema.getChildren().size() - 1; colIdx++) {
          switch (schema.getChildren().get(colIdx).getCategory()) {
            case LONG:
              ((LongColumnVector) b.cols[colIdx]).vector[b.size] = v;
              break;
            case DECIMAL:
              HiveDecimalWritable d = new HiveDecimalWritable();
              d.setFromLongAndScale(v, SCALE);
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
    return fs.getFileStatus(path).getLen();
  }

  private static TypeDescription createSchema(int colCount) {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("id", TypeDescription.createLong());
    for (int i = 1; i <= colCount; i++) {
      TypeDescription fieldType;
      switch (i % 3) {
        case 0:
          fieldType = TypeDescription.createString();
          break;
        case 1:
          fieldType = TypeDescription.createDecimal().withPrecision(20).withScale(SCALE);
          break;
        default:
          fieldType = TypeDescription.createLong();
          break;
      }
      schema.addField("f_" + i, fieldType);
    }
    return schema;
  }
}
