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
package org.apache.orc.bench.core.convert.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.CompressionKind;
import org.apache.orc.bench.core.convert.BatchReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;

public class CsvReader implements BatchReader {
  private final Iterator<CSVRecord> parser;
  private final ColumnReader[] readers;

  interface ColumnReader {
    void read(String value, ColumnVector vect, int row);
  }

  static class LongColumnReader implements ColumnReader {
    public void read(String value, ColumnVector vect, int row) {
      if ("".equals(value)) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        LongColumnVector vector = (LongColumnVector) vect;
        vector.vector[row] = Long.parseLong(value);
      }
    }
  }

  static class DoubleColumnReader implements ColumnReader {
    public void read(String value, ColumnVector vect, int row) {
      if ("".equals(value)) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        DoubleColumnVector vector = (DoubleColumnVector) vect;
        vector.vector[row] = Double.parseDouble(value);
      }
    }
  }

  static class StringColumnReader implements ColumnReader {
    public void read(String value, ColumnVector vect, int row) {
      if ("".equals(value)) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        BytesColumnVector vector = (BytesColumnVector) vect;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        vector.setRef(row, bytes, 0, bytes.length);
      }
    }
  }

  static class TimestampColumnReader implements ColumnReader {
    public void read(String value, ColumnVector vect, int row) {
      if ("".equals(value)) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        TimestampColumnVector vector = (TimestampColumnVector) vect;
        vector.set(row, Timestamp.valueOf(value));
      }
    }
  }

  static class DecimalColumnReader implements ColumnReader {
    public void read(String value, ColumnVector vect, int row) {
      if ("".equals(value)) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        DecimalColumnVector vector = (DecimalColumnVector) vect;
        vector.vector[row].set(HiveDecimal.create(value));
      }
    }
  }

  ColumnReader createReader(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongColumnReader();
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnReader();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new StringColumnReader();
      case DECIMAL:
        return new DecimalColumnReader();
      case TIMESTAMP:
        return new TimestampColumnReader();
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  public CsvReader(Path path,
                   TypeDescription schema,
                   Configuration conf,
                   CompressionKind compress) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    InputStream input = compress.read(fs.open(path));
    parser = new CSVParser(new InputStreamReader(input, StandardCharsets.UTF_8),
        CSVFormat.RFC4180.withHeader()).iterator();
    List<TypeDescription> columnTypes = schema.getChildren();
    readers = new ColumnReader[columnTypes.size()];
    int c = 0;
    for(TypeDescription columnType: columnTypes) {
      readers[c++] = createReader(columnType);
    }
  }

  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    int maxSize = batch.getMaxSize();
    while (parser.hasNext() && batch.size < maxSize) {
      CSVRecord record = parser.next();
      int c = 0;
      for(String val: record) {
        readers[c].read(val, batch.cols[c], batch.size);
        c += 1;
      }
      batch.size++;
    }
    return batch.size != 0;
  }

  public void close() {
    // PASS
  }
}
