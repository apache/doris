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

package org.apache.orc.bench.core.convert.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.convert.BatchReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class AvroReader implements BatchReader {
  private final DataFileReader<GenericRecord> dataFileReader;
  private GenericRecord record = null;
  private final AvroConverter[] converters;

  public AvroReader(Path path,
                    TypeDescription schema,
                    Configuration conf) throws IOException {
    FsInput file = new FsInput(path, conf);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    dataFileReader = new DataFileReader<>(file, datumReader);
    converters = buildConverters(schema);
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    int maxSize = batch.getMaxSize();
    while (dataFileReader.hasNext() && batch.size < maxSize) {
      record = dataFileReader.next(record);
      int row = batch.size++;
      for(int c=0; c < converters.length; ++c) {
        converters[c].convert(batch.cols[c], row, record.get(c));
      }
    }
    return batch.size != 0;
  }

  @Override
  public void close() throws IOException {
    dataFileReader.close();
  }

  public interface AvroConverter {
    void convert(ColumnVector vector, int row, Object value);
  }

  public static AvroConverter[] buildConverters(TypeDescription orcType) {
    List<TypeDescription> children = orcType.getChildren();
    AvroConverter[] result = new AvroConverter[children.size()];
    for(int c=0; c < result.length; ++c) {
      result[c] = createConverter(children.get(c));
    }
    return result;
  }

  private static class BooleanConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((LongColumnVector) cv).vector[row] =
            ((Boolean) value).booleanValue() ? 1 : 0;
      }
    }
  }

  private static class IntConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((LongColumnVector) cv).vector[row] =
            ((Integer) value).intValue();
      }
    }
  }

  private static class LongConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((LongColumnVector) cv).vector[row] =
            ((Long) value).longValue();
      }
    }
  }

  private static class FloatConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((DoubleColumnVector) cv).vector[row] =
            ((Float) value).floatValue();
      }
    }
  }

  private static class DoubleConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ((DoubleColumnVector) cv).vector[row] =
            ((Double) value).doubleValue();
      }
    }
  }

  private static class StringConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        byte[] bytes = ((Utf8) value).getBytes();
        ((BytesColumnVector) cv).setRef(row, bytes, 0, bytes.length);
      }
    }
  }

  private static class BinaryConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ByteBuffer buf = (ByteBuffer) value;
        ((BytesColumnVector) cv).setVal(row, buf.array(), buf.arrayOffset(),
            buf.remaining());
      }
    }
  }

  private static class TimestampConverter implements AvroConverter {
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        TimestampColumnVector tc = (TimestampColumnVector) cv;
        tc.time[row] = ((Long) value).longValue();
        tc.nanos[row] = 0;
      }
    }
  }

  private static class DecimalConverter implements AvroConverter {
    final int scale;
    final double multiplier;
    DecimalConverter(int scale) {
      this.scale = scale;
      this.multiplier = Math.pow(10.0, this.scale);
    }
    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        DecimalColumnVector tc = (DecimalColumnVector) cv;
        tc.vector[row].set(HiveDecimal.create(Math.round((double) value * multiplier)));
      }
    }
  }

  private static class ListConverter implements AvroConverter {
    final AvroConverter childConverter;

    ListConverter(TypeDescription schema) {
      childConverter = createConverter(schema.getChildren().get(0));
    }

    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        ListColumnVector tc = (ListColumnVector) cv;
        List array = (List) value;
        int start = tc.childCount;
        int len = array.size();
        tc.childCount += len;
        tc.child.ensureSize(tc.childCount, true);
        for (int i = 0; i < len; ++i) {
          childConverter.convert(tc.child, start + i, array.get(i));
        }
      }
    }
  }

  private static class StructConverter implements AvroConverter {
    final AvroConverter[] childConverters;

    StructConverter(TypeDescription schema) {
      List<TypeDescription> children = schema.getChildren();
      childConverters = new AvroConverter[children.size()];
      for(int i=0; i < childConverters.length; ++i) {
        childConverters[i] = createConverter(children.get(i));
      }
    }

    public void convert(ColumnVector cv, int row, Object value) {
      if (value == null) {
        cv.noNulls = false;
        cv.isNull[row] = true;
      } else {
        StructColumnVector tc = (StructColumnVector) cv;
        GenericData.Record record = (GenericData.Record) value;
        for(int c=0; c < tc.fields.length; ++c) {
          childConverters[c].convert(tc.fields[c], row, record.get(c));
        }
      }
    }
  }

  static AvroConverter createConverter(TypeDescription types) {
    switch (types.getCategory()) {
      case BINARY:
        return new BinaryConverter();
      case BOOLEAN:
        return new BooleanConverter();
      case BYTE:
      case SHORT:
      case INT:
        return new IntConverter();
      case LONG:
        return new LongConverter();
      case FLOAT:
        return new FloatConverter();
      case DOUBLE:
        return new DoubleConverter();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new StringConverter();
      case TIMESTAMP:
        return new TimestampConverter();
      case DECIMAL:
        return new DecimalConverter(types.getScale());
      case LIST:
        return new ListConverter(types);
      case STRUCT:
        return new StructConverter(types);
      default:
        throw new IllegalArgumentException("Unhandled type " + types);
    }
  }

  static byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
    byteBuffer.rewind();
    byte[] result = new byte[byteBuffer.limit()];
    byteBuffer.get(result);
    return result;
  }
}
