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
package org.apache.orc.mapred;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This record reader implements the org.apache.hadoop.mapred API.
 * @param <V> the root type of the file
 */
public class OrcMapredRecordReader<V extends WritableComparable>
    implements org.apache.hadoop.mapred.RecordReader<NullWritable, V> {
  private final TypeDescription schema;
  private final RecordReader batchReader;
  private final VectorizedRowBatch batch;
  private int rowInBatch;

  public OrcMapredRecordReader(RecordReader reader,
                               TypeDescription schema) throws IOException {
    this.batchReader = reader;
    this.batch = schema.createRowBatch();
    this.schema = schema;
    rowInBatch = 0;
  }

  protected OrcMapredRecordReader(Reader fileReader,
                                  Reader.Options options) throws IOException {
    this(fileReader, options, options.getRowBatchSize());
  }

  protected OrcMapredRecordReader(Reader fileReader,
                                  Reader.Options options,
                                  int rowBatchSize) throws IOException {
    this.batchReader = fileReader.rows(options);
    if (options.getSchema() == null) {
      schema = fileReader.getSchema();
    } else {
      schema = options.getSchema();
    }
    this.batch = schema.createRowBatch(rowBatchSize);
    rowInBatch = 0;
  }

  /**
   * If the current batch is empty, get a new one.
   * @return true if we have rows available.
   * @throws IOException
   */
  boolean ensureBatch() throws IOException {
    if (rowInBatch >= batch.size) {
      rowInBatch = 0;
      return batchReader.nextBatch(batch);
    }
    return true;
  }

  @Override
  public boolean next(NullWritable key, V value) throws IOException {
    if (!ensureBatch()) {
      return false;
    }
    int rowIdx = batch.selectedInUse ? batch.selected[rowInBatch] : rowInBatch;
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      OrcStruct result = (OrcStruct) value;
      List<TypeDescription> children = schema.getChildren();
      int numberOfChildren = children.size();
      for(int i=0; i < numberOfChildren; ++i) {
        result.setFieldValue(i, nextValue(batch.cols[i], rowIdx,
            children.get(i), result.getFieldValue(i)));
      }
    } else {
      nextValue(batch.cols[0], rowIdx, schema, value);
    }
    rowInBatch += 1;
    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public V createValue() {
    return (V) OrcStruct.createValue(schema);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    batchReader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  static BooleanWritable nextBoolean(ColumnVector vector,
                                     int row,
                                     Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      BooleanWritable result;
      if (previous == null || previous.getClass() != BooleanWritable.class) {
        result = new BooleanWritable();
      } else {
        result = (BooleanWritable) previous;
      }
      result.set(((LongColumnVector) vector).vector[row] != 0);
      return result;
    } else {
      return null;
    }
  }

  static ByteWritable nextByte(ColumnVector vector,
                               int row,
                               Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      ByteWritable result;
      if (previous == null || previous.getClass() != ByteWritable.class) {
        result = new ByteWritable();
      } else {
        result = (ByteWritable) previous;
      }
      result.set((byte) ((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static ShortWritable nextShort(ColumnVector vector,
                                 int row,
                                 Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      ShortWritable result;
      if (previous == null || previous.getClass() != ShortWritable.class) {
        result = new ShortWritable();
      } else {
        result = (ShortWritable) previous;
      }
      result.set((short) ((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static IntWritable nextInt(ColumnVector vector,
                             int row,
                             Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      IntWritable result;
      if (previous == null || previous.getClass() != IntWritable.class) {
        result = new IntWritable();
      } else {
        result = (IntWritable) previous;
      }
      result.set((int) ((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static LongWritable nextLong(ColumnVector vector,
                               int row,
                               Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      LongWritable result;
      if (previous == null || previous.getClass() != LongWritable.class) {
        result = new LongWritable();
      } else {
        result = (LongWritable) previous;
      }
      result.set(((LongColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static FloatWritable nextFloat(ColumnVector vector,
                                 int row,
                                 Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      FloatWritable result;
      if (previous == null || previous.getClass() != FloatWritable.class) {
        result = new FloatWritable();
      } else {
        result = (FloatWritable) previous;
      }
      result.set((float) ((DoubleColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static DoubleWritable nextDouble(ColumnVector vector,
                                   int row,
                                   Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      DoubleWritable result;
      if (previous == null || previous.getClass() != DoubleWritable.class) {
        result = new DoubleWritable();
      } else {
        result = (DoubleWritable) previous;
      }
      result.set(((DoubleColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static Text nextString(ColumnVector vector,
                         int row,
                         Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      Text result;
      if (previous == null || previous.getClass() != Text.class) {
        result = new Text();
      } else {
        result = (Text) previous;
      }
      BytesColumnVector bytes = (BytesColumnVector) vector;
      result.set(bytes.vector[row], bytes.start[row], bytes.length[row]);
      return result;
    } else {
      return null;
    }
  }

  static BytesWritable nextBinary(ColumnVector vector,
                                  int row,
                                  Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      BytesWritable result;
      if (previous == null || previous.getClass() != BytesWritable.class) {
        result = new BytesWritable();
      } else {
        result = (BytesWritable) previous;
      }
      BytesColumnVector bytes = (BytesColumnVector) vector;
      result.set(bytes.vector[row], bytes.start[row], bytes.length[row]);
      return result;
    } else {
      return null;
    }
  }

  static HiveDecimalWritable nextDecimal(ColumnVector vector,
                                         int row,
                                         Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      HiveDecimalWritable result;
      if (previous == null || previous.getClass() != HiveDecimalWritable.class) {
        result = new HiveDecimalWritable();
      } else {
        result = (HiveDecimalWritable) previous;
      }
      result.set(((DecimalColumnVector) vector).vector[row]);
      return result;
    } else {
      return null;
    }
  }

  static DateWritable nextDate(ColumnVector vector,
                               int row,
                               Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      DateWritable result;
      if (previous == null || previous.getClass() != DateWritable.class) {
        result = new DateWritable();
      } else {
        result = (DateWritable) previous;
      }
      int date = (int) ((LongColumnVector) vector).vector[row];
      result.set(date);
      return result;
    } else {
      return null;
    }
  }

  static OrcTimestamp nextTimestamp(ColumnVector vector,
                                    int row,
                                    Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      OrcTimestamp result;
      if (previous == null || previous.getClass() != OrcTimestamp.class) {
        result = new OrcTimestamp();
      } else {
        result = (OrcTimestamp) previous;
      }
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      result.setTime(tcv.time[row]);
      result.setNanos(tcv.nanos[row]);
      return result;
    } else {
      return null;
    }
  }

  static OrcStruct nextStruct(ColumnVector vector,
                              int row,
                              TypeDescription schema,
                              Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      OrcStruct result;
      List<TypeDescription> childrenTypes = schema.getChildren();
      int numChildren = childrenTypes.size();
      if (isReusable(previous, schema)) {
        result = (OrcStruct) previous;
      } else {
        result = new OrcStruct(schema);
      }
      StructColumnVector struct = (StructColumnVector) vector;
      for(int f=0; f < numChildren; ++f) {
        result.setFieldValue(f, nextValue(struct.fields[f], row,
            childrenTypes.get(f), result.getFieldValue(f)));
      }
      return result;
    } else {
      return null;
    }
  }

  /**
   * Determine if a OrcStruct object is reusable.
   */
  private static boolean isReusable(Object previous, TypeDescription schema) {
    if (previous == null || previous.getClass() != OrcStruct.class) {
      return false;
    }

    return ((OrcStruct) previous).getSchema().equals(schema);
  }

  static OrcUnion nextUnion(ColumnVector vector,
                            int row,
                            TypeDescription schema,
                            Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      OrcUnion result;
      List<TypeDescription> childrenTypes = schema.getChildren();
      if (previous == null || previous.getClass() != OrcUnion.class) {
        result = new OrcUnion(schema);
      } else {
        result = (OrcUnion) previous;
      }
      UnionColumnVector union = (UnionColumnVector) vector;
      byte tag = (byte) union.tags[row];
      result.set(tag, nextValue(union.fields[tag], row, childrenTypes.get(tag),
          result.getObject()));
      return result;
    } else {
      return null;
    }
  }

  static OrcList nextList(ColumnVector vector,
                          int row,
                          TypeDescription schema,
                          Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      OrcList result;
      List<TypeDescription> childrenTypes = schema.getChildren();
      TypeDescription valueType = childrenTypes.get(0);
      if (previous == null ||
          previous.getClass() != ArrayList.class) {
        result = new OrcList(schema);
      } else {
        result = (OrcList) previous;
      }
      ListColumnVector list = (ListColumnVector) vector;
      int length = (int) list.lengths[row];
      int offset = (int) list.offsets[row];
      result.ensureCapacity(length);
      int oldLength = result.size();
      int idx = 0;
      while (idx < length && idx < oldLength) {
        result.set(idx, nextValue(list.child, offset + idx, valueType,
            result.get(idx)));
        idx += 1;
      }
      if (length < oldLength) {
        for(int i= oldLength - 1; i >= length; --i) {
          result.remove(i);
        }
      } else if (oldLength < length) {
        while (idx < length) {
          result.add(nextValue(list.child, offset + idx, valueType, null));
          idx += 1;
        }
      }
      return result;
    } else {
      return null;
    }
  }

  static OrcMap nextMap(ColumnVector vector,
                        int row,
                        TypeDescription schema,
                        Object previous) {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      MapColumnVector map = (MapColumnVector) vector;
      int length = (int) map.lengths[row];
      int offset = (int) map.offsets[row];
      OrcMap result;
      List<TypeDescription> childrenTypes = schema.getChildren();
      TypeDescription keyType = childrenTypes.get(0);
      TypeDescription valueType = childrenTypes.get(1);
      if (previous == null ||
          previous.getClass() != OrcMap.class) {
        result = new OrcMap(schema);
      } else {
        result = (OrcMap) previous;
        // I couldn't think of a good way to reuse the keys and value objects
        // without even more allocations, so take the easy and safe approach.
        result.clear();
      }
      for(int e=0; e < length; ++e) {
        result.put(nextValue(map.keys, e + offset, keyType, null),
                   nextValue(map.values, e + offset, valueType, null));
      }
      return result;
    } else {
      return null;
    }
  }

  public static WritableComparable nextValue(ColumnVector vector,
                                             int row,
                                             TypeDescription schema,
                                             Object previous) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return nextBoolean(vector, row, previous);
      case BYTE:
        return nextByte(vector, row, previous);
      case SHORT:
        return nextShort(vector, row, previous);
      case INT:
        return nextInt(vector, row, previous);
      case LONG:
        return nextLong(vector, row, previous);
      case FLOAT:
        return nextFloat(vector, row, previous);
      case DOUBLE:
        return nextDouble(vector, row, previous);
      case STRING:
      case CHAR:
      case VARCHAR:
        return nextString(vector, row, previous);
      case BINARY:
        return nextBinary(vector, row, previous);
      case DECIMAL:
        return nextDecimal(vector, row, previous);
      case DATE:
        return nextDate(vector, row, previous);
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return nextTimestamp(vector, row, previous);
      case STRUCT:
        return nextStruct(vector, row, schema, previous);
      case UNION:
        return nextUnion(vector, row, schema, previous);
      case LIST:
        return nextList(vector, row, schema, previous);
      case MAP:
        return nextMap(vector, row, schema, previous);
      default:
        throw new IllegalArgumentException("Unknown type " + schema);
    }
  }
}
