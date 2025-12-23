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
import org.apache.hadoop.hive.ql.exec.vector.MultiValuedColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BinaryComparable;
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class OrcMapredRecordWriter<V extends Writable>
    implements RecordWriter<NullWritable, V> {
  // The factor that we grow lists and maps by when they are too small.
  private static final int GROWTH_FACTOR = 3;
  private final Writer writer;
  private final VectorizedRowBatch batch;
  private final TypeDescription schema;
  private final boolean isTopStruct;
  private final List<MultiValuedColumnVector> variableLengthColumns =
      new ArrayList<>();
  private final int maxChildLength;

  public OrcMapredRecordWriter(Writer writer) {
    this(writer, VectorizedRowBatch.DEFAULT_SIZE);
  }

  public OrcMapredRecordWriter(Writer writer,
                               int rowBatchSize) {
    this(writer, rowBatchSize,
        (Integer) OrcConf.ROW_BATCH_CHILD_LIMIT.getDefaultValue());
  }

  public OrcMapredRecordWriter(Writer writer,
                               int rowBatchSize,
                               int maxChildLength) {
    this.writer = writer;
    schema = writer.getSchema();
    this.batch = schema.createRowBatch(rowBatchSize);
    addVariableLengthColumns(variableLengthColumns, batch);
    isTopStruct = schema.getCategory() == TypeDescription.Category.STRUCT;
    this.maxChildLength = maxChildLength;
  }

  /**
   * Find variable length columns and add them to the list.
   * @param result the list to be appended to
   * @param vector the column vector to scan
   */
  private static void addVariableLengthColumns(List<MultiValuedColumnVector> result,
                                               ColumnVector vector) {
    switch (vector.type) {
      case LIST: {
        ListColumnVector cv = (ListColumnVector) vector;
        result.add(cv);
        addVariableLengthColumns(result, cv.child);
        break;
      }
      case MAP: {
        MapColumnVector cv = (MapColumnVector) vector;
        result.add(cv);
        addVariableLengthColumns(result, cv.keys);
        addVariableLengthColumns(result, cv.values);
        break;
      }
      case STRUCT: {
        for(ColumnVector child: ((StructColumnVector) vector).fields) {
          addVariableLengthColumns(result, child);
        }
        break;
      }
      case UNION: {
        for(ColumnVector child: ((UnionColumnVector) vector).fields) {
          addVariableLengthColumns(result, child);
        }
        break;
      }
      default:
        break;
    }
  }

  /**
   * Find variable length columns and add them to the list.
   * @param result the list to be appended to
   * @param batch the batch to scan
   */
  public static void addVariableLengthColumns(List<MultiValuedColumnVector> result,
                                              VectorizedRowBatch batch) {
    for(ColumnVector cv: batch.cols) {
      addVariableLengthColumns(result, cv);
    }
  }

  static void setLongValue(ColumnVector vector, int row, long value) {
    ((LongColumnVector) vector).vector[row] = value;
  }

  static void setDoubleValue(ColumnVector vector, int row, double value) {
    ((DoubleColumnVector) vector).vector[row] = value;
  }

  static void setBinaryValue(ColumnVector vector, int row,
                             BinaryComparable value) {
    ((BytesColumnVector) vector).setVal(row, value.getBytes(), 0,
        value.getLength());
  }

  static void setBinaryValue(ColumnVector vector, int row,
                             BinaryComparable value, int maxLength) {
    ((BytesColumnVector) vector).setVal(row, value.getBytes(), 0,
        Math.min(maxLength, value.getLength()));
  }

  private static final ThreadLocal<byte[]> SPACE_BUFFER =
      new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
          byte[] result = new byte[100];
          Arrays.fill(result, (byte) ' ');
          return result;
        }
      };

  static void setCharValue(BytesColumnVector vector,
                           int row,
                           Text value,
                           int length) {
    // we need to trim or pad the string with spaces to required length
    int actualLength = value.getLength();
    if (actualLength >= length) {
      setBinaryValue(vector, row, value, length);
    } else {
      byte[] spaces = SPACE_BUFFER.get();
      if (length - actualLength > spaces.length) {
        spaces = new byte[length - actualLength];
        Arrays.fill(spaces, (byte)' ');
        SPACE_BUFFER.set(spaces);
      }
      vector.setConcat(row, value.getBytes(), 0, actualLength, spaces, 0,
          length - actualLength);
    }
  }

  static void setStructValue(TypeDescription schema,
                             StructColumnVector vector,
                             int row,
                             OrcStruct value) {
    List<TypeDescription> children = schema.getChildren();
    for(int c=0; c < value.getNumFields(); ++c) {
      setColumn(children.get(c), vector.fields[c], row, value.getFieldValue(c));
    }
  }

  static void setUnionValue(TypeDescription schema,
                            UnionColumnVector vector,
                            int row,
                            OrcUnion value) {
    List<TypeDescription> children = schema.getChildren();
    int tag = value.getTag() & 0xff;
    vector.tags[row] = tag;
    setColumn(children.get(tag), vector.fields[tag], row, value.getObject());
  }


  static void setListValue(TypeDescription schema,
                           ListColumnVector vector,
                           int row,
                           OrcList value) {
    TypeDescription elemType = schema.getChildren().get(0);
    vector.offsets[row] = vector.childCount;
    vector.lengths[row] = value.size();
    vector.childCount += vector.lengths[row];
    if (vector.child.isNull.length < vector.childCount) {
      vector.child.ensureSize(vector.childCount * GROWTH_FACTOR,
          vector.offsets[row] != 0);
    }

    for(int e=0; e < vector.lengths[row]; ++e) {
      setColumn(elemType, vector.child, (int) vector.offsets[row] + e,
          (Writable) value.get(e));
    }
  }

  static void setMapValue(TypeDescription schema,
                          MapColumnVector vector,
                          int row,
                          OrcMap<?,?> value) {
    TypeDescription keyType = schema.getChildren().get(0);
    TypeDescription valueType = schema.getChildren().get(1);
    vector.offsets[row] = vector.childCount;
    vector.lengths[row] = value.size();
    vector.childCount += vector.lengths[row];
    if (vector.keys.isNull.length < vector.childCount) {
      vector.keys.ensureSize(vector.childCount * GROWTH_FACTOR,
          vector.offsets[row] != 0);
    }
    if (vector.values.isNull.length < vector.childCount) {
      vector.values.ensureSize(vector.childCount * GROWTH_FACTOR,
          vector.offsets[row] != 0);
    }
    int e = 0;
    for(Map.Entry<?,?> entry: value.entrySet()) {
      setColumn(keyType, vector.keys, (int) vector.offsets[row] + e,
          (Writable) entry.getKey());
      setColumn(valueType, vector.values, (int) vector.offsets[row] + e,
          (Writable) entry.getValue());
      e += 1;
    }
  }

  public static void setColumn(TypeDescription schema,
                               ColumnVector vector,
                               int row,
                               Writable value) {
    if (value == null) {
      vector.noNulls = false;
      vector.isNull[row] = true;
    } else {
      switch (schema.getCategory()) {
        case BOOLEAN:
          setLongValue(vector, row, ((BooleanWritable) value).get() ? 1 : 0);
          break;
        case BYTE:
          setLongValue(vector, row, ((ByteWritable) value).get());
          break;
        case SHORT:
          setLongValue(vector, row, ((ShortWritable) value).get());
          break;
        case INT:
          setLongValue(vector, row, ((IntWritable) value).get());
          break;
        case LONG:
          setLongValue(vector, row, ((LongWritable) value).get());
          break;
        case FLOAT:
          setDoubleValue(vector, row, ((FloatWritable) value).get());
          break;
        case DOUBLE:
          setDoubleValue(vector, row, ((DoubleWritable) value).get());
          break;
        case STRING:
          setBinaryValue(vector, row, (Text) value);
          break;
        case CHAR:
          setCharValue((BytesColumnVector) vector, row, (Text) value,
              schema.getMaxLength());
          break;
        case VARCHAR:
          setBinaryValue(vector, row, (Text) value, schema.getMaxLength());
          break;
        case BINARY:
          setBinaryValue(vector, row, (BytesWritable) value);
          break;
        case DATE:
          setLongValue(vector, row, ((DateWritable) value).getDays());
          break;
        case TIMESTAMP:
        case TIMESTAMP_INSTANT:
          ((TimestampColumnVector) vector).set(row, (OrcTimestamp) value);
          break;
        case DECIMAL:
          ((DecimalColumnVector) vector).set(row, (HiveDecimalWritable) value);
          break;
        case STRUCT:
          setStructValue(schema, (StructColumnVector) vector, row,
              (OrcStruct) value);
          break;
        case UNION:
          setUnionValue(schema, (UnionColumnVector) vector, row,
              (OrcUnion) value);
          break;
        case LIST:
          setListValue(schema, (ListColumnVector) vector, row, (OrcList) value);
          break;
        case MAP:
          setMapValue(schema, (MapColumnVector) vector, row, (OrcMap) value);
          break;
        default:
          throw new IllegalArgumentException("Unknown type " + schema);
      }
    }
  }

  /**
   * Get the longest variable length vector in a column vector
   * @return the length of the longest sub-column
   */
  public static int getMaxChildLength(List<MultiValuedColumnVector> columns) {
    int result = 0;
    for(MultiValuedColumnVector cv: columns) {
      result = Math.max(result, cv.childCount);
    }
    return result;
  }

  @Override
  public void write(NullWritable nullWritable, V v) throws IOException {
    // if the batch is full, write it out.
    if (batch.size == batch.getMaxSize() ||
        getMaxChildLength(variableLengthColumns) >= maxChildLength) {
      writer.addRowBatch(batch);
      batch.reset();
    }

    // add the new row
    int row = batch.size++;
    // skip over the OrcKey or OrcValue
    if (v instanceof OrcKey) {
      v = (V)((OrcKey) v).key;
    } else if (v instanceof OrcValue) {
      v = (V)((OrcValue) v).value;
    }
    if (isTopStruct) {
      for(int f=0; f < schema.getChildren().size(); ++f) {
        setColumn(schema.getChildren().get(f), batch.cols[f], row,
            ((OrcStruct) v).getFieldValue(f));
      }
    } else {
      setColumn(schema, batch.cols[0], row, v);
    }
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    if (batch.size != 0) {
      writer.addRowBatch(batch);
      batch.reset();
    }
    writer.close();
  }
}
