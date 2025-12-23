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

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
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
import org.apache.orc.bench.core.CompressionKind;
import org.apache.orc.bench.core.convert.BatchWriter;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AvroWriter implements BatchWriter {

  public interface AvroConverter {
    Object convert(ColumnVector vector, int row);
  }

  private static class BooleanConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        LongColumnVector vector = (LongColumnVector) cv;
        return vector.vector[row] != 0;
      } else {
        return null;
      }
    }
  }

  private static class IntConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        LongColumnVector vector = (LongColumnVector) cv;
        return (int) vector.vector[row];
      } else {
        return null;
      }
    }
  }

  private static class LongConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        LongColumnVector vector = (LongColumnVector) cv;
        return vector.vector[row];
      } else {
        return null;
      }
    }
  }

  private static class FloatConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        DoubleColumnVector vector = (DoubleColumnVector) cv;
        return (float) vector.vector[row];
      } else {
        return null;
      }
    }
  }

  private static class DoubleConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        DoubleColumnVector vector = (DoubleColumnVector) cv;
        return vector.vector[row];
      } else {
        return null;
      }
    }
  }

  private static class StringConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        BytesColumnVector vector = (BytesColumnVector) cv;
        return new String(vector.vector[row], vector.start[row],
            vector.length[row], StandardCharsets.UTF_8);
      } else {
        return null;
      }
    }
  }

  private static class BinaryConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        BytesColumnVector vector = (BytesColumnVector) cv;
        return ByteBuffer.wrap(vector.vector[row], vector.start[row],
            vector.length[row]);
      } else {
        return null;
      }
    }
  }

  private static class TimestampConverter implements AvroConverter {
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        TimestampColumnVector vector = (TimestampColumnVector) cv;
        return vector.time[row];
      } else {
        return null;
      }
    }
  }

  private static class DecimalConverter implements AvroConverter {
    final int scale;
    DecimalConverter(int scale) {
      this.scale = scale;
    }
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        DecimalColumnVector vector = (DecimalColumnVector) cv;
        return getBufferFromDecimal(
            vector.vector[row].getHiveDecimal(), scale);
      } else {
        return null;
      }
    }
  }

  private static class ListConverter implements AvroConverter {
    final Schema avroSchema;
    final AvroConverter childConverter;

    ListConverter(TypeDescription schema, Schema avroSchema) {
      this.avroSchema = avroSchema;
      childConverter = createConverter(schema.getChildren().get(0),
          removeNullable(avroSchema.getElementType()));
    }

    @SuppressWarnings("unchecked")
    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        ListColumnVector vector = (ListColumnVector) cv;
        int offset = (int) vector.offsets[row];
        int length = (int) vector.lengths[row];
        GenericData.Array result = new GenericData.Array(length, avroSchema);
        for(int i=0; i < length; ++i) {
          result.add(childConverter.convert(vector.child, offset + i));
        }
        return result;
      } else {
        return null;
      }
    }
  }

  private static class StructConverter implements AvroConverter {
    final Schema avroSchema;
    final AvroConverter[] childConverters;

    StructConverter(TypeDescription schema, Schema avroSchema) {
      this.avroSchema = avroSchema;
      List<TypeDescription> childrenTypes = schema.getChildren();
      childConverters = new AvroConverter[childrenTypes.size()];
      List<Schema.Field> fields = avroSchema.getFields();
      for(int f=0; f < childConverters.length; ++f) {
        childConverters[f] = createConverter(childrenTypes.get(f),
            removeNullable(fields.get(f).schema()));
      }
    }

    public Object convert(ColumnVector cv, int row) {
      if (cv.isRepeating) {
        row = 0;
      }
      if (cv.noNulls || !cv.isNull[row]) {
        StructColumnVector vector = (StructColumnVector) cv;
        GenericData.Record result = new GenericData.Record(avroSchema);
        for(int f=0; f < childConverters.length; ++f) {
          result.put(f, childConverters[f].convert(vector.fields[f], row));
        }
        return result;
      } else {
        return null;
      }
    }
  }

  public static AvroConverter createConverter(TypeDescription types,
                                              Schema avroSchema) {
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
        return new ListConverter(types, avroSchema);
      case STRUCT:
        return new StructConverter(types, avroSchema);
      default:
        throw new IllegalArgumentException("Unhandled type " + types);
    }
  }

  /**
   * Remove the union(null, ...) wrapper around the schema.
   *
   * All of the types in Hive are nullable and in Avro those are represented
   * by wrapping each type in a union type with the void type.
   * @param avro The avro type
   * @return The avro type with the nullable layer removed
   */
  static Schema removeNullable(Schema avro) {
    while (avro.getType() == Schema.Type.UNION) {
      List<Schema> children = avro.getTypes();
      if (children.size() == 2 &&
          children.get(0).getType() == Schema.Type.NULL) {
        avro = children.get(1);
      } else {
        break;
      }
    }
    return avro;
  }

  private final AvroConverter[] converters;
  private final DataFileWriter<GenericData.Record> writer;
  private final GenericData.Record record;

  public static AvroConverter[] buildConverters(TypeDescription orcType,
                                                Schema avroSchema) {
    List<TypeDescription> childTypes = orcType.getChildren();
    List<Schema.Field> avroFields = avroSchema.getFields();
    AvroConverter[] result = new AvroConverter[childTypes.size()];
    for(int c=0; c < result.length; ++c) {
      result[c] = createConverter(childTypes.get(c),
          removeNullable(avroFields.get(c).schema()));
    }
    return result;
  }

  public AvroWriter(Path path, TypeDescription schema,
                    Configuration conf,
                    CompressionKind compression) throws IOException {
    Schema avroSchema = AvroSchemaUtils.createAvroSchema(schema);
    GenericDatumWriter<GenericData.Record> gdw = new GenericDatumWriter<>(avroSchema);
    writer = new DataFileWriter<>(gdw);
    converters = buildConverters(schema, avroSchema);
    switch (compression) {
      case NONE:
        break;
      case ZLIB:
        writer.setCodec(CodecFactory.deflateCodec(-1));
        break;
      case SNAPPY:
        writer.setCodec(CodecFactory.snappyCodec());
        break;
      case ZSTD:
        writer.setCodec(CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL));
        break;
      default:
        throw new IllegalArgumentException("Compression unsupported " + compression);
    }
    writer.create(avroSchema, path.getFileSystem(conf).create(path));
    record = new GenericData.Record(avroSchema);
  }

  public void writeBatch(VectorizedRowBatch batch) throws IOException {
    for(int r=0; r < batch.size; ++r) {
      for(int f=0; f < batch.cols.length; ++f) {
        record.put(f, converters[f].convert(batch.cols[f], r));
      }
      writer.append(record);
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  static Buffer getBufferFromDecimal(HiveDecimal dec, int scale) {
    if (dec == null) {
      return null;
    }

    return ByteBuffer.wrap(dec.bigIntegerBytesScaled(scale));
  }
}
