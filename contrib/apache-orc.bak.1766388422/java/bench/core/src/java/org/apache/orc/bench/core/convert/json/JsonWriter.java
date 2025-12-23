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

package org.apache.orc.bench.core.convert.json;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.CompressionKind;
import org.apache.orc.bench.core.convert.BatchWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class JsonWriter implements BatchWriter {
  private final Writer outStream;
  private final com.google.gson.stream.JsonWriter writer;
  private final TypeDescription schema;

  public JsonWriter(Path path, TypeDescription schema,
                    Configuration conf,
                    CompressionKind compression) throws IOException {
    OutputStream file = path.getFileSystem(conf).create(path, true);
    outStream = new OutputStreamWriter(compression.create(file),
        StandardCharsets.UTF_8);
    writer = new com.google.gson.stream.JsonWriter(outStream);
    writer.setLenient(true);
    this.schema = schema;
  }

  private static void printMap(com.google.gson.stream.JsonWriter writer,
                               MapColumnVector vector,
                               TypeDescription schema,
                               int row) throws IOException {
    writer.beginArray();
    TypeDescription keyType = schema.getChildren().get(0);
    TypeDescription valueType = schema.getChildren().get(1);
    int offset = (int) vector.offsets[row];
    for (int i = 0; i < vector.lengths[row]; ++i) {
      writer.beginObject();
      writer.name("_key");
      printValue(writer, vector.keys, keyType, offset + i);
      writer.name("_value");
      printValue(writer, vector.values, valueType, offset + i);
      writer.endObject();
    }
    writer.endArray();
  }

  private static void printList(com.google.gson.stream.JsonWriter writer,
                                ListColumnVector vector,
                                TypeDescription schema,
                                int row) throws IOException {
    writer.beginArray();
    int offset = (int) vector.offsets[row];
    TypeDescription childType = schema.getChildren().get(0);
    for (int i = 0; i < vector.lengths[row]; ++i) {
      printValue(writer, vector.child, childType, offset + i);
    }
    writer.endArray();
  }

  private static void printUnion(com.google.gson.stream.JsonWriter writer,
                                 UnionColumnVector vector,
                                 TypeDescription schema,
                                 int row) throws IOException {
    int tag = vector.tags[row];
    printValue(writer, vector.fields[tag], schema.getChildren().get(tag), row);
  }

  static void printStruct(com.google.gson.stream.JsonWriter writer,
                          StructColumnVector batch,
                          TypeDescription schema,
                          int row) throws IOException {
    writer.beginObject();
    List<String> fieldNames = schema.getFieldNames();
    List<TypeDescription> fieldTypes = schema.getChildren();
    for (int i = 0; i < fieldTypes.size(); ++i) {
      writer.name(fieldNames.get(i));
      printValue(writer, batch.fields[i], fieldTypes.get(i), row);
    }
    writer.endObject();
  }

  static void printBinary(com.google.gson.stream.JsonWriter writer, BytesColumnVector vector,
                          int row) throws IOException {
    StringBuilder buffer = new StringBuilder();
    int offset = vector.start[row];
    for(int i=0; i < vector.length[row]; ++i) {
      int value = 0xff & (int) vector.vector[row][offset + i];
      buffer.append(String.format("%02x", value));
    }
    writer.value(buffer.toString());
  }

  static void printValue(com.google.gson.stream.JsonWriter writer, ColumnVector vector,
                         TypeDescription schema, int row) throws IOException {
    if (vector.isRepeating) {
      row = 0;
    }
    if (vector.noNulls || !vector.isNull[row]) {
      switch (schema.getCategory()) {
        case BOOLEAN:
          writer.value(((LongColumnVector) vector).vector[row] != 0);
          break;
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
          writer.value(((LongColumnVector) vector).vector[row]);
          break;
        case FLOAT:
        case DOUBLE:
          writer.value(((DoubleColumnVector) vector).vector[row]);
          break;
        case STRING:
        case CHAR:
        case VARCHAR:
          writer.value(((BytesColumnVector) vector).toString(row));
          break;
        case BINARY:
          printBinary(writer, (BytesColumnVector) vector, row);
          break;
        case DECIMAL:
          writer.value(((DecimalColumnVector) vector).vector[row].toString());
          break;
        case DATE:
          writer.value(new DateWritable(
              (int) ((LongColumnVector) vector).vector[row]).toString());
          break;
        case TIMESTAMP:
          writer.value(((TimestampColumnVector) vector)
              .asScratchTimestamp(row).toString());
          break;
        case LIST:
          printList(writer, (ListColumnVector) vector, schema, row);
          break;
        case MAP:
          printMap(writer, (MapColumnVector) vector, schema, row);
          break;
        case STRUCT:
          printStruct(writer, (StructColumnVector) vector, schema, row);
          break;
        case UNION:
          printUnion(writer, (UnionColumnVector) vector, schema, row);
          break;
        default:
          throw new IllegalArgumentException("Unknown type " + schema);
      }
    } else {
      writer.nullValue();
    }
  }

  static void printRow(com.google.gson.stream.JsonWriter writer,
                       VectorizedRowBatch batch,
                       TypeDescription schema,
                       int row) throws IOException {
    if (schema.getCategory() == TypeDescription.Category.STRUCT) {
      List<TypeDescription> fieldTypes = schema.getChildren();
      List<String> fieldNames = schema.getFieldNames();
      writer.beginObject();
      for (int c = 0; c < batch.cols.length; ++c) {
        writer.name(fieldNames.get(c));
        printValue(writer, batch.cols[c], fieldTypes.get(c), row);
      }
      writer.endObject();
    } else {
      printValue(writer, batch.cols[0], schema, row);
    }
  }

  public void writeBatch(VectorizedRowBatch batch) throws IOException {
    for (int r = 0; r < batch.size; ++r) {
      printRow(writer, batch, schema, r);
      outStream.write("\n");
    }
  }

  public void close() throws IOException {
    writer.close();
  }
}
