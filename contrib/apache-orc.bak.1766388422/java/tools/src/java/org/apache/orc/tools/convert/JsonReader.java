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
package org.apache.orc.tools.convert;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonStreamParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DateColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.UnionColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonReader implements RecordReader {

  private final TypeDescription schema;
  private final Iterator<JsonElement> parser;
  private final JsonConverter[] converters;
  private final long totalSize;
  private final FSDataInputStream input;
  private long rowNumber = 0;
  private final DateTimeFormatter dateTimeFormatter;
  private String unionTag = "tag";
  private String unionValue = "value";

  interface JsonConverter {
    void convert(JsonElement value, ColumnVector vect, int row);
  }

  static class BooleanColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        LongColumnVector vector = (LongColumnVector) vect;
        vector.vector[row] = value.getAsBoolean() ? 1 : 0;
      }
    }
  }

  static class LongColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        LongColumnVector vector = (LongColumnVector) vect;
        vector.vector[row] = value.getAsLong();
      }
    }
  }

  static class DoubleColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        DoubleColumnVector vector = (DoubleColumnVector) vect;
        vector.vector[row] = value.getAsDouble();
      }
    }
  }

  static class StringColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        BytesColumnVector vector = (BytesColumnVector) vect;
        byte[] bytes = value.getAsString().getBytes(StandardCharsets.UTF_8);
        vector.setRef(row, bytes, 0, bytes.length);
      }
    }
  }

  static class BinaryColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        BytesColumnVector vector = (BytesColumnVector) vect;
        String binStr = value.getAsString();
        byte[] bytes = new byte[binStr.length()/2];
        for(int i=0; i < bytes.length; ++i) {
          bytes[i] = (byte) Integer.parseInt(binStr.substring(i*2, i*2+2), 16);
        }
        vector.setRef(row, bytes, 0, bytes.length);
      }
    }
  }

  static class DateColumnConverter implements JsonConverter {
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        DateColumnVector vector = (DateColumnVector) vect;

        final LocalDate dt = LocalDate.parse(value.getAsString());

        if (dt != null) {
          vector.vector[row] = dt.toEpochDay();
        } else {
          vect.noNulls = false;
          vect.isNull[row] = true;
        }
      }
    }
  }

  class TimestampColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        TimestampColumnVector vector = (TimestampColumnVector) vect;
        TemporalAccessor temporalAccessor = dateTimeFormatter.parseBest(value.getAsString(),
            ZonedDateTime::from, OffsetDateTime::from, LocalDateTime::from);
        if (temporalAccessor instanceof ZonedDateTime) {
          ZonedDateTime zonedDateTime = ((ZonedDateTime) temporalAccessor);
          Timestamp timestamp = Timestamp.from(zonedDateTime.toInstant());
          vector.set(row, timestamp);
        } else if (temporalAccessor instanceof OffsetDateTime) {
          OffsetDateTime offsetDateTime = (OffsetDateTime) temporalAccessor;
          Timestamp timestamp = Timestamp.from(offsetDateTime.toInstant());
          vector.set(row, timestamp);
        } else if (temporalAccessor instanceof LocalDateTime) {
          ZonedDateTime tz = ((LocalDateTime) temporalAccessor).atZone(ZoneId.systemDefault());
          Timestamp timestamp = Timestamp.from(tz.toInstant());
          vector.set(row, timestamp);
        } else {
          vect.noNulls = false;
          vect.isNull[row] = true;
        }
      }
    }
  }

  static class DecimalColumnConverter implements JsonConverter {
    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        DecimalColumnVector vector = (DecimalColumnVector) vect;
        vector.vector[row].set(HiveDecimal.create(value.getAsString()));
      }
    }
  }

  class StructColumnConverter implements JsonConverter {
    private JsonConverter[] childrenConverters;
    private List<String> fieldNames;

    StructColumnConverter(TypeDescription schema) {
      List<TypeDescription> kids = schema.getChildren();
      childrenConverters = new JsonConverter[kids.size()];
      for(int c=0; c < childrenConverters.length; ++c) {
        childrenConverters[c] = createConverter(kids.get(c));
      }
      fieldNames = schema.getFieldNames();
    }

    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        StructColumnVector vector = (StructColumnVector) vect;
        JsonObject obj = value.getAsJsonObject();
        for(int c=0; c < childrenConverters.length; ++c) {
          JsonElement elem = obj.get(fieldNames.get(c));
          childrenConverters[c].convert(elem, vector.fields[c], row);
        }
      }
    }
  }

  class ListColumnConverter implements JsonConverter {
    private JsonConverter childrenConverter;

    ListColumnConverter(TypeDescription schema) {
      childrenConverter = createConverter(schema.getChildren().get(0));
    }

    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        ListColumnVector vector = (ListColumnVector) vect;
        JsonArray obj = value.getAsJsonArray();
        vector.lengths[row] = obj.size();
        vector.offsets[row] = vector.childCount;
        vector.childCount += vector.lengths[row];
        vector.child.ensureSize(vector.childCount, true);
        for(int c=0; c < obj.size(); ++c) {
          childrenConverter.convert(obj.get(c), vector.child,
              (int) vector.offsets[row] + c);
        }
      }
    }
  }

  class MapColumnConverter implements JsonConverter {
    private JsonConverter keyConverter;
    private JsonConverter valueConverter;

    MapColumnConverter(TypeDescription schema) {
      TypeDescription keyType = schema.getChildren().get(0);
      if (keyType.getCategory() != TypeDescription.Category.STRING) {
        throw new IllegalArgumentException("JSON can only support MAP key in STRING type: " + schema);
      }
      keyConverter = createConverter(keyType);
      valueConverter = createConverter(schema.getChildren().get(1));
    }

    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        MapColumnVector vector = (MapColumnVector) vect;
        JsonObject obj = value.getAsJsonObject();
        vector.lengths[row] = obj.entrySet().size();
        vector.offsets[row] = vector.childCount;
        vector.childCount += vector.lengths[row];
        vector.keys.ensureSize(vector.childCount, true);
        vector.values.ensureSize(vector.childCount, true);
        int cnt = 0;
        for (Map.Entry<String, JsonElement> entry : obj.entrySet()) {
          int offset = (int) vector.offsets[row] + cnt++;
          keyConverter.convert(new JsonPrimitive(entry.getKey()), vector.keys, offset);
          valueConverter.convert(entry.getValue(), vector.values, offset);
        }
      }
    }
  }

  class UnionColumnConverter implements JsonConverter {
    private JsonConverter[] childConverter;

    UnionColumnConverter(TypeDescription schema) {
      int size = schema.getChildren().size();
      childConverter = new JsonConverter[size];
      for (int i = 0; i < size; i++) {
        childConverter[i] = createConverter(schema.getChildren().get(i));
      }
    }

    @Override
    public void convert(JsonElement value, ColumnVector vect, int row) {
      if (value == null || value.isJsonNull()) {
        vect.noNulls = false;
        vect.isNull[row] = true;
      } else {
        UnionColumnVector vector = (UnionColumnVector) vect;
        JsonObject obj = value.getAsJsonObject();
        int tag = obj.get(unionTag).getAsInt();
        vector.tags[row] = tag;
        childConverter[tag].convert(obj.get(unionValue), vector.fields[tag], row);
      }
    }
  }

  JsonConverter createConverter(TypeDescription schema) {
    switch (schema.getCategory()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return new LongColumnConverter();
      case FLOAT:
      case DOUBLE:
        return new DoubleColumnConverter();
      case CHAR:
      case VARCHAR:
      case STRING:
        return new StringColumnConverter();
      case DECIMAL:
        return new DecimalColumnConverter();
      case DATE:
        return new DateColumnConverter();
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        return new TimestampColumnConverter();
      case BINARY:
        return new BinaryColumnConverter();
      case BOOLEAN:
        return new BooleanColumnConverter();
      case STRUCT:
        return new StructColumnConverter(schema);
      case LIST:
        return new ListColumnConverter(schema);
      case MAP:
        return new MapColumnConverter(schema);
      case UNION:
        return new UnionColumnConverter(schema);
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }

  public JsonReader(Reader reader,
                    FSDataInputStream underlying,
                    long size,
                    TypeDescription schema,
                    String timestampFormat,
                    String unionTag,
                    String unionValue) throws IOException {
    this(new JsonStreamParser(reader), underlying, size, schema, timestampFormat);
    this.unionTag = unionTag;
    this.unionValue = unionValue;
  }

  public JsonReader(Reader reader,
                    FSDataInputStream underlying,
                    long size,
                    TypeDescription schema,
                    String timestampFormat) throws IOException {
    this(new JsonStreamParser(reader), underlying, size, schema, timestampFormat);
  }

  public JsonReader(Iterator<JsonElement> parser,
                    FSDataInputStream underlying,
                    long size,
                    TypeDescription schema,
                    String timestampFormat) throws IOException {
    this.schema = schema;
    if (schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IllegalArgumentException("Root must be struct - " + schema);
    }
    this.input = underlying;
    this.totalSize = size;
    this.parser = parser;
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(timestampFormat);
    List<TypeDescription> fieldTypes = schema.getChildren();
    converters = new JsonConverter[fieldTypes.size()];
    for(int c = 0; c < converters.length; ++c) {
      converters[c] = createConverter(fieldTypes.get(c));
    }
  }

  @Override
  public boolean nextBatch(VectorizedRowBatch batch) throws IOException {
    batch.reset();
    int maxSize = batch.getMaxSize();
    List<String> fieldNames = schema.getFieldNames();
    while (parser.hasNext() && batch.size < maxSize) {
      JsonObject elem = parser.next().getAsJsonObject();
      for(int c=0; c < converters.length; ++c) {
        // look up each field to see if it is in the input, otherwise
        // set it to null.
        JsonElement field = elem.get(fieldNames.get(c));
        if (field == null) {
          batch.cols[c].noNulls = false;
          batch.cols[c].isNull[batch.size] = true;
        } else {
          converters[c].convert(field, batch.cols[c], batch.size);
        }
      }
      batch.size++;
    }
    rowNumber += batch.size;
    return batch.size != 0;
  }

  @Override
  public long getRowNumber() throws IOException {
    return rowNumber;
  }

  @Override
  public float getProgress() throws IOException {
    long pos = input.getPos();
    return totalSize != 0 && pos < totalSize ? (float) pos / totalSize : 1;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void seekToRow(long rowCount) throws IOException {
    throw new UnsupportedOperationException("Seek is not supported by JsonReader");
  }
}
