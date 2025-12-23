/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.bench.core.convert.avro;

import org.apache.avro.Schema;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * GenerateVariants Hive TypeInfo to an Avro Schema
 */
public class AvroSchemaUtils {

  private AvroSchemaUtils() {
    // No instances
  }

  public static Schema createAvroSchema(TypeDescription typeInfo) {
    Schema schema;
    switch (typeInfo.getCategory()) {
      case STRING:
        schema = Schema.create(Schema.Type.STRING);
        break;
      case CHAR:
        schema = getSchemaFor("{" +
            "\"type\":\"string\"," +
            "\"logicalType\":\"char\"," +
            "\"maxLength\":" + typeInfo.getMaxLength() + "}");
        break;
      case VARCHAR:
        schema = getSchemaFor("{" +
            "\"type\":\"string\"," +
            "\"logicalType\":\"varchar\"," +
            "\"maxLength\":" + typeInfo.getMaxLength() + "}");
        break;
      case BINARY:
        schema = Schema.create(Schema.Type.BYTES);
        break;
      case BYTE:
        schema = Schema.create(Schema.Type.INT);
        break;
      case SHORT:
        schema = Schema.create(Schema.Type.INT);
        break;
      case INT:
        schema = Schema.create(Schema.Type.INT);
        break;
      case LONG:
        schema = Schema.create(Schema.Type.LONG);
        break;
      case FLOAT:
        schema = Schema.create(Schema.Type.FLOAT);
        break;
      case DOUBLE:
        schema = Schema.create(Schema.Type.DOUBLE);
        break;
      case BOOLEAN:
        schema = Schema.create(Schema.Type.BOOLEAN);
        break;
      case DECIMAL:
        String precision = String.valueOf(typeInfo.getPrecision());
        String scale = String.valueOf(typeInfo.getScale());
        schema = getSchemaFor("{" +
            "\"type\":\"bytes\"," +
            "\"logicalType\":\"decimal\"," +
            "\"precision\":" + precision + "," +
            "\"scale\":" + scale + "}");
        break;
      case DATE:
        schema = getSchemaFor("{" +
            "\"type\":\"int\"," +
            "\"logicalType\":\"date\"}");
        break;
      case TIMESTAMP:
        schema = getSchemaFor("{" +
            "\"type\":\"long\"," +
            "\"logicalType\":\"timestamp-millis\"}");
        break;
      case LIST:
        schema = createAvroArray(typeInfo);
        break;
      case MAP:
        schema = createAvroMap(typeInfo);
        break;
      case STRUCT:
        schema = createAvroRecord(typeInfo);
        break;
      case UNION:
        schema = createAvroUnion(typeInfo);
        break;
      default:
        throw new UnsupportedOperationException(typeInfo + " is not supported.");
    }

    return schema;
  }

  private static Schema createAvroUnion(TypeDescription typeInfo) {
    List<Schema> childSchemas = new ArrayList<>();
    for (TypeDescription childTypeInfo : typeInfo.getChildren()) {
      Schema childSchema = createAvroSchema(childTypeInfo);
      if (childSchema.getType() == Schema.Type.UNION) {
        for (Schema grandkid: childSchema.getTypes()) {
          if (childSchema.getType() != Schema.Type.NULL) {
            childSchemas.add(grandkid);
          }
        }
      } else {
        childSchemas.add(childSchema);
      }
    }

    return wrapInUnionWithNull(Schema.createUnion(childSchemas));
  }

  private static Schema createAvroRecord(TypeDescription typeInfo) {
    List<Schema.Field> childFields = new ArrayList<>();

    List<String> fieldNames = typeInfo.getFieldNames();
    List<TypeDescription> fieldTypes = typeInfo.getChildren();

    for (int i = 0; i < fieldNames.size(); ++i) {
      TypeDescription childTypeInfo = fieldTypes.get(i);
      Schema.Field field = new Schema.Field(fieldNames.get(i),
          wrapInUnionWithNull(createAvroSchema(childTypeInfo)),
          childTypeInfo.toString(),
          (Object) null);
      childFields.add(field);
    }

    Schema recordSchema = Schema.createRecord("record_" + typeInfo.getId(),
        typeInfo.toString(), null, false);
    recordSchema.setFields(childFields);
    return recordSchema;
  }

  private static Schema createAvroMap(TypeDescription typeInfo) {
    TypeDescription keyTypeInfo = typeInfo.getChildren().get(0);
    if (keyTypeInfo.getCategory() != TypeDescription.Category.STRING) {
      throw new UnsupportedOperationException("Avro only supports maps with string keys "
          + typeInfo);
    }

    Schema valueSchema = wrapInUnionWithNull(createAvroSchema
        (typeInfo.getChildren().get(1)));

    return Schema.createMap(valueSchema);
  }

  private static Schema createAvroArray(TypeDescription typeInfo) {
    Schema child = createAvroSchema(typeInfo.getChildren().get(0));
    return Schema.createArray(wrapInUnionWithNull(child));
  }

  private static Schema wrapInUnionWithNull(Schema schema) {
    Schema NULL = Schema.create(Schema.Type.NULL);
    switch (schema.getType()) {
      case NULL:
        return schema;
      case UNION:
        List<Schema> kids = schema.getTypes();
        List<Schema> newKids = new ArrayList<>(kids.size() + 1);
        newKids.add(NULL);
        return Schema.createUnion(newKids);
      default:
        return Schema.createUnion(Arrays.asList(NULL, schema));
    }
  }

  private static Schema getSchemaFor(String str) {
    Schema.Parser parser = new Schema.Parser();
    return parser.parse(str);
  }
}
