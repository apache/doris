// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.avro;

import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.common.jni.vec.TableSchema.SchemaColumn;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.compress.utils.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AvroTypeUtils {

    protected static TableSchema parseTableSchema(Schema schema) throws UnsupportedOperationException {
        List<Field> schemaFields = schema.getFields();
        List<SchemaColumn> schemaColumns = new ArrayList<>();
        for (Field schemaField : schemaFields) {
            Schema avroSchema = schemaField.schema();
            String columnName = schemaField.name();

            SchemaColumn schemaColumn = new SchemaColumn();
            TPrimitiveType tPrimitiveType = typeFromAvro(avroSchema, schemaColumn);
            schemaColumn.setName(columnName);
            schemaColumn.setType(tPrimitiveType);
            schemaColumns.add(schemaColumn);
        }
        return new TableSchema(schemaColumns);
    }

    private static TPrimitiveType typeFromAvro(Schema avroSchema, SchemaColumn schemaColumn)
            throws UnsupportedOperationException {
        Schema.Type type = avroSchema.getType();
        switch (type) {
            case ENUM:
            case STRING:
                return TPrimitiveType.STRING;
            case INT:
                return TPrimitiveType.INT;
            case BOOLEAN:
                return TPrimitiveType.BOOLEAN;
            case LONG:
                return TPrimitiveType.BIGINT;
            case FLOAT:
                return TPrimitiveType.FLOAT;
            case FIXED:
            case BYTES:
                return TPrimitiveType.BINARY;
            case DOUBLE:
                return TPrimitiveType.DOUBLE;
            case ARRAY:
                SchemaColumn arrayChildColumn = new SchemaColumn();
                schemaColumn.addChildColumns(Collections.singletonList(arrayChildColumn));
                arrayChildColumn.setType(typeFromAvro(avroSchema.getElementType(), arrayChildColumn));
                return TPrimitiveType.ARRAY;
            case MAP:
                // The default type of AVRO MAP structure key is STRING
                SchemaColumn keyChildColumn = new SchemaColumn();
                keyChildColumn.setType(TPrimitiveType.STRING);
                SchemaColumn valueChildColumn = new SchemaColumn();
                valueChildColumn.setType(typeFromAvro(avroSchema.getValueType(), valueChildColumn));

                schemaColumn.addChildColumns(Arrays.asList(keyChildColumn, valueChildColumn));
                return TPrimitiveType.MAP;
            case RECORD:
                List<Field> fields = avroSchema.getFields();
                List<SchemaColumn> childSchemaColumn = Lists.newArrayList();
                for (Field field : fields) {
                    SchemaColumn structChildColumn = new SchemaColumn();
                    structChildColumn.setName(field.name());
                    structChildColumn.setType(typeFromAvro(field.schema(), structChildColumn));
                    childSchemaColumn.add(structChildColumn);
                }
                schemaColumn.addChildColumns(childSchemaColumn);
                return TPrimitiveType.STRUCT;
            case UNION:
                List<Schema> nonNullableMembers = filterNullableUnion(avroSchema);
                Preconditions.checkArgument(!nonNullableMembers.isEmpty(),
                        avroSchema.getName() + "Union child type not all nullAble type");
                List<SchemaColumn> childSchemaColumns = Lists.newArrayList();
                for (Schema nullableMember : nonNullableMembers) {
                    SchemaColumn childColumn = new SchemaColumn();
                    childColumn.setName(nullableMember.getName());
                    childColumn.setType(typeFromAvro(nullableMember, childColumn));
                    childSchemaColumns.add(childColumn);
                }
                schemaColumn.addChildColumns(childSchemaColumns);
                return TPrimitiveType.STRUCT;
            default:
                throw new UnsupportedOperationException(
                        "avro format: " + avroSchema.getName() + type.getName() + " is not supported.");
        }
    }

    private static List<Schema> filterNullableUnion(Schema schema) {
        Preconditions.checkArgument(schema.isUnion(), "Schema must be union");
        return schema.getTypes().stream().filter(s -> !s.isNullable()).collect(Collectors.toList());
    }

}
