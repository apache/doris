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

package org.apache.doris.catalog;

import com.google.common.base.Preconditions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HudiUtils {
    public static String fromAvroHudiTypeToHiveTypeString(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();
        switch (columnType) {
            case BOOLEAN:
                return "boolean";
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return "date";
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    break;
                } else {
                    return "int";
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    break;
                } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                    return "timestamp(3)";
                } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                    return "timestamp(6)";
                } else {
                    return "bigint";
                }
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case STRING:
                return "string";
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                    int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                    return String.format("decimal(%s,%s)", precision, scale);
                } else {
                    return "string";
                }
            case ARRAY:
                String elementType = fromAvroHudiTypeToHiveTypeString(avroSchema.getElementType());
                return String.format("array<%s>", elementType);
            case RECORD:
                List<Field> fields = avroSchema.getFields();
                Preconditions.checkArgument(fields.size() > 0);
                String nameToType = fields.stream()
                        .map(f -> String.format("%s:%s", f.name(),
                                fromAvroHudiTypeToHiveTypeString(f.schema())))
                        .collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case MAP:
                Schema value = avroSchema.getValueType();
                String valueType = fromAvroHudiTypeToHiveTypeString(value);
                return String.format("map<%s,%s>", "string", valueType);
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());
                // The nullable column in hudi is the union type with schemas [null, real column type]
                if (nonNullMembers.size() == 1) {
                    return fromAvroHudiTypeToHiveTypeString(nonNullMembers.get(0));
                }
                break;
            default:
                break;
        }
        String errorMsg = String.format("Unsupported hudi %s type of column %s", avroSchema.getType().getName(),
                avroSchema.getName());
        throw new IllegalArgumentException(errorMsg);
    }

    public static Type fromAvroHudiTypeToDorisType(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();
        switch (columnType) {
            case BOOLEAN:
                return Type.BOOLEAN;
            case INT:
                if (logicalType instanceof LogicalTypes.Date) {
                    return ScalarType.createDateV2Type();
                } else if (logicalType instanceof LogicalTypes.TimeMillis) {
                    return ScalarType.createTimeV2Type(3);
                } else {
                    return Type.INT;
                }
            case LONG:
                if (logicalType instanceof LogicalTypes.TimeMicros) {
                    return ScalarType.createTimeV2Type(6);
                } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
                    return ScalarType.createDatetimeV2Type(3);
                } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                    return ScalarType.createDatetimeV2Type(6);
                } else {
                    return Type.BIGINT;
                }
            case FLOAT:
                return Type.FLOAT;
            case DOUBLE:
                return Type.DOUBLE;
            case STRING:
                return Type.STRING;
            case FIXED:
            case BYTES:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
                    int scale = ((LogicalTypes.Decimal) logicalType).getScale();
                    return ScalarType.createDecimalV3Type(precision, scale);
                } else {
                    return Type.STRING;
                }
            case ARRAY:
                Type innerType = fromAvroHudiTypeToDorisType(avroSchema.getElementType());
                return ArrayType.create(innerType, true);
            case RECORD:
                ArrayList<StructField> fields = new ArrayList<>();
                avroSchema.getFields().forEach(
                        f -> fields.add(new StructField(f.name(), fromAvroHudiTypeToDorisType(f.schema()))));
                return new StructType(fields);
            case MAP:
                // Hudi map's key must be string
                return new MapType(Type.STRING, fromAvroHudiTypeToDorisType(avroSchema.getValueType()));
            case UNION:
                List<Schema> nonNullMembers = avroSchema.getTypes().stream()
                        .filter(schema -> !Schema.Type.NULL.equals(schema.getType()))
                        .collect(Collectors.toList());
                // The nullable column in hudi is the union type with schemas [null, real column type]
                if (nonNullMembers.size() == 1) {
                    return fromAvroHudiTypeToDorisType(nonNullMembers.get(0));
                }
                break;
            default:
                break;
        }
        return Type.UNSUPPORTED;
    }
}
