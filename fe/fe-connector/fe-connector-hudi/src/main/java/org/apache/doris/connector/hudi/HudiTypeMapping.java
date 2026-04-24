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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorType;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Maps Avro {@link Schema} types (as used by Hudi) to Doris {@link ConnectorType}.
 *
 * <p>Hudi tables use Avro as their internal schema representation. This mapper
 * converts Avro primitive, logical, and complex types into the corresponding
 * Doris ConnectorType, following the same mapping logic as
 * {@code HudiUtils.fromAvroHudiTypeToDorisType()} in fe-core.</p>
 */
public final class HudiTypeMapping {

    private HudiTypeMapping() {
    }

    /**
     * Convert an Avro schema (non-nullable, already unwrapped from UNION) to ConnectorType.
     */
    public static ConnectorType fromAvroSchema(Schema avroSchema) {
        Schema.Type type = avroSchema.getType();
        LogicalType logicalType = avroSchema.getLogicalType();

        switch (type) {
            case BOOLEAN:
                return ConnectorType.of("BOOLEAN");
            case INT:
                return mapIntType(logicalType);
            case LONG:
                return mapLongType(logicalType);
            case FLOAT:
                return ConnectorType.of("FLOAT");
            case DOUBLE:
                return ConnectorType.of("DOUBLE");
            case STRING:
                return ConnectorType.of("STRING");
            case FIXED:
            case BYTES:
                return mapFixedOrBytesType(logicalType);
            case ARRAY:
                return mapArrayType(avroSchema);
            case RECORD:
                return mapRecordType(avroSchema);
            case MAP:
                return mapMapType(avroSchema);
            case UNION:
                return mapUnionType(avroSchema);
            case ENUM:
                return ConnectorType.of("STRING");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private static ConnectorType mapIntType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.Date) {
            return ConnectorType.of("DATEV2");
        }
        if (logicalType instanceof LogicalTypes.TimeMillis) {
            return ConnectorType.of("TIMEV2", 3, 0);
        }
        return ConnectorType.of("INT");
    }

    private static ConnectorType mapLongType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.TimeMicros) {
            return ConnectorType.of("TIMEV2", 6, 0);
        }
        if (logicalType instanceof LogicalTypes.TimestampMillis) {
            return ConnectorType.of("DATETIMEV2", 3, 0);
        }
        if (logicalType instanceof LogicalTypes.TimestampMicros) {
            return ConnectorType.of("DATETIMEV2", 6, 0);
        }
        return ConnectorType.of("BIGINT");
    }

    private static ConnectorType mapFixedOrBytesType(LogicalType logicalType) {
        if (logicalType instanceof LogicalTypes.Decimal) {
            int precision = ((LogicalTypes.Decimal) logicalType).getPrecision();
            int scale = ((LogicalTypes.Decimal) logicalType).getScale();
            return ConnectorType.of("DECIMALV3", precision, scale);
        }
        return ConnectorType.of("STRING");
    }

    private static ConnectorType mapArrayType(Schema avroSchema) {
        ConnectorType elementType = fromAvroSchema(avroSchema.getElementType());
        return ConnectorType.arrayOf(elementType);
    }

    private static ConnectorType mapRecordType(Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        List<String> names = new ArrayList<>(fields.size());
        List<ConnectorType> types = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            names.add(field.name());
            Schema fieldSchema = unwrapNullable(field.schema());
            types.add(fromAvroSchema(fieldSchema));
        }
        return ConnectorType.structOf(names, types);
    }

    private static ConnectorType mapMapType(Schema avroSchema) {
        ConnectorType valueType = fromAvroSchema(avroSchema.getValueType());
        return ConnectorType.mapOf(ConnectorType.of("STRING"), valueType);
    }

    private static ConnectorType mapUnionType(Schema avroSchema) {
        List<Schema> nonNull = avroSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .collect(Collectors.toList());
        if (nonNull.size() == 1) {
            return fromAvroSchema(nonNull.get(0));
        }
        return ConnectorType.of("UNSUPPORTED");
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> nonNull = new ArrayList<>();
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    nonNull.add(s);
                }
            }
            if (nonNull.size() == 1) {
                return nonNull.get(0);
            }
        }
        return schema;
    }
}
