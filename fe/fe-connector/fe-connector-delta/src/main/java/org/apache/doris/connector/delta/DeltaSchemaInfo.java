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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Base64;
import java.util.List;
import java.util.Locale;

/** Builds the external schema metadata consumed by Doris' generic Parquet mapper. */
final class DeltaSchemaInfo {
    static final String SERIALIZED_SCHEMA_PROPERTY = "delta.schema.thrift";
    static final String SCHEMA_VERSION_PROPERTY = "delta.schema.version";
    static final int EXTERNAL_SCAN_SEMANTICS_VERSION = 1;

    private static final String COLUMN_MAPPING_MODE = "delta.columnMapping.mode";
    private static final String PHYSICAL_NAME = "delta.columnMapping.physicalName";
    private static final String COLUMN_MAPPING_ID = "delta.columnMapping.id";

    private DeltaSchemaInfo() {
    }

    static String serializeIfMapped(DeltaKernelSnapshot snapshot) {
        String mode = snapshot.getTableProperties().getOrDefault(COLUMN_MAPPING_MODE, "none")
                .toLowerCase(Locale.ROOT);
        if ("none".equals(mode)) {
            return null;
        }
        if (!"name".equals(mode) && !"id".equals(mode)) {
            throw new UnsupportedOperationException(
                    "Unsupported Delta column mapping mode: " + mode);
        }

        TSchema schema = new TSchema();
        schema.setSchemaId(snapshot.getVersion());
        schema.setRootField(toStructField(snapshot.getSchema(), mode));
        try {
            return Base64.getEncoder().encodeToString(new TSerializer().serialize(schema));
        } catch (TException e) {
            throw new DorisConnectorException(
                    "Failed to serialize Delta column mapping schema for snapshot "
                            + snapshot.getVersion(), e);
        }
    }

    private static TStructField toStructField(StructType structType, String mode) {
        TStructField result = new TStructField();
        for (StructField field : structType.fields()) {
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(toField(field, mode, true));
            result.addToFields(fieldPtr);
        }
        return result;
    }

    private static TField toField(StructField field, String mode, boolean mapPhysicalName) {
        TField result = new TField();
        result.setName(field.getName());
        result.setIsOptional(field.isNullable());
        if (mapPhysicalName) {
            setMappingMetadata(result, field.getMetadata(), mode);
        }

        DataType dataType = field.getDataType();
        TColumnType columnType = new TColumnType();
        TNestedField nestedField = new TNestedField();
        if (dataType instanceof StructType) {
            columnType.setType(TPrimitiveType.STRUCT);
            nestedField.setStructField(toStructField((StructType) dataType, mode));
            result.setNestedField(nestedField);
        } else if (dataType instanceof ArrayType) {
            columnType.setType(TPrimitiveType.ARRAY);
            TArrayField arrayField = new TArrayField();
            TFieldPtr element = new TFieldPtr();
            element.setFieldPtr(toField(new StructField("element",
                    ((ArrayType) dataType).getElementType(), true), mode, false));
            arrayField.setItemField(element);
            nestedField.setArrayField(arrayField);
            result.setNestedField(nestedField);
        } else if (dataType instanceof MapType) {
            columnType.setType(TPrimitiveType.MAP);
            MapType mapType = (MapType) dataType;
            TMapField mapField = new TMapField();
            TFieldPtr key = new TFieldPtr();
            key.setFieldPtr(toField(new StructField("key", mapType.getKeyType(), false), mode,
                    false));
            TFieldPtr value = new TFieldPtr();
            value.setFieldPtr(toField(new StructField("value", mapType.getValueType(), true), mode,
                    false));
            mapField.setKeyField(key);
            mapField.setValueField(value);
            nestedField.setMapField(mapField);
            result.setNestedField(nestedField);
        } else {
            setPrimitiveType(columnType, dataType);
        }
        result.setType(columnType);
        return result;
    }

    private static void setMappingMetadata(TField field, FieldMetadata metadata, String mode) {
        String physicalName = metadata.getString(PHYSICAL_NAME);
        if (physicalName == null || physicalName.trim().isEmpty()) {
            throw new UnsupportedOperationException(
                    "Delta column mapping field has no physical name");
        }
        field.setNameMapping(List.of(physicalName));
        field.setNameMappingIsAuthoritative(true);

        Object idValue = metadata.get(COLUMN_MAPPING_ID);
        if (idValue instanceof Number) {
            long id = ((Number) idValue).longValue();
            if (id < Integer.MIN_VALUE || id > Integer.MAX_VALUE) {
                throw new UnsupportedOperationException(
                        "Delta column mapping field id is outside Doris thrift range: " + id);
            }
            field.setId((int) id);
        } else if ("id".equals(mode)) {
            throw new UnsupportedOperationException(
                    "Delta id column mapping field has no field id");
        }
    }

    private static void setPrimitiveType(TColumnType result, DataType dataType) {
        if (dataType instanceof BooleanType) {
            result.setType(TPrimitiveType.BOOLEAN);
        } else if (dataType instanceof ByteType) {
            result.setType(TPrimitiveType.TINYINT);
        } else if (dataType instanceof ShortType) {
            result.setType(TPrimitiveType.SMALLINT);
        } else if (dataType instanceof IntegerType) {
            result.setType(TPrimitiveType.INT);
        } else if (dataType instanceof LongType) {
            result.setType(TPrimitiveType.BIGINT);
        } else if (dataType instanceof FloatType) {
            result.setType(TPrimitiveType.FLOAT);
        } else if (dataType instanceof DoubleType) {
            result.setType(TPrimitiveType.DOUBLE);
        } else if (dataType instanceof StringType) {
            result.setType(TPrimitiveType.STRING);
        } else if (dataType instanceof BinaryType) {
            result.setType(TPrimitiveType.VARBINARY);
        } else if (dataType instanceof DateType) {
            result.setType(TPrimitiveType.DATEV2);
        } else if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
            result.setType(TPrimitiveType.DATETIMEV2);
        } else if (dataType instanceof DecimalType) {
            DecimalType decimal = (DecimalType) dataType;
            result.setType(decimalPrimitive(decimal.getPrecision()));
            result.setPrecision(decimal.getPrecision());
            result.setScale(decimal.getScale());
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Delta column mapping data type: " + dataType);
        }
    }

    private static TPrimitiveType decimalPrimitive(int precision) {
        if (precision <= 9) {
            return TPrimitiveType.DECIMAL32;
        }
        if (precision <= 18) {
            return TPrimitiveType.DECIMAL64;
        }
        if (precision <= 38) {
            return TPrimitiveType.DECIMAL128I;
        }
        if (precision <= 76) {
            return TPrimitiveType.DECIMAL256;
        }
        throw new UnsupportedOperationException(
                "Delta decimal precision exceeds Doris support: " + precision);
    }
}
