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

package org.apache.doris.datasource.deltalake;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.Type;

import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.util.ArrayList;

/**
 * Utility class for Delta Lake type mapping and conversions.
 */
public class DeltaLakeUtils {

    private DeltaLakeUtils() {
        // utility class
    }

    /**
     * Convert a Delta Kernel StructField to a Doris Column.
     */
    public static Column convertDeltaFieldToDoris(io.delta.kernel.types.StructField field) {
        Type dorisType = convertDeltaTypeToDoris(field.getDataType());
        return new Column(field.getName(), dorisType, true, null, field.isNullable(),
                null, field.getMetadata().toString());
    }

    /**
     * Convert a Delta Kernel DataType to a Doris Type.
     */
    public static Type convertDeltaTypeToDoris(DataType deltaType) {
        if (deltaType instanceof BooleanType) {
            return Type.BOOLEAN;
        } else if (deltaType instanceof ByteType) {
            return Type.TINYINT;
        } else if (deltaType instanceof ShortType) {
            return Type.SMALLINT;
        } else if (deltaType instanceof IntegerType) {
            return Type.INT;
        } else if (deltaType instanceof LongType) {
            return Type.BIGINT;
        } else if (deltaType instanceof FloatType) {
            return Type.FLOAT;
        } else if (deltaType instanceof DoubleType) {
            return Type.DOUBLE;
        } else if (deltaType instanceof DateType) {
            return ScalarType.createDateV2Type();
        } else if (deltaType instanceof TimestampType) {
            return ScalarType.createDatetimeV2Type(6); // microsecond precision
        } else if (deltaType instanceof TimestampNTZType) {
            return ScalarType.createDatetimeV2Type(6);
        } else if (deltaType instanceof StringType) {
            return Type.STRING;
        } else if (deltaType instanceof BinaryType) {
            return Type.STRING; // map binary to string initially
        } else if (deltaType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) deltaType;
            return ScalarType.createDecimalV3Type(decimalType.getPrecision(), decimalType.getScale());
        } else if (deltaType instanceof io.delta.kernel.types.ArrayType) {
            io.delta.kernel.types.ArrayType arrayType = (io.delta.kernel.types.ArrayType) deltaType;
            Type elementType = convertDeltaTypeToDoris(arrayType.getElementType());
            return org.apache.doris.catalog.ArrayType.create(elementType, arrayType.containsNull());
        } else if (deltaType instanceof io.delta.kernel.types.MapType) {
            io.delta.kernel.types.MapType mapType = (io.delta.kernel.types.MapType) deltaType;
            Type keyType = convertDeltaTypeToDoris(mapType.getKeyType());
            Type valueType = convertDeltaTypeToDoris(mapType.getValueType());
            return new org.apache.doris.catalog.MapType(keyType, valueType);
        } else if (deltaType instanceof io.delta.kernel.types.StructType) {
            io.delta.kernel.types.StructType structType = (io.delta.kernel.types.StructType) deltaType;
            ArrayList<StructField> fields = new ArrayList<>();
            for (io.delta.kernel.types.StructField field : structType.fields()) {
                Type fieldType = convertDeltaTypeToDoris(field.getDataType());
                fields.add(new StructField(field.getName(), fieldType));
            }
            return new org.apache.doris.catalog.StructType(fields);
        }
        // Unknown type fallback
        return Type.STRING;
    }
}
