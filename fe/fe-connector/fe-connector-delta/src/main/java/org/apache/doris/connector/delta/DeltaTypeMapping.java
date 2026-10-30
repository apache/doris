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

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.types.ArrayType;
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
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.util.ArrayList;
import java.util.List;

/** Maps the subset of Delta logical types supported by the first native scan slice. */
public final class DeltaTypeMapping {

    private DeltaTypeMapping() {
    }

    public static ConnectorType fromDeltaType(DataType dataType) {
        if (dataType instanceof BooleanType) {
            return ConnectorType.of("BOOLEAN");
        }
        if (dataType instanceof ByteType) {
            return ConnectorType.of("TINYINT");
        }
        if (dataType instanceof ShortType) {
            return ConnectorType.of("SMALLINT");
        }
        if (dataType instanceof IntegerType) {
            return ConnectorType.of("INT");
        }
        if (dataType instanceof LongType) {
            return ConnectorType.of("BIGINT");
        }
        if (dataType instanceof FloatType) {
            return ConnectorType.of("FLOAT");
        }
        if (dataType instanceof DoubleType) {
            return ConnectorType.of("DOUBLE");
        }
        if (dataType instanceof StringType) {
            StringType stringType = (StringType) dataType;
            if (!stringType.isUTF8BinaryCollated()) {
                throw unsupported(dataType, "non-binary string collation");
            }
            return ConnectorType.of("STRING");
        }
        if (dataType instanceof BinaryType) {
            return ConnectorType.of("VARBINARY");
        }
        if (dataType instanceof DateType) {
            return ConnectorType.of("DATE");
        }
        if (dataType instanceof TimestampType || dataType instanceof TimestampNTZType) {
            return ConnectorType.of("DATETIMEV2", 6, -1);
        }
        if (dataType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dataType;
            return ConnectorType.of("DECIMALV3", decimalType.getPrecision(), decimalType.getScale());
        }
        if (dataType instanceof ArrayType) {
            return ConnectorType.arrayOf(fromDeltaType(((ArrayType) dataType).getElementType()));
        }
        if (dataType instanceof MapType) {
            MapType mapType = (MapType) dataType;
            return ConnectorType.mapOf(fromDeltaType(mapType.getKeyType()),
                    fromDeltaType(mapType.getValueType()));
        }
        if (dataType instanceof StructType) {
            StructType structType = (StructType) dataType;
            List<String> fieldNames = new ArrayList<>(structType.length());
            List<ConnectorType> fieldTypes = new ArrayList<>(structType.length());
            for (StructField field : structType.fields()) {
                fieldNames.add(field.getName());
                fieldTypes.add(fromDeltaType(field.getDataType()));
            }
            return ConnectorType.structOf(fieldNames, fieldTypes);
        }
        throw unsupported(dataType, "logical type is not mapped");
    }

    public static StructType toDeltaSchema(List<ConnectorColumn> columns) {
        StructType schema = new StructType();
        for (ConnectorColumn column : columns) {
            schema = schema.add(column.getName(), toDeltaType(column.getType()),
                    column.isNullable());
        }
        return schema;
    }

    public static DataType toDeltaType(ConnectorType type) {
        switch (type.getTypeName()) {
            case "BOOLEAN":
                return BooleanType.BOOLEAN;
            case "TINYINT":
                return ByteType.BYTE;
            case "SMALLINT":
                return ShortType.SHORT;
            case "INT":
                return IntegerType.INTEGER;
            case "BIGINT":
                return LongType.LONG;
            case "FLOAT":
                return FloatType.FLOAT;
            case "DOUBLE":
                return DoubleType.DOUBLE;
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                return StringType.STRING;
            case "BINARY":
            case "VARBINARY":
                return BinaryType.BINARY;
            case "DATE":
            case "DATEV2":
                return DateType.DATE;
            case "DATETIME":
            case "DATETIMEV2":
                return TimestampNTZType.TIMESTAMP_NTZ;
            case "TIMESTAMPTZ":
                return TimestampType.TIMESTAMP;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                if (type.getPrecision() <= 0 || type.getPrecision() > 38
                        || type.getScale() < 0 || type.getScale() > type.getPrecision()) {
                    throw new DorisConnectorException(
                            "Delta decimal requires 1-38 digits and a valid scale: " + type);
                }
                return new DecimalType(type.getPrecision(), type.getScale());
            case "ARRAY":
                requireChildren(type, 1);
                return new ArrayType(toDeltaType(type.getChildren().get(0)), true);
            case "MAP":
                requireChildren(type, 2);
                return new MapType(toDeltaType(type.getChildren().get(0)),
                        toDeltaType(type.getChildren().get(1)), true);
            case "STRUCT":
                if (type.getFieldNames().size() != type.getChildren().size()) {
                    throw new DorisConnectorException(
                            "Delta STRUCT field names and types have different sizes");
                }
                StructType struct = new StructType();
                for (int i = 0; i < type.getChildren().size(); i++) {
                    struct = struct.add(type.getFieldNames().get(i),
                            toDeltaType(type.getChildren().get(i)), true);
                }
                return struct;
            default:
                throw new DorisConnectorException(
                        "Unsupported Doris type for Delta create: " + type);
        }
    }

    private static void requireChildren(ConnectorType type, int expected) {
        if (type.getChildren().size() != expected) {
            throw new DorisConnectorException(
                    "Delta " + type.getTypeName() + " requires " + expected + " child types");
        }
    }

    private static DorisConnectorException unsupported(DataType dataType, String reason) {
        return new DorisConnectorException(
                "Unsupported Delta type " + dataType + ": " + reason);
    }
}
