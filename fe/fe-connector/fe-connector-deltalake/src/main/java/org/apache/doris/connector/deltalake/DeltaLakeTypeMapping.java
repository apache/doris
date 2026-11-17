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

package org.apache.doris.connector.deltalake;

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
import java.util.Locale;
import java.util.Map;

/** Maps the current Delta schema; physical schema evolution remains the BE mapper's responsibility. */
final class DeltaLakeTypeMapping {
    private static final Map<DataType, String> PRIMITIVES = Map.of(
            BooleanType.BOOLEAN, "BOOLEAN",
            ByteType.BYTE, "TINYINT",
            ShortType.SHORT, "SMALLINT",
            IntegerType.INTEGER, "INT",
            LongType.LONG, "BIGINT",
            FloatType.FLOAT, "FLOAT",
            DoubleType.DOUBLE, "DOUBLE",
            DateType.DATE, "DATEV2");

    private DeltaLakeTypeMapping() {
    }

    static ConnectorType toConnectorType(DataType type, boolean mapTimestampTz, boolean mapVarbinary) {
        String primitive = PRIMITIVES.get(type);
        if (primitive != null) {
            return ConnectorType.of(primitive);
        }
        if (type instanceof StringType && ((StringType) type).isUTF8BinaryCollated()) {
            return ConnectorType.of("STRING");
        }
        if (type instanceof BinaryType) {
            return ConnectorType.of(mapVarbinary ? "VARBINARY" : "STRING");
        }
        if (type instanceof DecimalType) {
            DecimalType decimal = (DecimalType) type;
            return ConnectorType.of("DECIMALV3", decimal.getPrecision(), decimal.getScale());
        }
        if (type instanceof TimestampType) {
            return ConnectorType.of(mapTimestampTz ? "TIMESTAMPTZ" : "DATETIMEV2", 6, 0);
        }
        if (type instanceof TimestampNTZType) {
            return ConnectorType.of("DATETIMEV2", 6, 0);
        }
        if (type instanceof ArrayType) {
            ArrayType array = (ArrayType) type;
            return ConnectorType.arrayOf(toConnectorType(array.getElementType(), mapTimestampTz, mapVarbinary),
                    array.containsNull());
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return ConnectorType.mapOf(toConnectorType(map.getKeyType(), mapTimestampTz, mapVarbinary),
                    toConnectorType(map.getValueType(), mapTimestampTz, mapVarbinary),
                    map.getValueField().isNullable());
        }
        if (type instanceof StructType) {
            List<String> names = new ArrayList<>();
            List<ConnectorType> types = new ArrayList<>();
            List<Boolean> nullable = new ArrayList<>();
            List<String> comments = new ArrayList<>();
            for (StructField field : ((StructType) type).fields()) {
                names.add(field.getName().toLowerCase(Locale.ROOT));
                types.add(toConnectorType(field.getDataType(), mapTimestampTz, mapVarbinary));
                nullable.add(field.isNullable());
                comments.add(field.getMetadata().contains("comment")
                        ? field.getMetadata().getString("comment") : "");
            }
            return ConnectorType.structOf(names, types, nullable, comments);
        }
        throw new DorisConnectorException("Unsupported Delta Lake type: " + type);
    }
}
