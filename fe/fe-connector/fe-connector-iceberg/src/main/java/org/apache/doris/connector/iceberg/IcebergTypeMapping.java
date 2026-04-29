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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorType;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.List;

/**
 * Maps Iceberg {@link Type} to Doris {@link ConnectorType}.
 *
 * <p>Follows the same mapping logic as {@code IcebergUtils.icebergTypeToDorisType()}
 * in fe-core, producing ConnectorType instead of fe-core's Column/Type.</p>
 */
public final class IcebergTypeMapping {

    static final int ICEBERG_DATETIME_SCALE_MS = 6;

    private IcebergTypeMapping() {
    }

    /**
     * Convert an Iceberg type to ConnectorType.
     *
     * @param icebergType the Iceberg type
     * @param enableMappingVarbinary if true, map BINARY/UUID/FIXED to VARBINARY; otherwise STRING/CHAR
     * @param enableMappingTimestampTz if true, map TIMESTAMP with TZ to TIMESTAMPTV2; otherwise DATETIMEV2
     */
    public static ConnectorType fromIcebergType(Type icebergType,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        if (icebergType.isPrimitiveType()) {
            return fromPrimitive((Type.PrimitiveType) icebergType,
                    enableMappingVarbinary, enableMappingTimestampTz);
        }
        switch (icebergType.typeId()) {
            case LIST:
                Types.ListType list = (Types.ListType) icebergType;
                ConnectorType elemType = fromIcebergType(
                        list.elementType(), enableMappingVarbinary, enableMappingTimestampTz);
                return ConnectorType.arrayOf(elemType);
            case MAP:
                Types.MapType map = (Types.MapType) icebergType;
                ConnectorType keyType = fromIcebergType(
                        map.keyType(), enableMappingVarbinary, enableMappingTimestampTz);
                ConnectorType valType = fromIcebergType(
                        map.valueType(), enableMappingVarbinary, enableMappingTimestampTz);
                return ConnectorType.mapOf(keyType, valType);
            case STRUCT:
                Types.StructType struct = (Types.StructType) icebergType;
                List<String> names = new ArrayList<>(struct.fields().size());
                List<ConnectorType> types = new ArrayList<>(struct.fields().size());
                for (Types.NestedField f : struct.fields()) {
                    names.add(f.name());
                    types.add(fromIcebergType(
                            f.type(), enableMappingVarbinary, enableMappingTimestampTz));
                }
                return ConnectorType.structOf(names, types);
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private static ConnectorType fromPrimitive(Type.PrimitiveType primitive,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        switch (primitive.typeId()) {
            case BOOLEAN:
                return ConnectorType.of("BOOLEAN");
            case INTEGER:
                return ConnectorType.of("INT");
            case LONG:
                return ConnectorType.of("BIGINT");
            case FLOAT:
                return ConnectorType.of("FLOAT");
            case DOUBLE:
                return ConnectorType.of("DOUBLE");
            case STRING:
                return ConnectorType.of("STRING");
            case UUID:
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", 16, 0) : ConnectorType.of("STRING");
            case BINARY:
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", 65535, 0) : ConnectorType.of("STRING");
            case FIXED:
                int fixedLen = ((Types.FixedType) primitive).length();
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY", fixedLen, 0)
                        : ConnectorType.of("CHAR", fixedLen, 0);
            case DECIMAL:
                Types.DecimalType decimal = (Types.DecimalType) primitive;
                return ConnectorType.of("DECIMALV3", decimal.precision(), decimal.scale());
            case DATE:
                return ConnectorType.of("DATEV2");
            case TIMESTAMP:
                if (enableMappingTimestampTz
                        && ((Types.TimestampType) primitive).shouldAdjustToUTC()) {
                    return ConnectorType.of("TIMESTAMPTZV2", ICEBERG_DATETIME_SCALE_MS, 0);
                }
                return ConnectorType.of("DATETIMEV2", ICEBERG_DATETIME_SCALE_MS, 0);
            case TIME:
                return ConnectorType.of("UNSUPPORTED");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }
}
