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
import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

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
     * @param enableMappingTimestampTz if true, map TIMESTAMP with TZ to TIMESTAMPTZ; otherwise DATETIMEV2
     */
    public static ConnectorType fromIcebergType(Type icebergType,
            boolean enableMappingVarbinary, boolean enableMappingTimestampTz) {
        if (icebergType.isPrimitiveType()) {
            return fromPrimitive((Type.PrimitiveType) icebergType,
                    enableMappingVarbinary, enableMappingTimestampTz);
        }
        switch (icebergType.typeId()) {
            case LIST:
                // Carry the element field-id (legacy IcebergUtils.updateIcebergColumnUniqueId recurses into
                // the element with ListType.fields().get(0).fieldId()) so the BE field-id scan path matches
                // a pruned array-of-struct leaf by id; a -1 leaf is skipped and returns NULL.
                Types.ListType list = (Types.ListType) icebergType;
                ConnectorType elemType = fromIcebergType(
                        list.elementType(), enableMappingVarbinary, enableMappingTimestampTz);
                return ConnectorType.arrayOf(elemType)
                        .withChildrenFieldIds(Collections.singletonList(list.elementId()));
            case MAP:
                // Carry key + value field-ids (legacy recurses into both via MapType.fields()).
                Types.MapType map = (Types.MapType) icebergType;
                ConnectorType keyType = fromIcebergType(
                        map.keyType(), enableMappingVarbinary, enableMappingTimestampTz);
                ConnectorType valType = fromIcebergType(
                        map.valueType(), enableMappingVarbinary, enableMappingTimestampTz);
                return ConnectorType.mapOf(keyType, valType)
                        .withChildrenFieldIds(Arrays.asList(map.keyId(), map.valueId()));
            case STRUCT:
                // Carry each field's field-id, parallel to the field types (legacy recurses field-by-field).
                Types.StructType struct = (Types.StructType) icebergType;
                List<String> names = new ArrayList<>(struct.fields().size());
                List<ConnectorType> types = new ArrayList<>(struct.fields().size());
                List<Integer> fieldIds = new ArrayList<>(struct.fields().size());
                for (Types.NestedField f : struct.fields()) {
                    names.add(f.name());
                    types.add(fromIcebergType(
                            f.type(), enableMappingVarbinary, enableMappingTimestampTz));
                    fieldIds.add(f.fieldId());
                }
                return ConnectorType.structOf(names, types).withChildrenFieldIds(fieldIds);
            default:
                // Any non-primitive iceberg type Doris cannot represent (VARIANT today; future non-primitive
                // typeIds) degrades to UNSUPPORTED: the table still LOADS and only this column is
                // present-but-unqueryable. This DIVERGES from legacy fe-core (IcebergUtils.icebergTypeToDorisType
                // threw IllegalArgumentException at schema-load, failing the whole table). Graceful degradation
                // is INTENTIONAL (user decision 2026-07-13: map every unrepresentable column uniformly to
                // UNSUPPORTED so one exotic column does not make a wide table unloadable). Registered DV-051.
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
                // Iceberg BINARY is unbounded. Emit VARBINARY with NO explicit length so
                // ConnectorColumnConverter applies ScalarType.MAX_VARBINARY_LENGTH — byte-identical to
                // legacy IcebergUtils createVarbinaryType(VarBinaryType.MAX_VARBINARY_LENGTH). A
                // concrete length (e.g. 65535) would render a different DESCRIBE / SHOW CREATE type.
                return enableMappingVarbinary
                        ? ConnectorType.of("VARBINARY") : ConnectorType.of("STRING");
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
                    // Must be "TIMESTAMPTZ" (not "TIMESTAMPTZV2"): ConnectorColumnConverter only
                    // recognizes TIMESTAMPTZ -> ScalarType.createTimeStampTzType(precision); an
                    // unrecognized name degrades the column to UNSUPPORTED. Legacy
                    // IcebergUtils maps this to createTimeStampTzType(ICEBERG_DATETIME_SCALE_MS).
                    return ConnectorType.of("TIMESTAMPTZ", ICEBERG_DATETIME_SCALE_MS, 0);
                }
                return ConnectorType.of("DATETIMEV2", ICEBERG_DATETIME_SCALE_MS, 0);
            case TIME:
                // iceberg TIME has no Doris analogue -> UNSUPPORTED (explicit, byte-parity with legacy
                // IcebergUtils which also mapped TIME to Type.UNSUPPORTED).
                return ConnectorType.of("UNSUPPORTED");
            default:
                // Any primitive iceberg type Doris cannot represent — notably the v3 types TIMESTAMP_NANO /
                // GEOMETRY / GEOGRAPHY / UNKNOWN — degrades to UNSUPPORTED: the table still LOADS and only this
                // column is present-but-unqueryable. This DIVERGES from legacy (IcebergUtils.icebergPrimitiveType-
                // ToDorisType threw IllegalArgumentException "Cannot transform unknown type", failing the whole
                // table). Graceful degradation is INTENTIONAL (user decision 2026-07-13: map every unrepresentable
                // column uniformly to UNSUPPORTED so one exotic column does not make a wide table unloadable).
                // Registered DV-051. (The write direction toIcebergPrimitive still throws — CREATE TABLE must not
                // silently accept a type it cannot round-trip.)
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    /**
     * Maps a SCALAR {@link ConnectorType} (a Doris column type carried across the SPI by name) to an
     * Iceberg {@link Type} for CREATE TABLE. The inverse of {@link #fromPrimitive}, this is a string-driven
     * port of the legacy fe-core {@code DorisTypeToIcebergType.atomic} (which walked a Doris {@code Type}
     * object): the connector only has the neutral {@code typeName} (the Doris {@code PrimitiveType.toString()})
     * plus precision/scale, so the same set of supported types is matched here.
     *
     * <p>Complex types (ARRAY / MAP / STRUCT) are NOT handled here — {@link IcebergSchemaBuilder} owns the
     * recursive tree walk + field-id allocation and calls this only for scalar leaves. Any type legacy did
     * not support (TINYINT / SMALLINT / LARGEINT / TIME / JSON / VARIANT / IPv*, ...) fails loud, matching
     * legacy's {@code UnsupportedOperationException}.</p>
     */
    static Type toIcebergPrimitive(ConnectorType type) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        switch (name) {
            case "BOOLEAN":
                return Types.BooleanType.get();
            case "INT":
            case "INTEGER":
                return Types.IntegerType.get();
            case "BIGINT":
                return Types.LongType.get();
            case "FLOAT":
                return Types.FloatType.get();
            case "DOUBLE":
                return Types.DoubleType.get();
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                // Legacy parity: every char-family Doris type maps to Iceberg STRING (declared length
                // dropped) — DorisTypeToIcebergType.atomic uses primitiveType.isCharFamily().
                return Types.StringType.get();
            case "DATE":
            case "DATEV2":
                return Types.DateType.get();
            case "DECIMALV2":
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                // Precision/scale carried in the ConnectorType (ConnectorColumnConverter.toConnectorType
                // sets them from ScalarType.getScalarPrecision()/getScalarScale()).
                return Types.DecimalType.of(type.getPrecision(), Math.max(type.getScale(), 0));
            case "DATETIME":
            case "DATETIMEV2":
                // Legacy parity: timestamp WITHOUT zone (datetime scale dropped — Iceberg timestamps are
                // microsecond). DorisTypeToIcebergType.atomic returns TimestampType.withoutZone().
                return Types.TimestampType.withoutZone();
            case "TIMESTAMPTZ":
                return Types.TimestampType.withZone();
            default:
                throw new DorisConnectorException("Unsupported type for Iceberg: " + type.getTypeName());
        }
    }
}
