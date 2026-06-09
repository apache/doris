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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.types.VariantType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Maps Paimon {@link DataType} to {@link ConnectorType}.
 *
 * <p>This mirrors the type conversion logic in the existing
 * {@code PaimonUtil.paimonTypeToDorisType()} but produces
 * SPI-layer {@link ConnectorType} values instead of fe-core types.
 */
public final class PaimonTypeMapping {

    private static final int MAX_TIMESTAMP_SCALE = 6;
    private static final int DEFAULT_TIMESTAMP_SCALE = 6;
    private static final int MAX_CHAR_LENGTH = 255;

    private PaimonTypeMapping() {
    }

    /**
     * Convert a Paimon {@link DataType} to a {@link ConnectorType}.
     */
    public static ConnectorType toConnectorType(DataType dataType, Options options) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return ConnectorType.of("BOOLEAN");
            case TINYINT:
                return ConnectorType.of("TINYINT");
            case SMALLINT:
                return ConnectorType.of("SMALLINT");
            case INTEGER:
                return ConnectorType.of("INT");
            case BIGINT:
                return ConnectorType.of("BIGINT");
            case FLOAT:
                return ConnectorType.of("FLOAT");
            case DOUBLE:
                return ConnectorType.of("DOUBLE");
            case VARCHAR:
                return toVarcharType((VarCharType) dataType);
            case CHAR:
                return toCharType((CharType) dataType);
            case BINARY:
                return toBinaryType((BinaryType) dataType, options);
            case VARBINARY:
                return toVarbinaryType((VarBinaryType) dataType, options);
            case DECIMAL:
                return toDecimalType((DecimalType) dataType);
            case DATE:
                return ConnectorType.of("DATEV2");
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return toTimestampType(dataType);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return toTimestampTzType(dataType, options);
            case ARRAY:
                return toArrayType((ArrayType) dataType, options);
            case MAP:
                return toMapType((MapType) dataType, options);
            case ROW:
                return toStructType((RowType) dataType, options);
            case TIME_WITHOUT_TIME_ZONE:
                return ConnectorType.of("UNSUPPORTED");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private static ConnectorType toVarcharType(VarCharType type) {
        int len = type.getLength();
        if (len <= 0 || len >= 65533) {
            return ConnectorType.of("STRING");
        }
        return ConnectorType.of("VARCHAR", len, 0);
    }

    private static ConnectorType toCharType(CharType type) {
        int len = type.getLength();
        if (len > MAX_CHAR_LENGTH) {
            return ConnectorType.of("STRING");
        }
        return ConnectorType.of("CHAR", len, 0);
    }

    private static ConnectorType toBinaryType(BinaryType type, Options options) {
        if (options.isMapBinaryToVarbinary()) {
            return ConnectorType.of("VARBINARY", type.getLength(), 0);
        }
        return ConnectorType.of("STRING");
    }

    private static ConnectorType toVarbinaryType(VarBinaryType type, Options options) {
        if (options.isMapBinaryToVarbinary()) {
            return ConnectorType.of("VARBINARY", type.getLength(), 0);
        }
        return ConnectorType.of("STRING");
    }

    private static ConnectorType toDecimalType(DecimalType type) {
        return ConnectorType.of("DECIMALV3", type.getPrecision(), type.getScale());
    }

    private static ConnectorType toTimestampType(DataType dataType) {
        int scale = DEFAULT_TIMESTAMP_SCALE;
        if (dataType instanceof TimestampType) {
            scale = Math.min(((TimestampType) dataType).getPrecision(), MAX_TIMESTAMP_SCALE);
        } else if (dataType instanceof LocalZonedTimestampType) {
            scale = Math.min(((LocalZonedTimestampType) dataType).getPrecision(), MAX_TIMESTAMP_SCALE);
        }
        return ConnectorType.of("DATETIMEV2", scale, 0);
    }

    private static ConnectorType toTimestampTzType(DataType dataType, Options options) {
        int scale = DEFAULT_TIMESTAMP_SCALE;
        if (dataType instanceof LocalZonedTimestampType) {
            scale = Math.min(((LocalZonedTimestampType) dataType).getPrecision(), MAX_TIMESTAMP_SCALE);
        }
        if (options.isMapTimestampTz()) {
            return ConnectorType.of("TIMESTAMPTZ", scale, 0);
        }
        return ConnectorType.of("DATETIMEV2", scale, 0);
    }

    private static ConnectorType toArrayType(ArrayType type, Options options) {
        ConnectorType elementType = toConnectorType(type.getElementType(), options);
        return ConnectorType.arrayOf(elementType);
    }

    private static ConnectorType toMapType(MapType type, Options options) {
        ConnectorType keyType = toConnectorType(type.getKeyType(), options);
        ConnectorType valueType = toConnectorType(type.getValueType(), options);
        return ConnectorType.mapOf(keyType, valueType);
    }

    private static ConnectorType toStructType(RowType rowType, Options options) {
        List<DataField> fields = rowType.getFields();
        List<String> names = fields.stream()
                .map(DataField::name)
                .collect(Collectors.toCollection(ArrayList::new));
        List<ConnectorType> types = fields.stream()
                .map(f -> toConnectorType(f.type(), options))
                .collect(Collectors.toCollection(ArrayList::new));
        return ConnectorType.structOf(names, types);
    }

    /**
     * Convert a Doris {@link ConnectorType} (as produced by the CREATE TABLE request path)
     * to a Paimon {@link DataType}.
     *
     * <p>This is the faithful reverse of the legacy fe-core
     * {@code DorisToPaimonTypeVisitor}: the scalar set is intentionally narrow (it mirrors
     * the visitor's {@code atomic} branches and NOT MaxCompute's richer set), CHAR/VARCHAR/STRING
     * all collapse to {@code VarChar(MAX)} (declared length dropped), DATETIME/DATETIMEV2 map to a
     * plain {@code TimestampType()} (scale dropped), and the MAP key is forced non-null. Types the
     * legacy visitor did not handle (TINYINT, SMALLINT, LARGEINT, TIME, IPV4/6, JSON, ...) throw,
     * preserving the legacy gap.</p>
     *
     * <p>The returned type carries Paimon's default (nullable) flag; column-level nullability is
     * applied by the caller via {@code .copy(nullable)} (mirroring legacy
     * {@code PaimonMetadataOps.toPaimonSchema}). The map-key {@code .copy(false)} below is part of
     * the type structure (not column nullability) and is kept.</p>
     *
     * @throws DorisConnectorException if the type cannot be represented in Paimon
     */
    public static DataType toPaimonType(ConnectorType type) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        switch (name) {
            case "BOOLEAN":
                return new BooleanType();
            case "INT":
            case "INTEGER":
                return new IntType();
            case "BIGINT":
                return new BigIntType();
            case "FLOAT":
                return new FloatType();
            case "DOUBLE":
                return new DoubleType();
            case "CHAR":
            case "VARCHAR":
            case "STRING":
                // Legacy parity: all char-family types collapse to VarChar(MAX); declared
                // length is intentionally dropped (DorisToPaimonTypeVisitor.atomic isCharFamily).
                return new VarCharType(VarCharType.MAX_LENGTH);
            case "DATE":
            case "DATEV2":
                return new DateType();
            case "DECIMALV2":
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                return new DecimalType(type.getPrecision(), type.getScale());
            case "DATETIME":
            case "DATETIMEV2":
                // Legacy parity: no-arg TimestampType (precision defaults to 6); the datetime
                // scale is intentionally dropped to match DorisToPaimonTypeVisitor.atomic, and it
                // is a plain timestamp (NOT LocalZonedTimestampType).
                return new TimestampType();
            case "VARBINARY":
                return new VarBinaryType(VarBinaryType.MAX_LENGTH);
            case "VARIANT":
                return new VariantType();
            case "ARRAY":
                return new ArrayType(toPaimonType(type.getChildren().get(0)));
            case "MAP":
                // Legacy forces the map key non-null via .copy(false).
                return new MapType(
                        toPaimonType(type.getChildren().get(0)).copy(false),
                        toPaimonType(type.getChildren().get(1)));
            case "STRUCT":
            case "ROW":
                return toPaimonRowType(type);
            default:
                throw new DorisConnectorException(
                        "Unsupported type for Paimon: " + type.getTypeName());
        }
    }

    private static DataType toPaimonRowType(ConnectorType type) {
        List<ConnectorType> children = type.getChildren();
        List<String> names = type.getFieldNames();
        List<DataField> fields = new ArrayList<>(children.size());
        // Legacy uses new AtomicInteger(-1).incrementAndGet() -> sequential ids 0,1,2,...
        AtomicInteger fieldId = new AtomicInteger(-1);
        for (int i = 0; i < children.size(); i++) {
            String fieldName = i < names.size() && names.get(i) != null ? names.get(i) : "col" + i;
            fields.add(new DataField(
                    fieldId.incrementAndGet(), fieldName, toPaimonType(children.get(i))));
        }
        return new RowType(fields);
    }

    /**
     * Type mapping options.
     */
    public static final class Options {

        public static final Options DEFAULT = new Options(false, false);

        private final boolean mapBinaryToVarbinary;
        private final boolean mapTimestampTz;

        public Options(boolean mapBinaryToVarbinary, boolean mapTimestampTz) {
            this.mapBinaryToVarbinary = mapBinaryToVarbinary;
            this.mapTimestampTz = mapTimestampTz;
        }

        public boolean isMapBinaryToVarbinary() {
            return mapBinaryToVarbinary;
        }

        public boolean isMapTimestampTz() {
            return mapTimestampTz;
        }
    }
}
