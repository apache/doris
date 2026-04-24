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

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

import java.util.ArrayList;
import java.util.List;
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
