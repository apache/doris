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

package org.apache.doris.datasource;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Converts between the connector SPI type system ({@link ConnectorColumn}/{@link ConnectorType})
 * and the Doris internal type system ({@link Column}/{@link Type}).
 *
 * <p>This converter lives in fe-core because it depends on both the SPI API types
 * (from fe-connector-api) and the internal Doris catalog types (from fe-type/fe-core).</p>
 */
public final class ConnectorColumnConverter {

    private static final Logger LOG = LogManager.getLogger(ConnectorColumnConverter.class);

    private ConnectorColumnConverter() {
    }

    /**
     * Converts a list of {@link ConnectorColumn} to a list of Doris {@link Column}.
     */
    public static List<Column> convertColumns(List<ConnectorColumn> connectorColumns) {
        return connectorColumns.stream()
                .map(ConnectorColumnConverter::convertColumn)
                .collect(Collectors.toList());
    }

    /**
     * Converts a single {@link ConnectorColumn} to a Doris {@link Column}.
     */
    public static Column convertColumn(ConnectorColumn cc) {
        Type dorisType = convertType(cc.getType());
        return new Column(cc.getName(), dorisType, cc.isKey(), null,
                cc.isNullable(), cc.getDefaultValue(),
                cc.getComment() != null ? cc.getComment() : "");
    }

    /**
     * Converts a Doris {@link Column} to a {@link ConnectorColumn}.
     * This is the inverse of {@link #convertColumn(ConnectorColumn)}.
     */
    public static ConnectorColumn toConnectorColumn(Column col) {
        ConnectorType connectorType = toConnectorType(col.getType());
        return new ConnectorColumn(
                col.getName(),
                connectorType,
                col.getComment(),
                col.isAllowNull(),
                col.getDefaultValue());
    }

    /**
     * Converts a Doris {@link Type} to a {@link ConnectorType}, handling
     * complex types (ARRAY, MAP, STRUCT) recursively.
     * This is the inverse of {@link #convertType(ConnectorType)}.
     */
    public static ConnectorType toConnectorType(Type dorisType) {
        if (dorisType instanceof ArrayType) {
            ArrayType arr = (ArrayType) dorisType;
            return ConnectorType.arrayOf(toConnectorType(arr.getItemType()));
        } else if (dorisType instanceof MapType) {
            MapType map = (MapType) dorisType;
            return ConnectorType.mapOf(
                    toConnectorType(map.getKeyType()),
                    toConnectorType(map.getValueType()));
        } else if (dorisType instanceof StructType) {
            StructType struct = (StructType) dorisType;
            List<String> names = new ArrayList<>();
            List<ConnectorType> types = new ArrayList<>();
            for (StructField f : struct.getFields()) {
                names.add(f.getName());
                types.add(toConnectorType(f.getType()));
            }
            return ConnectorType.structOf(names, types);
        } else if (dorisType instanceof ScalarType) {
            ScalarType scalar = (ScalarType) dorisType;
            return ConnectorType.of(
                    scalar.getPrimitiveType().toString(),
                    scalar.getScalarPrecision(),
                    scalar.getScalarScale());
        } else {
            return ConnectorType.of(dorisType.toString(), -1, -1);
        }
    }

    /**
     * Converts a {@link ConnectorType} to a Doris {@link Type}, handling
     * complex types (ARRAY, MAP, STRUCT) recursively.
     */
    public static Type convertType(ConnectorType ct) {
        String typeName = ct.getTypeName().toUpperCase(Locale.ROOT);
        switch (typeName) {
            case "ARRAY":
                return convertArrayType(ct);
            case "MAP":
                return convertMapType(ct);
            case "STRUCT":
                return convertStructType(ct);
            default:
                return convertScalarType(typeName, ct.getPrecision(), ct.getScale());
        }
    }

    private static Type convertArrayType(ConnectorType ct) {
        List<ConnectorType> children = ct.getChildren();
        if (children.isEmpty()) {
            return ArrayType.create(Type.NULL, true);
        }
        return ArrayType.create(convertType(children.get(0)), true);
    }

    private static Type convertMapType(ConnectorType ct) {
        List<ConnectorType> children = ct.getChildren();
        if (children.size() < 2) {
            return new MapType(Type.NULL, Type.NULL);
        }
        return new MapType(convertType(children.get(0)), convertType(children.get(1)));
    }

    private static Type convertStructType(ConnectorType ct) {
        List<ConnectorType> children = ct.getChildren();
        List<String> fieldNames = ct.getFieldNames();
        ArrayList<StructField> fields = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            String fieldName = i < fieldNames.size() ? fieldNames.get(i) : "col" + i;
            fields.add(new StructField(fieldName, convertType(children.get(i))));
        }
        return new StructType(fields);
    }

    private static Type convertScalarType(String typeName, int precision, int scale) {
        switch (typeName) {
            case "CHAR":
                if (precision > 0) {
                    return ScalarType.createCharType(precision);
                }
                return ScalarType.CHAR;
            case "VARCHAR":
                if (precision > 0) {
                    return ScalarType.createVarcharType(precision);
                }
                return ScalarType.createVarcharType();
            case "DECIMAL":
            case "DECIMALV2":
                if (precision > 0) {
                    return ScalarType.createDecimalType(precision, Math.max(scale, 0));
                }
                return ScalarType.createDecimalType();
            case "DECIMALV3":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                if (precision > 0) {
                    return ScalarType.createDecimalV3Type(precision, Math.max(scale, 0));
                }
                return ScalarType.createDecimalV3Type();
            case "DATETIMEV2":
                // Connectors encode datetime scale in the precision field of ConnectorType.
                if (precision >= 0) {
                    return ScalarType.createDatetimeV2Type(precision);
                }
                return ScalarType.DATETIMEV2;
            case "TIMESTAMPTZ":
                if (precision >= 0) {
                    return ScalarType.createTimeStampTzType(precision);
                }
                return ScalarType.createTimeStampTzType(0);
            case "VARBINARY":
                if (precision > 0) {
                    return ScalarType.createVarbinaryType(precision);
                }
                return ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH);
            case "JSONB":
                return ScalarType.createType("JSON");
            case "UNSUPPORTED":
                return Type.UNSUPPORTED;
            default:
                try {
                    return ScalarType.createType(typeName);
                } catch (Exception e) {
                    LOG.warn("Unrecognized connector type '{}', marking as UNSUPPORTED", typeName);
                    return Type.UNSUPPORTED;
                }
        }
    }
}
