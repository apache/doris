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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.type.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Maps MaxCompute (ODPS) type system to Doris ConnectorType.
 * Adapted from MaxComputeExternalTable.mcTypeToDorisType().
 */
public final class MCTypeMapping {
    private MCTypeMapping() {
    }

    /**
     * Converts a MaxCompute TypeInfo to a ConnectorType descriptor.
     */
    public static ConnectorType toConnectorType(TypeInfo typeInfo) {
        OdpsType odpsType = typeInfo.getOdpsType();
        switch (odpsType) {
            case VOID:
                return ConnectorType.of("NULL");
            case BOOLEAN:
                return ConnectorType.of("BOOLEAN");
            case TINYINT:
                return ConnectorType.of("TINYINT");
            case SMALLINT:
                return ConnectorType.of("SMALLINT");
            case INT:
                return ConnectorType.of("INT");
            case BIGINT:
                return ConnectorType.of("BIGINT");
            case FLOAT:
                return ConnectorType.of("FLOAT");
            case DOUBLE:
                return ConnectorType.of("DOUBLE");
            case CHAR:
                return ConnectorType.of("CHAR",
                        ((CharTypeInfo) typeInfo).getLength(), 0);
            case STRING:
                return ConnectorType.of("STRING");
            case VARCHAR:
                return ConnectorType.of("VARCHAR",
                        ((VarcharTypeInfo) typeInfo).getLength(), 0);
            case JSON:
                return ConnectorType.of("UNSUPPORTED");
            case DECIMAL: {
                DecimalTypeInfo decimal = (DecimalTypeInfo) typeInfo;
                return ConnectorType.of("DECIMALV3",
                        decimal.getPrecision(), decimal.getScale());
            }
            case DATE:
                return ConnectorType.of("DATEV2");
            case DATETIME:
                return ConnectorType.of("DATETIMEV2", 3, 0);
            case TIMESTAMP:
            case TIMESTAMP_NTZ:
                return ConnectorType.of("DATETIMEV2", 6, 0);
            case ARRAY:
                return mapArrayType((ArrayTypeInfo) typeInfo);
            case MAP:
                return mapMapType((MapTypeInfo) typeInfo);
            case STRUCT:
                return mapStructType((StructTypeInfo) typeInfo);
            case BINARY:
            case INTERVAL_DAY_TIME:
            case INTERVAL_YEAR_MONTH:
                return ConnectorType.of("UNSUPPORTED");
            default:
                return ConnectorType.of("UNSUPPORTED");
        }
    }

    private static ConnectorType mapArrayType(ArrayTypeInfo arrayType) {
        ConnectorType elementType =
                toConnectorType(arrayType.getElementTypeInfo());
        return ConnectorType.arrayOf(elementType);
    }

    private static ConnectorType mapMapType(MapTypeInfo mapType) {
        ConnectorType keyType =
                toConnectorType(mapType.getKeyTypeInfo());
        ConnectorType valueType =
                toConnectorType(mapType.getValueTypeInfo());
        return ConnectorType.mapOf(keyType, valueType);
    }

    private static ConnectorType mapStructType(StructTypeInfo structType) {
        List<String> fieldNames = structType.getFieldNames();
        List<TypeInfo> fieldTypeInfos = structType.getFieldTypeInfos();
        List<ConnectorType> fieldTypes = new ArrayList<>(fieldNames.size());
        List<String> names = new ArrayList<>(fieldNames.size());
        for (int i = 0; i < structType.getFieldCount(); i++) {
            names.add(fieldNames.get(i));
            fieldTypes.add(toConnectorType(fieldTypeInfos.get(i)));
        }
        return ConnectorType.structOf(names, fieldTypes);
    }

    /**
     * Converts a {@link ConnectorType} (as produced by the CREATE TABLE request
     * path) to a MaxCompute (ODPS) {@link TypeInfo}. Faithful reverse of the
     * legacy {@code MaxComputeMetadataOps.dorisTypeToMcType}; the scalar type
     * name is the Doris {@code PrimitiveType} name (e.g. INT, DECIMAL64,
     * DATETIMEV2), with CHAR/VARCHAR length and DECIMAL precision/scale carried
     * in the {@link ConnectorType} precision/scale fields.
     *
     * @throws DorisConnectorException if the type cannot be represented in MaxCompute
     */
    public static TypeInfo toMcType(ConnectorType type) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        switch (name) {
            case "ARRAY":
                return TypeInfoFactory.getArrayTypeInfo(
                        toMcType(type.getChildren().get(0)));
            case "MAP":
                return TypeInfoFactory.getMapTypeInfo(
                        toMcType(type.getChildren().get(0)),
                        toMcType(type.getChildren().get(1)));
            case "STRUCT":
                return toMcStructType(type);
            default:
                return toMcScalarType(name, type);
        }
    }

    private static TypeInfo toMcScalarType(String name, ConnectorType type) {
        switch (name) {
            case "BOOLEAN":
                return TypeInfoFactory.BOOLEAN;
            case "TINYINT":
                return TypeInfoFactory.TINYINT;
            case "SMALLINT":
                return TypeInfoFactory.SMALLINT;
            case "INT":
                return TypeInfoFactory.INT;
            case "BIGINT":
                return TypeInfoFactory.BIGINT;
            case "FLOAT":
                return TypeInfoFactory.FLOAT;
            case "DOUBLE":
                return TypeInfoFactory.DOUBLE;
            case "CHAR":
                return TypeInfoFactory.getCharTypeInfo(type.getPrecision());
            case "VARCHAR":
                return TypeInfoFactory.getVarcharTypeInfo(type.getPrecision());
            case "STRING":
                return TypeInfoFactory.STRING;
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
            case "DECIMAL256":
                return TypeInfoFactory.getDecimalTypeInfo(
                        type.getPrecision(), type.getScale());
            case "DATE":
            case "DATEV2":
                return TypeInfoFactory.DATE;
            case "DATETIME":
            case "DATETIMEV2":
                return TypeInfoFactory.DATETIME;
            default:
                throw new DorisConnectorException(
                        "Unsupported type for MaxCompute: " + type);
        }
    }

    private static TypeInfo toMcStructType(ConnectorType type) {
        List<ConnectorType> children = type.getChildren();
        List<String> names = type.getFieldNames();
        List<String> fieldNames = new ArrayList<>(children.size());
        List<TypeInfo> fieldTypes = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            fieldNames.add(i < names.size() ? names.get(i) : "col" + i);
            fieldTypes.add(toMcType(children.get(i)));
        }
        return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypes);
    }
}
