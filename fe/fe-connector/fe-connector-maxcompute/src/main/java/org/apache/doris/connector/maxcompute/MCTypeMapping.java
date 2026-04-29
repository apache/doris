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

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.type.ArrayTypeInfo;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.MapTypeInfo;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;

import java.util.ArrayList;
import java.util.List;

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
}
