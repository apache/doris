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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.vec.ColumnType;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the Paimon read type that matches the nested column shape requested by Doris.
 *
 * <p>The BE applies access paths to the JNI {@link ColumnType} before creating this scanner. This
 * class mirrors that shape with Paimon's types so {@code withReadType} can push the same projection
 * through ROW, ARRAY, and MAP readers while retaining Paimon's field IDs and nullability.
 */
final class PaimonReadTypeProjection {
    private PaimonReadTypeProjection() {
    }

    static DataType project(DataType tableType, ColumnType requiredType) {
        if (requiredType.isStruct()) {
            return projectRow(tableType, requiredType);
        }
        if (requiredType.isArray()) {
            return projectArray(tableType, requiredType);
        }
        if (requiredType.isMap()) {
            return projectMap(tableType, requiredType);
        }
        // Doris' scalar type can differ from Paimon's logical type (for example timestamp
        // precision). Keep the table type and only use ColumnType to describe nested shape.
        return tableType;
    }

    private static DataType projectRow(DataType tableType, ColumnType requiredType) {
        if (!(tableType instanceof RowType)) {
            throw incompatibleType(requiredType, tableType);
        }
        RowType rowType = (RowType) tableType;
        List<String> childNames = requiredType.getChildNames();
        List<ColumnType> childTypes = requiredType.getChildTypes();
        if (childNames == null || childTypes == null || childNames.size() != childTypes.size()) {
            throw new IllegalArgumentException(
                    "Invalid Doris STRUCT projection for column " + requiredType.getName());
        }

        List<DataField> projectedFields = new ArrayList<>(childNames.size());
        for (int i = 0; i < childNames.size(); i++) {
            DataField tableField = findField(rowType, childNames.get(i));
            if (tableField == null) {
                throw new IllegalArgumentException(String.format(
                        "Doris requested nested field '%s' which does not exist in Paimon type %s",
                        childNames.get(i), rowType.asSQLString()));
            }
            DataType projectedType = project(tableField.type(), childTypes.get(i));
            projectedFields.add(tableField.newType(projectedType));
        }
        return rowType.copy(projectedFields);
    }

    private static DataType projectArray(DataType tableType, ColumnType requiredType) {
        if (!(tableType instanceof ArrayType)) {
            throw incompatibleType(requiredType, tableType);
        }
        List<ColumnType> childTypes = requiredType.getChildTypes();
        if (childTypes == null || childTypes.size() != 1) {
            throw new IllegalArgumentException(
                    "Invalid Doris ARRAY projection for column " + requiredType.getName());
        }
        ArrayType arrayType = (ArrayType) tableType;
        return arrayType.newElementType(project(arrayType.getElementType(), childTypes.get(0)));
    }

    private static DataType projectMap(DataType tableType, ColumnType requiredType) {
        if (!(tableType instanceof MapType)) {
            throw incompatibleType(requiredType, tableType);
        }
        List<ColumnType> childTypes = requiredType.getChildTypes();
        if (childTypes == null || childTypes.size() != 2) {
            throw new IllegalArgumentException(
                    "Invalid Doris MAP projection for column " + requiredType.getName());
        }
        MapType mapType = (MapType) tableType;
        return mapType.newKeyValueType(
                project(mapType.getKeyType(), childTypes.get(0)),
                project(mapType.getValueType(), childTypes.get(1)));
    }

    private static DataField findField(RowType rowType, String requiredName) {
        for (DataField field : rowType.getFields()) {
            if (field.name().equalsIgnoreCase(requiredName)) {
                return field;
            }
        }
        return null;
    }

    private static IllegalArgumentException incompatibleType(
            ColumnType requiredType, DataType tableType) {
        return new IllegalArgumentException(String.format(
                "Doris requested %s for column '%s', but the Paimon type is %s",
                requiredType.getType(), requiredType.getName(), tableType.asSQLString()));
    }
}
