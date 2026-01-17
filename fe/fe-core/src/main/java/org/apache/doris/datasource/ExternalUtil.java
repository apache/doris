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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExternalUtil {
    private static TField getExternalSchema(Column column) {
        TField root = new TField();
        root.setName(column.getName());
        root.setId(column.getUniqueId());
        root.setIsOptional(column.isAllowNull());
        root.setType(column.getType().toColumnTypeThrift());

        TNestedField nestedField = new TNestedField();
        if (column.getType().isStructType()) {
            nestedField.setStructField(getExternalSchema(column.getChildren()));
            root.setNestedField(nestedField);
        } else if (column.getType().isArrayType()) {
            TArrayField listField = new TArrayField();
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(getExternalSchema(column.getChildren().get(0)));
            listField.setItemField(fieldPtr);
            nestedField.setArrayField(listField);
            root.setNestedField(nestedField);
        } else if (column.getType().isMapType()) {
            TMapField mapField = new TMapField();
            TFieldPtr keyPtr = new TFieldPtr();
            keyPtr.setFieldPtr(getExternalSchema(column.getChildren().get(0)));
            mapField.setKeyField(keyPtr);
            TFieldPtr valuePtr = new TFieldPtr();
            valuePtr.setFieldPtr(getExternalSchema(column.getChildren().get(1)));
            mapField.setValueField(valuePtr);
            nestedField.setMapField(mapField);
            root.setNestedField(nestedField);
        }
        return root;
    }

    private static TStructField getExternalSchema(List<Column> columns) {
        TStructField structField = new TStructField();
        for (Column child : columns) {
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(getExternalSchema(child));
            structField.addToFields(fieldPtr);
        }
        return structField;
    }


    public static void initSchemaInfo(TFileScanRangeParams params, Long schemaId, List<Column> columns) {
        params.setCurrentSchemaId(schemaId);
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(schemaId);
        tSchema.setRootField(getExternalSchema(columns));
        params.addToHistorySchemaInfo(tSchema);
    }


    /**
     * Initialize schema info based on SlotDescriptors, only including columns that are actually needed.
     * For nested columns, only include sub-columns that are accessed according to pruned type.
     *
     * @param params TFileScanRangeParams to fill
     * @param schemaId Schema ID
     * @param slots List of SlotDescriptors that are actually needed
     * @param nameMapping NameMapping from Iceberg table properties (can be null and empty.)
     */
    public static void initSchemaInfo(TFileScanRangeParams params, Long schemaId,
            List<SlotDescriptor> slots, Map<Integer, List<String>> nameMapping) {
        params.setCurrentSchemaId(schemaId);
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(schemaId);
        tSchema.setRootField(getExternalSchema(slots, nameMapping));
        params.addToHistorySchemaInfo(tSchema);
    }

    private static TStructField getExternalSchema(List<SlotDescriptor> slots,
            Map<Integer, List<String>> nameMapping) {
        TStructField structField = new TStructField();
        for (SlotDescriptor slot : slots) {
            String colName = slot.getColumn().getName();
            if (colName.startsWith(Column.GLOBAL_ROWID_COL)) {
                continue;
            }

            TFieldPtr fieldPtr = new TFieldPtr();
            TField field = getExternalSchema(slot.getType(), slot.getColumn(), nameMapping);
            fieldPtr.setFieldPtr(field);
            structField.addToFields(fieldPtr);
        }
        return structField;
    }

    private static TField getExternalSchema(Type columnType, Column dorisColumn,
            Map<Integer, List<String>> nameMapping) {
        TField root = new TField();
        root.setName(dorisColumn.getName());
        root.setId(dorisColumn.getUniqueId());
        root.setIsOptional(dorisColumn.isAllowNull());
        root.setType(dorisColumn.getType().toColumnTypeThrift());

        if (nameMapping != null && nameMapping.containsKey(dorisColumn.getUniqueId())) {
            // for iceberg set name mapping.
            root.setNameMapping(new ArrayList<>(nameMapping.get(dorisColumn.getUniqueId())));
        }

        TNestedField nestedField = new TNestedField();

        if (columnType.isStructType()) {
            StructType dorisStructType = (StructType) columnType;
            TStructField structField = new TStructField();

            Map<String, Column> subNameToSubColumn = new HashMap<>();
            for (int i = 0; i < dorisColumn.getChildren().size(); i++) {
                Column subColumn = dorisColumn.getChildren().get(i);
                subNameToSubColumn.put(subColumn.getName(), subColumn);
            }

            for (StructField subField : dorisStructType.getFields()) {
                TFieldPtr fieldPtr = new TFieldPtr();
                Column subColumn = subNameToSubColumn.get(subField.getName());
                fieldPtr.setFieldPtr(getExternalSchema(subField.getType(), subColumn, nameMapping));
                structField.addToFields(fieldPtr);
            }

            nestedField.setStructField(structField);
            root.setNestedField(nestedField);
        } else if (columnType.isArrayType()) {
            ArrayType dorisArrayType = (ArrayType) columnType;

            TArrayField listField = new TArrayField();
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(getExternalSchema(
                    dorisArrayType.getItemType(), dorisColumn.getChildren().get(0), nameMapping));
            listField.setItemField(fieldPtr);
            nestedField.setArrayField(listField);
            root.setNestedField(nestedField);
        } else if (columnType.isMapType()) {
            MapType dorisMapType = (MapType) columnType;

            TMapField mapField = new TMapField();
            TFieldPtr keyPtr = new TFieldPtr();
            keyPtr.setFieldPtr(getExternalSchema(
                    dorisMapType.getKeyType(), dorisColumn.getChildren().get(0), nameMapping));

            mapField.setKeyField(keyPtr);
            TFieldPtr valuePtr = new TFieldPtr();
            valuePtr.setFieldPtr(getExternalSchema(
                    dorisMapType.getKeyType(), dorisColumn.getChildren().get(1), nameMapping));
            mapField.setValueField(valuePtr);
            nestedField.setMapField(mapField);
            root.setNestedField(nestedField);
        }
        return root;
    }
}

