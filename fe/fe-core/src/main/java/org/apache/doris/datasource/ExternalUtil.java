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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExternalUtil {
    private static TField getExternalSchema(Column column) {
        TField root = new TField();
        root.setName(column.getName());
        root.setId(column.getUniqueId());
        root.setIsOptional(column.isAllowNull());
        root.setType(column.getType().toColumnTypeThrift());
        if (column.getDefaultValue() != null) {
            root.setInitialDefaultValue(column.getDefaultValue());
        }

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
    public static void initSchemaInfoForPrunedColumn(TFileScanRangeParams params, Long schemaId,
            List<SlotDescriptor> slots, Map<Integer, List<String>> nameMapping) {
        params.setCurrentSchemaId(schemaId);
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(schemaId);
        tSchema.setRootField(getExternalSchemaForPrunedColumn(slots, nameMapping));
        params.addToHistorySchemaInfo(tSchema);
    }

    private static TStructField getExternalSchemaForPrunedColumn(List<SlotDescriptor> slots,
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

    public static void initSchemaInfoForAllColumn(TFileScanRangeParams params, Long schemaId,
            List<Column> columns, Map<Integer, List<String>> nameMapping) {
        initSchemaInfoForAllColumn(params, schemaId, columns, nameMapping,
                nameMapping != null && !nameMapping.isEmpty(), Collections.emptyMap());
    }

    public static void initSchemaInfoForAllColumn(TFileScanRangeParams params, Long schemaId,
            List<Column> columns, Map<Integer, List<String>> nameMapping,
            Map<Integer, String> base64InitialDefaults) {
        initSchemaInfoForAllColumn(params, schemaId, columns, nameMapping,
                nameMapping != null && !nameMapping.isEmpty(), base64InitialDefaults);
    }

    public static void initSchemaInfoForAllColumn(TFileScanRangeParams params, Long schemaId,
            List<Column> columns, Map<Integer, List<String>> nameMapping, boolean hasNameMapping,
            Map<Integer, String> base64InitialDefaults) {
        initSchemaInfoForAllColumn(params, schemaId, columns, nameMapping, hasNameMapping,
                base64InitialDefaults, base64InitialDefaults.keySet());
    }

    public static void initSchemaInfoForAllColumn(TFileScanRangeParams params, Long schemaId,
            List<Column> columns, Map<Integer, List<String>> nameMapping,
            Map<Integer, String> initialDefaults, Set<Integer> binaryLikeFieldIds) {
        initSchemaInfoForAllColumn(params, schemaId, columns, nameMapping,
                nameMapping != null && !nameMapping.isEmpty(), initialDefaults, binaryLikeFieldIds);
    }

    public static void initSchemaInfoForAllColumn(TFileScanRangeParams params, Long schemaId,
            List<Column> columns, Map<Integer, List<String>> nameMapping, boolean hasNameMapping,
            Map<Integer, String> initialDefaults, Set<Integer> binaryLikeFieldIds) {
        params.setCurrentSchemaId(schemaId);
        TSchema tSchema = new TSchema();
        tSchema.setSchemaId(schemaId);
        tSchema.setRootField(getExternalSchemaForAllColumn(
                columns, nameMapping, hasNameMapping, initialDefaults, binaryLikeFieldIds));
        params.addToHistorySchemaInfo(tSchema);
    }

    private static TStructField getExternalSchemaForAllColumn(List<Column> columns,
            Map<Integer, List<String>> nameMapping, boolean hasNameMapping,
            Map<Integer, String> initialDefaults, Set<Integer> binaryLikeFieldIds) {
        TStructField structField = new TStructField();
        for (Column child : columns) {
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(getExternalSchema(
                    child.getType(), child, nameMapping, hasNameMapping, initialDefaults,
                    binaryLikeFieldIds));
            structField.addToFields(fieldPtr);
        }
        return structField;
    }

    private static TField getExternalSchema(Type columnType, Column dorisColumn,
            Map<Integer, List<String>> nameMapping) {
        return getExternalSchema(columnType, dorisColumn, nameMapping,
                nameMapping != null && !nameMapping.isEmpty(), Collections.emptyMap());
    }

    private static TField getExternalSchema(Type columnType, Column dorisColumn,
            Map<Integer, List<String>> nameMapping, boolean hasNameMapping,
            Map<Integer, String> base64InitialDefaults) {
        return getExternalSchema(columnType, dorisColumn, nameMapping, hasNameMapping,
                base64InitialDefaults, base64InitialDefaults.keySet());
    }

    private static TField getExternalSchema(Type columnType, Column dorisColumn,
            Map<Integer, List<String>> nameMapping, boolean hasNameMapping,
            Map<Integer, String> initialDefaults, Set<Integer> binaryLikeFieldIds) {
        TField root = new TField();
        root.setName(dorisColumn.getName());
        root.setId(dorisColumn.getUniqueId());
        root.setIsOptional(dorisColumn.isAllowNull());
        root.setType(dorisColumn.getType().toColumnTypeThrift());
        if (binaryLikeFieldIds.contains(dorisColumn.getUniqueId())) {
            // For a direct primitive default this marks Base64 transport. For a binary value
            // nested in a complex default it preserves the Iceberg type identity needed to decode
            // the parent's JSON single-value representation.
            root.setInitialDefaultValueIsBase64(true);
        }
        if (initialDefaults.containsKey(dorisColumn.getUniqueId())) {
            root.setInitialDefaultValue(initialDefaults.get(dorisColumn.getUniqueId()));
        } else if (dorisColumn.getDefaultValue() != null) {
            root.setInitialDefaultValue(dorisColumn.getDefaultValue());
        }

        if (hasNameMapping) {
            // The explicit capability keeps old-FE plans on legacy fallback while making an empty
            // per-field mapping authoritative for plans produced by a compatible FE.
            root.setNameMapping(new ArrayList<>(
                    nameMapping.getOrDefault(dorisColumn.getUniqueId(), Collections.emptyList())));
            root.setNameMappingIsAuthoritative(true);
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
                fieldPtr.setFieldPtr(getExternalSchema(
                        subField.getType(), subColumn, nameMapping, hasNameMapping,
                        initialDefaults, binaryLikeFieldIds));
                structField.addToFields(fieldPtr);
            }

            nestedField.setStructField(structField);
            root.setNestedField(nestedField);
        } else if (columnType.isArrayType()) {
            ArrayType dorisArrayType = (ArrayType) columnType;

            TArrayField listField = new TArrayField();
            TFieldPtr fieldPtr = new TFieldPtr();
            fieldPtr.setFieldPtr(getExternalSchema(
                    dorisArrayType.getItemType(), dorisColumn.getChildren().get(0), nameMapping,
                    hasNameMapping, initialDefaults, binaryLikeFieldIds));
            listField.setItemField(fieldPtr);
            nestedField.setArrayField(listField);
            root.setNestedField(nestedField);
        } else if (columnType.isMapType()) {
            MapType dorisMapType = (MapType) columnType;

            TMapField mapField = new TMapField();
            TFieldPtr keyPtr = new TFieldPtr();
            keyPtr.setFieldPtr(getExternalSchema(
                    dorisMapType.getKeyType(), dorisColumn.getChildren().get(0), nameMapping,
                    hasNameMapping, initialDefaults, binaryLikeFieldIds));
            mapField.setKeyField(keyPtr);

            TFieldPtr valuePtr = new TFieldPtr();
            valuePtr.setFieldPtr(getExternalSchema(
                    dorisMapType.getValueType(), dorisColumn.getChildren().get(1), nameMapping,
                    hasNameMapping, initialDefaults, binaryLikeFieldIds));
            mapField.setValueField(valuePtr);
            nestedField.setMapField(mapField);
            root.setNestedField(nestedField);
        }
        return root;
    }
}
