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

import org.apache.doris.catalog.Column;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import java.util.List;

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
}
