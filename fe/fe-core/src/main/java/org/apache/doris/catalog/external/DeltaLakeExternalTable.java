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

package org.apache.doris.catalog.external;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.HMSExternalCatalog;
import org.apache.doris.datasource.deltalake.DeltaLakeExternalCatalog;

import com.google.common.collect.Lists;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DeltaLakeExternalTable extends HMSExternalTable {
    public DeltaLakeExternalTable(long id, String name, String dbName,
            DeltaLakeExternalCatalog catalog) {
        super(id, name, dbName, catalog, TableType.DELTALAKE_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            remoteTable = ((HMSExternalCatalog) catalog).getClient().getTable(dbName, name);
            if (remoteTable == null) {
                dlaType = DLAType.UNKNOWN;
            } else {
                if (supportedDeltaLakeTable()) {
                    dlaType = DLAType.DELTALAKE;
                }  else {
                    dlaType = DLAType.UNKNOWN;
                }
            }
            objectCreated = true;
        }
    }

    private boolean supportedDeltaLakeTable() {
        Map<String, String> parameters = remoteTable.getParameters();
        if (parameters == null) {
            return false;
        }
        // Check that the 'spark.sql.sources.provider' parameter exists and has a value of 'delta'
        return "delta".equalsIgnoreCase(parameters.get("spark.sql.sources.provider"));
    }

    @Override
    public List<Column> initSchema() {
        makeSureInitialized();
        List<Column> columns;
        List<FieldSchema> schema = ((DeltaLakeExternalCatalog) catalog).getClient().getSchema(dbName, name);
        io.delta.standalone.types.StructType deltaSchema = getDeltaTableSchema(this);
        List<Column> tmpSchema = Lists.newArrayListWithCapacity(schema.size());
        for (StructField field : deltaSchema.getFields()) {
            String columnName = field.getName();
            tmpSchema.add(new Column(columnName, fromDeltaTypeToDorisType(field.getDataType()),
                    true, null, true, null, "", true, null, -1, null));
        }
        columns = tmpSchema;
        initPartitionColumns(columns);
        return columns;
    }

    private static io.delta.standalone.types.StructType getDeltaTableSchema(DeltaLakeExternalTable table) {
        String path = table.getRemoteTable().getSd().getLocation();
        Configuration conf = HiveMetaStoreClientHelper.getConfiguration(table);
        DeltaLog deltaLog = DeltaLog.forTable(conf, path);
        Metadata metadata = deltaLog.snapshot().getMetadata();
        io.delta.standalone.types.StructType tableSchema = metadata.getSchema();
        return tableSchema;
    }

    private static Type fromDeltaTypeToDorisType(DataType dataType) {
        String typeName = dataType.getTypeName();
        switch (typeName) {
            case "boolean":
                return Type.BOOLEAN;
            case "byte":
            case "tinyint":
                return Type.TINYINT;
            case "smallint":
                return Type.SMALLINT;
            case "integer":
                return Type.INT;
            case "long":
                return Type.BIGINT;
            case "float":
                return Type.FLOAT;
            case "double":
                return Type.DOUBLE;
            case "date":
                return Type.DATEV2;
            case "timestamp":
                return ScalarType.createDatetimeV2Type(6);
            case "string":
                return Type.STRING;
            case "decimal":
                int precision = ((io.delta.standalone.types.DecimalType) dataType).getPrecision();
                int scale = ((io.delta.standalone.types.DecimalType) dataType).getScale();
                return ScalarType.createDecimalV3Type(precision, scale);
            case "array":
                io.delta.standalone.types.ArrayType arrayType = (io.delta.standalone.types.ArrayType) dataType;
                Type innerType = fromDeltaTypeToDorisType(arrayType.getElementType());
                return ArrayType.create(innerType, true);
            case "map":
                io.delta.standalone.types.MapType mapType = (io.delta.standalone.types.MapType) dataType;
                return new MapType(Type.STRING, fromDeltaTypeToDorisType(mapType.getValueType()));
            case "struct":
                io.delta.standalone.types.StructType deltaStructType = (io.delta.standalone.types.StructType) dataType;
                ArrayList<org.apache.doris.catalog.StructField> dorisFields = new ArrayList<>();
                for (io.delta.standalone.types.StructField deltaField : deltaStructType.getFields()) {
                    // Convert the Delta field type to a Doris type
                    Type dorisFieldType = fromDeltaTypeToDorisType(deltaField.getDataType());

                    // Create a Doris struct field with the same name and type
                    org.apache.doris.catalog.StructField dorisField =  new org.apache.doris.catalog.StructField(
                            deltaField.getName(), dorisFieldType);

                    // Add the Doris field to the list
                    dorisFields.add(dorisField);
                }
                // Create a Doris struct type with the converted fields
                return new StructType(dorisFields);
            case "null":
                return Type.NULL;
            case "binary":
            default:
                return Type.UNSUPPORTED;
        }
    }
}
