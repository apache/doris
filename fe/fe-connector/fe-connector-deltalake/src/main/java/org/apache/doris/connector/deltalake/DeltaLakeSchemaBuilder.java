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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.thrift.TColumnType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.schema.external.TArrayField;
import org.apache.doris.thrift.schema.external.TField;
import org.apache.doris.thrift.schema.external.TFieldPtr;
import org.apache.doris.thrift.schema.external.TMapField;
import org.apache.doris.thrift.schema.external.TNestedField;
import org.apache.doris.thrift.schema.external.TSchema;
import org.apache.doris.thrift.schema.external.TStructField;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

final class DeltaLakeSchemaBuilder {
    static final long TABLE_SCHEMA_ID = 0;
    static final long FILE_SCHEMA_ID = 1;
    private static final String COLUMN_ID = "delta.columnMapping.id";
    private static final String PHYSICAL_NAME = "delta.columnMapping.physicalName";
    private static final String NESTED_IDS = "delta.columnMapping.nested.ids";

    private DeltaLakeSchemaBuilder() {
    }

    static ConnectorTableSchema build(String table, Snapshot snapshot, DeltaLakeCatalogProperties properties) {
        List<ConnectorColumn> columns = new ArrayList<>();
        for (StructField field : snapshot.getSchema().fields()) {
            String comment = field.getMetadata().contains("comment") ? field.getMetadata().getString("comment") : "";
            // Root names stay remote names, matching partition keys and column handles. The engine
            // owns root-name mapping; only nested field names are normalized by the type mapper.
            ConnectorColumn column = new ConnectorColumn(field.getName(),
                    DeltaLakeTypeMapping.toConnectorType(field.getDataType(),
                            properties.isMapTimestampTz(), properties.isMapVarbinary()),
                    comment, field.isNullable(), null);
            if (field.getDataType() instanceof TimestampType) {
                column = column.withTimeZone();
            }
            columns.add(column);
        }
        Map<String, String> tableProperties = new HashMap<>(snapshot.getTableProperties());
        tableProperties.put(ConnectorTableSchema.SHOW_LOCATION_KEY, snapshot.getPath());
        tableProperties.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY,
                String.join(",", snapshot.getPartitionColumnNames()));
        return new ConnectorTableSchema(table, columns, "deltalake", tableProperties);
    }

    static boolean hasColumnMapping(Snapshot snapshot) {
        return !"none".equals(snapshot.getTableProperties().getOrDefault("delta.columnMapping.mode", "none"));
    }

    static String encodeMapping(StructType schema) {
        TFileScanRangeParams carrier = new TFileScanRangeParams();
        carrier.setCurrentSchemaId(TABLE_SCHEMA_ID);
        carrier.setHistorySchemaInfo(List.of(mappingSchema(schema, false), mappingSchema(schema, true)));
        try {
            byte[] bytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(carrier);
            return Base64.getEncoder().encodeToString(bytes);
        } catch (TException e) {
            throw new DorisConnectorException("Cannot serialize Delta column mapping", e);
        }
    }

    static void applyMapping(TFileScanRangeParams params, String encoded) {
        if (encoded == null) {
            // The none mode sends no schema dictionary and uses the reader's BY_NAME path.
            return;
        }
        TFileScanRangeParams carrier = new TFileScanRangeParams();
        try {
            new TDeserializer(new TBinaryProtocol.Factory()).deserialize(carrier, Base64.getDecoder().decode(encoded));
        } catch (TException | IllegalArgumentException e) {
            throw new DorisConnectorException("Cannot deserialize Delta column mapping", e);
        }
        params.setCurrentSchemaId(carrier.getCurrentSchemaId());
        params.setHistorySchemaInfo(carrier.getHistorySchemaInfo());
    }

    static TSchema mappingSchema(StructType schema, boolean physical) {
        TSchema result = new TSchema();
        result.setSchemaId(physical ? FILE_SCHEMA_ID : TABLE_SCHEMA_ID);
        result.setRootField(mappingStruct(schema, physical));
        return result;
    }

    private static TStructField mappingStruct(StructType schema, boolean physical) {
        TStructField result = new TStructField();
        result.setFields(new ArrayList<>());
        for (StructField field : schema.fields()) {
            FieldMetadata metadata = field.getMetadata();
            String physicalName = metadata.getString(PHYSICAL_NAME);
            TField mapped = mappingField(field, physical ? physicalName : field.getName().toLowerCase(Locale.ROOT),
                    metadata.contains(NESTED_IDS) ? metadata.getMetadata(NESTED_IDS) : FieldMetadata.empty(),
                    physicalName, physical);
            mapped.setId(Math.toIntExact(metadata.getLong(COLUMN_ID)));
            result.addToFields(pointer(mapped));
        }
        return result;
    }

    private static TField mappingField(StructField field, String name, FieldMetadata nestedIds,
            String path, boolean physical) {
        TField result = new TField();
        result.setName(name);
        result.setIsOptional(field.isNullable());
        DataType type = field.getDataType();
        TColumnType columnType = new TColumnType();
        columnType.setType(TPrimitiveType.STRING);
        if (type instanceof StructType) {
            columnType.setType(TPrimitiveType.STRUCT);
            TNestedField nested = new TNestedField();
            nested.setStructField(mappingStruct((StructType) type, physical));
            result.setNestedField(nested);
        } else if (type instanceof ArrayType) {
            columnType.setType(TPrimitiveType.ARRAY);
            TArrayField array = new TArrayField();
            array.setItemField(mappingCollectionField(((ArrayType) type).getElementField(), nestedIds, path, physical));
            TNestedField nested = new TNestedField();
            nested.setArrayField(array);
            result.setNestedField(nested);
        } else if (type instanceof MapType) {
            columnType.setType(TPrimitiveType.MAP);
            MapType mapType = (MapType) type;
            TMapField map = new TMapField();
            map.setKeyField(mappingCollectionField(mapType.getKeyField(), nestedIds, path, physical));
            map.setValueField(mappingCollectionField(mapType.getValueField(), nestedIds, path, physical));
            TNestedField nested = new TNestedField();
            nested.setMapField(map);
            result.setNestedField(nested);
        } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
            result.setTimestampIsAdjustedToUtc(type instanceof TimestampType);
        }
        result.setType(columnType);
        return result;
    }

    private static TFieldPtr mappingCollectionField(StructField field, FieldMetadata nestedIds,
            String parentPath, boolean physical) {
        String path = parentPath + "." + field.getName();
        TField result = mappingField(field, field.getName(), nestedIds, path, physical);
        // Array/map element IDs are optional even in id mode. Iceberg-compatible writers store
        // them under the nearest real struct field; retain them whenever they exist.
        if (nestedIds.contains(path)) {
            result.setId(Math.toIntExact(nestedIds.getLong(path)));
        }
        return pointer(result);
    }

    private static TFieldPtr pointer(TField field) {
        TFieldPtr pointer = new TFieldPtr();
        pointer.setFieldPtr(field);
        return pointer;
    }

}
