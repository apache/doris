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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.hudi.HudiHMSExternalCatalog;
import org.apache.doris.thrift.THudiTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HudiExternalTable extends ExternalTable {

    public HudiExternalTable(long id, String name, String dbName, HudiHMSExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.HUDI_EXTERNAL_TABLE);
    }

    public String getHudiCatalogType() {
        return ((HudiHMSExternalCatalog) catalog).getHudiCatalogType();
    }

    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        List<Column> schema = getFullSchema();
        String hudiCatalogType = getHudiCatalogType();
        if (hudiCatalogType.equals("hms")) {
            THudiTable hudiTable = new THudiTable();
            hudiTable.setDbName(dbName);
            hudiTable.setTableName(dbName);

            TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.HIVE_TABLE, schema.size(), 0,
                    getName(), dbName);
            tTableDescriptor.setHudiTable(hudiTable);
            return tTableDescriptor;
        }
        throw new UnsupportedOperationException("hudi not supported  the catalog type :" + hudiCatalogType);
    }

    public static String fromAvroHudiTypeToHiveTypeString(Schema avroSchema) {
        Schema.Type columnType = avroSchema.getType();
        switch (columnType) {
            case BOOLEAN:
                return Type.BOOLEAN.toString();
            case INT:
                return Type.INT.toString();
            case LONG:
                return Type.BIGINT.toString();
            case FLOAT:
                return Type.FLOAT.toString();
            case DOUBLE:
                return Type.DOUBLE.toString();
            case STRING:
                return Type.STRING.toString();
            case ARRAY:
                String elementType = fromAvroHudiTypeToHiveTypeString(avroSchema.getElementType());
                return String.format("array<%s>", elementType);
            case RECORD:
                List<Schema.Field> fields = avroSchema.getFields();
                Preconditions.checkArgument(fields.size() > 0);
                String nameToType = fields.stream()
                        .map(f -> String.format("%s:%s", f.name(),
                                fromAvroHudiTypeToHiveTypeString(f.schema())))
                        .collect(Collectors.joining(","));
                return String.format("struct<%s>", nameToType);
            case MAP:
                Schema value = avroSchema.getValueType();
                String valueType = fromAvroHudiTypeToHiveTypeString(value);
                return String.format("map<%s,%s>", "string", valueType);
            default:
                String errorMsg = String.format("Unsupported hudi %s type of column %s", avroSchema.getType().getName(),
                        avroSchema.getName());
                throw new IllegalArgumentException(errorMsg);
        }
    }

    public static List<String> getPartitionColumnNames(Table table, HoodieTableConfig tableConfig) {
        List<String> partitionColumnNames = Lists.newArrayList();
        Option<String[]> hudiPartitionFields = tableConfig.getPartitionFields();

        if (hudiPartitionFields.isPresent()) {
            partitionColumnNames.addAll(Arrays.asList(hudiPartitionFields.get()));
        } else if (!table.getPartitionKeys().isEmpty()) {
            for (FieldSchema fieldSchema : table.getPartitionKeys()) {
                String partField = fieldSchema.getName();
                partitionColumnNames.add(partField);
            }
        }
        return partitionColumnNames;
    }
}
