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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.SchemaCacheValue;

import org.apache.paimon.schema.TableSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class PaimonSchemaCacheValue extends SchemaCacheValue {

    public static class SchemaEntry {
        private final List<Column> schema;
        private final List<Column> partitionColumns;
        private final TableSchema tableSchema;

        public SchemaEntry(List<Column> schema, List<Column> partitionColumns, TableSchema tableSchema) {
            this.schema = schema;
            this.partitionColumns = partitionColumns;
            this.tableSchema = tableSchema;
        }

        public List<Column> getSchema() {
            return schema;
        }

        public List<Column> getPartitionColumns() {
            return partitionColumns;
        }

        public TableSchema getTableSchema() {
            return tableSchema;
        }
    }

    private final Map<Long, List<Column>> partitionColumnsBySchemaId;
    private final Map<Long, TableSchema> tableSchemaById;
    private final Function<Long, SchemaEntry> schemaLoader;
    // Caching TableSchema can reduce the reading of schema files and json parsing.

    public PaimonSchemaCacheValue(long schemaId, SchemaEntry entry, Function<Long, SchemaEntry> schemaLoader) {
        super(Collections.singletonMap(schemaId, entry.getSchema()), schemaId);
        this.partitionColumnsBySchemaId = new ConcurrentHashMap<>();
        this.partitionColumnsBySchemaId.put(schemaId, entry.getPartitionColumns());
        this.tableSchemaById = new ConcurrentHashMap<>();
        this.tableSchemaById.put(schemaId, entry.getTableSchema());
        this.schemaLoader = schemaLoader;
    }

    public List<Column> getPartitionColumns() {
        return getPartitionColumns(primaryVersionId);
    }

    public List<Column> getPartitionColumns(long schemaId) {
        return ensureSchema(schemaId).getPartitionColumns();
    }

    public TableSchema getTableSchema() {
        return getTableSchema(primaryVersionId);
    }

    public TableSchema getTableSchema(long schemaId) {
        return ensureSchema(schemaId).getTableSchema();
    }

    @Override
    public List<Column> getSchema() {
        return getSchema(primaryVersionId);
    }

    @Override
    public List<Column> getSchema(long schemaId) {
        return ensureSchema(schemaId).getSchema();
    }

    private SchemaEntry ensureSchema(long schemaId) {
        List<Column> schema = schemas.get(schemaId);
        List<Column> partitions = partitionColumnsBySchemaId.get(schemaId);
        TableSchema tableSchema = tableSchemaById.get(schemaId);
        if (schema != null && partitions != null && tableSchema != null) {
            primaryVersionId = schemaId;
            return new SchemaEntry(schema, partitions, tableSchema);
        }
        SchemaEntry entry = schemaLoader.apply(schemaId);
        addSchema(schemaId, entry.getSchema());
        partitionColumnsBySchemaId.put(schemaId, entry.getPartitionColumns());
        tableSchemaById.put(schemaId, entry.getTableSchema());
        primaryVersionId = schemaId;
        return entry;
    }
}
