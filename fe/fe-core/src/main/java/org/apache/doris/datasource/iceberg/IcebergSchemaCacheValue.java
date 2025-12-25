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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.SchemaCacheValue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class IcebergSchemaCacheValue extends SchemaCacheValue {

    public static class SchemaEntry {
        private final List<Column> schema;
        private final List<Column> partitionColumns;

        public SchemaEntry(List<Column> schema, List<Column> partitionColumns) {
            this.schema = schema;
            this.partitionColumns = partitionColumns;
        }

        public List<Column> getSchema() {
            return schema;
        }

        public List<Column> getPartitionColumns() {
            return partitionColumns;
        }
    }

    private final Map<Long, List<Column>> partitionColumnsBySchemaId;
    private final Function<Long, SchemaEntry> schemaLoader;

    public IcebergSchemaCacheValue(long schemaId, SchemaEntry entry, Function<Long, SchemaEntry> schemaLoader) {
        super(Collections.singletonMap(schemaId, entry.getSchema()), schemaId);
        this.partitionColumnsBySchemaId = new ConcurrentHashMap<>();
        this.partitionColumnsBySchemaId.put(schemaId, entry.getPartitionColumns());
        this.schemaLoader = schemaLoader;
    }

    public SchemaEntry ensureSchema(long schemaId) {
        List<Column> schema = schemas.get(schemaId);
        List<Column> partitions = partitionColumnsBySchemaId.get(schemaId);
        if (schema != null && partitions != null) {
            primaryVersionId = schemaId;
            return new SchemaEntry(schema, partitions);
        }
        SchemaEntry loaded = schemaLoader.apply(schemaId);
        addSchema(schemaId, loaded.getSchema());
        partitionColumnsBySchemaId.put(schemaId, loaded.getPartitionColumns());
        primaryVersionId = schemaId;
        return loaded;
    }

    @Override
    public List<Column> getSchema() {
        return getSchema(primaryVersionId);
    }

    @Override
    public List<Column> getSchema(long versionId) {
        return ensureSchema(versionId).getSchema();
    }

    public List<Column> getPartitionColumns() {
        return getPartitionColumns(primaryVersionId);
    }

    public List<Column> getPartitionColumns(long schemaId) {
        return ensureSchema(schemaId).getPartitionColumns();
    }
}
