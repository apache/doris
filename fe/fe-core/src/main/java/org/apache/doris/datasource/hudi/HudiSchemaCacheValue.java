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

package org.apache.doris.datasource.hudi;

import org.apache.doris.catalog.Column;
import org.apache.doris.datasource.hive.HMSSchemaCacheValue;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.InternalSchemaCache;
import org.apache.hudi.internal.schema.InternalSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class HudiSchemaCacheValue extends HMSSchemaCacheValue {

    public static class SchemaEntry {
        private final List<Column> schema;
        private final List<Column> partitionColumns;
        private final List<String> colTypes;
        private final boolean enableSchemaEvolution;

        public SchemaEntry(List<Column> schema, List<Column> partitionColumns, List<String> colTypes,
                boolean enableSchemaEvolution) {
            this.schema = schema;
            this.partitionColumns = partitionColumns;
            this.colTypes = colTypes;
            this.enableSchemaEvolution = enableSchemaEvolution;
        }

        public List<Column> getSchema() {
            return schema;
        }

        public List<Column> getPartitionColumns() {
            return partitionColumns;
        }

        public List<String> getColTypes() {
            return colTypes;
        }

        public boolean isEnableSchemaEvolution() {
            return enableSchemaEvolution;
        }
    }

    private final Map<Long, List<String>> colTypesByTimestamp;
    private final Map<Long, Boolean> enableSchemaEvolutionByTimestamp;
    private final Function<Long, SchemaEntry> schemaLoader;

    public HudiSchemaCacheValue(long timestamp, SchemaEntry entry, Function<Long, SchemaEntry> schemaLoader) {
        super(Collections.singletonMap(timestamp, entry.getSchema()), timestamp,
                Collections.singletonMap(timestamp, entry.getPartitionColumns()));
        this.colTypesByTimestamp = new ConcurrentHashMap<>();
        this.colTypesByTimestamp.put(timestamp, entry.getColTypes());
        this.enableSchemaEvolutionByTimestamp = new ConcurrentHashMap<>();
        this.enableSchemaEvolutionByTimestamp.put(timestamp, entry.isEnableSchemaEvolution());
        this.schemaLoader = schemaLoader;
    }

    public List<String> getColTypes() {
        return getColTypes(primaryVersionId);
    }

    public List<String> getColTypes(long timestamp) {
        ensureSchema(timestamp);
        return colTypesByTimestamp.get(timestamp);
    }

    public InternalSchema getCommitInstantInternalSchema(HoodieTableMetaClient metaClient, Long commitInstantTime) {
        return InternalSchemaCache.searchSchemaAndCache(commitInstantTime, metaClient);
    }

    public boolean isEnableSchemaEvolution() {
        return isEnableSchemaEvolution(primaryVersionId);
    }

    public boolean isEnableSchemaEvolution(long timestamp) {
        ensureSchema(timestamp);
        return enableSchemaEvolutionByTimestamp.getOrDefault(timestamp, false);
    }

    @Override
    public List<Column> getSchema(long versionId) {
        return ensureSchema(versionId).getSchema();
    }

    @Override
    public List<Column> getSchema() {
        return getSchema(primaryVersionId);
    }

    @Override
    public List<Column> getPartitionColumns(long versionId) {
        return ensureSchema(versionId).getPartitionColumns();
    }

    @Override
    public List<Column> getPartitionColumns() {
        return getPartitionColumns(primaryVersionId);
    }

    private SchemaEntry ensureSchema(long timestamp) {
        List<Column> schema = schemas.get(timestamp);
        List<Column> partitionColumns = partitionColumnsById.get(timestamp);
        if (schema != null && partitionColumns != null && colTypesByTimestamp.containsKey(timestamp)
                && enableSchemaEvolutionByTimestamp.containsKey(timestamp)) {
            primaryVersionId = timestamp;
            return new SchemaEntry(schema, partitionColumns, colTypesByTimestamp.get(timestamp),
                    enableSchemaEvolutionByTimestamp.get(timestamp));
        }
        SchemaEntry entry = schemaLoader.apply(timestamp);
        addSchema(timestamp, entry.getSchema());
        partitionColumnsById.put(timestamp, entry.getPartitionColumns());
        colTypesByTimestamp.put(timestamp, entry.getColTypes());
        enableSchemaEvolutionByTimestamp.put(timestamp, entry.isEnableSchemaEvolution());
        primaryVersionId = timestamp;
        return entry;
    }
}
