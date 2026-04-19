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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsTableInfo;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Hudi catalogs.
 *
 * <p>Phase 1 provides read-only metadata operations:
 * <ul>
 *   <li>List databases and tables (via HMS)</li>
 *   <li>Get table schema from Hudi's Avro schema (via HoodieTableMetaClient)</li>
 *   <li>Detect Hudi table type (COW vs MOR)</li>
 * </ul>
 *
 * <p>Schema is derived from the Hudi MetaClient's latest Avro schema rather than
 * HMS columns, because Hudi tables with schema evolution may have a different
 * schema than what is registered in HMS.</p>
 */
public class HudiConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(HudiConnectorMetadata.class);

    private final HmsClient hmsClient;
    private final Map<String, String> properties;

    public HudiConnectorMetadata(HmsClient hmsClient, Map<String, String> properties) {
        this.hmsClient = hmsClient;
        this.properties = properties;
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return hmsClient.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            hmsClient.getDatabase(dbName);
            return true;
        } catch (HmsClientException e) {
            LOG.debug("Database '{}' not found: {}", dbName, e.getMessage());
            return false;
        }
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return hmsClient.listTables(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (!hmsClient.tableExists(dbName, tableName)) {
            return Optional.empty();
        }
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        String location = tableInfo.getLocation();
        String hudiTableType = detectHudiTableType(tableInfo);

        // Extract partition key names
        List<String> partKeyNames = Collections.emptyList();
        if (tableInfo.getPartitionKeys() != null && !tableInfo.getPartitionKeys().isEmpty()) {
            partKeyNames = tableInfo.getPartitionKeys().stream()
                    .map(ConnectorColumn::getName)
                    .collect(Collectors.toList());
        }

        return Optional.of(new HudiTableHandle.Builder(dbName, tableName, location, hudiTableType)
                .inputFormat(tableInfo.getInputFormat())
                .serdeLib(tableInfo.getSerializationLib())
                .partitionKeyNames(partKeyNames)
                .tableParameters(tableInfo.getParameters())
                .build());
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        ConnectorTableSchema schema = getTableSchema(session, handle);
        List<String> partKeyNames = hudiHandle.getPartitionKeyNames();

        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
        for (ConnectorColumn col : schema.getColumns()) {
            boolean isPartKey = partKeyNames != null
                    && partKeyNames.contains(col.getName());
            handles.put(col.getName(),
                    new HudiColumnHandle(col.getName(),
                            col.getType().getTypeName(), isPartKey));
        }
        return handles;
    }

    @Override
    public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorFilterConstraint constraint) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        List<String> partKeyNames = hudiHandle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Optional.empty();
        }

        // List all partition names from HMS (e.g. "year=2024/month=01")
        // These are relative paths that double as partition identifiers
        List<String> partitionNames = hmsClient.listPartitionNames(
                hudiHandle.getDbName(), hudiHandle.getTableName(), -1);
        if (partitionNames == null || partitionNames.isEmpty()) {
            return Optional.empty();
        }

        // Build updated handle with partition paths for scan planning
        HudiTableHandle updatedHandle = hudiHandle.toBuilder()
                .prunedPartitionPaths(partitionNames)
                .build();

        return Optional.of(new FilterApplicationResult<>(updatedHandle, constraint.getExpression(), false));
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        HudiTableHandle hudiHandle = (HudiTableHandle) handle;
        String basePath = hudiHandle.getBasePath();

        List<ConnectorColumn> columns;
        if (basePath != null && !basePath.isEmpty()) {
            columns = getSchemaFromMetaClient(basePath);
        } else {
            columns = getSchemaFromHms(hudiHandle.getDbName(), hudiHandle.getTableName());
        }

        Map<String, String> tableProperties = Collections.singletonMap(
                "hudi.table.type", hudiHandle.getHudiTableType());
        return new ConnectorTableSchema(
                hudiHandle.getTableName(), columns, "HUDI", tableProperties);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== Internal helpers ==========

    /**
     * Read schema from HoodieTableMetaClient's latest Avro schema.
     * This is the authoritative schema for Hudi tables.
     */
    private List<ConnectorColumn> getSchemaFromMetaClient(String basePath) {
        try {
            Configuration conf = buildHadoopConf();
            HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
                    .setConf(new org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(conf))
                    .setBasePath(basePath)
                    .build();
            TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
            Schema avroSchema = schemaResolver.getTableAvroSchema();
            return avroSchemaToColumns(avroSchema);
        } catch (Exception e) {
            LOG.warn("Failed to get schema from Hudi MetaClient for path '{}': {}",
                    basePath, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Fallback: read schema from HMS if MetaClient fails.
     */
    private List<ConnectorColumn> getSchemaFromHms(String dbName, String tableName) {
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        List<ConnectorColumn> columns = new ArrayList<>();
        if (tableInfo.getColumns() != null) {
            columns.addAll(tableInfo.getColumns());
        }
        if (tableInfo.getPartitionKeys() != null) {
            columns.addAll(tableInfo.getPartitionKeys());
        }
        return columns;
    }

    /**
     * Convert Avro schema fields to ConnectorColumn list.
     */
    private List<ConnectorColumn> avroSchemaToColumns(Schema avroSchema) {
        List<Schema.Field> fields = avroSchema.getFields();
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (Schema.Field field : fields) {
            boolean nullable = isNullable(field.schema());
            Schema fieldSchema = unwrapNullable(field.schema());
            ConnectorType connectorType = HudiTypeMapping.fromAvroSchema(fieldSchema);
            String comment = field.doc() != null ? field.doc() : "";
            columns.add(new ConnectorColumn(field.name(), connectorType, comment, nullable, null));
        }
        return columns;
    }

    private static boolean isNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            return schema.getTypes().stream()
                    .anyMatch(s -> s.getType() == Schema.Type.NULL);
        }
        return false;
    }

    private static Schema unwrapNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> nonNull = new ArrayList<>();
            for (Schema s : schema.getTypes()) {
                if (s.getType() != Schema.Type.NULL) {
                    nonNull.add(s);
                }
            }
            if (nonNull.size() == 1) {
                return nonNull.get(0);
            }
        }
        return schema;
    }

    /**
     * Detect whether this is a COW (Copy on Write) or MOR (Merge on Read) Hudi table.
     */
    private String detectHudiTableType(HmsTableInfo tableInfo) {
        String inputFormat = tableInfo.getInputFormat();
        if (inputFormat != null) {
            if (inputFormat.contains("HoodieParquetRealtimeInputFormat")
                    || inputFormat.contains("realtime")) {
                return "MERGE_ON_READ";
            }
            if (inputFormat.contains("HoodieParquetInputFormat")
                    || inputFormat.contains("hoodie")) {
                return "COPY_ON_WRITE";
            }
        }
        Map<String, String> params = tableInfo.getParameters();
        if (params != null) {
            String sparkProvider = params.get("spark.sql.sources.provider");
            if ("hudi".equalsIgnoreCase(sparkProvider)) {
                return "COPY_ON_WRITE";
            }
        }
        return "UNKNOWN";
    }

    private Configuration buildHadoopConf() {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("hadoop.") || key.startsWith("fs.")
                    || key.startsWith("dfs.") || key.startsWith("hive.")) {
                conf.set(key, entry.getValue());
            }
        }
        return conf;
    }
}
