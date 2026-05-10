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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link ConnectorMetadata} implementation for Paimon.
 *
 * <p>Phase 1 (metadata-only): supports listing databases and tables,
 * getting table handles, and reading table schema. Scan planning,
 * predicate pushdown, and DML operations remain in fe-core.
 */
public class PaimonConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(PaimonConnectorMetadata.class);

    private final Catalog catalog;
    private final PaimonTypeMapping.Options typeMappingOptions;

    public PaimonConnectorMetadata(Catalog catalog, Map<String, String> properties) {
        this.catalog = catalog;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        try {
            return catalog.listDatabases();
        } catch (Exception e) {
            LOG.warn("Failed to list Paimon databases", e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            catalog.getDatabase(dbName);
            return true;
        } catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        try {
            return catalog.listTables(dbName);
        } catch (Catalog.DatabaseNotExistException e) {
            LOG.warn("Database does not exist: {}", dbName);
            return Collections.emptyList();
        } catch (Exception e) {
            LOG.warn("Failed to list tables in database: {}", dbName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        Identifier identifier = Identifier.create(dbName, tableName);
        try {
            Table table = catalog.getTable(identifier);
            List<String> partitionKeys = table.partitionKeys();
            List<String> primaryKeys = table.primaryKeys();
            PaimonTableHandle handle = new PaimonTableHandle(
                    dbName, tableName,
                    partitionKeys != null ? partitionKeys : Collections.emptyList(),
                    primaryKeys != null ? primaryKeys : Collections.emptyList());
            handle.setPaimonTable(table);
            return Optional.of(handle);
        } catch (Catalog.TableNotExistException e) {
            return Optional.empty();
        } catch (Exception e) {
            LOG.warn("Failed to get Paimon table handle: {}.{}", dbName, tableName, e);
            return Optional.empty();
        }
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Identifier identifier = Identifier.create(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        try {
            Table table = catalog.getTable(identifier);
            RowType rowType = table.rowType();
            List<String> primaryKeys = table.primaryKeys();
            List<ConnectorColumn> columns = mapFields(rowType, primaryKeys);

            Map<String, String> schemaProps = new HashMap<>();
            if (paimonHandle.getPartitionKeys() != null
                    && !paimonHandle.getPartitionKeys().isEmpty()) {
                schemaProps.put("partition_keys",
                        String.join(",", paimonHandle.getPartitionKeys()));
            }
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                schemaProps.put("primary_keys", String.join(",", primaryKeys));
            }

            return new ConnectorTableSchema(
                    paimonHandle.getTableName(),
                    columns,
                    "PAIMON",
                    schemaProps);
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException("Paimon table not found: " + identifier, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Paimon table schema: " + identifier, e);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = paimonHandle.getPaimonTable();
        if (table == null) {
            // Fallback: re-load from catalog
            Identifier id = Identifier.create(
                    paimonHandle.getDatabaseName(), paimonHandle.getTableName());
            try {
                table = catalog.getTable(id);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load Paimon table: " + id, e);
            }
        }
        RowType rowType = table.rowType();
        List<DataField> fields = rowType.getFields();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i).name().toLowerCase();
            handles.put(name, new PaimonColumnHandle(name, i));
        }
        return handles;
    }

    private List<ConnectorColumn> mapFields(RowType rowType, List<String> primaryKeys) {
        List<DataField> fields = rowType.getFields();
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            ConnectorType connectorType = PaimonTypeMapping.toConnectorType(
                    field.type(), typeMappingOptions);
            String comment = field.description();
            boolean nullable = field.type().isNullable();
            columns.add(new ConnectorColumn(
                    field.name().toLowerCase(),
                    connectorType,
                    comment,
                    nullable,
                    null));
        }
        return columns;
    }

    private static PaimonTypeMapping.Options buildTypeMappingOptions(Map<String, String> props) {
        boolean binaryAsVarbinary = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_BINARY_AS_VARBINARY,
                        "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ,
                        "false"));
        return new PaimonTypeMapping.Options(binaryAsVarbinary, timestampTz);
    }
}
