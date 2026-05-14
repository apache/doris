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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Iceberg catalogs.
 *
 * <p>Phase 1 provides read-only metadata operations:
 * <ul>
 *   <li>List databases (namespaces) and tables</li>
 *   <li>Get table schema from Iceberg's native Schema</li>
 *   <li>Partition spec info in table properties</li>
 * </ul>
 *
 * <p>Uses the Iceberg SDK Catalog API directly. All catalog backends (REST, HMS,
 * Glue, etc.) are transparent — the Iceberg Catalog interface abstracts them.</p>
 */
public class IcebergConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergConnectorMetadata.class);

    private final Catalog catalog;
    private final Map<String, String> properties;

    public IcebergConnectorMetadata(Catalog catalog, Map<String, String> properties) {
        this.catalog = catalog;
        this.properties = properties;
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        if (!(catalog instanceof SupportsNamespaces)) {
            LOG.warn("Iceberg catalog does not support namespaces");
            return Collections.emptyList();
        }
        SupportsNamespaces nsCatalog = (SupportsNamespaces) catalog;
        return nsCatalog.listNamespaces(Namespace.empty()).stream()
                .map(ns -> ns.level(ns.length() - 1))
                .collect(Collectors.toList());
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        if (!(catalog instanceof SupportsNamespaces)) {
            return false;
        }
        return ((SupportsNamespaces) catalog).namespaceExists(Namespace.of(dbName));
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        Namespace ns = Namespace.of(dbName);
        return catalog.listTables(ns).stream()
                .map(TableIdentifier::name)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        TableIdentifier tableId = TableIdentifier.of(dbName, tableName);
        if (!catalog.tableExists(tableId)) {
            return Optional.empty();
        }
        return Optional.of(new IcebergTableHandle(dbName, tableName));
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        String dbName = iceHandle.getDbName();
        String tableName = iceHandle.getTableName();

        Table table = catalog.loadTable(TableIdentifier.of(dbName, tableName));
        Schema icebergSchema = table.schema();
        List<ConnectorColumn> columns = parseSchema(icebergSchema);

        Map<String, String> tableProps = new HashMap<>();
        tableProps.putAll(table.properties());
        tableProps.put("iceberg.format-version",
                String.valueOf(table.spec().specId() >= 0 ? 2 : 1));
        if (table.location() != null) {
            tableProps.put("location", table.location());
        }
        if (!table.spec().isUnpartitioned()) {
            tableProps.put("iceberg.partition-spec", table.spec().toString());
        }

        return new ConnectorTableSchema(tableName, columns, "ICEBERG", tableProps);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== Internal helpers ==========

    /**
     * Convert an Iceberg Schema to a list of ConnectorColumn.
     */
    private List<ConnectorColumn> parseSchema(Schema schema) {
        List<Types.NestedField> fields = schema.columns();
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        boolean enableVarbinary = Boolean.parseBoolean(
                properties.getOrDefault(
                        IcebergConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
        boolean enableTimestampTz = Boolean.parseBoolean(
                properties.getOrDefault(
                        IcebergConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));

        for (Types.NestedField field : fields) {
            columns.add(new ConnectorColumn(
                    field.name(),
                    IcebergTypeMapping.fromIcebergType(
                            field.type(), enableVarbinary, enableTimestampTz),
                    field.doc() != null ? field.doc() : "",
                    field.isOptional(),
                    null));
        }
        return columns;
    }
}
