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
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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
 * <p>Depends on the {@link IcebergCatalogOps} seam rather than a raw Iceberg {@code Catalog}, so it is
 * unit-testable offline with a recording fake (no live REST/HMS/Glue/... catalog). All catalog
 * backends are transparent behind the seam — the Iceberg {@code Catalog} interface abstracts them.
 */
public class IcebergConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(IcebergConnectorMetadata.class);

    private final IcebergCatalogOps catalogOps;
    private final Map<String, String> properties;
    // Every remote metadata READ is wrapped in context.executeAuthenticated(...) so the FE-injected
    // Kerberos UGI applies — legacy IcebergMetadataOps wrapped each call in executionAuthenticator.execute,
    // and the paimon mirror (PaimonConnectorMetadata) wraps the equivalent reads. The default
    // executeAuthenticated is a pass-through, so simple-auth catalogs are unaffected.
    private final ConnectorContext context;

    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context) {
        this.catalogOps = catalogOps;
        this.properties = properties;
        this.context = context;
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        // Mirror legacy IcebergMetadataOps.listDatabaseNames: wrap in the auth context, warn + rethrow as
        // RuntimeException on failure (never swallow to an empty list — that would mask a transient
        // metastore failure as "zero databases").
        try {
            return context.executeAuthenticated(catalogOps::listDatabaseNames);
        } catch (Exception e) {
            LOG.warn("failed to list database names in catalog {}", context.getCatalogName(), e);
            throw new RuntimeException("Failed to list database names, error message is:" + e.getMessage(), e);
        }
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        // Mirror legacy IcebergMetadataOps.databaseExist: wrap in the auth context, rethrow on failure.
        try {
            return context.executeAuthenticated(() -> catalogOps.databaseExists(dbName));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check database exist, error message is:" + e.getMessage(), e);
        }
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        // Mirror legacy IcebergMetadataOps.listTableNames: wrap in the auth context; a RuntimeException
        // (e.g. NoSuchNamespaceException — iceberg's exceptions are unchecked, so UGI.doAs does NOT wrap
        // them) is rethrown verbatim, other failures are wrapped.
        try {
            return context.executeAuthenticated(() -> catalogOps.listTableNames(dbName));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list table names, error message is: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        // Mirror legacy IcebergMetadataOps.tableExist: wrap the remote existence check in the auth context
        // (the handle build below is pure — no remote call).
        boolean exists;
        try {
            exists = context.executeAuthenticated(() -> catalogOps.tableExists(dbName, tableName));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check table exist, error message is:" + e.getMessage(), e);
        }
        if (!exists) {
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

        // Mirror legacy IcebergMetadataOps.loadTable: wrap the remote load in the auth context. The schema
        // + table-property assembly below is pure (operates on the already-loaded Table).
        Table table;
        try {
            table = context.executeAuthenticated(() -> catalogOps.loadTable(dbName, tableName));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table, error message is:" + e.getMessage(), e);
        }
        Schema icebergSchema = table.schema();
        List<ConnectorColumn> columns = parseSchema(icebergSchema);

        Map<String, String> tableProps = new HashMap<>();
        tableProps.putAll(table.properties());
        tableProps.put("iceberg.format-version", String.valueOf(getFormatVersion(table)));
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
            // Legacy IcebergUtils.parseSchema parity (mirrors PaimonConnectorMetadata): the column name is
            // lowercased (Locale.ROOT), isKey is always true (external-table semantics: DESC shows Key=true),
            // and isAllowNull is always true regardless of the Iceberg required/optional flag (rows can
            // still read NULL under schema-evolution default-fill; do NOT propagate the NOT NULL constraint).
            ConnectorColumn column = new ConnectorColumn(
                    field.name().toLowerCase(Locale.ROOT),
                    IcebergTypeMapping.fromIcebergType(
                            field.type(), enableVarbinary, enableTimestampTz),
                    field.doc() != null ? field.doc() : "",
                    true,
                    null,
                    true);
            // Legacy parity: a TIMESTAMP-with-zone source field carries the WITH_TIMEZONE "Extra" marker via
            // Column.setWithTZExtraInfo(), keyed on the SOURCE iceberg type root and INDEPENDENT of the
            // enable.mapping.timestamp_tz flag. fe-core's ConnectorColumnConverter re-applies it.
            if (isTimestampWithZone(field.type())) {
                column = column.withTimeZone();
            }
            columns.add(column);
        }
        return columns;
    }

    /** A TIMESTAMP whose values are stored in UTC ({@code shouldAdjustToUTC()}); carries the WITH_TIMEZONE marker. */
    private static boolean isTimestampWithZone(Type type) {
        return type.isPrimitiveType()
                && type.typeId() == Type.TypeID.TIMESTAMP
                && ((Types.TimestampType) type).shouldAdjustToUTC();
    }

    /**
     * Reads the real table format version, mirroring legacy {@code IcebergUtils.getFormatVersion}: from a
     * {@link BaseTable}'s current metadata when available, else from the {@code format-version} table
     * property, defaulting to 2. NOT derived from the partition spec id (the old skeleton stamped
     * {@code spec().specId() >= 0 ? 2 : 1}, which is always 2 since every spec — including unpartitioned —
     * has specId >= 0).
     */
    private static int getFormatVersion(Table table) {
        int formatVersion = 2;
        if (table instanceof BaseTable) {
            formatVersion = ((BaseTable) table).operations().current().formatVersion();
        } else if (table != null && table.properties() != null) {
            String version = table.properties().get(TableProperties.FORMAT_VERSION);
            if (version != null) {
                try {
                    formatVersion = Integer.parseInt(version);
                } catch (NumberFormatException ignored) {
                    // keep the default
                }
            }
        }
        return formatVersion;
    }
}
