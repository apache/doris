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
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.DateTimeUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link ConnectorMetadata} implementation for Paimon.
 *
 * <p>Phase 1 (metadata-only): supports listing databases and tables,
 * getting table handles, and reading table schema. Scan planning,
 * predicate pushdown, and DML operations remain in fe-core.
 */
public class PaimonConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(PaimonConnectorMetadata.class);

    private final PaimonCatalogOps catalogOps;
    private final PaimonTypeMapping.Options typeMappingOptions;
    private final ConnectorContext context;
    // The connector's own injected catalog property map. Retained to resolve the catalog flavor
    // for the HMS-only-props gate in createDatabase. This is the same data as
    // session.getCatalogProperties() (the FE injects both from one source), but using the
    // directly-injected map avoids depending on the session being populated and is simpler.
    private final Map<String, String> catalogProperties;

    public PaimonConnectorMetadata(PaimonCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context) {
        this.catalogOps = catalogOps;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
        this.context = context;
        this.catalogProperties = properties;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        try {
            return catalogOps.listDatabases();
        } catch (Exception e) {
            LOG.warn("Failed to list Paimon databases", e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            catalogOps.getDatabase(dbName);
            return true;
        } catch (Catalog.DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        try {
            return catalogOps.listTables(dbName);
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
            Table table = catalogOps.getTable(identifier);
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
            Table table = catalogOps.getTable(identifier);
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

    // ==================== DDL: Create/Drop Table ====================

    /**
     * Creates a Paimon table from the full {@link ConnectorCreateTableRequest}.
     *
     * <p>fe-core already pre-probes existence (via {@code getTableHandle}) and short-circuits the
     * {@code IF NOT EXISTS} case, so this body has no redundant existence check — it mirrors the
     * legacy {@code PaimonMetadataOps.performCreateTable}, which simply delegated to
     * {@code catalog.createTable(id, schema, ignoreIfExists)}. Passing
     * {@link ConnectorCreateTableRequest#isIfNotExists()} as paimon's {@code ignoreIfExists} keeps
     * it idempotent: paimon no-ops when {@code ifNotExists && exists}, and throws
     * {@code TableAlreadyExistException} (wrapped here as {@link DorisConnectorException}) when
     * {@code !ifNotExists && exists}.
     *
     * <p>Per D7=B (legacy parity) the remote call is wrapped in
     * {@link ConnectorContext#executeAuthenticated} so the FE-injected auth context (e.g. Kerberos
     * UGI) applies, exactly as legacy {@code PaimonMetadataOps} wrapped every remote DDL call.
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorCreateTableRequest request) {
        Identifier id = Identifier.create(request.getDbName(), request.getTableName());
        Schema schema = PaimonSchemaBuilder.build(request);
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createTable(id, schema, request.isIfNotExists());
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to create Paimon table " + id + ": " + e.getMessage(), e);
        }
        LOG.info("created Paimon table {}", id);
    }

    /**
     * Drops the Paimon table behind {@code handle}.
     *
     * <p>The SPI {@code dropTable} carries no {@code ifExists} flag and is handle-based: fe-core
     * pre-resolves the handle (absent => this is never reached), so the remote drop is issued
     * idempotently with {@code ignoreIfNotExists = true}, mirroring
     * {@code MaxComputeConnectorMetadata.dropTable}. The remote call is wrapped in
     * {@link ConnectorContext#executeAuthenticated} (D7=B legacy parity).
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        Identifier id = Identifier.create(h.getDatabaseName(), h.getTableName());
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropTable(id, true);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to drop Paimon table " + id + ": " + e.getMessage(), e);
        }
        LOG.info("dropped Paimon table {}", id);
    }

    // ==================== DDL: Create/Drop Database ====================

    @Override
    public boolean supportsCreateDatabase() {
        return true;
    }

    /**
     * Creates a Paimon database.
     *
     * <p>fe-core already does the {@code IF NOT EXISTS} short-circuit before reaching here: since
     * {@link #supportsCreateDatabase()} is true, {@code PluginDrivenExternalCatalog.createDb}
     * consults BOTH the FE db-name cache AND the remote {@code databaseExists} and no-ops when the
     * db already exists, so this body passes {@code ignoreIfExists = false} to the seam (mirrors
     * {@code MaxComputeConnectorMetadata.createDatabase}). If the db somehow exists, paimon throws
     * {@code DatabaseAlreadyExistException}, wrapped here as {@link DorisConnectorException}.
     *
     * <p>The HMS-only-props gate is a pure local arg check (no remote call), so it runs BEFORE the
     * authenticator — mirroring legacy {@code PaimonMetadataOps.performCreateDb}, which rejected
     * non-empty properties for every catalog type except HMS. The remote create then runs inside
     * {@link ConnectorContext#executeAuthenticated} (D7=B legacy parity).
     */
    @Override
    public void createDatabase(ConnectorSession session, String dbName,
            Map<String, String> properties) {
        String flavor = PaimonCatalogFactory.resolveFlavor(catalogProperties);
        if (!properties.isEmpty() && !PaimonConnectorProperties.HMS.equals(flavor)) {
            throw new DorisConnectorException(
                    "Not supported: create database with properties for paimon catalog type: " + flavor);
        }
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createDatabase(dbName, /*ignoreIfExists*/ false, properties);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to create Paimon database " + dbName + ": " + e.getMessage(), e);
        }
        LOG.info("created Paimon database {}", dbName);
    }

    /**
     * Drops a Paimon database, cascading to its tables when {@code force} is true.
     *
     * <p>Mirrors legacy {@code PaimonMetadataOps.performDropDb}: when {@code force}, it enumerates
     * the db's tables and drops each (idempotently) BEFORE dropping the db, AND passes {@code force}
     * as paimon's native cascade flag — belt-and-suspenders, exactly like legacy (NOT enumerate-only
     * like MaxCompute, whose ODPS schema delete does not cascade). When {@code !force} and the db is
     * non-empty, paimon's {@code dropDatabase(dbName, ifExists, cascade=false)} throws
     * {@code DatabaseNotEmptyException}, wrapped here as {@link DorisConnectorException}.
     *
     * <p>The whole op (enumerate + per-table drops + db drop) is a single logical DDL op, so it runs
     * under ONE {@link ConnectorContext#executeAuthenticated} scope (D7=B legacy parity). fe-core
     * already short-circuits the {@code IF EXISTS} no-op when the db is absent from its cache.
     */
    @Override
    public void dropDatabase(ConnectorSession session, String dbName,
            boolean ifExists, boolean force) {
        try {
            context.executeAuthenticated(() -> {
                if (force) {
                    for (String table : catalogOps.listTables(dbName)) {
                        catalogOps.dropTable(Identifier.create(dbName, table), /*ignoreIfNotExists*/ true);
                    }
                }
                catalogOps.dropDatabase(dbName, ifExists, /*cascade*/ force);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to drop Paimon database " + dbName + ": " + e.getMessage(), e);
        }
        LOG.info("dropped Paimon database {} (force={})", dbName, force);
    }

    /**
     * Disables pushing predicates that contain implicit CAST expressions down to Paimon.
     *
     * <p>The shared {@code ExprToConnectorExpressionConverter} unwraps CAST shells, so without this
     * a predicate like {@code CAST(str_col AS INT) = 5} would be pushed to the Paimon read as the
     * source-side filter {@code str_col = "5"}, which Paimon evaluates as exact equality and uses
     * for file/partition pruning — dropping rows like {@code "05"}/{@code " 5"} <b>at the source</b>,
     * which BE re-evaluation can never recover. Returning {@code false} makes
     * {@code PluginDrivenScanNode.buildRemainingFilter} keep CAST-bearing conjuncts BE-only.
     * Mirrors {@code MaxComputeConnectorMetadata} / {@code JdbcConnectorMetadata}.
     */
    @Override
    public boolean supportsCastPredicatePushdown(ConnectorSession session) {
        return false;
    }

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table = resolveTable(paimonHandle);
        RowType rowType = table.rowType();
        List<DataField> fields = rowType.getFields();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i).name().toLowerCase();
            handles.put(name, new PaimonColumnHandle(name, i));
        }
        return handles;
    }

    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        List<ConnectorPartitionInfo> partitions = collectPartitions((PaimonTableHandle) handle);
        List<String> names = new ArrayList<>(partitions.size());
        for (ConnectorPartitionInfo partition : partitions) {
            names.add(partition.getPartitionName());
        }
        return names;
    }

    /**
     * Lists all partitions with metadata. The {@code filter} is intentionally ignored: legacy
     * {@code PaimonExternalCatalog.getPaimonPartitions} returns the full partition set without
     * pushing predicates into the Paimon catalog, and this preserves that behavior (mirrors
     * {@code MaxComputeConnectorMetadata}).
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        return collectPartitions((PaimonTableHandle) handle);
    }

    @Override
    public List<List<String>> listPartitionValues(ConnectorSession session,
            ConnectorTableHandle handle, List<String> partitionColumns) {
        List<ConnectorPartitionInfo> partitions = collectPartitions((PaimonTableHandle) handle);
        List<List<String>> result = new ArrayList<>(partitions.size());
        for (ConnectorPartitionInfo partition : partitions) {
            Map<String, String> rawValues = partition.getPartitionValues();
            // Preserve the requested partitionColumns order (NOT Paimon's native spec order):
            // this feeds the partition_values() TVF whose inner-list order must match the input.
            List<String> values = new ArrayList<>(partitionColumns.size());
            for (String column : partitionColumns) {
                values.add(rawValues.get(column));
            }
            result.add(values);
        }
        return result;
    }

    /**
     * Shared partition collector backing {@link #listPartitionNames}, {@link #listPartitions} and
     * {@link #listPartitionValues}. Replicates the legacy fe-core display-name logic
     * ({@code PaimonUtil.generatePartitionInfo} + {@code isLegacyPartitionName}) so the rendered
     * partition names stay byte-identical to the pre-migration behavior.
     */
    private List<ConnectorPartitionInfo> collectPartitions(PaimonTableHandle paimonHandle) {
        List<String> partitionKeys = paimonHandle.getPartitionKeys();
        // Legacy never lists partitions for unpartitioned tables: PaimonPartitionInfoLoader.load
        // returns EMPTY when partitionColumns is empty, so guard before touching the seam.
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptyList();
        }

        Table table = resolveTable(paimonHandle);
        Identifier identifier = Identifier.create(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        List<Partition> paimonPartitions;
        try {
            paimonPartitions = catalogOps.listPartitions(identifier);
        } catch (Catalog.TableNotExistException e) {
            // Legacy getPaimonPartitions swallows TableNotExistException and returns empty.
            LOG.warn("Paimon table not found while listing partitions: {}", identifier, e);
            return Collections.emptyList();
        }

        boolean legacyName = Boolean.parseBoolean(
                table.options().getOrDefault("partition.legacy-name", "true"));

        // Connector cannot import Doris Type: detect DATE partition columns straight from the
        // Paimon RowType (DataTypeRoot.DATE) instead of the legacy columnNameToType.isDateV2().
        Set<String> partitionKeyNames = new HashSet<>(partitionKeys);
        Set<String> dateColumns = new HashSet<>();
        for (DataField field : table.rowType().getFields()) {
            if (partitionKeyNames.contains(field.name())
                    && field.type().getTypeRoot() == DataTypeRoot.DATE) {
                dateColumns.add(field.name());
            }
        }

        List<ConnectorPartitionInfo> result = new ArrayList<>(paimonPartitions.size());
        for (Partition partition : paimonPartitions) {
            Map<String, String> spec = partition.spec();
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : spec.entrySet()) {
                sb.append(entry.getKey()).append("=");
                // When partition.legacy-name = true (default), Paimon stores DATE as days since
                // 1970-01-01 (epoch integer), so render it via the Paimon SDK formatDate; when
                // false the value is already a human-readable date string.
                if (legacyName && dateColumns.contains(entry.getKey())) {
                    sb.append(DateTimeUtils.formatDate(Integer.parseInt(entry.getValue()))).append("/");
                } else {
                    sb.append(entry.getValue()).append("/");
                }
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            String partitionName = sb.toString();
            // partitionValues = RAW spec (un-rendered): downstream indexes by raw remote keys.
            result.add(new ConnectorPartitionInfo(
                    partitionName,
                    spec,
                    Collections.emptyMap(),
                    partition.recordCount(),
                    partition.fileSizeInBytes(),
                    partition.lastFileCreationTime()));
        }
        return result;
    }

    /**
     * Resolves the live {@link Table} for a handle: prefer the transient reference, else re-load
     * from the catalog seam. Mirrors the reload fallback originally inlined in
     * {@link #getColumnHandles}.
     */
    private Table resolveTable(PaimonTableHandle paimonHandle) {
        Table table = paimonHandle.getPaimonTable();
        if (table == null) {
            // Fallback: re-load from catalog
            Identifier id = Identifier.create(
                    paimonHandle.getDatabaseName(), paimonHandle.getTableName());
            try {
                table = catalogOps.getTable(id);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load Paimon table: " + id, e);
            }
        }
        return table;
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
