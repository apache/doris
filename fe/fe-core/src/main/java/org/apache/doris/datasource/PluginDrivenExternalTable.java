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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorCapability;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.systable.PluginDrivenSysTable;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Generic {@link ExternalTable} for plugin-driven catalogs.
 *
 * <p>Provides table implementation that fetches schema from the connector SPI.
 * Connector-specific behavior is accessed through the parent catalog's
 * {@link org.apache.doris.connector.api.Connector} using opaque handles.</p>
 */
public class PluginDrivenExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(PluginDrivenExternalTable.class);

    /**
     * Whether this table is actually a view, resolved once from the connector in
     * {@link #makeSureInitialized()} (gated on {@link #supportsView()}) and recomputed after GSON replay
     * (the {@code objectCreated} reset). Mirrors legacy {@code IcebergExternalTable.isView}; derived
     * metadata, not persisted.
     */
    private boolean isView;

    /** No-arg constructor for GSON deserialization. */
    public PluginDrivenExternalTable() {
    }

    public PluginDrivenExternalTable(long id, String name, String remoteName,
            ExternalCatalog catalog, ExternalDatabase db) {
        super(id, name, remoteName, catalog, db, TableType.PLUGIN_EXTERNAL_TABLE);
    }

    /**
     * Single seam for acquiring this table's {@link ConnectorTableHandle}. The base class resolves
     * the handle for its own remote name; {@link PluginDrivenSysExternalTable} overrides this to
     * thread a system-table handle through {@code initSchema}/{@code getNameToPartitionItems}/
     * {@code fetchRowCount} without duplicating the metadata round-trip in each site.
     */
    protected Optional<ConnectorTableHandle> resolveConnectorTableHandle(
            ConnectorSession session, ConnectorMetadata metadata) {
        String dbName = db != null ? db.getRemoteName() : "";
        return metadata.getTableHandle(session, dbName, getRemoteName());
    }

    /**
     * Returns whether the underlying connector supports multiple concurrent writers.
     * Used by the planner to decide GATHER (single writer) vs parallel distribution.
     */
    public boolean supportsParallelWrite() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_PARALLEL_WRITE);
    }

    /**
     * Returns whether the underlying connector's tables support background per-column auto-analyze.
     * The statistics auto-collector consults this (in place of the legacy {@code instanceof
     * IcebergExternalTable} whitelist) to admit a flipped plugin table into the auto-analyze framework
     * and to force FULL analyze (sample analyze is unimplemented for external SQL-driven tables).
     */
    public boolean supportsColumnAutoAnalyze() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE);
    }

    /**
     * Returns whether the underlying connector's file-scan tables support Top-N lazy materialization.
     * The nereids Top-N lazy-materialize probe consults this (in place of the legacy exact-class
     * {@code SUPPORT_RELATION_TYPES} membership) to enable lazy materialization for a flipped plugin table.
     */
    public boolean supportsTopNLazyMaterialize() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE);
    }

    /**
     * Returns whether the underlying connector's table properties are user-facing and safe to render in
     * SHOW CREATE TABLE. The SHOW CREATE TABLE plugin-driven arm renders LOCATION + PROPERTIES (+ the
     * pre-rendered PARTITION BY / ORDER BY clauses) only when this is true (in place of the legacy
     * paimon-only engine-name gate, which doubled as the JDBC/ES credential-leak guard).
     */
    public boolean supportsShowCreateDdl() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_SHOW_CREATE_DDL);
    }

    /**
     * Returns whether the underlying connector exposes views (declares {@code SUPPORTS_VIEW}). When true,
     * {@link #isView()} resolves this table's view-ness from the connector ({@code viewExists}) and the
     * catalog merges the connector's {@code listViewNames} back into {@code SHOW TABLES}. View-less
     * connectors (jdbc/es) return false and keep every object a non-view. Mirror of the other capability
     * helpers.
     */
    public boolean supportsView() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_VIEW);
    }

    /**
     * Returns whether the underlying connector requires dynamic-partition writes to be
     * hash-distributed by partition columns and locally sorted by them (e.g. MaxCompute Storage
     * API). Used by {@code PhysicalConnectorTableSink} to require that distribution + sort for
     * dynamic-partition writes; defaults to false so non-partitioned connectors are unaffected.
     */
    public boolean requirePartitionLocalSortOnWrite() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SINK_REQUIRE_PARTITION_LOCAL_SORT);
    }

    /**
     * Returns whether the underlying connector maps write data columns positionally against the full
     * table schema (e.g. MaxCompute), requiring the sink to project rows to full-schema order with
     * unmentioned columns filled. Name-mapped connectors (e.g. JDBC) return false and keep their data
     * in user/cols order. Used by {@code BindSink.bindConnectorTableSink}; defaults to false.
     */
    public boolean requiresFullSchemaWriteOrder() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SINK_REQUIRE_FULL_SCHEMA_ORDER);
    }

    @Override
    public boolean supportsExternalMetadataPreload() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        // Keep plugin-driven preload limited to JDBC until other connector types are validated.
        return "jdbc".equalsIgnoreCase(((PluginDrivenExternalCatalog) catalog).getType());
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        // Keep the JDBC schema delay debug point available for manual regression verification.
        if ("jdbc".equalsIgnoreCase(pluginCatalog.getType())
                && DebugPointUtil.isEnable("PluginDrivenExternalTable.initSchema.sleep")) {
            long sleepMs = DebugPointUtil.getDebugParamOrDefault(
                    "PluginDrivenExternalTable.initSchema.sleep", "sleepMs", 0L);
            if (sleepMs > 0) {
                LOG.info("debug point PluginDrivenExternalTable.initSchema.sleep hit for {}.{}, sleep {}ms",
                        db != null ? db.getRemoteName() : "", getRemoteName(), sleepMs);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        String dbName = db != null ? db.getRemoteName() : "";
        String tableName = getRemoteName();
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            LOG.warn("Table handle not found for plugin-driven table: {}.{}", dbName, tableName);
            return Optional.empty();
        }

        ConnectorTableSchema tableSchema = metadata.getTableSchema(session, handleOpt.get());
        return Optional.of(toSchemaCacheValue(metadata, session, dbName, tableName, tableSchema));
    }

    /**
     * Converts a connector {@link ConnectorTableSchema} into a {@link PluginDrivenSchemaCacheValue}:
     * applies identifier mapping to the column names and derives the partition-column views from the
     * {@code partition_columns} property. Shared by {@link #initSchema()} (latest schema) and the
     * MVCC subclass (schema AS OF a pinned snapshot), so both produce byte-identical cache values.
     */
    protected PluginDrivenSchemaCacheValue toSchemaCacheValue(ConnectorMetadata metadata,
            ConnectorSession session, String dbName, String tableName, ConnectorTableSchema tableSchema) {
        // Apply identifier mapping to column names (lowercase / explicit mapping)
        List<ConnectorColumn> mappedColumns = new ArrayList<>(tableSchema.getColumns().size());
        for (ConnectorColumn col : tableSchema.getColumns()) {
            String mappedName = metadata.fromRemoteColumnName(session, dbName, tableName, col.getName());
            if (!mappedName.equals(col.getName())) {
                ConnectorColumn remapped = new ConnectorColumn(mappedName, col.getType(),
                        col.getComment(), col.isNullable(), col.getDefaultValue(), col.isKey());
                // Preserve the WITH_TIMEZONE marker across the name remap (the 6-arg ctor defaults it off)
                // so DESC still shows the Extra marker for renamed/explicitly-mapped TZ columns.
                if (col.isWithTimeZone()) {
                    remapped = remapped.withTimeZone();
                }
                mappedColumns.add(remapped);
            } else {
                mappedColumns.add(col);
            }
        }

        List<Column> columns = ConnectorColumnConverter.convertColumns(mappedColumns);

        // Identify partition columns from the connector's "partition_columns" property (a CSV of
        // RAW remote column names; producer: MaxComputeConnectorMetadata). We keep two aligned
        // views: the Doris Columns (with mapped/local names, used for getPartitionColumns + types)
        // and the raw remote names (used to index the raw-keyed partition-value maps from the SPI).
        // The columns themselves are already present in `columns` (the connector appends partition
        // columns to the schema, mirroring legacy); here we only mark which ones are partitions.
        List<Column> partitionColumns = new ArrayList<>();
        List<String> partitionColumnRemoteNames = new ArrayList<>();
        String partColsProp = tableSchema.getProperties().get("partition_columns");
        if (partColsProp != null && !partColsProp.isEmpty()) {
            Map<String, Column> byName = Maps.newHashMapWithExpectedSize(columns.size());
            for (Column c : columns) {
                byName.putIfAbsent(c.getName(), c);
            }
            for (String rawName : partColsProp.split(",")) {
                rawName = rawName.trim();
                if (rawName.isEmpty()) {
                    continue;
                }
                String mappedName = metadata.fromRemoteColumnName(session, dbName, tableName, rawName);
                Column col = byName.get(mappedName);
                if (col != null) {
                    partitionColumns.add(col);
                    partitionColumnRemoteNames.add(rawName);
                }
            }
        }
        return new PluginDrivenSchemaCacheValue(columns, partitionColumns, partitionColumnRemoteNames,
                tableSchema.getProperties());
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            objectCreated = true;
            isView = resolveIsView();
        }
    }

    @Override
    public boolean isView() {
        makeSureInitialized();
        return isView;
    }

    /**
     * Resolves whether this table is a view by consulting the connector ({@code viewExists}), mirroring
     * legacy {@code IcebergExternalTable.makeSureInitialized -> catalog.viewExists}. Gated on
     * {@link #supportsView()} so view-less connectors (jdbc/es/paimon/maxcompute) issue no remote call and
     * stay {@code isView()==false}. The system-table subclass overrides this to a constant {@code false}
     * (metadata tables like {@code $snapshots} are never views, and a {@code viewExists} on their synthetic
     * name would be a wasted — possibly failing — round-trip).
     */
    protected boolean resolveIsView() {
        if (!supportsView()) {
            return false;
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return false;
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        String dbName = db != null ? db.getRemoteName() : "";
        return metadata.viewExists(session, dbName, getRemoteName());
    }

    @Override
    public boolean isPartitionedTable() {
        makeSureInitialized();
        return !getPartitionColumns().isEmpty();
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        return getPartitionColumns();
    }

    public List<Column> getPartitionColumns() {
        makeSureInitialized();
        return getSchemaCacheValue()
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getPartitionColumns())
                .orElse(Collections.emptyList());
    }

    /**
     * Opens the hidden-column gate while a row-level DML over THIS table is in flight, mirroring legacy
     * {@code IcebergExternalTable.needInternalHiddenColumns}. The signal is the neutral per-table ctx flag
     * the generic {@code RowLevelDmlCommand} sets (not an iceberg concept); a connector with no synthetic
     * write columns appends nothing even with the gate open, so this stays correct for every connector type.
     */
    @Override
    protected boolean needInternalHiddenColumns() {
        ConnectContext ctx = ConnectContext.get();
        return ctx != null && ctx.needsSyntheticWriteColForTable(getId());
    }

    /**
     * Appends the connector's request-scoped synthetic write columns to the full schema when a write/DML
     * over this table is in flight. The base schema (including any always-present hidden columns the
     * connector declares through the schema cache, e.g. iceberg v3 row-lineage) comes from
     * {@code super.getFullSchema()}; the request-scoped columns (e.g. iceberg's row-id STRUCT) are fetched
     * live from the connector — they must not be cached — and appended only when the request gate is open:
     * show-hidden, or the synthetic-write-column ctx flag set for this table during row-level DML. Mirrors
     * legacy {@code IcebergExternalTable.getFullSchema}, but connector-agnostic (iron-law: no iceberg branch
     * here) — a connector with no synthetic write columns (jdbc/es/paimon/maxcompute) keeps its byte-identical
     * full schema.
     */
    @Override
    public List<Column> getFullSchema() {
        List<Column> schema = super.getFullSchema();
        if (schema == null || !(Util.showHiddenColumns() || needInternalHiddenColumns())) {
            return schema;
        }
        List<ConnectorColumn> synthetic = fetchSyntheticWriteColumns();
        if (synthetic.isEmpty()) {
            return schema;
        }
        List<Column> result = new ArrayList<>(schema);
        result.addAll(ConnectorColumnConverter.convertColumns(synthetic));
        return result;
    }

    /**
     * Fetches the connector's declared synthetic write columns for this table, in engine-neutral form.
     * Degrades to an empty list on any miss (non-plugin catalog, a read-only connector with no write-plan
     * provider, or an unresolvable table handle) and never throws — schema resolution must not fail a query.
     */
    private List<ConnectorColumn> fetchSyntheticWriteColumns() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Collections.emptyList();
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Collections.emptyList();
        }
        ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider();
        if (writePlanProvider == null) {
            return Collections.emptyList();
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyList();
        }
        return writePlanProvider.getSyntheticWriteColumns(session, handleOpt.get());
    }

    /** The raw connector-emitted table-property map (including FE-internal / render-hint keys). */
    private Map<String, String> rawTableProperties() {
        makeSureInitialized();
        return getSchemaCacheValue()
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getTableProperties())
                .orElse(Collections.emptyMap());
    }

    /**
     * The connector's user-facing table properties (e.g. paimon coreOptions: path / file.format /
     * write-only), used by SHOW CREATE TABLE to render the PROPERTIES(...) block (D-046). The
     * FE-internal schema-control keys ({@code partition_columns} / {@code primary_keys}, emitted by
     * the connector so {@link #initSchema()} can derive the partition columns) and the SHOW CREATE
     * render-hint keys ({@code show.location} / {@code show.partition-clause} / {@code show.sort-clause},
     * rendered as the LOCATION / PARTITION BY / ORDER BY clauses via {@link #getShowLocation()} etc.) are
     * stripped — they are not user-facing options and must not leak into the rendered PROPERTIES(...).
     */
    public Map<String, String> getTableProperties() {
        Map<String, String> raw = rawTableProperties();
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            String key = entry.getKey();
            if ("partition_columns".equals(key) || "primary_keys".equals(key)
                    || ConnectorTableSchema.SHOW_LOCATION_KEY.equals(key)
                    || ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY.equals(key)
                    || ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY.equals(key)) {
                continue;
            }
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * The table location string for the SHOW CREATE TABLE {@code LOCATION '...'} clause. Reads the
     * connector's {@code show.location} render-hint key, falling back to the user-facing {@code path}
     * property (paimon carries its location there, and keeps it in PROPERTIES). Returns "" if neither
     * is present.
     */
    public String getShowLocation() {
        Map<String, String> raw = rawTableProperties();
        String location = raw.getOrDefault(ConnectorTableSchema.SHOW_LOCATION_KEY, "");
        return location.isEmpty() ? raw.getOrDefault("path", "") : location;
    }

    /** The pre-rendered {@code PARTITION BY ...} clause for SHOW CREATE TABLE, or "" if none. */
    public String getShowPartitionClause() {
        return rawTableProperties().getOrDefault(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY, "");
    }

    /** The pre-rendered {@code ORDER BY (...)} clause for SHOW CREATE TABLE, or "" if none. */
    public String getShowSortClause() {
        return rawTableProperties().getOrDefault(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY, "");
    }

    @Override
    public boolean supportInternalPartitionPruned() {
        // Unconditional true, mirroring legacy MaxComputeExternalTable (and IcebergExternalTable).
        // This override is shared by every SPI-driven connector (jdbc/es/trino/max_compute via
        // CatalogFactory.SPI_READY_TYPES) and true is correct for all of them, partitioned or not:
        //   - partitioned     -> PruneFileScanPartition prunes to the surviving partitions;
        //   - non-partitioned -> PruneFileScanPartition takes its IF branch and pruneExternalPartitions
        //                        returns NOT_PRUNED for empty partition columns, so the scan reads all.
        // It must NOT be gated on `!getPartitionColumns().isEmpty()`: returning false for a
        // non-partitioned table sends PruneFileScanPartition down its ELSE branch, which overwrites the
        // selection with SelectedPartitions(0, {}, isPruned=true). PluginDrivenScanNode.getSplits() then
        // reads that as "pruned to zero partitions" and short-circuits to no splits, so a filtered query
        // over a non-partitioned table silently returns zero rows (data loss). See FIX-NONPART-PRUNE-DATALOSS.
        return true;
    }

    @Override
    public Map<String, PartitionItem> getNameToPartitionItems(Optional<MvccSnapshot> snapshot) {
        List<Column> partitionColumns = getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return Collections.emptyMap();
        }
        List<String> remoteNames = getSchemaCacheValue()
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getPartitionColumnRemoteNames())
                .orElse(Collections.emptyList());
        List<Type> types = partitionColumns.stream().map(Column::getType).collect(Collectors.toList());

        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyMap();
        }

        // One round-trip, no FE-side partition-value cache (per CACHE-P1: the cutover lists
        // partitions per query instead of maintaining a second-level cache). The connector returns
        // each partition's display name plus a raw-keyed value map; we extract values in
        // partition-column order via the cached remote names.
        List<ConnectorPartitionInfo> partitions =
                metadata.listPartitions(session, handleOpt.get(), Optional.empty());
        List<String> partitionNames = new ArrayList<>(partitions.size());
        List<List<String>> partitionValues = new ArrayList<>(partitions.size());
        for (ConnectorPartitionInfo partition : partitions) {
            partitionNames.add(partition.getPartitionName());
            List<String> values = new ArrayList<>(remoteNames.size());
            for (String remoteName : remoteNames) {
                values.add(partition.getPartitionValues().get(remoteName));
            }
            partitionValues.add(values);
        }

        // Reuse TablePartitionValues so the PartitionItem construction (ListPartitionItem,
        // isHive=false) is identical to legacy MaxComputeExternalMetaCache.loadPartitionValues,
        // then invert id->item via id->name (mirroring MaxComputeExternalTable.getNameToPartitionItems).
        TablePartitionValues tablePartitionValues = new TablePartitionValues();
        tablePartitionValues.addPartitions(partitionNames, partitionValues, types,
                Collections.nCopies(partitionNames.size(), 0L));
        Map<Long, PartitionItem> idToPartitionItem = tablePartitionValues.getIdToPartitionItem();
        Map<Long, String> idToNameMap = tablePartitionValues.getPartitionIdToNameMap();
        Map<String, PartitionItem> nameToPartitionItem = Maps.newHashMapWithExpectedSize(idToPartitionItem.size());
        for (Entry<Long, PartitionItem> entry : idToPartitionItem.entrySet()) {
            nameToPartitionItem.put(idToNameMap.get(entry.getKey()), entry.getValue());
        }
        return nameToPartitionItem;
    }

    @Override
    public long getCachedRowCount() {
        // Do NOT call makeSureInitialized() here.
        // ExternalTable.getCachedRowCount() intentionally returns -1 for uninitialized tables
        // so that SHOW TABLE STATUS / information_schema.tables stays non-blocking.
        if (!isObjectCreated()) {
            return -1;
        }
        return Env.getCurrentEnv().getExtMetaCacheMgr().getRowCountCache()
                .getCachedRowCount(catalog.getId(), dbId, id, false);
    }

    @Override
    public String getComment() {
        return getComment(false);
    }

    @Override
    public String getComment(boolean escapeQuota) {
        String remoteDbName = db != null ? db.getRemoteName() : "";
        try {
            PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
            Connector connector = pluginCatalog.getConnector();
            ConnectorSession session = pluginCatalog.buildConnectorSession();
            ConnectorMetadata metadata = connector.getMetadata(session);
            String tableName = getRemoteName();
            String comment = metadata.getTableComment(session, remoteDbName, tableName);
            if (escapeQuota && comment != null) {
                return comment.replace("'", "\\'");
            }
            return comment != null ? comment : "";
        } catch (Exception e) {
            LOG.debug("Failed to get table comment for {}.{}", remoteDbName, name, e);
            return "";
        }
    }

    /**
     * Exposes the connector's system tables (e.g. {@code tbl$snapshots}) through the live fe-core
     * system-table machinery. Delegates name discovery to the connector SPI
     * ({@link ConnectorMetadata#listSupportedSysTables}); each returned bare name (already lowercase)
     * is wrapped in a {@link PluginDrivenSysTable} so {@link org.apache.doris.catalog.TableIf#findSysTable}
     * resolves {@code tbl$name} and {@link org.apache.doris.datasource.systable.SysTableResolver} can
     * build the transient sys ExternalTable. Mirrors the legacy no-cache getTableHandle pattern: the
     * handle/name list is fetched per call (system-table planning is infrequent), so no extra caching.
     */
    @Override
    public Map<String, SysTable> getSupportedSysTables() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Collections.emptyMap();
        }
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyMap();
        }
        List<String> names = metadata.listSupportedSysTables(session, handleOpt.get());
        if (names.isEmpty()) {
            return Collections.emptyMap();
        }
        // Keep keys exactly as returned by the connector (already lowercase) so the inherited,
        // case-sensitive findSysTable exact-match works, mirroring legacy PaimonSysTable keys.
        Map<String, SysTable> result = Maps.newHashMapWithExpectedSize(names.size());
        for (String sysName : names) {
            result.put(sysName, new PluginDrivenSysTable(sysName));
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        // After deserializing a migrated old table (e.g., EsExternalTable → PluginDrivenExternalTable),
        // fix the table type so that BindRelation routes to LogicalFileScan (new path).
        if (type != TableType.PLUGIN_EXTERNAL_TABLE) {
            LOG.info("Migrating table '{}' type from {} to PLUGIN_EXTERNAL_TABLE", name, type);
            type = TableType.PLUGIN_EXTERNAL_TABLE;
        }
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        makeSureInitialized();
        return new ExternalAnalysisTask(info);
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return UNKNOWN_ROW_COUNT;
        }

        Optional<ConnectorTableStatistics> statsOpt = metadata.getTableStatistics(session, handleOpt.get());
        if (statsOpt.isPresent() && statsOpt.get().getRowCount() >= 0) {
            return statsOpt.get().getRowCount();
        }
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public String getEngine() {
        // Return the legacy engine name based on the actual catalog type,
        // not the generic "Plugin" from PLUGIN_EXTERNAL_TABLE.toEngineName().
        // This preserves user-visible compatibility for migrated JDBC/ES tables
        // across SHOW TABLE STATUS, information_schema.tables, REST API, etc.
        String catalogType = catalog instanceof PluginDrivenExternalCatalog
                ? ((PluginDrivenExternalCatalog) catalog).getType() : "";
        switch (catalogType) {
            case "jdbc":
                return TableType.JDBC_EXTERNAL_TABLE.toEngineName();
            case "es":
                return TableType.ES_EXTERNAL_TABLE.toEngineName();
            case "iceberg":
                // P6.5-T06: preserve the legacy IcebergExternalTable engine name "iceberg"
                // (TableType.ICEBERG_EXTERNAL_TABLE.toEngineName()) for migrated iceberg base/sys tables,
                // instead of the generic "Plugin" from PLUGIN_EXTERNAL_TABLE.
                return TableType.ICEBERG_EXTERNAL_TABLE.toEngineName();
            case "trino-connector":
                // TableType.TRINO_CONNECTOR_EXTERNAL_TABLE.toEngineName() returns null
                // (no switch case in TableType.toEngineName), matching legacy behavior.
                return TableType.TRINO_CONNECTOR_EXTERNAL_TABLE.toEngineName();
            case "max_compute":
                // TableType.MAX_COMPUTE_EXTERNAL_TABLE.toEngineName() returns null
                // (no switch case in TableType.toEngineName), matching legacy behavior.
                return TableType.MAX_COMPUTE_EXTERNAL_TABLE.toEngineName();
            case "paimon":
                // TableType.PAIMON_EXTERNAL_TABLE.toEngineName() returns "paimon",
                // preserving the legacy PaimonExternalTable engine name.
                return TableType.PAIMON_EXTERNAL_TABLE.toEngineName();
            default:
                return super.getEngine();
        }
    }

    @Override
    public String getEngineTableTypeName() {
        String catalogType = catalog instanceof PluginDrivenExternalCatalog
                ? ((PluginDrivenExternalCatalog) catalog).getType() : "";
        switch (catalogType) {
            case "jdbc":
                return TableType.JDBC_EXTERNAL_TABLE.name();
            case "es":
                return TableType.ES_EXTERNAL_TABLE.name();
            case "iceberg":
                return TableType.ICEBERG_EXTERNAL_TABLE.name();
            case "trino-connector":
                return TableType.TRINO_CONNECTOR_EXTERNAL_TABLE.name();
            case "max_compute":
                return TableType.MAX_COMPUTE_EXTERNAL_TABLE.name();
            case "paimon":
                return TableType.PAIMON_EXTERNAL_TABLE.name();
            default:
                return TableType.PLUGIN_EXTERNAL_TABLE.name();
        }
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = connector.getMetadata(session);

        String dbName = db != null ? db.getRemoteName() : "";
        List<Column> schema = getFullSchema();
        TTableDescriptor desc = metadata.buildTableDescriptor(session,
                getId(), getName(), dbName, getRemoteName(),
                schema.size(), pluginCatalog.getId());
        if (desc != null) {
            return desc;
        }
        LOG.warn("Connector returned null table descriptor for plugin table {}.{}, "
                + "using generic fallback", dbName, getName());
        return new TTableDescriptor(getId(), TTableType.SCHEMA_TABLE,
                schema.size(), 0, getName(), dbName);
    }
}
