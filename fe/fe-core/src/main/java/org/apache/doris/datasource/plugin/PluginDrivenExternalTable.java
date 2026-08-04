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

package org.apache.doris.datasource.plugin;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorColumnStatistics;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.ConnectorViewDefinition;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.TablePartitionValues;
import org.apache.doris.datasource.connector.converter.ConnectorColumnConverter;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.systable.PartitionsSysTable;
import org.apache.doris.datasource.systable.PluginDrivenSysTable;
import org.apache.doris.datasource.systable.SysTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;
import org.apache.doris.statistics.ExternalAnalysisTask;
import org.apache.doris.statistics.PluginDrivenSampleAnalysisTask;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generic {@link ExternalTable} for plugin-driven catalogs.
 *
 * <p>Provides table implementation that fetches schema from the connector SPI.
 * Connector-specific behavior is accessed through the parent catalog's
 * {@link org.apache.doris.connector.spi.Connector} using opaque handles.</p>
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
    public Optional<ConnectorTableHandle> resolveConnectorTableHandle(
            ConnectorSession session, ConnectorMetadata metadata) {
        String dbName = db != null ? db.getRemoteName() : "";
        return metadata.getTableHandle(session, dbName, getRemoteName());
    }

    /**
     * Resolves this table's write-target {@link ConnectorTableHandle} for the plugin-driven insert executor's
     * per-handle transaction selection, failing loud if it cannot be resolved. A heterogeneous gateway needs
     * the handle to open the SIBLING connector's transaction for a foreign (iceberg-on-HMS) table; a
     * single-format connector ignores it (its {@code beginTransaction} defaults to the connector-level one), so
     * this is byte-identical for it. Fails loud rather than returning null: a null handle is not an
     * {@code instanceof} the gateway's own handle type and would misroute a plain write to the sibling.
     */
    public ConnectorTableHandle resolveWriteTargetHandle() {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        return resolveConnectorTableHandle(session, PluginDrivenMetadata.get(session, pluginCatalog.getConnector()))
                .orElseThrow(() -> new DorisConnectorException(
                        "Cannot resolve the connector table handle for write target " + getName()));
    }

    /**
     * Returns the connector's synthetic scan predicates for this table at the given resolved MVCC
     * {@code snapshot} — a connector "residual predicate" the read cannot enforce by file selection alone
     * (e.g. a hudi incremental {@code _hoodie_commit_time} commit-time window), expressed in the neutral
     * {@link ConnectorExpression} grammar. The analysis-time synthetic-predicate rule reverse-converts these
     * into a {@code LogicalFilter} over this table's scan.
     *
     * <p>The {@code snapshot} is the one {@link org.apache.doris.datasource.mvcc.MvccTable#loadSnapshot}
     * resolved at analysis time (retrieved from {@code StatementContext}), so the row-filter window is the
     * SAME single resolution the scan-time {@code applySnapshot} threads onto the handle — file selection and
     * the row filter can never diverge. Returns empty when the snapshot is not a plugin MVCC snapshot, the
     * handle cannot be resolved, or the connector has no residual predicate (iceberg/paimon/... and every
     * non-incremental read inherit the empty SPI default) — so the plan stays byte-identical.</p>
     */
    public List<ConnectorExpression> getSyntheticScanPredicates(MvccSnapshot snapshot) {
        if (!(snapshot instanceof PluginDrivenMvccSnapshot) || !(catalog instanceof PluginDrivenExternalCatalog)) {
            return Collections.emptyList();
        }
        ConnectorMvccSnapshot connectorSnapshot = ((PluginDrivenMvccSnapshot) snapshot).getConnectorSnapshot();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, pluginCatalog.getConnector());
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyList();
        }
        return metadata.getSyntheticScanPredicates(session, handleOpt.get(), connectorSnapshot);
    }

    /**
     * Returns whether the underlying connector supports multiple concurrent writers.
     * Used by the planner to decide GATHER (single writer) vs parallel distribution.
     */
    public boolean supportsParallelWrite() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        ConnectorWritePlanProvider provider = writePlanProvider();
        // requiresParallelWrite is byte-inert for a heterogeneous gateway (hive and iceberg both true), so the
        // connector-level answer needs no per-handle resolution here.
        return provider != null && provider.requiresParallelWrite();
    }

    /**
     * Resolves this table's connector handle for a per-handle write-capability probe, or empty on any miss (a
     * null connector, or an unresolvable handle). A heterogeneous gateway needs the handle to answer write
     * capabilities per-table (its iceberg tables differ from its hive tables); a single-format connector ignores
     * the handle (the per-handle overloads default to connector-level), so this is byte-identical for it.
     */
    /**
     * The CONNECTOR-LEVEL write plan provider, or null when this catalog's connector is absent or declares no
     * write support. Callers must have already checked that the catalog is plugin-driven. Used by the write
     * traits whose answer is the same for every table of a heterogeneous gateway, so paying for a per-handle
     * resolution would buy nothing; the per-table ones go through
     * {@link #resolveWriteCapabilityHandle(Connector)} and {@code getWritePlanProvider(handle)} instead.
     */
    private ConnectorWritePlanProvider writePlanProvider() {
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector == null ? null : connector.getWritePlanProvider();
    }

    private Optional<ConnectorTableHandle> resolveWriteCapabilityHandle(Connector connector) {
        ConnectorSession session = ((PluginDrivenExternalCatalog) catalog).buildConnectorSession();
        return resolveConnectorTableHandle(session, PluginDrivenMetadata.get(session, connector));
    }

    /**
     * The write operations the connector admits for THIS table, resolved per-handle so a heterogeneous gateway
     * admits DELETE/MERGE/OVERWRITE for its iceberg tables but only INSERT/OVERWRITE for its hive tables.
     * Degrades to the empty set (all writes rejected) on any miss, mirroring {@link #fetchSyntheticWriteColumns()}.
     */
    public Set<WriteOperation> connectorSupportedWriteOperations() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return EnumSet.noneOf(WriteOperation.class);
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        if (connector == null) {
            return EnumSet.noneOf(WriteOperation.class);
        }
        return resolveWriteCapabilityHandle(connector)
                .map(connector::getWritePlanProvider)
                .map(ConnectorWritePlanProvider::supportedOperations)
                .orElseGet(() -> EnumSet.noneOf(WriteOperation.class));
    }

    /**
     * Whether the connector admits branch writes for THIS table, resolved per-handle (iceberg supports
     * write-to-branch, hive does not). Degrades to false on any miss.
     */
    public boolean connectorSupportsWriteBranch() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        if (connector == null) {
            return false;
        }
        return resolveWriteCapabilityHandle(connector)
                .map(connector::getWritePlanProvider)
                .map(ConnectorWritePlanProvider::supportsWriteBranch)
                .orElse(false);
    }

    /**
     * Returns whether THIS table supports background per-column auto-analyze. The statistics auto-collector
     * consults this (in place of the legacy {@code instanceof IcebergExternalTable} whitelist) to admit a flipped
     * plugin table into the auto-analyze framework. Resolved per-table via {@link #hasCapability} (not the
     * connector-wide set alone) so a heterogeneous hive catalog can express the legacy
     * {@code StatisticsUtil.supportAutoAnalyze} gate of {@code dlaType HIVE || ICEBERG} but NOT {@code HUDI}: a
     * uniform-format connector (native iceberg/paimon) still declares it connector-wide, while hive emits it
     * per-table for its plain-hive tables and reflects the iceberg sibling's connector-wide set onto an
     * iceberg-on-HMS table's delegated schema — so hudi-on-HMS, whose connector declares neither, is correctly
     * withheld. Mirrors {@link #supportsTopNLazyMaterialize} / {@link #supportsNestedColumnPrune}.
     */
    public boolean supportsColumnAutoAnalyze() {
        return hasCapability(ConnectorCapability.SUPPORTS_COLUMN_AUTO_ANALYZE);
    }

    /**
     * Returns whether THIS table supports Top-N lazy materialization. The nereids Top-N lazy-materialize probe
     * consults this (in place of the legacy exact-class {@code SUPPORT_RELATION_TYPES} membership) to enable
     * lazy materialization for a flipped plugin table. Resolved per-table via {@link #hasCapability}: a
     * uniform-format connector (iceberg) declares it connector-wide, a heterogeneous connector (hive) emits it
     * only for its orc/parquet tables — so a hive text/csv/json/view table is correctly excluded, as it was in
     * legacy {@code MaterializeProbeVisitor}.
     */
    public boolean supportsTopNLazyMaterialize() {
        return hasCapability(ConnectorCapability.SUPPORTS_TOPN_LAZY_MATERIALIZE);
    }

    /**
     * Returns whether THIS table supports nested-column pruning (reading only the accessed STRUCT/ARRAY/MAP
     * sub-fields). The nereids nested-column-prune probe ({@code LogicalFileScan.supportPruneNestedColumn})
     * consults this (in place of the legacy exact-class {@code IcebergExternalTable} arm) to enable pruning for a
     * flipped plugin table, and the {@code SlotTypeReplacer} name-to-field-id rewrite is gated on the same
     * answer. Resolved per-table via {@link #hasCapability} for the same reason as Top-N: legacy gated it on
     * the per-table file format (parquet/orc only), which a connector-wide capability cannot express for a
     * heterogeneous hive catalog.
     */
    public boolean supportsNestedColumnPrune() {
        return hasCapability(ConnectorCapability.SUPPORTS_NESTED_COLUMN_PRUNE);
    }

    /**
     * Returns whether THIS table supports {@code ALTER TABLE} column schema-change DDL (including dotted
     * nested paths and {@code MODIFY COLUMN ... COMMENT}). The nereids {@code AlterTableCommand} column-op
     * validation consults this (in place of the legacy exact-class {@code IcebergExternalTable} gate) to admit
     * the Iceberg-style clause set and to allow nested {@code ColumnPath} targets. Resolved per-table via
     * {@link #hasCapability} so an iceberg-on-HMS table inherits it through the reflected per-table
     * capability set, mirroring {@link #supportsNestedColumnPrune}.
     */
    public boolean supportsNestedColumnSchemaChange() {
        return hasCapability(ConnectorCapability.SUPPORTS_NESTED_COLUMN_SCHEMA_CHANGE);
    }

    /**
     * Returns whether THIS table accepts the relation-scoped {@code @options(...)} scan-param clause.
     * {@code BindRelation} consults this (in place of the legacy exact-class {@code instanceof
     * PaimonExternalTable} gate) BEFORE any connector round-trip, so a table type that cannot honor the
     * clause fails loudly instead of silently answering a historical query with latest data. Resolved
     * per-table via {@link #hasCapability} because a connector may honor the clause on its data tables
     * while declining it on the system tables whose readers cannot observe a selected snapshot (see
     * {@code PaimonConnectorMetadata}'s per-sys-table capability refinement). Mirrors
     * {@link #supportsNestedColumnPrune}.
     */
    public boolean supportsScanParamOptions() {
        return hasCapability(ConnectorCapability.SUPPORTS_SCAN_PARAM_OPTIONS);
    }

    /**
     * Returns whether THIS table supports {@code ANALYZE ... WITH SAMPLE}. Consulted by
     * {@code AnalysisManager.canSample}, {@code AnalyzeTableCommand.isSamplingPartition}, {@link
     * #createAnalysisTask} (to return a sample-capable task) and the background auto-analyze method choice.
     * Resolved per-table via {@link #hasCapability}: hive emits it for its plain-hive tables only (legacy
     * {@code dlaType==HIVE}), so iceberg/hudi-on-HMS are excluded; native iceberg/paimon never declare it (their
     * {@code doSample} is unimplemented), keeping their current build-time reject. Mirrors
     * {@link #supportsTopNLazyMaterialize}.
     */
    public boolean supportsSampleAnalyze() {
        return hasCapability(ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE);
    }

    /**
     * Whether this table supports a table-scoped capability, resolved connector-wide OR per-table. A
     * uniform-format connector (iceberg — every table orc/parquet) declares the capability for all its tables
     * via {@link Connector#getCapabilities()}; a heterogeneous connector (hive) whose eligibility is per-table
     * file-format gated instead declares it per-table in {@link ConnectorTableSchema#getTableCapabilities()},
     * read here from the already-cached schema (no remote round-trip). The two sources are additive, so
     * single-format connectors declare nothing per-table and behave exactly as before. fe-core never inspects
     * the file format — the connector decides which of its tables qualify.
     *
     * <p>Only the capabilities {@link ConnectorCapability} documents as table-scoped may be resolved through
     * here. Routing a catalog-scoped one through it would be a behaviour change, and for two of them a
     * damaging one: reading the per-table set touches the schema cache, and those two are consulted while the
     * table is being initialized / in order to decide whether to load metadata at all.</p>
     */
    private boolean hasCapability(ConnectorCapability capability) {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        if (connector == null) {
            return false;
        }
        return connector.getCapabilities().contains(capability) || tableCapabilities().contains(capability);
    }

    /** The connector-declared per-table capability set, from the cached schema; empty on any miss. */
    private Set<ConnectorCapability> tableCapabilities() {
        makeSureInitialized();
        return getSchemaCacheValue()
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getTableCapabilities())
                .orElse(Collections.emptySet());
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
        ConnectorWritePlanProvider provider = writePlanProvider();
        return provider != null && provider.requiresPartitionLocalSort();
    }

    /**
     * Returns whether the underlying connector requires dynamic-partition writes to be hash-distributed by
     * partition columns but <b>not</b> locally sorted (e.g. Hive: the file writer buffers a per-partition
     * writer, so the hash alone keeps each partition on one instance without a sort). Used by
     * {@code PhysicalConnectorTableSink} to require that hash distribution (no {@code MustLocalSortOrderSpec}) for
     * dynamic-partition writes; a connector sets at most one of this and {@link #requirePartitionLocalSortOnWrite()}.
     * Defaults to false so non-partitioned connectors are unaffected.
     */
    public boolean requirePartitionHashOnWrite() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        if (connector == null) {
            return false;
        }
        // Per-table: hive requires partition-hash writes but iceberg does not, so resolve the handle.
        return resolveWriteCapabilityHandle(connector)
                .map(connector::getWritePlanProvider)
                .map(ConnectorWritePlanProvider::requiresPartitionHashWrite)
                .orElse(false);
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
        ConnectorWritePlanProvider provider = writePlanProvider();
        return provider != null && provider.requiresFullSchemaWriteOrder();
    }

    /**
     * Returns whether the underlying connector's data files retain partition columns, so a static-partition
     * write must materialize the PARTITION-clause literal into the data column instead of NULL-filling it
     * (e.g. Iceberg). Connectors that strip partition columns and refill them from {@code
     * static_partition_values} (e.g. MaxCompute) return false. Used by {@code BindSink.bindConnectorTableSink};
     * defaults to false.
     */
    public boolean materializeStaticPartitionValues() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        if (connector == null) {
            return false;
        }
        // Per-table: iceberg retains partition columns and hive derives the partition directory from the row
        // (both materialize the PARTITION literal); maxcompute refills from the static spec instead.
        return resolveWriteCapabilityHandle(connector)
                .map(connector::getWritePlanProvider)
                .map(ConnectorWritePlanProvider::requiresMaterializeStaticPartitionValues)
                .orElse(false);
    }

    @Override
    public boolean supportsExternalMetadataPreload() {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        // F11: gate async metadata pre-load on the connector-declared SUPPORTS_METADATA_PRELOAD capability
        // (replacing the legacy engine-name "jdbc" string, per the iron rule). jdbc and iceberg both declare
        // it; connectors not yet validated for concurrent pre-warming (e.g. ES) do not, and fall back to
        // synchronous load at binding time. Pure planning/lock-latency optimization, no correctness effect.
        Connector connector = ((PluginDrivenExternalCatalog) catalog).getConnector();
        return connector != null
                && connector.getCapabilities().contains(ConnectorCapability.SUPPORTS_METADATA_PRELOAD);
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
        ConnectorSession session = pluginCatalog.buildCrossStatementSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);

        String dbName = db != null ? db.getRemoteName() : "";
        String tableName = getRemoteName();
        if (isView()) {
            // A connector view has no table handle (the SDK tableExists() is false for views); build the schema
            // from the view definition's columns instead. Mirrors legacy IcebergUtils.loadViewSchemaCacheValue
            // (icebergView.schema()). Gated on isView() => only view-supporting connectors (SUPPORTS_VIEW) reach
            // here; view-less connectors (jdbc/paimon/maxcompute) keep isView()==false and skip this.
            ConnectorViewDefinition viewDefinition = metadata.getViewDefinition(session, dbName, tableName);
            ConnectorTableSchema viewSchema = new ConnectorTableSchema(
                    tableName, viewDefinition.getColumns(), null, Collections.emptyMap());
            return Optional.of(toSchemaCacheValue(metadata, session, dbName, tableName, viewSchema));
        }
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

        // Identify partition columns from the connector's reserved PARTITION_COLUMNS_KEY property (a CSV of
        // RAW remote column names; producer: hive/hudi/iceberg/paimon/maxcompute). We keep two aligned
        // views: the Doris Columns (with mapped/local names, used for getPartitionColumns + types)
        // and the raw remote names (used to index the raw-keyed partition-value maps from the SPI).
        // The columns themselves are already present in `columns` (the connector appends partition
        // columns to the schema, mirroring legacy); here we only mark which ones are partitions.
        List<Column> partitionColumns = new ArrayList<>();
        List<String> partitionColumnRemoteNames = new ArrayList<>();
        String partColsProp = tableSchema.getProperties().get(ConnectorTableSchema.PARTITION_COLUMNS_KEY);
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
                tableSchema.getProperties(), tableSchema.getTableCapabilities());
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
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        String dbName = db != null ? db.getRemoteName() : "";
        return metadata.viewExists(session, dbName, getRemoteName());
    }

    /**
     * Returns the stored SQL text of this view, mirroring legacy {@code IcebergExternalTable.getViewText}.
     * Issues one connector round-trip ({@code getViewDefinition}) — the same single remote load the legacy
     * query path made — so {@code BindRelation} (and SHOW CREATE) can parse and analyze the view body.
     * Callers gate on {@link #supportsView()} + {@link #isView()}; on a view-less connector the SPI default
     * fails loud. (Legacy {@code getSqlDialect} is intentionally not ported — it has no caller; the view
     * body is converted by the session dialect in {@code BindRelation.parseAndAnalyzeExternalView}, and the
     * connector already uses the view's own dialect internally to pick the SQL representation.)
     */
    public String getViewText() {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        String dbName = db != null ? db.getRemoteName() : "";
        ConnectorViewDefinition definition = metadata.getViewDefinition(session, dbName, getRemoteName());
        return definition.getSql();
    }

    /**
     * Renders the connector's native {@code SHOW CREATE TABLE} DDL (a fresh, cache-bypassing metastore read) for
     * the SHOW CREATE TABLE command's connector arm. Mirrors {@link #getViewText}'s single connector round-trip
     * (and, like it, is safe to call under the command's table read-lock — no {@code makeSureInitialized}). Returns
     * {@link Optional#empty()} when the connector supplies no native DDL (iceberg/paimon/es/jdbc inherit the empty
     * SPI default {@link ConnectorMetadata#renderShowCreateTableDdl}), or when the handle cannot be resolved — the
     * command then falls through to the generic {@code Env.getDdlStmt} rendering unchanged. A native-rendering
     * connector (hive) returns the full statement, fetched fresh so it reflects a just-applied external ALTER even
     * while DESC serves a cached schema.
     */
    public Optional<String> getShowCreateTableDdl() {
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Optional.empty();
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Optional.empty();
        }
        return metadata.renderShowCreateTableDdl(session, handleOpt.get());
    }

    @Override
    public boolean isPartitionedTable() {
        makeSureInitialized();
        return !getPartitionColumns().isEmpty();
    }

    @Override
    public List<Column> getPartitionColumns(Optional<MvccSnapshot> snapshot) {
        makeSureInitialized();
        // Resolve against the CALLER's pin, not the ambient context: a statement pinning this table at two
        // versions cannot be disambiguated ambiently and would degrade to the LATEST partition columns.
        return getSchemaCacheValue(snapshot)
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getPartitionColumns())
                .orElse(Collections.emptyList());
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
        return appendSyntheticWriteColumns(super.getFullSchema());
    }

    /**
     * Same as {@link #getFullSchema()}, but the BASE schema is resolved AS OF {@code snapshot} (this
     * reference's own pin). The synthetic write columns are request-scoped, not version-scoped, so they are
     * appended identically for either form — only the base schema read is version-aware.
     *
     * <p><b>Every</b> arity of this method must go through {@link #appendSyntheticWriteColumns}. The plan
     * path ({@code LogicalFileScan.computePluginDrivenOutput}) calls THIS one, and it must not lose the
     * append: when it did, iceberg's row-id STRUCT vanished from the scan's output, breaking every
     * row-level DML with "Unknown column '__DORIS_ICEBERG_ROWID_COL__'" and dropping the column from
     * {@code SELECT *} under show-hidden. A new overload that reads the schema cache directly silently
     * bypasses this append — the compiler cannot catch it, since these are overloads, not overrides.</p>
     */
    @Override
    public List<Column> getFullSchema(Optional<MvccSnapshot> snapshot) {
        return appendSyntheticWriteColumns(super.getFullSchema(snapshot));
    }

    private List<Column> appendSyntheticWriteColumns(List<Column> schema) {
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
        // Resolve the handle first so the write provider is selected per-table (a heterogeneous gateway routes
        // iceberg-on-HMS to its sibling by the handle type); both null-degrade checks keep the empty fallback.
        // Equivalent result for single-format connectors (getWritePlanProvider(handle) defaults to the no-arg
        // one); this gated (show-hidden / row-level-DML) path resolves the handle before the provider-null check,
        // so a read-only connector now resolves the handle here — a no-op for a write-capable connector, which
        // resolved it regardless.
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyList();
        }
        ConnectorWritePlanProvider writePlanProvider = connector.getWritePlanProvider(handleOpt.get());
        if (writePlanProvider == null) {
            return Collections.emptyList();
        }
        return writePlanProvider.getSyntheticWriteColumns(session, handleOpt.get());
    }

    /**
     * The connector-declared synthetic write columns (e.g. the row-level DML row-id STRUCT), converted to
     * engine {@link Column}s. Unlike {@link #getFullSchema()}, this is NOT gated on show-hidden / in-flight
     * DML — it always asks the connector. Row-level DML uses it to source the row-id column identity from the
     * connector instead of reconstructing the STRUCT in fe-core. Empty on any miss (mirrors
     * {@link #fetchSyntheticWriteColumns()}).
     */
    public List<Column> getSyntheticWriteColumns() {
        return ConnectorColumnConverter.convertColumns(fetchSyntheticWriteColumns());
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
     * write-only), used by SHOW CREATE TABLE to render the PROPERTIES(...) block (D-046). Every FE-internal
     * reserved control key ({@link ConnectorTableSchema#RESERVED_CONTROL_KEYS} — the partition-columns and
     * distribution-columns markers plus the SHOW CREATE render hints, all namespaced under
     * {@code __internal.}) is stripped: they are not user-facing options and must not
     * leak into the rendered PROPERTIES(...). Because the reserved keys are namespaced, a source table's own
     * user property can never collide with one, so it flows through here unchanged.
     */
    public Map<String, String> getTableProperties() {
        Map<String, String> raw = rawTableProperties();
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : raw.entrySet()) {
            if (ConnectorTableSchema.RESERVED_CONTROL_KEYS.contains(entry.getKey())) {
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
        // This override is shared by every plugin-driven connector (jdbc/es/trino/max_compute among them)
        // and true is correct for all of them, partitioned or not:
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
        List<Column> partitionColumns = getPartitionColumns(snapshot);
        if (partitionColumns.isEmpty()) {
            return Collections.emptyMap();
        }
        List<String> remoteNames = getSchemaCacheValue(snapshot)
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getPartitionColumnRemoteNames())
                .orElse(Collections.emptyList());
        List<Type> types = partitionColumns.stream().map(Column::getType).collect(Collectors.toList());

        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
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

    /**
     * Partition display-name -> per-column partition values (in partition-column order), sourced from the
     * connector's {@code listPartitions} in one round-trip (no FE-side partition-value cache, per the
     * cutover's connector-owned caching). Values are extracted by the cached remote names; a value absent
     * from the connector's raw map is left {@code null} (the partition_values() TVF renders it as SQL NULL,
     * mirroring the legacy HMS {@code HivePartitionValues.getNameToPartitionValues()}). Empty for a
     * non-partitioned table or an unresolved handle. Partition order preserved via a {@link LinkedHashMap}.
     *
     * <p>Deliberately separate from {@link #getNameToPartitionItems} (not a shared helper) so that live
     * path stays byte- and cost-identical for paimon/iceberg — a shared name-keyed map would collapse the
     * pathological duplicate-partition-name case that the parallel-list build there tolerates.
     */
    public Map<String, List<String>> getNameToPartitionValues(Optional<MvccSnapshot> snapshot) {
        if (getPartitionColumns(snapshot).isEmpty()) {
            return Collections.emptyMap();
        }
        List<String> remoteNames = getSchemaCacheValue(snapshot)
                .map(value -> ((PluginDrivenSchemaCacheValue) value).getPartitionColumnRemoteNames())
                .orElse(Collections.emptyList());

        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyMap();
        }
        List<ConnectorPartitionInfo> partitions =
                metadata.listPartitions(session, handleOpt.get(), Optional.empty());
        Map<String, List<String>> nameToValues = Maps.newLinkedHashMap();
        for (ConnectorPartitionInfo partition : partitions) {
            List<String> values = new ArrayList<>(remoteNames.size());
            for (String remoteName : remoteNames) {
                values.add(partition.getPartitionValues().get(remoteName));
            }
            nameToValues.put(partition.getPartitionName(), values);
        }
        return nameToValues;
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
            ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
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
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
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
            if (metadata.isPartitionValuesSysTable(session, handleOpt.get(), sysName)) {
                // Connector declares this name is served by the generic partition_values TVF (e.g. hive
                // t$partitions), not a native scan. Key on the singleton's OWN name (== "partitions"):
                // PartitionsSysTable strips its hard-wired "$partitions" suffix in createFunction, so a
                // differing key would crash there; identical to sysName for hive today, strictly safer.
                result.put(PartitionsSysTable.INSTANCE.getSysTableName(), PartitionsSysTable.INSTANCE);
            } else {
                result.put(sysName, new PluginDrivenSysTable(sysName));
            }
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
        if (supportsSampleAnalyze()) {
            // A flipped plain-hive table keeps ANALYZE ... WITH SAMPLE working (ExternalAnalysisTask.doSample
            // throws NotImplementedException). iceberg/paimon do NOT declare the capability, so they stay on the
            // byte-identical ExternalAnalysisTask path — the extra check is one cached capability lookup.
            return new PluginDrivenSampleAnalysisTask(info);
        }
        return new ExternalAnalysisTask(info);
    }

    /**
     * The query-planner column-statistics fast path (consulted by {@code ColumnStatisticsCacheLoader} on a
     * stats-cache miss): asks the connector for the no-scan column stats it can serve cheaply and turns the raw
     * facts into a Doris {@link ColumnStatistic}. Empty (fe-core falls back to a full ANALYZE) when the
     * connector has no such stats. Mirrors legacy {@code HMSExternalTable.getHiveColumnStats} +
     * {@code setStatData}: the connector returns raw ndv / numNulls / (string) avgColLen, and THIS side does the
     * Doris-type-dependent size math the connector cannot (it must not import fe-type) — a string column's size
     * is {@code round(avgColLen * count)}, every other type's is {@code count * <slot width>}; min/max stay
     * unconstrained (the {@link ColumnStatisticBuilder} defaults, i.e. legacy's NEGATIVE/POSITIVE_INFINITY).
     */
    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        makeSureInitialized();
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Optional.empty();
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Optional.empty();
        }
        ConnectorSession session = pluginCatalog.buildCrossStatementSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Optional.empty();
        }
        Optional<ConnectorColumnStatistics> statsOpt =
                metadata.getColumnStatistics(session, handleOpt.get(), colName);
        if (!statsOpt.isPresent()) {
            return Optional.empty();
        }
        return toColumnStatistic(statsOpt.get(), getColumn(colName));
    }

    /**
     * The raw per-file byte sizes that {@code ANALYZE ... WITH SAMPLE} seed-shuffles and cumulates into a sample
     * scale factor, from the connector's file listing (like legacy {@code HMSExternalTable.getChunkSizes}). The
     * connector returns only the raw byte lengths; the Doris-type slot-width math stays fe-core-side in the sample
     * task. Overrides {@link ExternalTable#getChunkSizes()} (which throws {@code NotImplementedException}); returns
     * empty on any miss (non-plugin catalog / null connector / unresolved handle) so a connector that cannot list
     * degrades the sampler to scale factor 1. Inert for iceberg/paimon — only reached from the sample task, which
     * they never create. No TCCL pin here; the hive {@code listFileSizes} impl pins internally (parity with
     * {@link #fetchRowCount()} / {@link #getColumnStatistic}).
     */
    @Override
    public List<Long> getChunkSizes() {
        makeSureInitialized();
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return Collections.emptyList();
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Collections.emptyList();
        }
        ConnectorSession session = pluginCatalog.buildCrossStatementSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return Collections.emptyList();
        }
        return metadata.listFileSizes(session, handleOpt.get());
    }

    /**
     * The table's distribution (bucketing) column names, lowercased, from the connector's per-table
     * {@code connector.distribution-columns} schema marker (read from the already-cached schema, no round-trip).
     * Overrides the {@code TableIf} empty default so a flipped bucketed hive table matches legacy
     * {@code HMSExternalTable.getDistributionColumnNames} (which lowercased on this side too). Empty for a
     * non-bucketed table and for connectors that emit no marker (paimon/iceberg) — byte-invariant for them. Used by
     * sampled ANALYZE to pick the linear-vs-DUJ1 NDV estimator.
     */
    @Override
    public Set<String> getDistributionColumnNames() {
        String csv = rawTableProperties().get(ConnectorTableSchema.DISTRIBUTION_COLUMNS_KEY);
        if (csv == null || csv.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> result = new HashSet<>();
        for (String name : csv.split(",")) {
            String trimmed = name.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed.toLowerCase());
            }
        }
        return result;
    }

    /**
     * Turns the connector's raw {@link ConnectorColumnStatistics} into a Doris {@link ColumnStatistic},
     * doing the Doris-type-dependent size math the connector cannot (legacy {@code setStatData} parity): a
     * string column (avgSizeBytes &gt;= 0) sizes to {@code round(avgColLen * count)}, every other type to
     * {@code count * <slot width>}; {@code avgSizeByte = dataSize / count}; min/max stay at the builder's
     * unconstrained defaults. Empty when the column is unknown or the row count is non-positive (no size
     * basis, legacy returned empty). Package-private + static so the math can be unit-tested directly.
     */
    static Optional<ColumnStatistic> toColumnStatistic(ConnectorColumnStatistics stats, Column column) {
        long count = stats.getRowCount();
        if (column == null || count <= 0) {
            return Optional.empty();
        }
        double dataSize;
        if (stats.getAvgSizeBytes() >= 0) {
            dataSize = Math.round(stats.getAvgSizeBytes() * count);
        } else {
            // Long arithmetic (count * slotSize) exactly like legacy setStatData, then widened.
            dataSize = count * column.getType().getSlotSize();
        }
        ColumnStatisticBuilder builder = new ColumnStatisticBuilder(count);
        builder.setNdv(stats.getNdv());
        builder.setNumNulls(stats.getNumNulls());
        builder.setDataSize(dataSize);
        builder.setAvgSizeByte(dataSize / count);
        return Optional.of(builder.build());
    }

    @Override
    public long fetchRowCount() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildCrossStatementSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);

        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return UNKNOWN_ROW_COUNT;
        }

        Optional<ConnectorTableStatistics> statsOpt = metadata.getTableStatistics(session, handleOpt.get());
        if (statsOpt.isPresent()) {
            ConnectorTableStatistics stats = statsOpt.get();
            if (stats.getRowCount() >= 0) {
                return stats.getRowCount();
            }
            // The connector surfaced an on-disk data size but no exact row count (e.g. a hive table with
            // totalSize set but no numRows). Estimate the cardinality as dataSize / <average Doris row
            // width> — the Doris-type-dependent division the connector cannot perform (it must not import
            // fe-type). Connector-agnostic: every other connector reports dataSize -1, so this branch is
            // inert for them. Mirrors legacy StatisticsUtil.getHiveRowCount's totalSize/estimatedRowSize
            // path (row width summed over the FULL schema, partition columns included, exactly as legacy).
            // A quotient that truncates to 0 is NOT a valid "empty table" answer — legacy collapsed 0 ->
            // UNKNOWN and fell through to the file-list estimate below, so only a positive quotient returns.
            if (stats.getDataSize() > 0) {
                long rowWidth = estimatedRowWidth(false);
                if (rowWidth > 0) {
                    long rows = stats.getDataSize() / rowWidth;
                    if (rows > 0) {
                        return rows;
                    }
                }
            }
        }

        // Neither an exact count nor a metastore-recorded size: estimate the on-disk data size by listing
        // the table's data files (connector-provided; every non-file connector returns -1) and divide by the
        // row width, this time EXCLUDING partition columns because their values live in the directory path,
        // not the data files. Mirrors legacy HMSExternalTable.getRowCountFromFileList. Gated by the global
        // feature flag because the listing can be a costly remote round-trip; the connector self-samples,
        // pins its classloader, and degrades to -1 rather than throwing.
        if (GlobalVariable.enable_get_row_count_from_file_list) {
            long dataSize = metadata.estimateDataSizeByListingFiles(session, handleOpt.get());
            if (dataSize > 0) {
                long rowWidth = estimatedRowWidth(true);
                if (rowWidth > 0) {
                    // 0 -> UNKNOWN (legacy getRowCountFromFileList's `rows > 0 ? rows : UNKNOWN` gate).
                    long rows = dataSize / rowWidth;
                    if (rows > 0) {
                        return rows;
                    }
                }
            }
        }
        return UNKNOWN_ROW_COUNT;
    }

    @Override
    public long getRowCount() {
        // Time-travel row count: the shared cross-statement row-count cache is keyed by table only and
        // computes at the LATEST snapshot, so a FOR VERSION/TIME AS OF (or @branch/@tag) read would get the
        // latest cardinality while its scan reads the pinned snapshot -> skewed CBO estimate (estimate-only;
        // results stay correct). When THIS statement pins a genuine versioned snapshot for this table, compute
        // the row count directly AT that snapshot, bypassing the latest-keyed shared cache (a historical count
        // is not worth caching cross-statement). A plain/latest read has no versioned pin and keeps the cached
        // path unchanged; a call with no statement context (e.g. background stats) also keeps it.
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getStatementContext() != null) {
            Optional<MvccSnapshot> versioned = ctx.getStatementContext().getVersionedSnapshot(this);
            if (versioned.isPresent() && versioned.get() instanceof PluginDrivenMvccSnapshot) {
                long rows = fetchRowCountAtSnapshot(
                        ((PluginDrivenMvccSnapshot) versioned.get()).getConnectorSnapshot());
                if (rows != UNKNOWN_ROW_COUNT) {
                    return rows;
                }
                // The connector could not count at the snapshot -> fall through to the latest cached estimate.
            }
        }
        return super.getRowCount();
    }

    /**
     * Computes the row count AT a pinned snapshot for a time-travel read: mirrors the exact-count branch of
     * {@link #fetchRowCount()} but threads the snapshot into the 3-arg {@code getTableStatistics}. Runs in the
     * query thread (not the background cache loader, which has no statement context), so it is deliberately
     * NOT cached &mdash; the handful of versioned reads per statement is cheap. Returns
     * {@link #UNKNOWN_ROW_COUNT} when the connector cannot serve an exact count at the snapshot. Ordinary
     * tables may fall back to the latest estimate; system relations must preserve UNKNOWN to avoid snapshot
     * skew, so the decision belongs to the caller.
     */
    protected long fetchRowCountAtSnapshot(ConnectorMvccSnapshot snapshot) {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildCrossStatementSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> handleOpt = resolveConnectorTableHandle(session, metadata);
        if (!handleOpt.isPresent()) {
            return UNKNOWN_ROW_COUNT;
        }
        Optional<ConnectorTableStatistics> statsOpt =
                metadata.getTableStatistics(session, handleOpt.get(), snapshot);
        if (statsOpt.isPresent() && statsOpt.get().getRowCount() >= 0) {
            return statsOpt.get().getRowCount();
        }
        return UNKNOWN_ROW_COUNT;
    }

    /**
     * Sum of Doris slot sizes over the full schema (or over the non-partition columns when
     * {@code excludePartitionColumns}) — the average uncompressed row width used to turn a connector-reported
     * on-disk data size into an estimated row count. Mirrors the two legacy hive formulas: a metastore
     * {@code totalSize} is divided by the FULL-schema width ({@code StatisticsUtil.getHiveRowCount}), whereas
     * a file-listed size is divided by the width EXCLUDING partition columns (whose values are not stored in
     * the data files, {@code HMSExternalTable.getRowCountFromFileList}). Returns 0 for an empty/unavailable
     * schema, which {@link #fetchRowCount} treats as "cannot estimate" (-> UNKNOWN).
     */
    private long estimatedRowWidth(boolean excludePartitionColumns) {
        List<Column> schema = getFullSchema();
        if (schema == null) {
            return 0;
        }
        List<Column> partitionColumns = excludePartitionColumns ? getPartitionColumns() : null;
        long rowWidth = 0;
        for (Column column : schema) {
            if (partitionColumns != null && partitionColumns.contains(column)) {
                continue;
            }
            rowWidth += column.getDataType().getSlotSize();
        }
        return rowWidth;
    }

    /**
     * The engine name shown in the {@code ENGINE} column of {@code SHOW TABLE STATUS} and
     * {@code information_schema.tables} (and through the REST metadata API). Named by the connector, which
     * defaults it to the catalog type; the engine keeps no mapping from data source to displayed name.
     *
     * <p>Falls back to the generic {@code Plugin} only for a table whose catalog is not plugin-driven, which
     * no production path builds.</p>
     */
    @Override
    public String getEngine() {
        return catalog instanceof PluginDrivenExternalCatalog
                ? ((PluginDrivenExternalCatalog) catalog).getDisplayEngineName()
                : super.getEngine();
    }

    /**
     * What {@code SHOW CREATE TABLE} prints after {@code ENGINE=}. Deliberately the same string as
     * {@link #getEngine()}: one connector, one engine name, however the user reaches it.
     *
     * <p>It is display only, and was never round-trippable — an HMS catalog prints {@code hms} here while the
     * name it accepts back in {@code CREATE TABLE ... ENGINE=} is {@code hive}. Connectors that render their
     * own DDL ({@code ConnectorTableMetadataOps#renderShowCreateTableDdl}, which hive does) never reach this
     * at all; their statement carries no {@code ENGINE=} clause.</p>
     */
    @Override
    public String getEngineTableTypeName() {
        return getEngine();
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);

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
