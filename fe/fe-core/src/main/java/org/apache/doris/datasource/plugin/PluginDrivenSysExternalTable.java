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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.catalog.Column;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorCapability;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.datasource.SchemaCacheKey;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.systable.SysTable;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Generic {@link PluginDrivenExternalTable} for a connector system table (e.g. {@code tbl$snapshots}).
 *
 * <p>Created transiently by {@link org.apache.doris.datasource.systable.PluginDrivenSysTable} during
 * planning/describe (via {@code createSysExternalTable}); it is NEVER added to a persisted table map
 * and is NOT GSON-registered, mirroring legacy sys ExternalTables (e.g.
 * {@code PaimonSysExternalTable}).</p>
 *
 * <p>It reports {@link org.apache.doris.catalog.TableIf.TableType#PLUGIN_EXTERNAL_TABLE} (inherited);
 * no connector-specific table type is introduced. The whole schema/partition/row-count path is reused
 * from the base class; the only behavioral change is {@link #resolveConnectorTableHandle}, which threads
 * the connector's system-table handle (not the base handle) through every base-class site.</p>
 */
public class PluginDrivenSysExternalTable extends PluginDrivenExternalTable {

    private final PluginDrivenExternalTable sourceTable;
    private final String sysTableName;
    private volatile Optional<SchemaCacheValue> cachedSchemaValue;
    /** See {@link #resolveScanPin}: one resolution per selector, shared by binding and scanning. */
    private final Map<String, Optional<MvccSnapshot>> scanPinMemo = new ConcurrentHashMap<>();

    /**
     * @param source the underlying base table being wrapped
     * @param sysName the bare system-table name (e.g. "snapshots"), no "$" prefix
     */
    public PluginDrivenSysExternalTable(PluginDrivenExternalTable source, String sysName) {
        super(generateSysTableId(source.getId(), sysName),
                source.getName() + "$" + sysName,
                source.getRemoteName() + "$" + sysName,
                source.getCatalog(),
                source.getDb());
        this.sourceTable = source;
        this.sysTableName = sysName;
    }

    /**
     * Generate a unique ID from the source table ID and system table name (legacy parity with
     * {@code PaimonSysExternalTable.generateSysTableId}).
     */
    private static long generateSysTableId(long sourceTableId, String sysName) {
        return sourceTableId ^ (sysName.hashCode() * 31L);
    }

    /**
     * Resolve the connector handle for THIS system table: first acquire the BASE table handle using the
     * source's remote name (NOT this sys table's "$"-suffixed remote name), then ask the connector for
     * the system-table handle. Returning the sys handle here threads it through
     * {@code initSchema}/{@code getNameToPartitionItems}/{@code fetchRowCount} automatically, so a sys
     * query reads the sys table rather than the base.
     */
    @Override
    public Optional<ConnectorTableHandle> resolveConnectorTableHandle(
            ConnectorSession session, ConnectorMetadata metadata) {
        String dbName = db != null ? db.getRemoteName() : "";
        Optional<ConnectorTableHandle> baseHandle =
                metadata.getTableHandle(session, dbName, sourceTable.getRemoteName());
        if (!baseHandle.isPresent()) {
            return Optional.empty();
        }
        return metadata.getSysTableHandle(session, baseHandle.get(), sysTableName);
    }

    /**
     * A system/metadata table (e.g. {@code tbl$snapshots}) is never a view. Short-circuit to {@code false}
     * so the base {@code resolveIsView} does not issue a {@code viewExists} round-trip on this synthetic
     * {@code "$"}-suffixed name (which would be wasted work and could fail on an unparseable identifier).
     */
    @Override
    protected boolean resolveIsView() {
        return false;
    }

    /**
     * A system/metadata table (e.g. {@code tbl$snapshots}) can NEVER take part in Top-N lazy materialization,
     * regardless of what the connector declares. Lazy materialization reads the sort key plus the engine-wide
     * row-id ({@code __DORIS_GLOBAL_ROWID_COL__}) first, then re-fetches the surviving rows' other columns by
     * row-id — which requires a file+position row-id. A system table is served by the connector's JNI
     * serialized-split metadata reader, which synthesizes rows from table metadata and produces no such row-id,
     * so the injected row-id column comes back empty and BE aborts the scan
     * ({@code __DORIS_GLOBAL_ROWID_COL__... return column size 0 not equal to expected size 1}).
     *
     * <p>This restores legacy parity: legacy sys tables ({@code IcebergSysExternalTable} /
     * {@code PaimonSysExternalTable}) extend {@code ExternalTable} — NOT the base file-scan table class — so
     * they are absent from {@code MaterializeProbeVisitor.SUPPORT_RELATION_TYPES} and were never lazy-
     * materialized. The base {@link PluginDrivenExternalTable#supportsTopNLazyMaterialize()} keys off the
     * connector capability alone and would otherwise (wrongly) admit a flipped sys table; this override is the
     * sys-table opt-out. Nested-column prune is similarly opted out for sys tables — see
     * {@link #supportsNestedColumnPrune()}.
     */
    @Override
    public boolean supportsTopNLazyMaterialize() {
        return false;
    }

    /**
     * Whether THIS system table takes part in nested-column pruning — resolved from its OWN schema alone,
     * never from the connector-wide set (which the base class would also accept).
     *
     * <p>The answer is per system table because it is really a question about which reader serves it. A
     * metadata table (e.g. {@code tbl$snapshots}) is served by the connector's JNI metadata reader, which
     * indexes its record by the Doris child position ({@code IcebergSysTableColumnValue.unpackStruct}), so
     * handing it a pruned type makes it return a different field's value; and its scan intentionally ships no
     * field-id dictionary ({@code IcebergScanPlanProvider} skips {@code SCHEMA_EVOLUTION_PROP} when
     * {@code systemTable}). It must stay out no matter what its connector declares catalog-wide, which is
     * what declaring nothing per-table achieves.
     *
     * <p>A system table served by the ORDINARY data readers is a different case: fluss's {@code tbl$lake} is
     * the whole lake table read through the paimon sibling and {@code tbl$log} is the log half read through
     * the fluss scanner. Both honour a pruned type, and both are as large as the front door — leaving them
     * out costs exactly the read amplification pruning exists to avoid, and makes one query answer
     * differently through {@code tbl} than through {@code tbl$lake}. Those opt in by declaring
     * {@link ConnectorCapability#SUPPORTS_SYS_TABLE_NESTED_COLUMN_PRUNE} on their own
     * {@link ConnectorTableSchema}.
     *
     * <p>That opt-in deliberately does NOT reuse the data table's {@code SUPPORTS_NESTED_COLUMN_PRUNE}, which
     * reaches a system table's own schema for real: {@code HiveConnectorMetadata.reflectSiblingCapabilities}
     * copies the owning sibling's connector-wide set onto every schema it forwards, and an iceberg-on-HMS
     * {@code tbl$snapshots} is forwarded through exactly that path. Keying on it would admit the one reader
     * that cannot take a pruned type.
     *
     * <p>Historically this returned false unconditionally, because the prune capability also switched on the
     * name-to-field-id access-path rewrite ({@code SlotTypeReplacer.replaceAccessPathToFieldId}) and a
     * system-table scan ships no field-id dictionary, so BE rejected the rewritten path with
     * {@code AccessPathParser access path N does not match slot X}. That rewrite now has its own capability
     * ({@code SUPPORTS_FIELD_ID_ACCESS_PATH}), which no system table declares, so the blanket opt-out is no
     * longer what keeps it off.
     */
    @Override
    public boolean supportsNestedColumnPrune() {
        return tableCapabilities().contains(ConnectorCapability.SUPPORTS_SYS_TABLE_NESTED_COLUMN_PRUNE);
    }

    /**
     * Compute the schema directly on this transient instance instead of going through the base
     * {@link ExternalTable#getSchemaCacheValue()}, which routes through {@code ExternalCatalog.getSchema()}
     * and re-resolves the table by name in the db map. A system table (e.g. {@code tbl$snapshots}) is never
     * registered in that map, so the base path fails with "failed to load schema cache value". Memoized
     * (double-checked) to avoid repeated connector round-trips, mirroring legacy
     * {@code PaimonSysExternalTable.getSchemaCacheValue}. {@code initSchema()} (inherited from
     * {@link PluginDrivenExternalTable}) honors this class's {@link #resolveConnectorTableHandle}, so it
     * resolves the system-table schema rather than the base table's.
     */
    @Override
    public Optional<SchemaCacheValue> getSchemaCacheValue() {
        if (cachedSchemaValue == null) {
            synchronized (this) {
                if (cachedSchemaValue == null) {
                    cachedSchemaValue = initSchema();
                }
            }
        }
        return cachedSchemaValue;
    }

    @Override
    public Optional<SchemaCacheValue> initSchema(SchemaCacheKey key) {
        return getSchemaCacheValue();
    }

    /**
     * Delegate to the source table so DESCRIBE/SHOW on a system table still lists its sibling system
     * tables (legacy parity with {@code PaimonSysExternalTable.getSupportedSysTables}).
     */
    @Override
    public Map<String, SysTable> getSupportedSysTables() {
        return sourceTable.getSupportedSysTables();
    }

    @Override
    public String getComment() {
        return "Plugin system table: " + sysTableName + " for " + sourceTable.getName();
    }

    public PluginDrivenExternalTable getSourceTable() {
        return sourceTable;
    }

    public String getSysTableName() {
        return sysTableName;
    }

    /**
     * This reference's own pin, or empty when it has none / must not have one.
     *
     * <p>A system table is NOT an {@link MvccTable} and {@code BindRelation} returns from
     * {@code handleMetaTable} BEFORE {@code StatementContext.loadSnapshots}, so the statement's MVCC map
     * never holds an entry for it and {@code MvccUtil.getSnapshotFromContext} answers empty. The pin lives
     * on the SOURCE table, so resolve it from there.
     *
     * <p><b>Memoized per (tableSnapshot, scanParams) on this instance</b>, which is what keeps binding and
     * scanning on ONE resolution. The instance is built per relation by
     * {@code PluginDrivenSysTable.createSysExternalTable} and then carried on the {@code LogicalFileScan}
     * into the scan node, so every consumer — {@code LogicalFileScan.computePluginDrivenOutput},
     * {@code PluginDrivenScanNode.resolveSysTableSnapshotPin}, {@code PluginDrivenScanNode.buildColumnHandles}
     * — shares one entry. Resolving independently per consumer would re-open the very skew this closes:
     * a MUTABLE selector ({@code scan.mode=latest}, a wall-clock {@code scan.timestamp-millis}) is resolved
     * against the LIVE table on each call, so a commit landing between bind and scan would hand the two
     * different versions.
     *
     * <p>Returns empty — i.e. falls back to the latest schema — whenever the connector declines this
     * selector on this view, so {@code PluginDrivenScanNode.checkSysTableScanConstraints} keeps ownership of
     * the user-facing rejection instead of this path failing first with a worse message.
     */
    public Optional<MvccSnapshot> resolveScanPin(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams) {
        return resolveScanPin(tableSnapshot, scanParams,
                () -> Optional.of(((MvccTable) sourceTable).loadSnapshot(tableSnapshot, scanParams)));
    }

    /** Resolve through a caller-owned statement fence while preserving this relation's memo. */
    public Optional<MvccSnapshot> resolveScanPin(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams,
            Supplier<Optional<MvccSnapshot>> snapshotLoader) {
        if (!tableSnapshot.isPresent() && !scanParams.isPresent()) {
            return Optional.empty();
        }
        if (!(sourceTable instanceof MvccTable)) {
            return Optional.empty();
        }
        return scanPinMemo.computeIfAbsent(pinKeyOf(tableSnapshot, scanParams), key -> {
            if (!selectorSupported(scanParams)) {
                return Optional.empty();
            }
            // Binding and scanning must share the StatementContext fence; resolving a mutable latest
            // selector directly here can make a system alias observe a later commit than a plain alias.
            return snapshotLoader.get();
        });
    }

    @Override
    public long getRowCount() {
        for (Optional<MvccSnapshot> pin : scanPinMemo.values()) {
            if (pin.isPresent() && pin.get() instanceof PluginDrivenMvccSnapshot) {
                ConnectorMvccSnapshot connectorSnapshot =
                        ((PluginDrivenMvccSnapshot) pin.get()).getConnectorSnapshot();
                if (connectorSnapshot != null) {
                    // A system relation keeps its pin in this private memo, not StatementContext; falling
                    // back to the latest cache would give CBO cardinality from a different snapshot.
                    return fetchRowCountAtSnapshot(connectorSnapshot);
                }
            }
        }
        return super.getRowCount();
    }

    /**
     * The full schema of this system table AS OF {@code tableSnapshot}/{@code scanParams}, falling back to
     * the latest schema when this reference carries no pin.
     *
     * <p>Several metadata views derive their columns from the base table ({@code $audit_log} is
     * {@code rowkind} plus the base row type; so are {@code $ro} and {@code $binlog}), so their schema moves
     * with the selected snapshot. Binding them from the latest schema while the scan reads the pinned one
     * made a since-renamed column fail to bind at all and — worse — bound a since-retyped column silently at
     * the wrong type.
     */
    public List<Column> getFullSchemaAt(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams) {
        Optional<MvccSnapshot> pin = resolveScanPin(tableSnapshot, scanParams);
        if (!pin.isPresent()) {
            return getFullSchema();
        }
        return schemaCacheValueAt(pin.get())
                .map(SchemaCacheValue::getSchema)
                .orElseGet(this::getFullSchema);
    }

    /**
     * Reads this view's schema with {@code pin} threaded onto the SYS handle. Deliberately does NOT go
     * through {@link #getSchemaCacheValue()}: that memo is the LATEST schema and is shared by the
     * version-blind callers (DESCRIBE, {@code information_schema}).
     */
    private Optional<SchemaCacheValue> schemaCacheValueAt(MvccSnapshot pin) {
        if (!(pin instanceof PluginDrivenMvccSnapshot) || !(catalog instanceof PluginDrivenExternalCatalog)) {
            return Optional.empty();
        }
        ConnectorMvccSnapshot connectorSnapshot = ((PluginDrivenMvccSnapshot) pin).getConnectorSnapshot();
        if (connectorSnapshot == null) {
            return Optional.empty();
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return Optional.empty();
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> sysHandle = resolveConnectorTableHandle(session, metadata);
        if (!sysHandle.isPresent()) {
            return Optional.empty();
        }
        String dbName = db != null ? db.getRemoteName() : "";
        ConnectorTableSchema schema =
                metadata.getTableSchema(session, sysHandle.get(), connectorSnapshot);
        return Optional.of(toSchemaCacheValue(metadata, session, dbName, getRemoteName(), schema));
    }

    /**
     * Whether the connector honors THIS selector on THIS view — the exact mirror of
     * {@code PluginDrivenScanNode.sysTableSelectorSupported}, so a pin is resolved for precisely the queries
     * that guard lets through. A connector with no scan provider contributes no capability and declines.
     *
     * <p>Package-private + overridable so {@link #resolveScanPin} stays unit-testable without a live
     * connector, the same reason {@code PluginDrivenScanNode}'s capability questions are.
     */
    boolean selectorSupported(Optional<TableScanParams> scanParams) {
        String bareName = sysTableName == null ? "" : sysTableName.toLowerCase(Locale.ROOT);
        if (!scanParams.isPresent()) {
            return askScanProvider(ConnectorScanPlanProvider::supportsSystemTableTimeTravel);
        }
        TableScanParams params = scanParams.get();
        if (params.incrementalRead()) {
            return askScanProvider(p -> p.supportsSystemTableIncrementalRead(bareName));
        }
        if (params.isOptions()) {
            return askScanProvider(p -> p.supportsSystemTableOptions(bareName));
        }
        return askScanProvider(ConnectorScanPlanProvider::supportsSystemTableTimeTravel);
    }

    private boolean askScanProvider(Predicate<ConnectorScanPlanProvider> question) {
        if (!(catalog instanceof PluginDrivenExternalCatalog)) {
            return false;
        }
        PluginDrivenExternalCatalog pluginCatalog = (PluginDrivenExternalCatalog) catalog;
        Connector connector = pluginCatalog.getConnector();
        if (connector == null) {
            return false;
        }
        ConnectorSession session = pluginCatalog.buildConnectorSession();
        ConnectorMetadata metadata = PluginDrivenMetadata.get(session, connector);
        Optional<ConnectorTableHandle> sysHandle = resolveConnectorTableHandle(session, metadata);
        if (!sysHandle.isPresent()) {
            return false;
        }
        ConnectorScanPlanProvider scanProvider = connector.getScanPlanProvider(sysHandle.get());
        if (scanProvider == null) {
            return false;
        }
        return onPluginClassLoader(scanProvider, () -> question.test(scanProvider));
    }

    private static <T> T onPluginClassLoader(ConnectorScanPlanProvider provider, Supplier<T> body) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(provider.getClass().getClassLoader());
            return body.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * The memo key for one reference's selector, derived exactly the way
     * {@code StatementContext.versionKeyOf} derives its version key — field by field.
     *
     * <p>It must NOT be the selector objects themselves, nor their {@code toString()}:
     * {@link TableScanParams} defines neither value equality nor {@code toString()}, so an
     * identity-keyed memo would miss on every lookup and silently reintroduce the double resolution
     * {@link #resolveScanPin} exists to prevent.
     */
    private static String pinKeyOf(Optional<TableSnapshot> tableSnapshot,
            Optional<TableScanParams> scanParams) {
        StringBuilder key = new StringBuilder();
        if (tableSnapshot != null && tableSnapshot.isPresent()) {
            TableSnapshot ts = tableSnapshot.get();
            key.append("v:").append(ts.getType()).append(':').append(ts.getValue());
        }
        if (scanParams != null && scanParams.isPresent()) {
            TableScanParams sp = scanParams.get();
            key.append("p:").append(sp.getParamType()).append(':').append(sp.getMapParams())
                    .append(':').append(sp.getListParams());
        }
        return key.toString();
    }
}
