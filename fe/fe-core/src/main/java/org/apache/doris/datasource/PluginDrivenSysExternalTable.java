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

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.datasource.systable.SysTable;

import java.util.Map;
import java.util.Optional;

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
    protected Optional<ConnectorTableHandle> resolveConnectorTableHandle(
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
     * sys-table opt-out. Nested-column prune is intentionally NOT overridden — legacy DOES prune nested columns
     * on sys tables ({@code LogicalFileScan.supportPruneNestedColumn} lists {@code IcebergSysExternalTable}), so
     * that capability stays inherited.
     */
    @Override
    public boolean supportsTopNLazyMaterialize() {
        return false;
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
}
