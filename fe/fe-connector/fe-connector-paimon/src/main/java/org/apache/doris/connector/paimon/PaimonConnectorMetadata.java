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

import org.apache.doris.connector.cache.ConnectorMetadataCache;
import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorTableStatistics;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPath;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.ConnectorTransaction;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaValidation;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.DateTimeUtils;
import org.apache.paimon.utils.PartitionPathUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
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
    private final PaimonCatalogProperties catalogProperties;

    // FIX-B-MC2: time-travel schema-at-snapshot memo. Injected by PaimonConnector (the per-catalog,
    // long-lived owner) so the at-snapshot resolve hits across queries. The public 3-arg ctor gives each
    // metadata its OWN fresh memo (no cross-query benefit, but correct) so the ~15 existing construction
    // sites compile unchanged; production goes through the 4-arg ctor with the connector-shared memo.
    private final PaimonSchemaAtMemo schemaAtMemo;

    // FIX-4: per-catalog latest-snapshot-id cache (injected by PaimonConnector, the long-lived owner) so the
    // query-begin pin serves a STABLE snapshot id across queries within the TTL (restores the legacy table
    // cache). The 3-arg / 4-arg ctors give each metadata its OWN disabled cache (ttl<=0 => always live) so the
    // existing direct-construction tests compile unchanged; production goes through the 5-arg ctor.
    private final PaimonLatestSnapshotCache latestSnapshotCache;

    // PERF-06: cross-query DERIVED partition-view cache A (generic ConnectorMetadataCache), injected by the
    // owning PaimonConnector; null = no cross-query derived layer (the convenience/test ctors used by ~15
    // existing direct-construction tests pass null). Layered ABOVE the raw remote catalogOps.listPartitions
    // call: a hit skips both the derived-view BUILD (collectPartitions) and the remote round-trip, keyed by
    // (db, table, snapshotId, schemaId). Consumed by both partition-enumeration hooks (listPartitions,
    // listPartitionNames) via the shared cachedPartitions collector -- paimon does not
    // override getMvccPartitionView (see ConnectorMetadata's default), so the generic MTMV model already uses
    // listPartitions for its LIST/timestamp partition view.
    private final ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCache;

    public PaimonConnectorMetadata(PaimonCatalogOps catalogOps, PaimonCatalogProperties properties,
            ConnectorContext context) {
        this(catalogOps, properties, context, new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE));
    }

    PaimonConnectorMetadata(PaimonCatalogOps catalogOps, PaimonCatalogProperties properties,
            ConnectorContext context, PaimonSchemaAtMemo schemaAtMemo) {
        this(catalogOps, properties, context, schemaAtMemo, new PaimonLatestSnapshotCache(0L, 1));
    }

    /** Convenience ctor without the PERF-06 derived partition-view cache (null -> listPartitions always live). */
    PaimonConnectorMetadata(PaimonCatalogOps catalogOps, PaimonCatalogProperties properties,
            ConnectorContext context, PaimonSchemaAtMemo schemaAtMemo,
            PaimonLatestSnapshotCache latestSnapshotCache) {
        this(catalogOps, properties, context, schemaAtMemo, latestSnapshotCache, null);
    }

    /**
     * Full ctor used by {@link PaimonConnector#getMetadata}, adding the PERF-06 derived partition-view cache
     * (cache A): {@code partitionViewCache} memoizes {@link #listPartitions}'s built
     * {@code List<ConnectorPartitionInfo>}, keyed by {@code (db, table, snapshotId, schemaId)}. {@code null}
     * for the convenience/test ctors (no cross-query derived layer -&gt; compute directly every call).
     */
    PaimonConnectorMetadata(PaimonCatalogOps catalogOps, PaimonCatalogProperties properties,
            ConnectorContext context, PaimonSchemaAtMemo schemaAtMemo,
            PaimonLatestSnapshotCache latestSnapshotCache,
            ConnectorMetadataCache<List<ConnectorPartitionInfo>> partitionViewCache) {
        this.catalogOps = catalogOps;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
        this.context = context;
        this.catalogProperties = properties;
        this.schemaAtMemo = schemaAtMemo;
        this.latestSnapshotCache = latestSnapshotCache;
        this.partitionViewCache = partitionViewCache;
    }

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        // M-11: wrap the remote read in executeAuthenticated so the FE-injected Kerberos UGI applies (legacy
        // PaimonMetadataOps.listDatabaseNames wrapped it too). On failure, rethrow with the catalog name exactly
        // as legacy PaimonMetadataOps did (R3) — swallowing to an empty list would mask a transient metastore
        // failure as "zero databases" and diverges from every other connector (all propagate). Read-vs-DDL
        // parity (D-052).
        try {
            return context.executeAuthenticated(() -> catalogOps.listDatabases());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to list databases names, catalog name: " + context.getCatalogName(), e);
        }
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        // M-11: wrap the remote read in executeAuthenticated (D-052). DatabaseNotExistException is
        // caught INSIDE the lambda: under Kerberos UGI.doAs would otherwise wrap the checked
        // exception in UndeclaredThrowableException, so an outer catch would not match.
        try {
            return context.executeAuthenticated(() -> {
                try {
                    catalogOps.getDatabase(dbName);
                    return true;
                } catch (Catalog.DatabaseNotExistException e) {
                    return false;
                }
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to check Paimon database existence " + dbName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        // M-11: wrap the remote read in executeAuthenticated (D-052). DatabaseNotExistException is
        // caught INSIDE the lambda (Kerberos UGI.doAs would wrap it otherwise); other failures fall
        // to the outer catch, preserving the original empty-list-on-error behavior.
        try {
            return context.executeAuthenticated(() -> {
                try {
                    return catalogOps.listTables(dbName);
                } catch (Catalog.DatabaseNotExistException e) {
                    LOG.warn("Database does not exist: {}", dbName);
                    return Collections.<String>emptyList();
                }
            });
        } catch (Exception e) {
            LOG.warn("Failed to list tables in database: {}", dbName, e);
            return Collections.emptyList();
        }
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        Identifier identifier = Identifier.create(dbName, tableName);
        // M-11: wrap the remote getTable in executeAuthenticated (D-052). TableNotExistException is
        // caught INSIDE the lambda (Kerberos UGI.doAs would wrap it otherwise) and yields an empty
        // handle, exactly as before; the trailing handle build is pure (no remote call).
        try {
            return context.executeAuthenticated(() -> {
                Table table;
                try {
                    table = catalogOps.getTable(identifier);
                } catch (Catalog.TableNotExistException e) {
                    return Optional.<ConnectorTableHandle>empty();
                }
                List<String> partitionKeys = table.partitionKeys();
                List<String> primaryKeys = table.primaryKeys();
                PaimonTableHandle handle = new PaimonTableHandle(
                        dbName, tableName,
                        partitionKeys != null ? partitionKeys : Collections.emptyList(),
                        primaryKeys != null ? primaryKeys : Collections.emptyList());
                handle.setPaimonTable(table);
                return Optional.<ConnectorTableHandle>of(handle);
            });
        } catch (Exception e) {
            LOG.warn("Failed to get Paimon table handle: {}.{}", dbName, tableName, e);
            return Optional.empty();
        }
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        // resolveTable branches on isSystemTable() to pick the 4-arg sys Identifier vs the 2-arg
        // base Identifier on a transient-table-null reload, so a sys handle reads its OWN rowType.
        Table table = resolveTable(paimonHandle);
        // For a non-system data table, read the LATEST schema FRESH via the connector's schema manager
        // (schemaManager().latest()), NOT the cached Table's rowType(): paimon's CachingCatalog returns a
        // Table instance whose rowType() is FROZEN at load time, while an external ALTER ADD COLUMNS bumps
        // the schema file (new schema id) WITHOUT a new snapshot — so rowType() (and the latest snapshot's
        // schemaId) stay behind while schemaManager().latest() advances. Reading latest restores legacy
        // PaimonExternalTable parity so a no-cache catalog (meta.cache.paimon.table.ttl-second=0) — and a
        // with-cache catalog after REFRESH busts the FE schema cache — reflects the external schema change.
        // partitionKeys/primaryKeys also come from the resolved latest schema (parity with the at-snapshot
        // path; the handle's keys were built from the stale cached table). latestSchema() is empty for a
        // non-DataTable backend (e.g. FormatTable) or a schema-less table -> fall back to rowType(). System
        // tables (isSystemTable()) always keep their synthetic rowType() (no schema-version history; some
        // are not DataTable). Sharing buildTableSchema with the at-snapshot path keeps the two from drifting.
        if (!paimonHandle.isSystemTable()) {
            Optional<PaimonCatalogOps.PaimonSchemaSnapshot> latest = catalogOps.latestSchema(table);
            if (latest.isPresent()) {
                PaimonCatalogOps.PaimonSchemaSnapshot schema = latest.get();
                return buildTableSchema(
                        paimonHandle.getTableName(),
                        table,
                        schema.fields(),
                        schema.partitionKeys(),
                        schema.primaryKeys());
            }
        }
        return buildTableSchema(
                paimonHandle.getTableName(),
                table,
                table.rowType().getFields(),
                paimonHandle.getPartitionKeys(),
                table.primaryKeys());
    }

    /**
     * Returns the schema AS OF {@code snapshot.getSchemaId()} (the pinned schema version, for
     * time-travel reads under schema evolution). Falls back to the LATEST schema
     * ({@link #getTableSchema(ConnectorSession, ConnectorTableHandle)}) when there is no pinned
     * schema id (null snapshot or {@code schemaId < 0}), which also covers system tables (their
     * synthetic rowType is their own and has no schema-version history).
     *
     * <p>When a pinned schema id IS present, the schema at that version is resolved through the
     * {@link PaimonCatalogOps#schemaAt} seam and mapped with the SAME field mapping AND the same
     * {@code partition_columns}/{@code primary_keys} property emission as the latest path (via the
     * shared {@link #buildTableSchema}). Unlike the latest path, the partition keys come from the
     * RESOLVED historical schema (not the handle), because under schema evolution the partition set
     * may itself differ at the pinned version — mirroring legacy {@code initSchema(schemaId)}, which
     * read {@code tableSchema.partitionKeys()} of the pinned schema.
     */
    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        if (paimonHandle.isSystemTable()) {
            return systemTableSchemaAt(session, paimonHandle, snapshot);
        }
        if (snapshot == null || snapshot.getSchemaId() < 0) {
            return getTableSchema(session, handle);
        }
        long schemaId = snapshot.getSchemaId();
        // Resolve the table AT the snapshot's identity: applySnapshot routes a @branch read's
        // CoreOptions.BRANCH sentinel to withBranch, so schemaAt reads the branch's OWN schema dir
        // (.../branch/branch-<name>/schema/schema-<id>) rather than the base table's. Mirrors the
        // apply-before-resolve already in getTableStatistics(3-arg) / getPartitions. For a version/tag/time
        // pin (no branch sentinel) applySnapshot only threads scan options resolveTable ignores, so the
        // resolved table -- and its schemaAt read -- is byte-for-byte unchanged.
        PaimonTableHandle pinned = (PaimonTableHandle) applySnapshot(session, paimonHandle, snapshot);
        Table table = resolveTable(pinned);
        // FIX-B-MC2: memoize the schemaAt schema-file read across queries. resolveTable + buildTableSchema
        // still run every query (keeping the live coreOptions/properties current); only the schemaAt
        // round-trip is skipped on a repeat. The memo is keyed by (pinned-handle-identity, schemaId) -- a
        // pure function -- and owned by the per-catalog PaimonConnector. Key on the PINNED handle (which
        // carries branchName in equals/hashCode) so a branch@schemaId and a base@same-schemaId cannot
        // collide in this long-lived memo. resolveTable runs ONCE, outside the loader.
        PaimonCatalogOps.PaimonSchemaSnapshot schema =
                schemaAtMemo.getOrLoad(pinned, schemaId, () -> catalogOps.schemaAt(table, schemaId));
        return buildTableSchema(
                paimonHandle.getTableName(),
                table,
                schema.fields(),
                schema.partitionKeys(),
                schema.primaryKeys());
    }

    /**
     * The schema of a SYSTEM table AS OF {@code snapshot}.
     *
     * <p>A metadata view has no schema-version history, so there is nothing for {@code schemaAt} to read —
     * its schema IS its own {@code rowType()}. But several views DERIVE that rowType from the base table
     * ({@code $audit_log} = {@code rowkind} + the base rowType, plus {@code $ro} / {@code $binlog}), so it
     * follows whichever snapshot the pin selected. Resolve the view with the pin's options applied and map
     * THAT — the same {@code Table.copy} the scan path performs in
     * {@code PaimonScanPlanProvider.resolveScanTable}. Going through {@code schemaAt} instead would return
     * the BASE table's historical fields and drop the view's own columns.
     *
     * <p>Without this arm the view's schema was bound from LATEST while the scan read the pinned snapshot:
     * a column dropped/renamed after the pin failed to bind at all
     * ({@code $audit_log@options('scan.tag-name'=...)} -> "Unknown column"), and — worse — a column whose
     * TYPE changed bound silently at the wrong type. A pin-free call ({@code snapshot == null}, or one
     * carrying no scan options) leaves {@code applySnapshot} a no-op and degrades byte-for-byte to the
     * 2-arg system-table path.
     */
    private ConnectorTableSchema systemTableSchemaAt(ConnectorSession session,
            PaimonTableHandle paimonHandle, ConnectorMvccSnapshot snapshot) {
        Table table = resolveSystemTableAt(session, paimonHandle, snapshot);
        return buildTableSchema(
                paimonHandle.getTableName(),
                table,
                table.rowType().getFields(),
                paimonHandle.getPartitionKeys(),
                table.primaryKeys());
    }

    /**
     * The SYSTEM table resolved with {@code snapshot}'s scan options layered on — the metadata-side twin of
     * {@code PaimonScanPlanProvider.resolveScanTable}, so the schema the query binds and the columns the
     * scan reads come from the SAME view instance. A null snapshot, or one carrying no {@code @options} pin,
     * leaves the resolution byte-for-byte identical to the un-pinned path.
     */
    private Table resolveSystemTableAt(ConnectorSession session,
            PaimonTableHandle paimonHandle, ConnectorMvccSnapshot snapshot) {
        PaimonTableHandle pinned = snapshot == null
                ? paimonHandle
                : (PaimonTableHandle) applySnapshot(session, paimonHandle, snapshot);
        Table table = resolveTable(pinned);
        Map<String, String> scanOptions = pinned.getScanOptions();
        if (scanOptions != null && !scanOptions.isEmpty() && PaimonScanParams.isOptionsPin(scanOptions)) {
            return PaimonScanParams.applyOptions(table, scanOptions);
        }
        return table;
    }

    /**
     * Maps paimon {@code fields} to Doris columns and emits the {@code partition_columns} /
     * {@code primary_keys} schema properties exactly the way the latest path always has. Factored
     * out so the latest path and the at-snapshot path ({@link #getTableSchema(ConnectorSession,
     * ConnectorTableHandle, ConnectorMvccSnapshot)}) share ONE mapping and cannot drift.
     */
    private ConnectorTableSchema buildTableSchema(String tableName, Table table, List<DataField> fields,
            List<String> partitionKeys, List<String> primaryKeys) {
        List<ConnectorColumn> columns = mapFields(fields, primaryKeys);

        // LinkedHashMap so the table-options order (used by SHOW CREATE TABLE's PROPERTIES) is
        // deterministic across runs.
        Map<String, String> schemaProps = new LinkedHashMap<>();
        // D-046: surface the paimon table options (path, file.format, write-only, ...) so SHOW
        // CREATE TABLE can render LOCATION + PROPERTIES with legacy parity. Mirrors legacy
        // PaimonExternalTable.getTableProperties() = coreOptions().toMap() (+ injected primary-key).
        // System tables are not DataTable (legacy getTableProperties returns empty for them), so
        // the coreOptions() / "path" surface is guarded the same way. "path" is already a key inside
        // coreOptions().toMap(), which the fe-core LOCATION render reads. These are plain string keys
        // (no fe-core dependency); the fe-core consumer filters out the schema-control keys below.
        if (table instanceof DataTable) {
            schemaProps.putAll(((DataTable) table).coreOptions().toMap());
            if (primaryKeys != null && !primaryKeys.isEmpty()) {
                schemaProps.put(CoreOptions.PRIMARY_KEY.key(), String.join(",", primaryKeys));
            }
        }
        if (partitionKeys != null && !partitionKeys.isEmpty()) {
            // Emit "partition_columns" (NOT "partition_keys"): the generic fe-core consumer
            // PluginDrivenExternalTable.initSchema reads "partition_columns" — keying it under
            // "partition_keys" left the FE treating paimon as non-partitioned. Mirrors MaxCompute.
            // #65094 read-path alignment: column names are case-preserved above (mapFields/getColumnHandles
            // use bare .name()), and PluginDrivenExternalTable.initSchema matches each partition_columns
            // entry against those column names via a case-sensitive byName lookup (paimon does not override
            // fromRemoteColumnName), so the entries carry the SAME case as the columns to keep the two sides
            // matchable (a mixed-case paimon partition key would otherwise be silently missed and the table
            // treated as non-partitioned).
            schemaProps.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY, String.join(",", partitionKeys));
        }
        return new ConnectorTableSchema(tableName, columns, "PAIMON", schemaProps);
    }

    // ==================== E7: System Tables ====================

    /**
     * Lists the system-table names paimon exposes. Connector-global: legacy
     * {@code PaimonSysTable.SUPPORTED_SYS_TABLES} is built once from
     * {@code SystemTableLoader.SYSTEM_TABLES} and applies to every paimon table, so this returns
     * the same SDK list for any base handle (a defensive unmodifiable copy of the bare names,
     * no {@code "$"} prefix).
     */
    @Override
    public List<String> listSupportedSysTables(ConnectorSession session,
            ConnectorTableHandle baseTableHandle) {
        return Collections.unmodifiableList(new ArrayList<>(SystemTableLoader.SYSTEM_TABLES));
    }

    /**
     * Resolves a handle for the named system table of {@code baseTableHandle}, or empty when
     * paimon does not expose {@code sysName} (case-insensitive, per legacy
     * {@code shouldForceJniForSystemTable}'s {@code equalsIgnoreCase} use) or the base table no
     * longer exists.
     *
     * <p>The system {@link Table} is loaded through the EXISTING {@link PaimonCatalogOps#getTable}
     * seam by constructing the 4-arg sys {@link Identifier}
     * {@code new Identifier(db, table, "main", sysName)} — no new seam method is needed because
     * {@code CatalogBackedPaimonCatalogOps.getTable} passes the Identifier through to
     * {@code catalog.getTable(identifier)} unchanged, and paimon's catalog dispatches to the
     * system table when the Identifier carries a system-table name. The branch is HARDCODED
     * {@code "main"}: non-"main" branch system tables are unsupported (legacy parity, see
     * {@code PaimonSysExternalTable#getSysPaimonTable}).
     *
     * <p>{@code forceJni} mirrors legacy {@code PaimonScanNode.shouldForceJniForSystemTable}: only
     * {@code binlog} / {@code audit_log} / {@code row_tracking} are NAME-forced to the JNI reader (the
     * {@link PaimonScanParams#requiresPaimonReader} set). Other sys tables ("ro", metadata tables) are NOT
     * force-forced here; their JNI-vs-native routing is decided at scan time by split type (T19), so this
     * must not over-force.
     */
    @Override
    public Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        PaimonTableHandle base = (PaimonTableHandle) baseTableHandle;
        // Null-safe: a null/unknown sysName is "this connector does not expose that sys table"
        // (Optional.empty per the Javadoc contract), NOT an NPE/exception.
        if (!isSupportedSysTable(sysName)) {
            return Optional.empty();
        }
        // Normalize to lowercase for handle identity parity with legacy: SysTable renders the suffix
        // as "$" + sysTableName.toLowerCase(), so t$BINLOG and t$binlog must be the SAME handle
        // (identical equals/hashCode/toString and the same sys Identifier). The support check above
        // stays case-insensitive; only the canonical stored name is lowercased.
        String sys = sysName.toLowerCase(java.util.Locale.ROOT);
        // Build the wrapper over the base Table this handle ALREADY carries, exactly the way the
        // Paimon catalog builds it ({@code CatalogUtils#createSystemTable}), and keep that base on
        // the sys handle. Loading the wrapper from the catalog instead would give it its own schema
        // generation, independent of any base a consumer resolves later: the FE would then plan on
        // one generation while PaimonScanPlanProvider serializes to the BE a system table rebuilt
        // over the other one, and a system table whose row type follows the base schema
        // ($audit_log, $binlog, $ro) could reach the BE without the columns the FE asked for.
        //
        // Strictly the ALREADY-RESOLVED reference, never a reload: getTableHandle stashes it one
        // call earlier (PluginDrivenSysExternalTable#resolveConnectorTableHandle resolves the base
        // handle immediately before this), so the normal path needs no round-trip. Resolving it
        // here instead would enter the authenticator a second time and would turn a missing base
        // table into a thrown RuntimeException rather than this method's Optional.empty() contract.
        //
        // "Exactly the way the catalog builds it" includes building it over the UNDECORATED base:
        // CatalogUtils#loadTable hands createSystemTable the raw FileStoreTableFactory#create
        // result, and the decorator a catalog may add afterwards never reaches a system table,
        // because it only wraps a FileStoreTable and no system table is one. That matters for $ro:
        // ReadOptimizedTable#newScan builds its two-branch FallbackReadScan only when its immediate
        // wrapped object is a FallbackReadFileStoreTable, so with a PrivilegedFileStoreTable in
        // between it would plan the main branch alone through the pair's inherited
        // newSnapshotReader() and silently drop every fallback-only partition.
        Table baseTable = base.getPaimonTable();
        FileStoreTable sysBase = baseTable instanceof FileStoreTable
                ? PaimonTableDecorators.unwrapToFallbackOrBase((FileStoreTable) baseTable)
                : null;
        Table sysTable = sysBase == null ? null : SystemTableLoader.load(sys, sysBase);
        if (sysTable == null) {
            // Not a file store table (format / object table): let the catalog decide.
            // M-11: wrap the remote getTable in executeAuthenticated (D-052). TableNotExistException is
            // caught INSIDE the lambda (Kerberos UGI.doAs would wrap it otherwise) and signalled out as a
            // null Table so this method can still short-circuit to Optional.empty().
            Identifier sysId = new Identifier(
                    base.getDatabaseName(), base.getTableName(), "main", sys);
            try {
                sysTable = context.executeAuthenticated(() -> {
                    try {
                        return catalogOps.getTable(sysId);
                    } catch (Catalog.TableNotExistException e) {
                        return null;
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException("Failed to load Paimon system table: " + sysId, e);
            }
            if (sysTable == null) {
                return Optional.empty();
            }
            sysBase = null;
        }
        // #65984 widened the name-forced set to include row_tracking: like binlog/audit_log its rows
        // are materialized by the paimon reader itself, so the native reader would return wrong rows.
        // Single source of truth with the sys-table capability matrix (PaimonScanParams).
        boolean forceJni = PaimonScanParams.requiresPaimonReader(sys);
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable(
                base.getDatabaseName(), base.getTableName(), sys, forceJni);
        handle.setPaimonTable(sysTable);
        handle.setSysBaseTable(sysBase);
        // Validation must retain the decorated relation generation, while sysBaseTable intentionally
        // keeps the undecorated FileStoreTable used to rebuild the wrapper for the backend.
        handle.setSystemTableSource(baseTable);
        return Optional.of(handle);
    }

    private static boolean isSupportedSysTable(String sysName) {
        if (sysName == null) {
            return false;
        }
        for (String supported : SystemTableLoader.SYSTEM_TABLES) {
            if (supported.equalsIgnoreCase(sysName)) {
                return true;
            }
        }
        return false;
    }

    // ==================== E5: MVCC Snapshots / Time Travel ====================

    /**
     * Returns the query-begin MVCC pin: the table's LATEST snapshot, used as the consistent version
     * for every read of {@code handle} in this query (mirrors legacy
     * {@code PaimonExternalTable.getPaimonSnapshotCacheValue} using {@code latestSnapshot().id()}).
     *
     * <p>System tables MUST NOT expose MVCC (they are synthetic metadata views; pinning them to a
     * data snapshot is meaningless — see also the T19 scan-node fail-loud guard), so a sys handle
     * returns {@link Optional#empty()}.
     *
     * <p>An EMPTY table (no snapshot yet) returns a snapshot whose id is the legacy
     * {@code INVALID_SNAPSHOT_ID} (-1), NOT {@link Optional#empty()}: empty here means "no MVCC
     * support", but paimon DOES support MVCC, so the connector still pins (legacy seeded -1 and only
     * overwrote it when {@code latestSnapshot().isPresent()}).
     */
    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        if (paimonHandle.isSystemTable()) {
            return Optional.empty();
        }
        // FIX-4: serve the latest snapshot id through the per-catalog cache so the with-cache catalog pins a
        // STABLE id across queries (an external write made after the pin is invisible until the entry expires
        // or REFRESH TABLE/CATALOG invalidates it). The live read (resolveTable + latestSnapshotId) runs only
        // on a miss; when caching is disabled (ttl-second<=0, the no-cache catalog) it runs every call.
        Identifier identifier = Identifier.create(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        long id = latestSnapshotCache.getOrLoad(identifier,
                () -> catalogOps.latestSnapshotId(resolveTable(paimonHandle)).orElse(-1L));
        return Optional.of(ConnectorMvccSnapshot.builder().snapshotId(id).build());
    }

    @Override
    public boolean usesStatementSnapshotForOptions(
            ConnectorSession session, ConnectorTableHandle handle, Map<String, String> options) {
        // Explicit Paimon selectors own their version and must not be overwritten by a latest fence.
        return PaimonScanParams.usesStatementSnapshot(options);
    }

    /**
     * Resolves an explicit time-travel {@code spec} into a pinned {@link ConnectorMvccSnapshot},
     * owning ALL paimon-specific parsing (snapshot-id lookup, datetime parse, tag resolution). This
     * is the unified seam that supersedes the retired {@code getSnapshotById}/{@code getSnapshotAt}
     * (B5b). The returned snapshot carries (a) the resolved {@code snapshotId}, (b) the resolved
     * {@code schemaId} so schema-at-snapshot reads pick the historical schema, and (c) the
     * connector's scan-option {@code properties} (which {@link #applySnapshot} threads into the
     * scan handle).
     *
     * <p>Maps each {@link ConnectorTimeTravelSpec.Kind} to legacy
     * {@code PaimonExternalTable.getPaimonSnapshotCacheValue} (lines 124-144):
     * <ul>
     *   <li>{@code SNAPSHOT_ID} — {@code Long.parseLong(stringValue)}; if the snapshot does not
     *       exist returns {@link Optional#empty()}; pins {@code scan.snapshot-id}.</li>
     *   <li>{@code TIMESTAMP} — derives epoch-millis (digital ⇒ {@code Long.parseLong}; else paimon
     *       {@code DateTimeUtils.parseTimestampData(value, 3, sessionTZ)}, the byte-parity datetime
     *       parse), then the at-or-before snapshot; empty when none; pins {@code scan.snapshot-id}.
     *       </li>
     *   <li>{@code TAG} — resolves the tag's snapshot; empty when absent; pins {@code scan.tag-name}
     *       to the tag NAME (legacy pins the name, not the id).</li>
     *   <li>{@code INCREMENTAL} — {@code @incr(...)} read: validates the raw window params via
     *       {@link PaimonIncrementalScanParams#validate} (the ~180-line legacy validation, ported
     *       byte-faithfully) and pins at the LATEST snapshot (legacy {@code @incr} reads latest with
     *       EMPTY partition info and applies the {@code incremental-between*} options at scan time).
     *       The validated options are carried as {@code properties}; because that map is non-empty,
     *       {@link #applySnapshot} threads exactly those options and does NOT inject
     *       {@code scan.snapshot-id} (which would conflict with {@code incremental-between}).</li>
     *   <li>{@code BRANCH} — {@code @branch('name')} read: validates the branch on the BASE table via
     *       {@link PaimonCatalogOps#branchExists} (empty-if-absent, like snapshot/tag not-found), then
     *       loads the branch as its OWN table (independent schema/snapshots, via the 3-arg branch
     *       Identifier through {@link PaimonTableHandle#withBranch}) and pins its LATEST snapshot —
     *       branches have NO in-branch time-travel (legacy {@code PaimonExternalTable} reads the
     *       branch's {@code latestSnapshot()} only). The branch identity is carried to
     *       {@link #applySnapshot} via an internal sentinel ({@code CoreOptions.BRANCH} key, NOT a
     *       scan-copy option); no {@code scan.snapshot-id} is pinned (the branch reads its own latest).
     *       An empty branch (no snapshot) pins {@code snapshotId=-1} and {@code schemaId=-1}: a benign
     *       divergence from legacy's {@code schemaId=0L} — the resulting schema is identical (both
     *       resolve to the branch's current schema), mirroring the INCREMENTAL empty-table -1 note.</li>
     * </ul>
     *
     * <p>CONTRACT DIFFERENCE (intentional, documented): legacy {@code PaimonUtil} THREW a
     * {@code UserException} when the id/timestamp/tag was not found. The SPI contract here is
     * empty-if-none; the B5b-3 fe-core consumer translates {@link Optional#empty()} into the
     * user-facing error. Not-found is returned as empty; only a malformed spec (e.g. a non-digital
     * snapshot id) propagates as an exception, matching legacy {@code Long.parseLong}.
     *
     * <p>System tables do not expose time-travel (same guard as {@link #beginQuerySnapshot}) →
     * {@link Optional#empty()}.
     */
    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorTimeTravelSpec spec) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        if (paimonHandle.isSystemTable()) {
            return Optional.empty();
        }
        Table table = resolveTable(paimonHandle);
        switch (spec.getKind()) {
            case SNAPSHOT_ID: {
                long id = Long.parseLong(spec.getStringValue());
                if (!catalogOps.snapshotExists(table, id)) {
                    return Optional.empty();
                }
                long schemaId = catalogOps.snapshotSchemaId(table, id).orElse(-1L);
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(id)
                        .schemaId(schemaId)
                        .property(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(id))
                        .build());
            }
            case TIMESTAMP: {
                long millis = parseTimestampMillis(session, spec);
                OptionalLong id = catalogOps.snapshotIdAtOrBefore(table, millis);
                if (!id.isPresent()) {
                    return Optional.empty();
                }
                long snapshotId = id.getAsLong();
                long schemaId = catalogOps.snapshotSchemaId(table, snapshotId).orElse(-1L);
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(snapshotId)
                        .schemaId(schemaId)
                        .property(CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshotId))
                        .build());
            }
            // Non-numeric FOR VERSION AS OF resolves as a TAG in paimon (legacy parity:
            // PaimonExternalTable.getPaimonSnapshotCacheValue treats a non-digital FOR VERSION AS OF
            // value as a tag name). Empty fall-through to the @tag resolution — same behavior.
            case VERSION_REF:
            case TAG: {
                String tagName = spec.getStringValue();
                Optional<PaimonCatalogOps.TagSnapshot> tag =
                        catalogOps.getSnapshotByTag(table, tagName);
                if (!tag.isPresent()) {
                    return Optional.empty();
                }
                // Legacy pins the tag NAME (scan.tag-name=value), NOT the snapshot id
                // (PaimonExternalTable.java:137), so a later schema/data change to the tag is honored.
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(tag.get().snapshotId())
                        .schemaId(tag.get().schemaId())
                        .property(CoreOptions.SCAN_TAG_NAME.key(), tagName)
                        .build());
            }
            case INCREMENTAL: {
                // Validate the raw @incr window params and produce the paimon scan options. This is
                // the ~180-line legacy validation, ported byte-faithfully into the connector
                // (PaimonIncrementalScanParams). The produced opts hold incremental-between* keys ONLY
                // — the snapshot/handle stay null-free (shared SPI contract). The legacy null-valued
                // scan.snapshot-id/scan.mode resets are NOT carried here; they are reapplied at the
                // Table.copy chokepoint via PaimonIncrementalScanParams.applyResetsIfIncremental
                // (FIX-INCR-SCAN-RESET), so a base table that persists a stale scan.snapshot-id cannot
                // hijack incremental-between.
                Map<String, String> opts = PaimonIncrementalScanParams.validate(spec.getIncrementalParams());
                // Legacy @incr reads at the LATEST snapshot and applies incremental-between at scan time:
                // PaimonExternalTable.getPaimonSnapshotCacheValue falls through (neither tag/branch nor
                // FOR VERSION/TIME AS OF) to getLatestSnapshotCacheValue (the LATEST partition view + LATEST
                // schema), and PaimonScanNode.getProcessedTable copies the incremental options onto the base
                // table. fe-core (PluginDrivenMvccExternalTable.loadSnapshot) mirrors this: the INCREMENTAL
                // kind lists the LATEST partitions and uses the LATEST schema, carrying these incremental scan
                // options on the pin. Pin latest; an empty table (no snapshot) falls back to -1.
                long snapshotId = catalogOps.latestSnapshotId(table).orElse(-1L);
                long schemaId = snapshotId < 0
                        ? -1L
                        : catalogOps.snapshotSchemaId(table, snapshotId).orElse(-1L);
                // opts is NON-EMPTY, so applySnapshot threads exactly these (incremental-between*) and
                // does NOT inject scan.snapshot-id (which would conflict with incremental-between).
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(snapshotId)
                        .schemaId(schemaId)
                        .properties(opts)
                        .build());
            }
            case BRANCH: {
                String branchName = spec.getStringValue();
                // Validate on the BASE table (legacy resolvePaimonBranch validates the branch against
                // the base table's branchManager). Graceful empty-if-absent (fe-core B5b-3 translates
                // to the "can't find branch" UserException), consistent with snapshot/tag not-found.
                if (!catalogOps.branchExists(table, branchName)) {
                    return Optional.empty();
                }
                // Load the branch as its OWN table (independent schema/snapshots) and pin its LATEST
                // snapshot — branches do not support in-branch time-travel (legacy reads
                // latestSnapshot() only).
                Table branchTable = resolveTable(paimonHandle.withBranch(branchName));
                long snapshotId = catalogOps.latestSnapshotId(branchTable).orElse(-1L);
                long schemaId = snapshotId < 0
                        ? -1L
                        : catalogOps.snapshotSchemaId(branchTable, snapshotId).orElse(-1L);
                // Carry the branch identity to applySnapshot via an internal sentinel
                // (CoreOptions.BRANCH key). Branch is a handle-IDENTITY change, not a scan-copy
                // option: applySnapshot reads this sentinel and routes it to handle.withBranch (it is
                // never threaded into Table.copy). No scan.snapshot-id is pinned (the branch table
                // natively reads its own latest).
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(snapshotId)
                        .schemaId(schemaId)
                        .property(CoreOptions.BRANCH.key(), branchName)
                        .build());
            }
            case OPTIONS: {
                // @options carries paimon's OWN scan-option vocabulary. Validate the keys, then RESOLVE
                // the startup selector down to an immutable pin (scan.snapshot-id / scan.tag-name) right
                // here, at bind time: a mutable selector (scan.mode=latest, a tag, a wall-clock timestamp)
                // must not be re-evaluated later, or split planning would read a different version than
                // the one whose schema was bound. Resolution runs against the LATEST table, because the
                // options themselves are what selects the version.
                boolean usesStatementFence = spec.getLatestSnapshotFence().isPresent()
                        && PaimonScanParams.usesStatementSnapshot(spec.getOptions());
                Map<String, String> resolved;
                if (usesStatementFence) {
                    // Planning-only aliases own different table projections, not different
                    // versions. Reuse the statement fence even if latest advances between binds.
                    resolved = PaimonScanParams.pinOptionsToSnapshot(
                            spec.getOptions(), spec.getLatestSnapshotFence().getAsLong());
                } else {
                    resolved = PaimonScanParams.resolveOptions(table, spec.getOptions());
                }
                String pinnedTag = resolved.get(CoreOptions.SCAN_TAG_NAME.key());
                if (pinnedTag != null) {
                    // A tag selector (scan.tag-name, or a tag-valued scan.version that resolveOptions
                    // canonicalized into one) pins the TAG, never a snapshot file: paimon keeps the tag's
                    // own retained Snapshot copy under tag/ after snapshot/snapshot-<id> is expired. Read
                    // BOTH ids off that copy — exactly what the TAG case above does — because
                    // snapshotSchemaId() goes through snapshotManager().snapshot(id) and throws
                    // "Snapshot file ... does not exist" for an expired-but-tagged version.
                    Optional<PaimonCatalogOps.TagSnapshot> tag =
                            catalogOps.getSnapshotByTag(table, pinnedTag);
                    return Optional.of(ConnectorMvccSnapshot.builder()
                            .snapshotId(tag.map(PaimonCatalogOps.TagSnapshot::snapshotId).orElse(-1L))
                            .schemaId(tag.map(PaimonCatalogOps.TagSnapshot::schemaId).orElse(-1L))
                            .properties(PaimonScanParams.markAsOptions(resolved))
                            .build());
                }
                long pinnedId = usesStatementFence
                        ? spec.getLatestSnapshotFence().getAsLong()
                        : pinnedSnapshotId(table, resolved);
                // The statement fence pins data visibility, not schema time travel. Planning-only
                // aliases must retain the latest-schema projection used by the plain relation.
                long schemaId = usesStatementFence || pinnedId < 0
                        ? -1L
                        : catalogOps.snapshotSchemaId(table, pinnedId).orElse(-1L);
                // resolved is never empty for a startup selector; for a selector-free @options (e.g. only
                // scan.manifest-parallelism) it is the user map verbatim, which applySnapshot still
                // threads -- those keys tune HOW the scan runs, not WHICH version it reads.
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(pinnedId)
                        .schemaId(schemaId)
                        .properties(PaimonScanParams.markAsOptions(resolved))
                        .build());
            }
            default:
                throw new UnsupportedOperationException(
                        "unsupported time-travel kind: " + spec.getKind());
        }
    }

    /**
     * The snapshot id a resolved {@code @options} map pins, or {@code -1} when it pins none (a
     * selector-free option set or an empty table). Only used to stamp the
     * {@link ConnectorMvccSnapshot}'s identity — the authoritative selector is the resolved option map
     * itself, which {@link #applySnapshot} threads verbatim.
     *
     * <p>A TAG-pinning map never reaches here: {@link #resolveTimeTravel}'s {@code OPTIONS} case returns
     * before this call, because a tag's ids must come from the tag's own retained copy rather than from a
     * snapshot file that may already be expired.
     */
    private long pinnedSnapshotId(Table table, Map<String, String> resolved) {
        String snapshotId = resolved.get(CoreOptions.SCAN_SNAPSHOT_ID.key());
        if (snapshotId != null) {
            return Long.parseLong(snapshotId);
        }
        return catalogOps.latestSnapshotId(table).orElse(-1L);
    }

    /**
     * Doris session time-zone alias map, replicated from fe-core
     * {@code TimeUtils.timeZoneAliasMap} (TimeUtils.java:106-117). The connector cannot import
     * fe-core, so the map is rebuilt here byte-for-byte: {@link java.time.ZoneId#SHORT_IDS} (the
     * JDK-provided short ids, which is where "PST"/"EST" resolve) overlaid with the four Doris
     * overrides (CST/PRC -&gt; Asia/Shanghai, UTC/GMT -&gt; UTC). Case-insensitive, exactly like
     * legacy, because {@code SET time_zone} stores the alias verbatim in any case.
     *
     * <p>NOTE (FIX-TZ-ALIAS): the full {@code SHORT_IDS} map is required, NOT just the 4 explicit
     * overrides — PST and EST resolve via {@code SHORT_IDS}, so a 4-entry-only map would still
     * reject them (verified by JDK harness).
     */
    private static final Map<String, String> SESSION_TIME_ZONE_ALIASES;

    static {
        Map<String, String> m = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        m.putAll(java.time.ZoneId.SHORT_IDS);
        m.put("CST", "Asia/Shanghai");
        m.put("PRC", "Asia/Shanghai");
        m.put("UTC", "UTC");
        m.put("GMT", "UTC");
        SESSION_TIME_ZONE_ALIASES = Collections.unmodifiableMap(m);
    }

    /**
     * Derives epoch-millis from a {@code TIMESTAMP} spec, byte-faithful to legacy
     * {@code PaimonUtil.getPaimonSnapshotByTimestamp}: a digital value is {@code Long.parseLong};
     * a non-digital value is parsed by paimon {@code DateTimeUtils.parseTimestampData(value, 3, TZ)}
     * where TZ is the SESSION time zone.
     *
     * <p>BYTE-PARITY TZ DECISION: legacy passed {@code TimeUtils.getTimeZone()} =
     * {@code TimeZone.getTimeZone(ZoneId.of(sessionTz, dorisAliasMap))}. The connector cannot import
     * the fe-core Doris alias map, so it replicates it as {@link #SESSION_TIME_ZONE_ALIASES} and
     * resolves the zone via {@code ZoneId.of(tz, SESSION_TIME_ZONE_ALIASES)} — byte-identical to
     * legacy {@code TimeUtils.getTimeZone()} for every id legacy accepted (standard IANA ids,
     * offsets, the {@code SHORT_IDS} aliases like "PST"/"EST", and the Doris overrides
     * CST/PRC/UTC/GMT).
     *
     * <p>FAIL-LOUD on genuinely-unknown id (NOT silent degrade): an id absent from BOTH
     * {@code ZoneId.of}'s native set AND the alias map (e.g. "XYZ", "NOPE/ZZZ") is rejected with a
     * clear, actionable {@link DorisConnectorException}, never silently degraded to a wrong zone (a
     * wrong zone resolves the WRONG snapshot -> silently wrong rows). (This deliberately does NOT
     * follow the MaxComputePredicateConverter pattern of degrading to NO_PREDICATE on a bad alias:
     * that is safe only because BE re-applies the predicate, whereas a mis-resolved time-travel zone
     * has no such safety net.) The legacy {@code millis < 0} guard is preserved.
     */
    private long parseTimestampMillis(ConnectorSession session, ConnectorTimeTravelSpec spec) {
        String value = spec.getStringValue();
        if (spec.isDigital()) {
            return Long.parseLong(value);
        }
        // Resolve the session zone ONLY inside this catch so a legitimate
        // DateTimeUtils.parseTimestampData("can't parse time") below is NOT swallowed: a genuinely
        // unknown zone id (absent from ZoneId.of's native set AND the replicated alias map) must
        // fail loud with actionable guidance, never silently degrade to a wrong zone (a wrong zone
        // selects the WRONG snapshot -> silently wrong rows). The alias map resolves every id legacy
        // accepted (CST/PST/EST/... via SHORT_IDS + the 4 Doris overrides).
        java.time.ZoneId zoneId;
        try {
            zoneId = java.time.ZoneId.of(session.getTimeZone(), SESSION_TIME_ZONE_ALIASES);
        } catch (java.time.DateTimeException e) {
            throw new DorisConnectorException(
                    "session time zone '" + session.getTimeZone() + "' is not a standard zone id and "
                            + "cannot be used for FOR TIME AS OF with a datetime string; use a standard "
                            + "IANA zone id (e.g. 'Asia/Shanghai', 'UTC'), or specify epoch "
                            + "milliseconds, or use FOR VERSION AS OF <snapshot-id|tag>.", e);
        }
        java.util.TimeZone tz = java.util.TimeZone.getTimeZone(zoneId);
        long millis = DateTimeUtils.parseTimestampData(value, 3, tz).getMillisecond();
        if (millis < 0) {
            throw new java.time.DateTimeException("can't parse time: " + value);
        }
        return millis;
    }

    /**
     * Threads a pinned MVCC / time-travel {@code snapshot} into the handle BEFORE planScan: returns
     * a copy of {@code handle} carrying the connector's resolved scan options so the scan path reads
     * at that snapshot/tag (the scan provider applies them via {@code Table.copy}).
     *
     * <p>Threads the FULL {@code snapshot.getProperties()} map: this may be
     * {@code scan.snapshot-id=<id>} (snapshot-id / timestamp time-travel) OR
     * {@code scan.tag-name=<name>} (tag time-travel), whichever {@link #resolveTimeTravel} pinned.
     * When {@code properties} is empty (the {@link #beginQuerySnapshot} latest-pin path, which
     * carries no properties) it falls back to {@code scan.snapshot-id=<snapshotId>} for B5a parity.
     *
     * <p>BRANCH is special: when the snapshot carries the {@code CoreOptions.BRANCH} sentinel (set by
     * {@link #resolveTimeTravel}'s BRANCH case), it is a handle-IDENTITY change, not a scan option —
     * it is detected FIRST and routed to {@link PaimonTableHandle#withBranch} (which clears the
     * transient base Table so the branch reloads), never threaded into {@code Table.copy}.
     *
     * <p>System tables have no MVCC (they are synthetic metadata views — same guard as
     * {@link #beginQuerySnapshot}), so a sys handle is returned unchanged.
     */
    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        if (paimonHandle.isSystemTable()) {
            // A system table has no MVCC identity of its own, so a latest-pin (empty properties) leaves it
            // untouched as before. It CAN however be handed an explicit @options selector resolved against
            // its SOURCE table (fe-core's PluginDrivenScanNode.resolveSysTableSnapshotPin) -- thread those
            // options so the metadata view is materialized at the selected snapshot instead of latest.
            // Only the OPTIONS-shaped selector reaches here: which system table accepts it is gated by
            // PaimonScanPlanProvider.supportsSystemTableOptions, and @branch/@tag are refused connector-wide
            // by supportsSystemTableTimeTravel()==false.
            if (snapshot == null || snapshot.getProperties().isEmpty()) {
                return paimonHandle;
            }
            PaimonScanParams.validateSystemTableOptions(snapshot.getProperties());
            return paimonHandle.withScanOptions(snapshot.getProperties());
        }
        if (snapshot != null) {
            String branch = snapshot.getProperties().get(CoreOptions.BRANCH.key());
            if (branch != null) {
                // Branch time-travel is a handle-identity change (a different table load), not a scan
                // option: route to withBranch (which clears the transient base Table so resolveTable
                // reloads the branch). The branch reads its own latest, so no scan.snapshot-id is
                // pinned. Detected BEFORE the generic properties path so the branch sentinel never
                // becomes a scan-copy option.
                return paimonHandle.withBranch(branch);
            }
            if (!snapshot.getProperties().isEmpty()) {
                // Explicit time-travel: the connector already resolved the exact scan options
                // (scan.snapshot-id OR scan.tag-name etc.) in resolveTimeTravel — thread them verbatim.
                return paimonHandle.withScanOptions(snapshot.getProperties());
            }
        }
        if (snapshot == null) {
            return paimonHandle;
        }
        if (snapshot.getSnapshotId() < 0) {
            // Empty latest is still a statement-scoped state. Carry only Doris' internal marker;
            // Paimon's scan.snapshot-id=-1 would address a non-existent snapshot file.
            return paimonHandle.withScanOptions(
                    PaimonScanParams.pinOptionsToSnapshot(Collections.emptyMap(), -1L));
        }
        // Route through pinOptionsToSnapshot so the fence pin carries PRESERVE_BOUND_SCHEMA: the
        // pinned snapshot fixes the DATA version only. A bare scan.snapshot-id would make paimon's
        // Table.copy time-travel the SCHEMA to that snapshot's generation too, which breaks every
        // read issued between an ALTER and the next snapshot (the alter bumps the schema without
        // committing a snapshot, so the latest snapshot still carries the pre-alter generation).
        return paimonHandle.withScanOptions(
                PaimonScanParams.pinOptionsToSnapshot(Collections.emptyMap(), snapshot.getSnapshotId()));
    }

    /**
     * Builds the read-path Thrift descriptor for a paimon plugin table as a {@code HIVE_TABLE}
     * carrying a {@link THiveTable}, mirroring legacy paimon ({@code PaimonExternalTable.toThrift}
     * and {@code PaimonSysExternalTable.toThrift}, both of which send {@code TTableType.HIVE_TABLE}
     * with a {@code THiveTable}) and the MaxCompute pattern
     * ({@code MaxComputeConnectorMetadata.buildTableDescriptor}).
     *
     * <p>Without this override the SPI default returns {@code null}, so fe-core falls back to
     * {@code TTableType.SCHEMA_TABLE}; BE's {@code DescriptorTbl::create} then builds a
     * {@code SchemaTableDescriptor} instead of the {@code HiveTableDescriptor} it builds for
     * {@code HIVE_TABLE}, a descriptor-parity bug. This fix covers BOTH normal paimon plugin tables
     * (closing the latent B2 descriptor gap) AND system tables, which inherit it through
     * {@code PluginDrivenExternalTable.toThrift}.
     */
    @Override
    public TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        THiveTable tHiveTable = new THiveTable(dbName, tableName, new HashMap<>());
        TTableDescriptor desc = new TTableDescriptor(
                tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
        desc.setHiveTable(tHiveTable);
        return desc;
    }

    // ==================== DDL: Create/Drop Table ====================

    /**
     * Creates a Paimon table from the full {@link ConnectorCreateTableRequest}.
     *
     * <p>fe-core already pre-probes existence (via {@code getTableHandle}) and short-circuits the
     * {@code IF NOT EXISTS} case, so this body has no redundant existence check. The remote create is
     * therefore issued with {@code ignoreIfExists = false} <em>regardless</em> of
     * {@link ConnectorCreateTableRequest#isIfNotExists()} (upstream #66112 made the legacy
     * {@code PaimonMetadataOps.performCreateTable} do the same): the only way to reach an existing table
     * here is a creator that won the race after fe-core's probe, and paimon must REPORT that instead of
     * silently no-opping. Swallowing it would make this statement look like the creator, so a
     * {@code CREATE TABLE IF NOT EXISTS ... AS SELECT} would INSERT into — and on failure roll back — a
     * table the winner owns. {@code TableAlreadyExistException} is wrapped as a
     * {@link DorisConnectorException}, which fe-core's bridge turns back into an IF NOT EXISTS no-op.
     *
     * <p>Per D7=B (legacy parity) the remote call is wrapped in
     * {@link ConnectorContext#executeAuthenticated} so the FE-injected auth context (e.g. Kerberos
     * UGI) applies, exactly as legacy {@code PaimonMetadataOps} wrapped every remote DDL call.
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorCreateTableRequest request) {
        // Reject a DISTRIBUTE BY clause up front (before the executeAuthenticated try, whose catch would rewrap
        // the message). Moved off fe-core CreateTableInfo.validate — the connector owns the paimon DDL rule.
        rejectDistribution(request);
        Identifier id = Identifier.create(request.getDbName(), request.getTableName());
        Schema schema = PaimonSchemaBuilder.build(request);
        // REST catalogs can persist the schema before a client loads and validates the Vortex table.
        if ("vortex".equalsIgnoreCase(schema.options().get(CoreOptions.FILE_FORMAT.key()))) {
            try {
                SchemaValidation.validateTableSchema(TableSchema.create(0, schema));
            } catch (RuntimeException e) {
                throw new DorisConnectorException(
                        "Invalid Paimon table schema for " + id + ": " + e.getMessage(), e);
            }
        }
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createTable(id, schema, false);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to create Paimon table " + id + ": " + e.getMessage(), e);
        }
        LOG.info("created Paimon table {}", id);
    }

    /**
     * Rejects a {@code DISTRIBUTE BY} clause: paimon has no hash/random distribution, buckets are expressed via
     * {@code bucket(num, column)} in {@code PARTITIONED BY}. {@code request.getBucketSpec() != null} iff the user
     * wrote {@code DISTRIBUTE BY}, and {@code PaimonSchemaBuilder} deliberately ignores {@code bucketSpec}, so
     * without this reject the clause would silently succeed. Message kept byte-identical to the former fe-core
     * wording. Package-private for unit test; reached only via {@link #createTable} in production.
     */
    void rejectDistribution(ConnectorCreateTableRequest request) {
        if (request.getBucketSpec() != null) {
            throw new DorisConnectorException("Paimon doesn't support 'DISTRIBUTE BY', "
                    + "and you can use 'bucket(num, column)' in 'PARTITIONED BY'.");
        }
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

    /**
     * Rejects row-level DML on a table whose shape cannot express it, with a message that says how to fix it.
     *
     * <p>Whether a table can carry a row-level write is a property of the TABLE, not of the connector —
     * hence this per-table gate behind the connector-level {@code supportedOperations} declaration:
     *
     * <ul>
     * <li><b>Primary-key table</b>: DELETE, UPDATE and MERGE supported. A delete is a
     *     {@code RowKind.DELETE} record the merge engine cancels against the key; UPDATE/MERGE arrive as
     *     an operation-tagged stream whose tags the writer maps to keyed upserts and deletes.</li>
     * <li><b>DELETE, UPDATE and MERGE on an unaware-bucket append-only table with deletion vectors</b>:
     *     all supported. Every one of the three has to REMOVE the matched rows, which an append-only table
     *     records as their POSITIONS in a deletion-vector index (the scan projects the synthetic row
     *     locator for exactly this). An UPDATE/MERGE additionally APPENDS the replacement rows: it arrives
     *     as an operation-tagged stream whose delete tags drive the deletion vector and whose insert/update
     *     tags append new data files, both in one commit — the combined vector-plus-append write. A MERGE's
     *     NOT MATCHED clause is a plain append (no locator, nothing to remove).</li>
     * <li><b>DELETE/UPDATE/MERGE on an append-only table without deletion vectors</b>: rejected — a Paimon
     *     requirement, not a Doris one: no key to cancel against, no vector to mark, nowhere to record the
     *     removal that all three ops require.</li>
     * <li><b>DELETE/UPDATE/MERGE on a bucketed-append table</b> (a pinned bucket count): rejected — the
     *     vector must be filed under the file's REAL bucket, which the row locator does not carry yet.</li>
     * </ul>
     *
     * <p>{@code row-tracking.enabled} (the {@code _ROW_ID} whole-file rewrite Spark also implements) is a
     * separate shape, likewise not planned by this connector.
     */
    @Override
    public void validateRowLevelDmlMode(ConnectorSession session, ConnectorTableHandle handle,
            WriteOperation op) {
        if (op != WriteOperation.DELETE && op != WriteOperation.UPDATE && op != WriteOperation.MERGE) {
            return;
        }
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        Table table;
        try {
            table = context.executeAuthenticated(() -> resolveTable(paimonHandle));
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to load Paimon table "
                    + paimonHandle.getDatabaseName() + "." + paimonHandle.getTableName()
                    + " for " + op + " validation: " + e.getMessage(), e);
        }
        if (!table.primaryKeys().isEmpty()) {
            return;
        }
        // Every append-only row-level op — DELETE, UPDATE and MERGE — has to REMOVE the matched rows, which
        // an append-only table can only do by recording their POSITIONS in a deletion-vector index (an
        // UPDATE/MERGE additionally APPENDS the replacement rows, but the removal half is what gates the
        // shape). The remaining checks are therefore common to all three: the table must carry deletion
        // vectors, and the vector must be file-able under the row's bucket. UPDATE and MERGE need nothing
        // beyond what DELETE needs, so they pass through the SAME gate rather than a separate rejection.
        // A DELETE/UPDATE/MERGE is recorded as the row's POSITION in a deletion-vector index, so the table
        // must have deletion vectors enabled — a Paimon requirement, not a Doris one: with no key to cancel
        // against and no vector to mark, there is nowhere to record the removal.
        boolean deletionVectors = Boolean.parseBoolean(table.options()
                .getOrDefault(CoreOptions.DELETION_VECTORS_ENABLED.key(), "false"));
        if (!deletionVectors) {
            throw new DorisConnectorException(String.format(
                    "Doris does not support %s on the append-only Paimon table %s.%s: it has no "
                            + "primary key to cancel rows against and no deletion vectors to mark them. "
                            + "Either create the table with a primary key, or enable deletion vectors: "
                            + "ALTER TABLE %s.%s SET ('%s' = 'true').",
                    op, paimonHandle.getDatabaseName(), paimonHandle.getTableName(),
                    paimonHandle.getDatabaseName(), paimonHandle.getTableName(),
                    CoreOptions.DELETION_VECTORS_ENABLED.key()));
        }
        // The writer files a deletion vector under (partition, bucket). For an unaware-bucket table that
        // grouping is trivially right; a bucketed-append table (a pinned bucket count) needs the file's
        // REAL bucket, which the row locator does not carry yet. Reject rather than mis-file vectors
        // under the wrong bucket and corrupt reads of the others.
        int bucket = Integer.parseInt(table.options()
                .getOrDefault(CoreOptions.BUCKET.key(), "-1"));
        if (bucket != -1) {
            throw new DorisConnectorException(String.format(
                    "Doris supports %s on an append-only Paimon table only in unaware-bucket mode; "
                            + "%s.%s pins a fixed bucket count ('%s' = %d). Use a primary-key table, or "
                            + "rewrite the data with INSERT OVERWRITE.",
                    op, paimonHandle.getDatabaseName(), paimonHandle.getTableName(),
                    CoreOptions.BUCKET.key(), bucket));
        }
    }

    // ==================== Column evolution: ALTER TABLE ADD/DROP/RENAME/MODIFY COLUMN ====================
    // Paimon models every column operation as a SchemaChange applied through
    // Catalog.alterTable(identifier, List<SchemaChange>, ignoreIfNotExists). The neutral SPI column is
    // translated into paimon types PURELY here (outside the authenticator, mirroring the iceberg
    // connector), then the whole change list is committed through the seam inside ONE auth scope.
    //
    // Every op builds a LIST and commits once, so a MODIFY that changes type + nullability + position is a
    // single atomic schema commit rather than three partially-applied ones.
    //
    // Note a paimon ALTER bumps the schema id WITHOUT creating a snapshot; the read path already accounts
    // for this by reading schemaManager().latest() instead of the CachingCatalog-frozen rowType()
    // (see PaimonCatalogOps.latestSchema).

    /**
     * Adds a column at {@code position} ({@code null} = append at the end).
     *
     * <p>Column-level nullability rides on the paimon type via {@code .copy(nullable)}, exactly as the
     * CREATE TABLE path does in {@code PaimonSchemaBuilder}.
     */
    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        SchemaChange change = SchemaChange.addColumn(
                column.getName(),
                PaimonTypeMapping.toPaimonType(column.getType()).copy(column.isNullable()),
                column.getComment(),
                toMove(column.getName(), position));
        applySchemaChanges(h, Collections.singletonList(change),
                "add column " + column.getName());
    }

    /** Adds multiple columns, appended in order, as ONE schema commit. */
    @Override
    public void addColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumn> columns) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        List<SchemaChange> changes = new ArrayList<>(columns.size());
        for (ConnectorColumn column : columns) {
            changes.add(SchemaChange.addColumn(
                    column.getName(),
                    PaimonTypeMapping.toPaimonType(column.getType()).copy(column.isNullable()),
                    column.getComment()));
        }
        applySchemaChanges(h, changes, "add columns");
    }

    /** Drops the named top-level column. */
    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle handle, String columnName) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        applySchemaChanges(h, Collections.singletonList(SchemaChange.dropColumn(columnName)),
                "drop column " + columnName);
    }

    /** Renames a top-level column. */
    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle handle,
            String oldName, String newName) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        applySchemaChanges(h, Collections.singletonList(SchemaChange.renameColumn(oldName, newName)),
                "rename column " + oldName + " to " + newName);
    }

    /**
     * Modifies a column's type, nullability, comment and/or position in ONE schema commit.
     *
     * <p>Paimon splits what Doris expresses as a single {@code MODIFY COLUMN} across distinct
     * {@code SchemaChange}es, so up to four are emitted and committed atomically. The comment is only
     * carried when the statement actually specified one — {@code isCommentSpecified()} distinguishes
     * "no COMMENT clause" (keep the existing comment) from {@code COMMENT ''} (clear it), which a bare
     * null check would conflate.
     *
     * <p>Type widening rules are Paimon's own ({@code SchemaChange.updateColumnType} rejects an
     * unsupported narrowing server-side); this layer does not second-guess them.
     */
    @Override
    public void modifyColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        String name = column.getName();
        List<SchemaChange> changes = new ArrayList<>(4);
        // keepNullability=true: nullability is owned by the explicit updateColumnNullability change below,
        // emitted only when the statement actually specified NULL / NOT NULL. Letting the type change
        // carry it (the 2-arg overload, keepNullability=false) would reset a NOT NULL column to the
        // paimon default (nullable) on a bare "MODIFY COLUMN c BIGINT" that never mentioned nullability.
        changes.add(SchemaChange.updateColumnType(
                new String[] {name}, PaimonTypeMapping.toPaimonType(column.getType()),
                /*keepNullability*/ true));
        if (column.isNullableSpecified()) {
            changes.add(SchemaChange.updateColumnNullability(name, column.isNullable()));
        }
        if (column.isCommentSpecified()) {
            changes.add(SchemaChange.updateColumnComment(name, column.getComment()));
        }
        SchemaChange.Move move = toMove(name, position);
        if (move != null) {
            changes.add(SchemaChange.updateColumnPosition(move));
        }
        applySchemaChanges(h, changes, "modify column " + name);
    }

    /**
     * Reorders columns to match {@code newOrder} by chaining {@code AFTER} moves: the first column is
     * moved FIRST, each subsequent one AFTER its predecessor. Committing the whole chain as one schema
     * change list means an invalid order cannot leave the table half-reordered.
     */
    @Override
    public void reorderColumns(ConnectorSession session, ConnectorTableHandle handle, List<String> newOrder) {
        if (newOrder == null || newOrder.isEmpty()) {
            throw new DorisConnectorException("Reorder columns failed: the new order is empty");
        }
        PaimonTableHandle h = (PaimonTableHandle) handle;
        List<SchemaChange> changes = new ArrayList<>(newOrder.size());
        changes.add(SchemaChange.updateColumnPosition(SchemaChange.Move.first(newOrder.get(0))));
        for (int i = 1; i < newOrder.size(); i++) {
            changes.add(SchemaChange.updateColumnPosition(
                    SchemaChange.Move.after(newOrder.get(i), newOrder.get(i - 1))));
        }
        applySchemaChanges(h, changes, "reorder columns");
    }

    // ---- Nested (dotted-path) column evolution ----
    // The fe-core bridge routes ONLY nested paths here; a top-level column still flows through the flat
    // ops above. The single exception is modifyColumnComment, which is the sole entrypoint for
    // MODIFY COLUMN ... COMMENT and therefore receives flat and nested paths alike. Each op still
    // degrades to its flat counterpart on a single-part path so a direct call cannot bypass the flat
    // behaviour (mirrors the iceberg connector).

    /**
     * Adds a field inside a struct. {@code path} is the FULL path of the new field (parent struct plus
     * the new leaf name), so the parent is {@code path.getParentPath()}.
     */
    @Override
    public void addNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        if (!path.isNested()) {
            addColumn(session, handle, column, position);
            return;
        }
        PaimonTableHandle h = (PaimonTableHandle) handle;
        SchemaChange change = SchemaChange.addColumn(
                toFieldNames(path),
                PaimonTypeMapping.toPaimonType(column.getType()).copy(column.isNullable()),
                column.getComment(),
                toMove(path.getLeafName(), position));
        applySchemaChanges(h, Collections.singletonList(change),
                "add nested column " + path.getFullPath());
    }

    /** Drops the field at {@code path}. */
    @Override
    public void dropNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path) {
        if (!path.isNested()) {
            dropColumn(session, handle, path.getTopLevelName());
            return;
        }
        PaimonTableHandle h = (PaimonTableHandle) handle;
        applySchemaChanges(h, Collections.singletonList(SchemaChange.dropColumn(toFieldNames(path))),
                "drop nested column " + path.getFullPath());
    }

    /** Renames the field at {@code path} to the leaf name {@code newName}. */
    @Override
    public void renameNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String newName) {
        if (!path.isNested()) {
            renameColumn(session, handle, path.getTopLevelName(), newName);
            return;
        }
        PaimonTableHandle h = (PaimonTableHandle) handle;
        applySchemaChanges(h,
                Collections.singletonList(SchemaChange.renameColumn(toFieldNames(path), newName)),
                "rename nested column " + path.getFullPath() + " to " + newName);
    }

    /**
     * Modifies the field at {@code path} (type / nullability / comment), optionally repositioning it
     * within its parent struct. Like {@link #modifyColumn}, the changes are committed as one list.
     */
    @Override
    public void modifyNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        if (!path.isNested()) {
            modifyColumn(session, handle, column, position);
            return;
        }
        PaimonTableHandle h = (PaimonTableHandle) handle;
        String[] fieldNames = toFieldNames(path);
        List<SchemaChange> changes = new ArrayList<>(4);
        // keepNullability=true: nullability is owned by the explicit updateColumnNullability change below
        // (emitted only when the statement specified it), so the type update must not silently reset it.
        changes.add(SchemaChange.updateColumnType(
                fieldNames, PaimonTypeMapping.toPaimonType(column.getType()), /*keepNullability*/ true));
        if (column.isNullableSpecified()) {
            changes.add(SchemaChange.updateColumnNullability(fieldNames, column.isNullable()));
        }
        if (column.isCommentSpecified()) {
            changes.add(SchemaChange.updateColumnComment(fieldNames, column.getComment()));
        }
        SchemaChange.Move move = toMove(path.getLeafName(), position);
        if (move != null) {
            changes.add(SchemaChange.updateColumnPosition(move));
        }
        applySchemaChanges(h, changes, "modify nested column " + path.getFullPath());
    }

    /**
     * Sets (or clears, with {@code ""}) the comment of the field at {@code path}. This is the sole
     * entrypoint for {@code MODIFY COLUMN ... COMMENT} and accepts both flat and nested paths, so it
     * uses the {@code String[]} overload unconditionally (a single-element array IS the flat case in
     * paimon's API).
     */
    @Override
    public void modifyColumnComment(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String comment) {
        PaimonTableHandle h = (PaimonTableHandle) handle;
        applySchemaChanges(h,
                Collections.singletonList(SchemaChange.updateColumnComment(toFieldNames(path), comment)),
                "modify comment of column " + path.getFullPath());
    }

    // ---- Column-evolution helpers ----

    /**
     * Commits {@code changes} as ONE paimon schema commit inside the authenticator, wrapping any failure
     * with the operation and the qualified table name. {@code ignoreIfNotExists} is false: fe-core
     * pre-resolves the handle, so a missing table here is a genuine error worth surfacing (unlike
     * {@link #dropTable}, whose SPI contract is idempotent).
     */
    private void applySchemaChanges(PaimonTableHandle handle, List<SchemaChange> changes, String operation) {
        Identifier id = Identifier.create(handle.getDatabaseName(), handle.getTableName());
        try {
            context.executeAuthenticated(() -> {
                catalogOps.alterTable(id, changes, /*ignoreIfNotExists*/ false);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to " + operation + " in Paimon table " + id + ": " + e.getMessage(), e);
        }
        LOG.info("applied {} schema change(s) to Paimon table {} ({})", changes.size(), id, operation);
    }

    /**
     * Neutral position to paimon {@code Move}. Doris only expresses {@code FIRST | AFTER col} (there is
     * no BEFORE variant), and a null position means "no position clause" — return null so the caller
     * omits the reposition change entirely rather than emitting a no-op {@code Move.last()}.
     */
    private static SchemaChange.Move toMove(String fieldName, ConnectorColumnPosition position) {
        if (position == null) {
            return null;
        }
        return position.isFirst()
                ? SchemaChange.Move.first(fieldName)
                : SchemaChange.Move.after(fieldName, position.getAfterColumn());
    }

    /** Neutral dotted path to the {@code String[]} field-name path paimon's nested overloads take. */
    private static String[] toFieldNames(ConnectorColumnPath path) {
        return path.getParts().toArray(new String[0]);
    }

    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session) {
        return new PaimonConnectorTransaction(
                session.allocateTransactionId(), catalogOps, context);
    }

    @Override
    public void validateStaticPartitionColumns(ConnectorSession session,
            ConnectorTableHandle handle, List<String> staticPartitionColumnNames) {
        PaimonTableHandle table = (PaimonTableHandle) handle;
        Set<String> partitionColumns = new HashSet<>();
        for (String name : table.getPartitionKeys()) {
            partitionColumns.add(name.toLowerCase(java.util.Locale.ROOT));
        }
        for (String name : staticPartitionColumnNames) {
            if (!partitionColumns.contains(name.toLowerCase(java.util.Locale.ROOT))) {
                throw new DorisConnectorException("Column '" + name
                        + "' is not a partition column of Paimon table");
            }
        }
    }

    // ==================== DDL: Create/Drop Database ====================

    /**
     * Creates a Paimon database.
     *
     * <p>fe-core already does the {@code IF NOT EXISTS} short-circuit before reaching here:
     * {@code PluginDrivenExternalCatalog.createDb}
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
        String flavor = catalogProperties.getFlavor();
        if (!properties.isEmpty() && !PaimonCatalogProperties.HMS.equals(flavor)) {
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
        // Mirror getTableSchema(session, handle): for a non-system data table read the LATEST schema FRESH
        // via schemaManager().latest(), NOT the cached Table's rowType(). paimon's CachingCatalog freezes
        // rowType() at load time, while an external ALTER (e.g. RENAME COLUMN) bumps the schema file WITHOUT
        // a new snapshot — so a stale rowType() keeps the OLD column names. The handle map would then be
        // keyed by the old names, the renamed scan slot would miss the map and be silently dropped from the
        // scan's `columns`, and the schema-evolution dict's current(-1) entry would omit it -> the BE
        // StructNode built by field id lacks that column and children.contains(table_column_name) DCHECKs
        // (aborts the BE). latestSchema() is empty for a non-DataTable/schema-less backend -> fall back to
        // rowType(). System tables keep their synthetic rowType() (no schema-version history).
        if (!paimonHandle.isSystemTable()) {
            Optional<PaimonCatalogOps.PaimonSchemaSnapshot> latest = catalogOps.latestSchema(table);
            if (latest.isPresent()) {
                return buildColumnHandles(latest.get().fields());
            }
        }
        return buildColumnHandles(table.rowType().getFields());
    }

    /**
     * Returns column handles AT {@code snapshot.getSchemaId()} (the pinned schema version, for
     * time-travel reads under schema evolution). Falls back to the LATEST columns
     * ({@link #getColumnHandles(ConnectorSession, ConnectorTableHandle)}) when there is no pinned
     * schema id (null snapshot or {@code schemaId < 0}).
     *
     * <p>Keys the handles by the PINNED names via the SAME memoized {@link PaimonCatalogOps#schemaAt}
     * read the at-snapshot {@link #getTableSchema(ConnectorSession, ConnectorTableHandle,
     * ConnectorMvccSnapshot)} uses, so the handle names equal the pinned Doris schema the query slots
     * were bound to. Without this, a time-travel read across a RENAME would key the handles by the
     * latest names, the renamed column's pinned-name slot would miss the map and be silently dropped,
     * and the paimon field-id dict would omit that BE scan slot -&gt; BE StructNode out_of_range crash.</p>
     */
    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorMvccSnapshot snapshot) {
        PaimonTableHandle sysCandidate = (PaimonTableHandle) handle;
        if (sysCandidate.isSystemTable()) {
            // A metadata view has no schema-version history for schemaAt to read, and going through it would
            // return the BASE table's historical fields -- dropping the view's own columns (e.g. $audit_log's
            // leading `rowkind`). Build the handles from the pinned view's own rowType, the same source
            // systemTableSchemaAt binds the slots from, so the two cannot disagree.
            return buildColumnHandles(
                    resolveSystemTableAt(session, sysCandidate, snapshot).rowType().getFields());
        }
        if (snapshot == null || snapshot.getSchemaId() < 0) {
            return getColumnHandles(session, handle);
        }
        PaimonTableHandle paimonHandle = sysCandidate;
        long schemaId = snapshot.getSchemaId();
        // Resolve the table AT the snapshot's identity BEFORE reading the pinned schema. buildColumnHandles
        // (PluginDrivenScanNode) calls this with the BASE handle -- the branch/MVCC pin is threaded onto
        // the scan node's currentHandle only later, in pinMvccSnapshot -- so a @branch read would otherwise
        // resolve the branch's schemaId against the BASE table's schema dir -> "No such file
        // .../schema/schema-<id>". applySnapshot routes the CoreOptions.BRANCH sentinel to withBranch so
        // schemaAt reads the branch's own schema dir; mirrors getTableStatistics(3-arg) / getPartitions. A
        // version/tag/time pin only threads scan options resolveTable ignores -> table unchanged.
        PaimonTableHandle pinned = (PaimonTableHandle) applySnapshot(session, paimonHandle, snapshot);
        Table table = resolveTable(pinned);
        // Key the memo on the PINNED handle (carries branchName in equals/hashCode): schemaAtMemo is
        // per-catalog and long-lived, so keying on the base handle would let a branch@schemaId poison a
        // later base@same-schemaId read (each has its own independently-evolved schema-<id>).
        PaimonCatalogOps.PaimonSchemaSnapshot schema =
                schemaAtMemo.getOrLoad(pinned, schemaId, () -> catalogOps.schemaAt(table, schemaId));
        return buildColumnHandles(schema.fields());
    }

    /**
     * Whether {@link #getColumnHandles(ConnectorSession, ConnectorTableHandle, ConnectorMvccSnapshot)}
     * resolves handles at the pinned schema (it does &mdash; via {@code schemaAt}). Enables the generic
     * node's fail-loud check that no pinned-schema column is silently dropped.
     */
    @Override
    public boolean supportsColumnHandleSnapshotPin(ConnectorSession session) {
        return true;
    }

    private static Map<String, ConnectorColumnHandle> buildColumnHandles(List<DataField> fields) {
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            String name = fields.get(i).name();
            handles.put(name, new PaimonColumnHandle(name, i));
        }
        return handles;
    }

    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        List<ConnectorPartitionInfo> partitions = cachedPartitions((PaimonTableHandle) handle);
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
     *
     * <p>A present {@code filter} BYPASSES the derived cache (computes directly, never populates) — it is
     * not the pruning path and not keyed by filter. Every other case routes through {@link #cachedPartitions},
     * the shared cache-aware collector this hook shares with {@link #listPartitionNames}.
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        if (filter.isPresent()) {
            return collectPartitions(paimonHandle);
        }
        return cachedPartitions(paimonHandle);
    }

    /**
     * Shared cache-aware partition collector backing the no-filter path of {@link #listPartitions} plus
     * {@link #listPartitionNames}. Returns the BUILT
     * {@code List<ConnectorPartitionInfo>} from {@link #partitionViewCache} (PERF-06 cache A), keyed by
     * {@code (db, table, snapshotId, schemaId)} (see {@link #partitionViewCacheKey}) — a hit skips both
     * {@link #collectPartitions} and the remote {@code catalogOps.listPartitions} round-trip, so repeated
     * SHOW PARTITIONS / {@code partition_values()} / pruning over the same {@code (db, table, snapshotId)}
     * render the list once.
     *
     * <p>The cache is BYPASSED (compute directly via {@link #collectPartitions}, never populated) when
     * {@code partitionViewCache} is {@code null} (the convenience/test ctors) or the handle is unpartitioned
     * (mirrors {@link #collectPartitions}'s own empty-partitionKeys short-circuit, so an unpartitioned table
     * never touches {@link #latestSnapshotCache} either — preserving the "no seam call" contract the
     * unpartitioned path already guarantees).
     */
    private List<ConnectorPartitionInfo> cachedPartitions(PaimonTableHandle paimonHandle) {
        List<String> partitionKeys = paimonHandle.getPartitionKeys();
        Map<String, String> scanOptions = paimonHandle.getScanOptions();
        if (partitionViewCache == null || partitionKeys == null || partitionKeys.isEmpty()
                || (!scanOptions.isEmpty() && !PaimonScanParams.hasOnlyReaderOptions(scanOptions))) {
            return collectPartitions(paimonHandle);
        }
        ConnectorTableKey key = partitionViewCacheKey(paimonHandle);
        return partitionViewCache.get(key, () -> collectPartitions(paimonHandle));
    }

    /**
     * Builds cache A's key for {@code paimonHandle}: {@code (db, table, snapshotId, schemaId)}.
     *
     * <p><b>snapshotId</b>: cached calls have no startup-changing scan options (those bypass this cache), so
     * {@link #collectPartitions} enumerates the current resolved table copy. The key therefore reads the SAME
     * per-catalog
     * {@link #latestSnapshotCache} that {@link #beginQuerySnapshot} pins queries to (a cheap in-memory hit within
     * the query — {@code beginQuerySnapshot} already warmed it), so a repeat query within the TTL hits this cache,
     * and a new snapshot (data change, once the entry expires or REFRESH invalidates it) naturally mints a new key.
     *
     * <p><b>schemaId</b>: pinned {@code -1} ("unversioned" for that axis, matching
     * {@link ConnectorTableKey}'s documented convention). Unlike iceberg, paimon's {@link PaimonTableHandle}
     * carries no schemaId — {@code applySnapshot} threads only {@code scanOptions} (an opaque properties map;
     * see its javadoc) onto the handle, and {@link #beginQuerySnapshot} (the common latest-pin path) never
     * resolves a schemaId either (its {@code ConnectorMvccSnapshot} keeps the builder default {@code -1}). This
     * is not a loss for THIS view: {@link #collectPartitions} derives its output from {@code partitionKeys}
     * (fixed at handle-build time) and paimon's raw partition specs, and paimon partition columns are immutable
     * post-creation, so schema evolution (e.g. ADD COLUMN) does not change what this method computes.
     */
    private ConnectorTableKey partitionViewCacheKey(PaimonTableHandle paimonHandle) {
        Identifier identifier = Identifier.create(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        long snapshotId = latestSnapshotCache.getOrLoad(identifier,
                () -> catalogOps.latestSnapshotId(resolveTable(paimonHandle)).orElse(-1L));
        return new ConnectorTableKey(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName(), snapshotId, -1L);
    }

    /**
     * Shared (uncached) partition collector behind {@link #cachedPartitions} — the underlying compute for
     * {@link #listPartitionNames} and {@link #listPartitions}, also reached directly on the filter /
     * unpartitioned / null-cache bypass. Replicates the fe-core display-name logic
     * ({@code PaimonUtil.generatePartitionInfo} + {@code isLegacyPartitionName}) so the rendered
     * partition names stay byte-identical to fe-core — including #65904, which drives value order from
     * the partition columns and escapes path-special characters in the name via the Paimon SDK.
     */
    private List<ConnectorPartitionInfo> collectPartitions(PaimonTableHandle paimonHandle) {
        if (PaimonScanParams.isPinnedEmptyScan(paimonHandle.getScanOptions())) {
            // Do not reopen latest metadata after the statement fenced an empty table.
            return Collections.emptyList();
        }
        List<String> partitionKeys = paimonHandle.getPartitionKeys();
        // Legacy never lists partitions for unpartitioned tables: PaimonPartitionInfoLoader.load
        // returns EMPTY when partitionColumns is empty, so guard before touching the seam.
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptyList();
        }

        Table resolvedTable = resolveTable(paimonHandle);
        Map<String, String> scanOptions = paimonHandle.getScanOptions();
        boolean optionsPin = PaimonScanParams.isOptionsPin(scanOptions);
        Table table;
        if (optionsPin) {
            table = PaimonScanParams.applyOptions(resolvedTable, scanOptions);
        } else {
            String snapshotId = scanOptions.get(CoreOptions.SCAN_SNAPSHOT_ID.key());
            // Fence hydration receives an ordinary positive snapshot pin. Apply it before listing
            // partitions so EXPLAIN and block-rule accounting describe the same version as the scan.
            Table partitionTable = snapshotId == null
                    ? resolvedTable
                    : resolvedTable.copy(Collections.singletonMap(
                            CoreOptions.SCAN_SNAPSHOT_ID.key(), snapshotId));
            // Partition projection never opens a data reader, so reader-only settings must not
            // invalidate metadata that a later relation-scoped override can make safe.
            // Metadata planning can also touch Paimon's global manifest executor, so apply the
            // CPU-local cap to a disposable projection rather than the cached catalog handle.
            table = PaimonReaderOptions.runtimeSafeTable(partitionTable);
            PaimonReaderOptions.validateEffectivePlanningTable(table);
        }
        Identifier identifier = Identifier.create(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        List<Partition> paimonPartitions;
        try {
            paimonPartitions = context.executeAuthenticated(() -> {
                try {
                    // Always enumerate the exact resolved copy: both relation and catalog policies are
                    // query semantics and must survive this metadata-planning boundary.
                    return catalogOps.listPartitions(identifier, table);
                } catch (Catalog.TableNotExistException e) {
                    LOG.warn("Paimon table not found while listing partitions: {}", identifier, e);
                    return Collections.<Partition>emptyList();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list Paimon partitions: " + identifier, e);
        }

        boolean legacyName = Boolean.parseBoolean(
                table.options().getOrDefault("partition.legacy-name", "true"));

        // Paimon renders a genuine NULL partition value as its partition.default-name sentinel
        // (CoreOptions.PARTITION_DEFAULT_NAME, default "__DEFAULT_PARTITION__"). Read it the same way
        // as partition.legacy-name above so a table that overrides it is still honored.
        String defaultPartitionName = table.options()
                .getOrDefault("partition.default-name", "__DEFAULT_PARTITION__");

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
        // Two distinct specs whose values contain path-special characters could still render to the same
        // escaped name only if they are genuinely-duplicate remote metadata; fail loud rather than let a
        // later map-put silently drop one. Parity with fe-core #65904.
        Set<String> seenPartitionNames = new HashSet<>();
        for (Partition partition : paimonPartitions) {
            Map<String, String> spec = partition.spec();
            // Both lists are driven by partitionKeys (the partition-COLUMN order), NOT Paimon's spec
            // iteration order, so index i aligns with the partition-column type i that fe-core
            // (PluginDrivenMvccExternalTable.toListPartitionItem) zips them against.
            // Per-value SQL-NULL flags:
            List<Boolean> nullFlags = new ArrayList<>(partitionKeys.size());
            // Ordered rendered values, supplied so fe-core never parses values back out of the name:
            List<String> orderedValues = new ArrayList<>(partitionKeys.size());
            // Rendered spec fed to PartitionPathUtils.generatePartitionPath so the partition NAME escapes
            // path-special characters (/ = [ ] * ...) exactly like the Paimon SDK. Without escaping, two
            // distinct specs whose values contain '/' or '=' would concat to the same Hive-style name and
            // collide (one partition item silently lost). Parity with fe-core #65904. This same rendered map
            // is also handed to ConnectorPartitionInfo as the partition VALUE map (below), so the active
            // partition_values() TVF feeder (PluginDrivenExternalTable.getNameToPartitionValues) reads the
            // Hive-canonical rendered form (DATE formatted, genuine-null → NULL_PARTITION_NAME) instead of
            // paimon's raw spec (DATE=epoch-day, null=__DEFAULT_PARTITION__), which would fail the TVF
            // (convertStringToDateV2 throws) and mis-render null. Mirrors hive/iceberg, whose value maps
            // already hold decoded canonical strings.
            LinkedHashMap<String, String> renderedSpec = new LinkedHashMap<>();
            for (String partitionColumnName : partitionKeys) {
                String value = spec.get(partitionColumnName);
                boolean isNull = defaultPartitionName.equals(value);
                nullFlags.add(isNull);
                String rendered;
                if (isNull) {
                    // Genuine NULL partition value. Supply isNull=true so the FE bridge
                    // (PluginDrivenMvccExternalTable.toListPartitionItem) builds a typed NullLiteral and
                    // `col IS NULL` selects it (MTMV refresh materializes the null rows) — aligning prune with
                    // the native scan path, which already materializes it as SQL NULL from the typed Java-null.
                    // The name is still normalized to the Doris-canonical sentinel (partition-name identity is
                    // preserved; the value string is ignored once the flag marks it null). Handled before the
                    // DATE branch so a null DATE partition does not crash on Integer.parseInt("__DEFAULT_PARTITION__").
                    rendered = ConnectorPartitionValues.NULL_PARTITION_NAME;
                } else if (legacyName && dateColumns.contains(partitionColumnName)) {
                    // When partition.legacy-name = true (default), Paimon stores DATE as days since
                    // 1970-01-01 (epoch integer), so render it via the Paimon SDK formatDate; when
                    // false the value is already a human-readable date string.
                    rendered = DateTimeUtils.formatDate(Integer.parseInt(value));
                } else {
                    rendered = value;
                }
                orderedValues.add(rendered);
                renderedSpec.put(partitionColumnName, rendered);
            }
            String partitionName = renderPartitionName(renderedSpec);
            if (!seenPartitionNames.add(partitionName)) {
                throw new IllegalStateException("Duplicate Paimon partition name: " + partitionName);
            }
            // partitionValues = renderedSpec (rendered/normalized), keyed by the remote column name:
            // downstream indexes by raw remote keys but reads the Hive-canonical rendered value (see the
            // renderedSpec comment above for why the raw spec would break the partition_values() TVF).
            result.add(new ConnectorPartitionInfo(
                    partitionName,
                    renderedSpec,
                    Collections.emptyMap(),
                    partition.recordCount(),
                    partition.fileSizeInBytes(),
                    partition.lastFileCreationTime(),
                    partition.fileCount(),
                    orderedValues,
                    nullFlags));
        }
        return result;
    }

    /**
     * Renders a partition's Doris display name from its ordered rendered spec, exactly as
     * {@link #collectPartitions} does: {@code PartitionPathUtils.generatePartitionPath} yields the
     * escaped Hive-style {@code "k1=v1/k2=v2/"} (trailing separator), which is dropped. Shared so the
     * SHOW PARTITIONS name and the DROP PARTITION lookup name can never drift.
     */
    private static String renderPartitionName(LinkedHashMap<String, String> renderedSpec) {
        String partitionPath = PartitionPathUtils.generatePartitionPath(renderedSpec);
        return partitionPath.substring(0, partitionPath.length() - 1);
    }

    // ==================== ALTER TABLE ... DROP PARTITION ====================
    // DROP PARTITION on a paimon table is a DATA operation (clear the rows of the named partition), not a
    // schema change: it does NOT bump the schema id and does NOT go through Catalog.alterTable. Semantically
    // it is closest to INSERT OVERWRITE — a one-shot commit that rewrites (here, empties) a partition — so it
    // is committed through the paimon committer's truncatePartitions(), NOT the schema-commit path the column
    // ops use. It also does NOT ride the PaimonConnectorTransaction (that coordinates BE-produced write
    // fragments across a distributed INSERT); a partition truncate is FE-local metadata work with no BE side.

    /**
     * Clears the DATA of the named partitions. Each entry of {@code partitionNames} is a partition DISPLAY
     * name in {@link #listPartitionNames} form ({@code k1=v1/k2=v2}); it is resolved back to paimon's NATIVE
     * partition spec (DATE as epoch-day, genuine NULL as {@code partition.default-name}) via the same rendering
     * {@link #collectPartitions} uses, then truncated through the paimon committer.
     *
     * <p>{@code ifExists} follows the Doris {@code DROP PARTITION IF EXISTS} contract: a name absent from the
     * current partition set is a silent no-op when {@code true}, and a {@link DorisConnectorException} when
     * {@code false} (mirroring the internal-table {@code ERR_DROP_PARTITION_NON_EXISTENT}). Existence is decided
     * against the live partition listing, so an absent partition is never handed to the committer (paimon's
     * {@code truncatePartitions} would otherwise treat an unknown spec as an empty prefix and silently no-op,
     * losing the fail-loud contract). A no-op call (all names absent under {@code ifExists}) skips the commit
     * entirely.
     */
    @Override
    public void dropPartitions(ConnectorSession session, ConnectorTableHandle handle,
            List<String> partitionNames, boolean ifExists) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        if (paimonHandle.getPartitionKeys() == null || paimonHandle.getPartitionKeys().isEmpty()) {
            throw new DorisConnectorException("Cannot drop partition of non-partitioned Paimon table "
                    + paimonHandle.getDatabaseName() + "." + paimonHandle.getTableName());
        }
        // Authoritative display-name -> native paimon spec map for the CURRENT partitions, built with the
        // exact rendering listPartitionNames uses so a DROP name matches a SHOW PARTITIONS name byte-for-byte.
        Map<String, Map<String, String>> nativeSpecByName = nativePartitionSpecsByName(paimonHandle);
        List<Map<String, String>> specsToTruncate = new ArrayList<>(partitionNames.size());
        for (String partitionName : partitionNames) {
            Map<String, String> spec = nativeSpecByName.get(partitionName);
            if (spec == null) {
                if (ifExists) {
                    continue;
                }
                throw new DorisConnectorException("Partition '" + partitionName
                        + "' does not exist in Paimon table " + paimonHandle.getDatabaseName() + "."
                        + paimonHandle.getTableName());
            }
            specsToTruncate.add(spec);
        }
        if (specsToTruncate.isEmpty()) {
            // Every requested partition was absent under IF EXISTS: nothing to commit.
            return;
        }
        Identifier identifier = Identifier.create(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        try {
            context.executeAuthenticated(() -> {
                catalogOps.truncatePartitions(identifier, specsToTruncate);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop partition(s) " + partitionNames
                    + " in Paimon table " + identifier + ": " + e.getMessage(), e);
        }
        LOG.info("truncated {} partition(s) of Paimon table {}", specsToTruncate.size(), identifier);
    }

    /**
     * Builds a {@code displayName -> native paimon spec} map for the current partitions of {@code handle}.
     * The DISPLAY name is rendered exactly as {@link #collectPartitions} renders it (so a DROP PARTITION
     * name lines up with a SHOW PARTITIONS name), while the VALUE is paimon's RAW {@link Partition#spec()}
     * (DATE as epoch-day, genuine NULL as {@code partition.default-name}) — which is what
     * {@code truncatePartitions} matches partition files against, so it must NOT be the rendered form.
     *
     * <p>Reuses the same resolve + list + DATE-detection + null-sentinel logic as {@code collectPartitions};
     * only the map it accumulates differs (native spec as value instead of a ConnectorPartitionInfo).
     */
    private Map<String, Map<String, String>> nativePartitionSpecsByName(PaimonTableHandle paimonHandle) {
        List<String> partitionKeys = paimonHandle.getPartitionKeys();
        Table table = resolveTable(paimonHandle);
        Identifier identifier = Identifier.create(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        List<Partition> paimonPartitions;
        try {
            paimonPartitions = context.executeAuthenticated(() -> {
                try {
                    return catalogOps.listPartitions(identifier, table);
                } catch (Catalog.TableNotExistException e) {
                    LOG.warn("Paimon table not found while listing partitions for drop: {}", identifier, e);
                    return Collections.<Partition>emptyList();
                }
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to list Paimon partitions for drop: " + identifier, e);
        }

        boolean legacyName = Boolean.parseBoolean(
                table.options().getOrDefault("partition.legacy-name", "true"));
        String defaultPartitionName = table.options()
                .getOrDefault("partition.default-name", "__DEFAULT_PARTITION__");
        Set<String> partitionKeyNames = new HashSet<>(partitionKeys);
        Set<String> dateColumns = new HashSet<>();
        for (DataField field : table.rowType().getFields()) {
            if (partitionKeyNames.contains(field.name())
                    && field.type().getTypeRoot() == DataTypeRoot.DATE) {
                dateColumns.add(field.name());
            }
        }

        Map<String, Map<String, String>> result = new LinkedHashMap<>(paimonPartitions.size());
        for (Partition partition : paimonPartitions) {
            Map<String, String> spec = partition.spec();
            LinkedHashMap<String, String> renderedSpec = new LinkedHashMap<>();
            for (String partitionColumnName : partitionKeys) {
                String value = spec.get(partitionColumnName);
                String rendered;
                if (defaultPartitionName.equals(value)) {
                    rendered = ConnectorPartitionValues.NULL_PARTITION_NAME;
                } else if (legacyName && dateColumns.contains(partitionColumnName)) {
                    rendered = DateTimeUtils.formatDate(Integer.parseInt(value));
                } else {
                    rendered = value;
                }
                renderedSpec.put(partitionColumnName, rendered);
            }
            // Key = display name (collectPartitions parity); value = paimon's RAW spec (what truncate matches).
            result.put(renderPartitionName(renderedSpec), spec);
        }
        return result;
    }

    /**
     * Returns the base-table row count = sum of planned-split row counts (legacy
     * {@code PaimonExternalTable.fetchRowCount}: {@code rowCount > 0 ? rowCount : UNKNOWN}). Shared
     * by normal AND system paimon tables: fe-core {@code PluginDrivenSysExternalTable} inherits
     * {@code PluginDrivenExternalTable.fetchRowCount}, and {@link #resolveTable} is sys-aware, so a
     * sys handle plans its OWN synthetic table's splits (closes Finding 5.1 with one override).
     * Returns {@code Optional.empty()} (→ fe-core -1 / UNKNOWN) when the count is 0 (legacy parity)
     * or planning fails (best-effort, like the other connector read paths — stats run in background
     * analysis / SHOW and must not surface a transient remote error as a query-killing exception).
     * {@code dataSize} is left UNKNOWN (-1): legacy computed no base-table dataSize here.
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        long rowCount;
        try {
            Table table = PaimonReaderOptions.runtimeSafeTable(resolveTable(paimonHandle));
            table = runtimeSafeSystemTable(paimonHandle, table, Collections.emptyMap());
            PaimonReaderOptions.validateEffectiveTable(table);
            rowCount = catalogOps.rowCount(table);
        } catch (Exception e) {
            LOG.warn("Failed to compute Paimon row count for {}", paimonHandle, e);
            return Optional.empty();
        }
        if (rowCount > 0) {
            return Optional.of(new ConnectorTableStatistics(rowCount, -1));
        }
        return Optional.empty();   // 0 rows -> UNKNOWN, legacy parity
    }

    /**
     * Row count AS OF the pinned snapshot, for a time-travel read. Applies the snapshot to the handle (the
     * SAME {@link #applySnapshot} the scan path uses) and copies its scan options onto the resolved table,
     * so the summed split row counts reflect the pinned snapshot / branch / tag &mdash; matching the rows
     * the scan reads instead of the latest count. Any failure degrades to empty, and the caller then falls
     * back to the latest cached estimate (estimate-only, never a correctness concern).
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        if (snapshot == null) {
            return getTableStatistics(session, handle);
        }
        long rowCount;
        try {
            PaimonTableHandle pinned = (PaimonTableHandle) applySnapshot(session, handle, snapshot);
            if (PaimonScanParams.isPinnedEmptyScan(pinned.getScanOptions())) {
                // Empty is a real statement fence; reopening latest here could count a concurrent
                // first commit even though execution is still required to scan zero rows.
                return Optional.empty();
            }
            Table table = resolveTable(pinned);
            Map<String, String> scanOptions = pinned.getScanOptions();
            if (scanOptions != null && !scanOptions.isEmpty()) {
                table = PaimonScanParams.isOptionsPin(scanOptions)
                        ? PaimonScanParams.applyOptions(table, scanOptions)
                        : table.copy(scanOptions);
            }
            table = PaimonReaderOptions.runtimeSafeTable(table);
            table = runtimeSafeSystemTable(pinned, table, scanOptions);
            PaimonReaderOptions.validateEffectiveTable(table);
            rowCount = catalogOps.rowCount(table);
        } catch (Exception e) {
            LOG.warn("Failed to compute Paimon row count at snapshot {} for {}",
                    snapshot.getSnapshotId(), handle, e);
            return Optional.empty();
        }
        if (rowCount > 0) {
            return Optional.of(new ConnectorTableStatistics(rowCount, -1));
        }
        return Optional.empty();
    }

    private Table runtimeSafeSystemTable(
            PaimonTableHandle handle, Table systemTable, Map<String, String> scanOptions)
            throws Exception {
        if (!handle.isSystemTable()) {
            return systemTable;
        }
        Table dataTable = PaimonTableResolver.resolveSystemSource(catalogOps, handle, context);
        return PaimonReaderOptions.runtimeSafeSystemTable(
                handle.getSysTableName(), systemTable, dataTable, scanOptions);
    }

    /**
     * Resolves the live {@link Table} for a handle: prefer the transient reference, else re-load
     * from the catalog seam. Delegates to the single sys-aware {@link PaimonTableResolver} shared
     * with the scan path so there is exactly ONE reload rule (a sys handle reloads via the 4-arg
     * sys {@link Identifier}; see {@link PaimonTableResolver#resolve}). This keeps every metadata
     * read path ({@link #getTableSchema}, {@link #getColumnHandles}, {@link #collectPartitions})
     * sys-aware.
     *
     * <p>Preserves this site's original wrapping of a reload failure as a {@link RuntimeException}.
     */
    private Table resolveTable(PaimonTableHandle paimonHandle) {
        // M-11: wrap the (possibly remote) reload in executeAuthenticated (D-052) so every metadata
        // read path that resolves a table runs under the FE-injected Kerberos UGI. The transient-table
        // fast path inside resolve issues no RPC, so the wrap is a no-op there. The existing catch-all
        // absorbs the (under Kerberos, UGI.doAs-wrapped) reload failure exactly as before.
        try {
            return context.executeAuthenticated(() -> PaimonTableResolver.resolve(catalogOps, paimonHandle));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Paimon table: " + paimonHandle, e);
        }
    }

    private List<ConnectorColumn> mapFields(List<DataField> fields, List<String> primaryKeys) {
        List<ConnectorColumn> columns = new ArrayList<>(fields.size());
        for (DataField field : fields) {
            ConnectorType connectorType = PaimonTypeMapping.toConnectorType(
                    field.type(), typeMappingOptions);
            String comment = field.description();
            // Legacy parity (FIX-READ-NOTNULL): PaimonExternalTable / PaimonSysExternalTable always
            // built each Doris column with isAllowNull=true regardless of the paimon field's NOT NULL
            // flag. Paimon PK columns are always NOT NULL, so propagating that would flip nullability
            // metadata for almost every PK table and let nereids fold null-rejecting predicates the
            // legacy path never permitted (rows can still read as NULL under schema-evolution
            // default-fill). Keep columns nullable; do not propagate the paimon NOT NULL constraint
            // on the read path.
            boolean nullable = true;
            // Legacy DESC parity: PaimonExternalTable/PaimonSysExternalTable built every column (base AND
            // system table) with isKey=true (3rd positional Column arg), so DESC shows Key=true for all
            // paimon columns. The 5-arg ConnectorColumn ctor defaults isKey=false; pass true explicitly.
            ConnectorColumn column = new ConnectorColumn(
                    field.name(),
                    connectorType,
                    comment,
                    nullable,
                    null,
                    true);
            // Legacy DESC parity (PaimonExternalTable.initSchema:356 / PaimonSysExternalTable:270): a
            // TIMESTAMP_WITH_LOCAL_TIME_ZONE column carries the WITH_TIMEZONE "Extra" marker via
            // Column.setWithTZExtraInfo(). Mark it here so fe-core's ConnectorColumnConverter re-applies it.
            // The mark is driven by the SOURCE paimon type root, not the mapped Doris type, so it survives
            // whether enable.mapping.timestamp_tz maps the column to TIMESTAMPTZ (on) or DATETIMEV2 (off).
            if (field.type().getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                column = column.withTimeZone();
            }
            columns.add(column);
        }
        return columns;
    }

    private static PaimonTypeMapping.Options buildTypeMappingOptions(PaimonCatalogProperties props) {
        return new PaimonTypeMapping.Options(
                props.isEnableMappingVarbinary(), props.isEnableMappingTimestampTz());
    }
}
