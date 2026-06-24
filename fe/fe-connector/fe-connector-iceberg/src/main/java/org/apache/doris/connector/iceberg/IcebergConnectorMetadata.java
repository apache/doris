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
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

    // Internal sentinel property carrying a tag/branch ref name from resolveTimeTravel to applySnapshot (the
    // typed ConnectorMvccSnapshot has snapshotId/schemaId carriers but no ref field). NOT a BE scan option.
    static final String REF_PROPERTY = "iceberg.scan.ref";

    private final IcebergCatalogOps catalogOps;
    private final Map<String, String> properties;
    // Every remote metadata READ is wrapped in context.executeAuthenticated(...) so the FE-injected
    // Kerberos UGI applies — legacy IcebergMetadataOps wrapped each call in executionAuthenticator.execute,
    // and the paimon mirror (PaimonConnectorMetadata) wraps the equivalent reads. The default
    // executeAuthenticated is a pass-through, so simple-auth catalogs are unaffected.
    private final ConnectorContext context;
    // T08: per-catalog latest-snapshot cache, owned by the long-lived IcebergConnector and injected here so
    // beginQuerySnapshot pins a STABLE (possibly stale) snapshot across queries within the TTL (legacy
    // IcebergExternalMetaCache parity, mirrors paimon). The 3-arg ctor (direct-construction tests) passes a
    // DISABLED cache so those reads stay always-live.
    private final IcebergLatestSnapshotCache latestSnapshotCache;

    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context) {
        this(catalogOps, properties, context, new IcebergLatestSnapshotCache(0L, 1));
    }

    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, IcebergLatestSnapshotCache latestSnapshotCache) {
        this.catalogOps = catalogOps;
        this.properties = properties;
        this.context = context;
        this.latestSnapshotCache = latestSnapshotCache;
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
        if (iceHandle.isSystemTable()) {
            // System (metadata) table: load the base table and build the iceberg metadata-table, then
            // parse ITS schema (e.g. t$snapshots -> committed_at/snapshot_id/...). Mirrors legacy
            // IcebergSysExternalTable.getSysIcebergTable + getOrCreateSchemaCacheValue; the enable.mapping.*
            // flags are threaded by the shared buildTableSchema -> parseSchema (deviation 5).
            Table sysTable = loadSysTable(iceHandle);
            return buildTableSchema(iceHandle.getTableName(), sysTable, sysTable.schema());
        }
        // Mirror legacy IcebergMetadataOps.loadTable: wrap the remote load in the auth context. The schema
        // + table-property assembly is pure (operates on the already-loaded Table).
        Table table = loadTable(iceHandle);
        return buildTableSchema(iceHandle.getTableName(), table, table.schema());
    }

    /**
     * Returns the schema AS OF {@code snapshot.getSchemaId()} (the pinned schema version, for time-travel reads
     * under schema evolution), or the LATEST schema when there is no pinned schema id (null snapshot or
     * {@code schemaId < 0}). Mirrors legacy {@code IcebergUtils.getSchema}: {@code table.schemas().get(schemaId)}
     * when the id is set and a current snapshot exists, else {@code table.schema()}. Shares
     * {@link #buildTableSchema} with the latest path so the two cannot drift.
     */
    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        if (iceHandle.isSystemTable()) {
            // A metadata table has a FIXED schema, independent of snapshot/schema-version (t$snapshots
            // always exposes committed_at/snapshot_id/...; legacy has no schema-at-snapshot for sys
            // tables). The time-travel pin (deviation 1) selects which rows the SCAN reads (T05), not the
            // schema, so delegate to the latest path, which builds the metadata-table schema.
            return getTableSchema(session, handle);
        }
        if (snapshot == null || snapshot.getSchemaId() < 0) {
            return getTableSchema(session, handle);
        }
        Table table = loadTable(iceHandle);
        Schema schema;
        if (table.currentSnapshot() == null) {
            // Empty table: legacy getSchema falls back to the latest schema (NEWEST_SCHEMA_ID path).
            schema = table.schema();
        } else {
            schema = table.schemas().get((int) snapshot.getSchemaId());
            if (schema == null) {
                // Defensive: a pinned id absent from table.schemas() (legacy would NPE) -> latest.
                schema = table.schema();
            }
        }
        return buildTableSchema(iceHandle.getTableName(), table, schema);
    }

    /**
     * Assembles the {@link ConnectorTableSchema} for {@code table} from {@code schema} (the latest schema, or a
     * historical schema for a time-travel read). The {@code iceberg.format-version} / {@code location} /
     * {@code iceberg.partition-spec} properties are table-level (not schema-versioned). Factored out so the
     * latest and at-snapshot paths share ONE assembly.
     */
    private ConnectorTableSchema buildTableSchema(String tableName, Table table, Schema schema) {
        List<ConnectorColumn> columns = parseSchema(schema);

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

    /** Loads the iceberg {@link Table} through the seam, wrapped in the FE-injected auth context (Kerberos UGI). */
    private Table loadTable(IcebergTableHandle handle) {
        try {
            return context.executeAuthenticated(
                    () -> catalogOps.loadTable(handle.getDbName(), handle.getTableName()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table, error message is:" + e.getMessage(), e);
        }
    }

    /**
     * Loads the iceberg metadata (system) table for {@code handle} through the seam, wrapped in the
     * FE-injected auth context (Kerberos UGI). Mirrors legacy
     * {@code IcebergSysExternalTable.getSysIcebergTable}: load the base table by its BASE coordinates,
     * then build the metadata table via {@code MetadataTableUtils.createMetadataTableInstance}. Both the
     * base load and the (in-memory) metadata-table build run inside ONE {@code executeAuthenticated} so
     * the auth scope covers the remote base load. {@code handle.getSysTableName()} is the lower-cased name
     * already validated by {@code getSysTableHandle}, so {@code MetadataTableType.from} (case-insensitive)
     * never returns null.
     */
    private Table loadSysTable(IcebergTableHandle handle) {
        try {
            return context.executeAuthenticated(() -> {
                Table base = catalogOps.loadTable(handle.getDbName(), handle.getTableName());
                return MetadataTableUtils.createMetadataTableInstance(
                        base, MetadataTableType.from(handle.getSysTableName()));
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table, error message is:" + e.getMessage(), e);
        }
    }

    /**
     * Column handles keyed by (lowercased) column name, mirroring {@code PaimonConnectorMetadata}. The generic
     * {@code PluginDrivenScanNode.buildColumnHandles} looks each query slot up here by name, so the provider
     * receives the PRUNED set of requested columns — which the T06 field-id schema dictionary keys its
     * {@code current_schema_id = -1} entry off (the CI #969249 fix: the dict's top-level names == the BE
     * scan-slot names BY CONSTRUCTION). The field id is the iceberg {@code NestedField.fieldId()} (a permanent
     * invariant). The name is lowercased with {@code Locale.ROOT} to byte-match {@link #parseSchema} (so the
     * handle key == the Doris slot name).
     */
    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        // Mirror getTableSchema: wrap the remote load in the auth context. A sys handle resolves the
        // metadata-table columns (t$snapshots -> committed_at/...) so the generic scan node can look up
        // its pruned sys-table slots by name; a data handle resolves the base table's columns.
        Table table = iceHandle.isSystemTable() ? loadSysTable(iceHandle) : loadTable(iceHandle);
        List<Types.NestedField> fields = table.schema().columns();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (Types.NestedField field : fields) {
            String name = field.name().toLowerCase(Locale.ROOT);
            handles.put(name, new IcebergColumnHandle(name, field.fieldId()));
        }
        return handles;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Builds the read-path Thrift descriptor for an iceberg plugin table, forking on the catalog type
     * exactly as legacy {@code IcebergExternalTable.toThrift} / {@code IcebergSysExternalTable.toThrift}:
     * an {@code hms}-backed catalog sends {@code TTableType.HIVE_TABLE} carrying a {@link THiveTable}, every
     * other flavor sends {@code TTableType.ICEBERG_TABLE} carrying a {@link TIcebergTable}. The {@code hms}
     * predicate is CASE-INSENSITIVE to match legacy: legacy compares the FIXED constant
     * {@code getIcebergCatalogType()} (= {@code "hms"}) while the raw user value is lower-cased for factory
     * dispatch, so {@code iceberg.catalog.type="HMS"}/{@code "Hms"} still bound a HiveCatalog and emitted
     * {@code HIVE_TABLE}; matching that here keeps descriptor parity (P6.5-T07). Null-safe: an absent
     * {@code iceberg.catalog.type} -&gt; the ICEBERG_TABLE branch.
     *
     * <p>Without this override the SPI default returns {@code null}, so fe-core
     * ({@code PluginDrivenExternalTable.toThrift}) falls back to {@code TTableType.SCHEMA_TABLE} and BE's
     * {@code DescriptorTbl::create} builds a {@code SchemaTableDescriptor} instead of the
     * {@code Hive/IcebergTableDescriptor} legacy produced. BE never consults the descriptor table type for an
     * iceberg sys (JNI) scan, so this is FE-side parity (EXPLAIN/profile) + closes the latent base-table
     * descriptor gap. The SPI signature carries no handle, so this single override covers BOTH base and system
     * tables (legacy uses an identical fork for both), mirroring paimon's connector-level override.
     */
    @Override
    public TTableDescriptor buildTableDescriptor(
            ConnectorSession session,
            long tableId, String tableName, String dbName,
            String remoteName, int numCols, long catalogId) {
        if (IcebergConnectorProperties.TYPE_HMS.equalsIgnoreCase(
                properties.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE))) {
            THiveTable tHiveTable = new THiveTable(dbName, tableName, new HashMap<>());
            TTableDescriptor desc = new TTableDescriptor(
                    tableId, TTableType.HIVE_TABLE, numCols, 0, tableName, dbName);
            desc.setHiveTable(tHiveTable);
            return desc;
        }
        TIcebergTable tIcebergTable = new TIcebergTable(dbName, tableName, new HashMap<>());
        TTableDescriptor desc = new TTableDescriptor(
                tableId, TTableType.ICEBERG_TABLE, numCols, 0, tableName, dbName);
        desc.setIcebergTable(tIcebergTable);
        return desc;
    }

    // ========== E7: System Tables (P6.5) ==========

    /**
     * Lists the system-table names iceberg exposes. Connector-global: legacy
     * {@code IcebergSysTable.SUPPORTED_SYS_TABLES} is built once from {@code MetadataTableType.values()}
     * (minus {@code POSITION_DELETES}) and applies to every iceberg table, so this returns the same
     * lower-cased names for any base handle — a defensive unmodifiable copy. {@code POSITION_DELETES} is
     * NOT exposed (Q2): querying {@code t$position_deletes} degrades to the generic fe-core not-found path
     * rather than legacy's bespoke "not supported yet" message.
     */
    @Override
    public List<String> listSupportedSysTables(ConnectorSession session,
            ConnectorTableHandle baseTableHandle) {
        List<String> names = new ArrayList<>();
        for (MetadataTableType type : MetadataTableType.values()) {
            if (type != MetadataTableType.POSITION_DELETES) {
                names.add(type.name().toLowerCase(Locale.ROOT));
            }
        }
        return Collections.unmodifiableList(names);
    }

    /**
     * Resolves a handle for the named system table of {@code baseTableHandle}, or empty when iceberg does
     * not expose {@code sysName} (case-insensitive; includes a {@code null} name, an unknown name, and
     * {@code position_deletes}, Q2).
     *
     * <p>Resolution is LAZY and pure — no catalog round-trip. Unlike paimon (whose handle stashes a
     * transient SDK {@code Table}, so {@code getSysTableHandle} eagerly loads it), the iceberg handle
     * carries no SDK {@code Table}; the metadata-table is built on demand in {@code getTableSchema}/scan
     * via {@code MetadataTableUtils} (mirroring legacy {@code IcebergSysExternalTable.getSysIcebergTable},
     * which builds it lazily). Eager-loading here would be wasted work — the result cannot be stashed on
     * the handle, so it would be rebuilt downstream — and a remote round-trip not present in legacy. The
     * base table's existence has already been verified by {@code getTableHandle} (the generic
     * {@code PluginDrivenSysExternalTable.resolveConnectorTableHandle} acquires the base handle first).
     *
     * <p>Deviation 1 (time travel): the base handle's snapshot/ref/schema pin is RETAINED on the sys
     * handle (the OPPOSITE of paimon's pin-clearing {@code forSystemTable}) — iceberg system tables
     * legally time-travel ({@code t$snapshots FOR VERSION/TIME AS OF ...}), so a pinned sys read must
     * honor the pin.
     */
    @Override
    public Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
            ConnectorTableHandle baseTableHandle, String sysName) {
        // Null-safe: a null / unknown / position_deletes sysName is "this connector does not expose that
        // sys table" (Optional.empty per the contract), NOT an NPE/exception.
        if (!isSupportedSysTable(sysName)) {
            return Optional.empty();
        }
        // Normalize to lower case for handle-identity parity with legacy (SysTable renders the suffix as
        // "$" + name.toLowerCase()), so t$SNAPSHOTS and t$snapshots are the SAME handle. The support check
        // above is case-insensitive; only the canonical stored name is lower-cased.
        String sys = sysName.toLowerCase(Locale.ROOT);
        IcebergTableHandle base = (IcebergTableHandle) baseTableHandle;
        return Optional.of(IcebergTableHandle.forSystemTable(
                base.getDbName(), base.getTableName(), sys,
                base.getSnapshotId(), base.getRef(), base.getSchemaId()));
    }

    /**
     * Whether iceberg exposes a system table named {@code sysName} (case-insensitive). Mirrors legacy
     * {@code IcebergSysTable.SUPPORTED_SYS_TABLES}: every {@code MetadataTableType} except
     * {@code POSITION_DELETES} (Q2). A {@code null} name is simply not exposed (returns false, not NPE).
     */
    private static boolean isSupportedSysTable(String sysName) {
        if (sysName == null) {
            return false;
        }
        for (MetadataTableType type : MetadataTableType.values()) {
            if (type == MetadataTableType.POSITION_DELETES) {
                continue;
            }
            if (type.name().equalsIgnoreCase(sysName)) {
                return true;
            }
        }
        return false;
    }

    // ========== Write / Transaction (P6.3) ==========

    /**
     * Opens a connector transaction for an iceberg write statement. The transaction id is the
     * engine-side id allocated through the session, so it matches the id registered in the engine
     * transaction registry (by the generic {@code PluginDrivenTransactionManager}, in both the
     * per-manager map and {@code GlobalExternalTransactionInfoMgr}) and stamped into the data sink —
     * the BE&rarr;FE report path finds the txn by this id to feed it commit fragments.
     *
     * <p>Gate-closed / dormant until the P6.6 cutover: nothing routes plugin-driven iceberg writes
     * through this path yet. The single SDK {@code org.apache.iceberg.Transaction} that backs commit is
     * opened lazily by the write plan via {@link IcebergConnectorTransaction#beginWrite}; op selection
     * (T04), the commit-validation suite (T05), the sink (T06), and the {@code supportsInsert/Delete/
     * Merge} capability declarations (T06/T07) land in later tasks.</p>
     */
    @Override
    public ConnectorTransaction beginTransaction(ConnectorSession session) {
        return new IcebergConnectorTransaction(session.allocateTransactionId(), catalogOps, context);
    }

    /**
     * Iceberg is the first connector to support row-level DELETE / MERGE (merge-on-read position deletes /
     * deletion vectors). These capabilities route the generic {@code RowLevelDmlCommand} shell to iceberg's
     * fe-resident plan synthesis (T07c). The {@code format-version >= 2} requirement is enforced fail-loud at
     * {@code IcebergConnectorTransaction.beginWrite} (the begin-guards), matching legacy command-analysis
     * rejection. Gate-closed until P6.6: iceberg is not yet a {@code PluginDrivenExternalTable}, so the
     * capability dispatch is unreachable and the legacy {@code instanceof}-routed commands still run.
     */
    @Override
    public boolean supportsDelete() {
        return true;
    }

    @Override
    public boolean supportsMerge() {
        return true;
    }

    // ========== E5: MVCC snapshots / time travel ==========

    /**
     * The query-begin MVCC pin: the table's LATEST snapshot, used as the consistent version for every read of
     * {@code handle} in this query. Mirrors legacy {@code IcebergUtils.getLatestIcebergSnapshot}: the current
     * snapshot id (or {@code -1} for an empty table — iceberg DOES support MVCC, so it still pins, mirroring
     * paimon), and the LATEST schema id (NOT {@code currentSnapshot().schemaId()} — a schema-only change without
     * a new snapshot advances the schema while the snapshot's id lags; legacy reads {@code table.schema()
     * .schemaId()}). T08 serves this through the per-catalog {@link IcebergLatestSnapshotCache}: within the TTL
     * a HIT returns the cached pin without re-loading the table (saves the load I/O and keeps the snapshot
     * STABLE across queries — the legacy with-cache catalog). The cached value carries BOTH ids atomically so a
     * schema-only ALTER between two queries cannot skew snapshotId vs schemaId. A disabled cache
     * ({@code meta.cache.iceberg.table.ttl-second <= 0}) reads live every call (the legacy no-cache catalog).
     */
    @Override
    public Optional<ConnectorMvccSnapshot> beginQuerySnapshot(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        TableIdentifier id = TableIdentifier.of(iceHandle.getDbName(), iceHandle.getTableName());
        IcebergLatestSnapshotCache.CachedSnapshot pin = latestSnapshotCache.getOrLoad(id, () -> {
            Table table = loadTable(iceHandle);
            Snapshot current = table.currentSnapshot();
            return new IcebergLatestSnapshotCache.CachedSnapshot(
                    current == null ? -1L : current.snapshotId(), table.schema().schemaId());
        });
        return Optional.of(
                ConnectorMvccSnapshot.builder().snapshotId(pin.snapshotId).schemaId(pin.schemaId).build());
    }

    /**
     * Resolves an explicit time-travel {@code spec} into a pinned {@link ConnectorMvccSnapshot}, owning ALL
     * iceberg-specific parsing. Mirrors legacy {@code IcebergUtils.getQuerySpecSnapshot}, returning
     * {@link Optional#empty()} when the target is not found (the fe-core consumer renders the user-facing
     * "can't find …" error); only a MALFORMED spec (an unparseable snapshot id / datetime) throws.
     *
     * <ul>
     *   <li>{@code SNAPSHOT_ID} — {@code table.snapshot(Long.parseLong(v))}; absent ⇒ empty.</li>
     *   <li>{@code TIMESTAMP} — millis (digital ⇒ {@code parseLong}, else {@link IcebergTimeUtils#datetimeToMillis}
     *       in the session zone) ⇒ {@code SnapshotUtil.snapshotIdAsOfTime} (throws when none ⇒ caught → empty).</li>
     *   <li>{@code TAG}/{@code BRANCH} — {@code table.refs().get(name)}, validated as a tag/branch; pins by REF
     *       (carried in {@code properties[iceberg.scan.ref]}) so a later commit to the ref is honored (legacy
     *       {@code createTableScan} uses {@code scan.useRef(name)}). Schema id from
     *       {@code SnapshotUtil.schemaFor(table, name)}.</li>
     *   <li>{@code INCREMENTAL} — unsupported for iceberg (legacy {@code getQuerySpecSnapshot} never dispatches
     *       {@code @incr}); fail loud rather than silently read latest.</li>
     * </ul>
     */
    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(
            ConnectorSession session, ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
        Table table = loadTable((IcebergTableHandle) handle);
        switch (spec.getKind()) {
            case SNAPSHOT_ID: {
                long id = Long.parseLong(spec.getStringValue());
                Snapshot snapshot = table.snapshot(id);
                if (snapshot == null) {
                    return Optional.empty();
                }
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(id)
                        .schemaId(snapshot.schemaId())
                        .build());
            }
            case TIMESTAMP: {
                long millis = parseTimestampMillis(session, spec);
                long snapshotId;
                try {
                    snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, millis);
                } catch (IllegalArgumentException e) {
                    // No snapshot at or before the timestamp (legacy threw a UserException; the SPI contract is
                    // empty-if-none, and fe-core renders "can't find snapshot earlier than or equal to time").
                    return Optional.empty();
                }
                return Optional.of(ConnectorMvccSnapshot.builder()
                        .snapshotId(snapshotId)
                        .schemaId(table.snapshot(snapshotId).schemaId())
                        .build());
            }
            case TAG:
                return resolveRef(table, spec.getStringValue(), false);
            case BRANCH:
                return resolveRef(table, spec.getStringValue(), true);
            case INCREMENTAL:
            default:
                throw new DorisConnectorException(
                        "incremental read (@incr) is not supported for Iceberg tables");
        }
    }

    /**
     * Resolves a tag ({@code wantBranch=false}) or branch ({@code wantBranch=true}) ref to a pinned snapshot.
     * Empty when the ref is absent or the wrong kind (legacy threw "Table X does not have branch/tag named Y";
     * the SPI returns empty so fe-core renders the user-facing message). Pins the ref NAME, not its current
     * snapshot id — {@code applySnapshot} routes it to {@code scan.useRef(name)} (legacy parity).
     */
    private Optional<ConnectorMvccSnapshot> resolveRef(Table table, String refName, boolean wantBranch) {
        SnapshotRef ref = table.refs().get(refName);
        if (ref == null || (wantBranch ? !ref.isBranch() : !ref.isTag())) {
            return Optional.empty();
        }
        long schemaId = SnapshotUtil.schemaFor(table, refName).schemaId();
        return Optional.of(ConnectorMvccSnapshot.builder()
                .snapshotId(ref.snapshotId())
                .schemaId(schemaId)
                .property(REF_PROPERTY, refName)
                .build());
    }

    /**
     * Derives epoch-millis from a {@code TIMESTAMP} spec: a digital value is {@code Long.parseLong} (epoch
     * millis), a datetime string is parsed in the session zone, byte-faithful to legacy
     * {@code TimeUtils.timeStringToLong}. (Honoring the digital form is a benign superset — legacy iceberg always
     * parsed the value as a datetime string, where a digital value would have failed.)
     */
    private long parseTimestampMillis(ConnectorSession session, ConnectorTimeTravelSpec spec) {
        if (spec.isDigital()) {
            return Long.parseLong(spec.getStringValue());
        }
        return IcebergTimeUtils.datetimeToMillis(
                spec.getStringValue(), IcebergTimeUtils.resolveSessionZone(session));
    }

    /**
     * Threads a resolved MVCC / time-travel pin onto the handle BEFORE the scan reads it (the generic
     * {@code PluginDrivenScanNode} calls this via {@code applyMvccSnapshotPin}). Reads the typed
     * {@code snapshotId}/{@code schemaId} and the {@code iceberg.scan.ref} property; an empty-table / query-begin
     * latest pin ({@code snapshotId<0} and no ref) returns the handle UNCHANGED (read latest — a
     * {@code useSnapshot(-1)} would be a non-existent snapshot; mirrors paimon's {@code -1} guard).
     */
    @Override
    public ConnectorTableHandle applySnapshot(ConnectorSession session,
            ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        if (snapshot == null) {
            return iceHandle;
        }
        String ref = snapshot.getProperties().get(REF_PROPERTY);
        long snapshotId = snapshot.getSnapshotId();
        if (snapshotId < 0 && ref == null) {
            return iceHandle;
        }
        return iceHandle.withSnapshot(snapshotId, ref, snapshot.getSchemaId());
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
