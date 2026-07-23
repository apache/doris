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
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorPartitionValues;
import org.apache.doris.connector.cache.ConnectorPartitionViewCache;
import org.apache.doris.connector.cache.PartitionViewCacheKey;
import org.apache.doris.connector.spi.ConnectorContext;
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
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowType;
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
    private final Map<String, String> catalogProperties;

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

    // PERF-06: cross-query DERIVED partition-view cache A (generic ConnectorPartitionViewCache), injected by the
    // owning PaimonConnector; null = no cross-query derived layer (the convenience/test ctors used by ~15
    // existing direct-construction tests pass null). Layered ABOVE the raw remote catalogOps.listPartitions
    // call: a hit skips both the derived-view BUILD (collectPartitions) and the remote round-trip, keyed by
    // (db, table, snapshotId, schemaId). Consumed only by listPartitions -- paimon does not override
    // getMvccPartitionView (see ConnectorMetadata's default), so the generic MTMV model already uses
    // listPartitions for its LIST/timestamp partition view; there is no second hook to wrap.
    private final ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> partitionViewCache;

    public PaimonConnectorMetadata(PaimonCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context) {
        this(catalogOps, properties, context, new PaimonSchemaAtMemo(PaimonSchemaAtMemo.DEFAULT_MAX_SIZE));
    }

    PaimonConnectorMetadata(PaimonCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, PaimonSchemaAtMemo schemaAtMemo) {
        this(catalogOps, properties, context, schemaAtMemo, new PaimonLatestSnapshotCache(0L, 1));
    }

    /** Convenience ctor without the PERF-06 derived partition-view cache (null -> listPartitions always live). */
    PaimonConnectorMetadata(PaimonCatalogOps catalogOps, Map<String, String> properties,
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
    PaimonConnectorMetadata(PaimonCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, PaimonSchemaAtMemo schemaAtMemo,
            PaimonLatestSnapshotCache latestSnapshotCache,
            ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> partitionViewCache) {
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
        if (snapshot == null || snapshot.getSchemaId() < 0) {
            return getTableSchema(session, handle);
        }
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        long schemaId = snapshot.getSchemaId();
        Table table = resolveTable(paimonHandle);
        // FIX-B-MC2: memoize the schemaAt schema-file read across queries. resolveTable + buildTableSchema
        // still run every query (keeping the live coreOptions/properties current); only the schemaAt
        // round-trip is skipped on a repeat. The memo is keyed by (handle-identity, schemaId) — a pure
        // function — and owned by the per-catalog PaimonConnector. resolveTable runs ONCE, outside the
        // loader, so a branch handle's getTable reload happens at most once per query (= the pre-fix path).
        PaimonCatalogOps.PaimonSchemaSnapshot schema =
                schemaAtMemo.getOrLoad(paimonHandle, schemaId, () -> catalogOps.schemaAt(table, schemaId));
        return buildTableSchema(
                paimonHandle.getTableName(),
                table,
                schema.fields(),
                schema.partitionKeys(),
                schema.primaryKeys());
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
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            schemaProps.put(ConnectorTableSchema.PRIMARY_KEYS_KEY, String.join(",", primaryKeys));
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
     * {@code binlog} / {@code audit_log} are NAME-forced to the JNI reader. Other sys tables ("ro",
     * metadata tables) are NOT force-forced here; their JNI-vs-native routing is decided at scan
     * time by split type (T19), so this must not over-force.
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
        Identifier sysId = new Identifier(
                base.getDatabaseName(), base.getTableName(), "main", sys);
        // M-11: wrap the remote getTable in executeAuthenticated (D-052). TableNotExistException is
        // caught INSIDE the lambda (Kerberos UGI.doAs would wrap it otherwise) and signalled out as a
        // null Table so this method can still short-circuit to Optional.empty().
        Table sysTable;
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
        boolean forceJni = "binlog".equals(sys) || "audit_log".equals(sys);
        PaimonTableHandle handle = PaimonTableHandle.forSystemTable(
                base.getDatabaseName(), base.getTableName(), sys, forceJni);
        handle.setPaimonTable(sysTable);
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

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
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
            default:
                throw new UnsupportedOperationException(
                        "unsupported time-travel kind: " + spec.getKind());
        }
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
            return paimonHandle;
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
        // Empty-properties latest-pin (beginQuerySnapshot) path. Empty-table / query-begin parity:
        // beginQuerySnapshot pins INVALID_SNAPSHOT_ID (-1) for an empty table rather than
        // Optional.empty(). A -1 (or a null snapshot) must NOT become scan.snapshot-id=-1, because
        // Table.copy(scan.snapshot-id=-1) resolves to a non-existent snapshot in the paimon SDK
        // (confusing "snapshot/file not found"). Legacy never copied an invalid id: its empty /
        // query-begin path reads latest WITHOUT a copy. So return the handle UNCHANGED (read latest).
        if (snapshot == null || snapshot.getSnapshotId() < 0) {
            return paimonHandle;
        }
        Map<String, String> scanOptions = Collections.singletonMap(
                CoreOptions.SCAN_SNAPSHOT_ID.key(), String.valueOf(snapshot.getSnapshotId()));
        return paimonHandle.withScanOptions(scanOptions);
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
        // Reject a DISTRIBUTE BY clause up front (before the executeAuthenticated try, whose catch would rewrap
        // the message). Moved off fe-core CreateTableInfo.validate — the connector owns the paimon DDL rule.
        rejectDistribution(request);
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
            String name = fields.get(i).name();
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
     *
     * <p>PERF-06 cache A: the BUILT {@code List<ConnectorPartitionInfo>} is memoized across queries in
     * {@link #partitionViewCache}, keyed by {@code (db, table, snapshotId, schemaId)} (see
     * {@link #partitionViewCacheKey}) — a hit skips both {@link #collectPartitions} and the remote
     * {@code catalogOps.listPartitions} round-trip. The cache is BYPASSED (compute directly, never populated)
     * when: {@code partitionViewCache} is {@code null} (the convenience/test ctors); {@code filter} is present
     * (not the pruning path, and not keyed by filter); or the handle is unpartitioned (mirrors
     * {@link #collectPartitions}'s own empty-partitionKeys short-circuit, so an unpartitioned table never
     * touches {@link #latestSnapshotCache} either — preserving the "no seam call" contract the unpartitioned
     * path already guarantees).
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
        List<String> partitionKeys = paimonHandle.getPartitionKeys();
        if (partitionViewCache == null || filter.isPresent()
                || partitionKeys == null || partitionKeys.isEmpty()) {
            return collectPartitions(paimonHandle);
        }
        PartitionViewCacheKey key = partitionViewCacheKey(paimonHandle);
        return partitionViewCache.get(key, () -> collectPartitions(paimonHandle));
    }

    /**
     * Builds cache A's key for {@code paimonHandle}: {@code (db, table, snapshotId, schemaId)}.
     *
     * <p><b>snapshotId</b>: {@link #collectPartitions}'s remote call ({@code catalogOps.listPartitions(Identifier)})
     * is BASE-identifier-only — it does not apply the handle's pinned {@code scanOptions} (unlike the scan path),
     * so it always reflects the CURRENT catalog state, never a time-travel pin (branch / time-travel reads never
     * reach this path at all — see {@link #collectPartitions}). The key must therefore track "current", not
     * whatever snapshot happens to be threaded on the handle: it reads the SAME per-catalog
     * {@link #latestSnapshotCache} that {@link #beginQuerySnapshot} pins queries to (a cheap in-memory hit within
     * the query — {@code beginQuerySnapshot} already warmed it), so a repeat query within the TTL hits this cache,
     * and a new snapshot (data change, once the entry expires or REFRESH invalidates it) naturally mints a new key.
     *
     * <p><b>schemaId</b>: pinned {@code -1} ("unversioned" for that axis, matching
     * {@link PartitionViewCacheKey}'s documented convention). Unlike iceberg, paimon's {@link PaimonTableHandle}
     * carries no schemaId — {@code applySnapshot} threads only {@code scanOptions} (an opaque properties map;
     * see its javadoc) onto the handle, and {@link #beginQuerySnapshot} (the common latest-pin path) never
     * resolves a schemaId either (its {@code ConnectorMvccSnapshot} keeps the builder default {@code -1}). This
     * is not a loss for THIS view: {@link #collectPartitions} derives its output from {@code partitionKeys}
     * (fixed at handle-build time) and paimon's raw partition specs, and paimon partition columns are immutable
     * post-creation, so schema evolution (e.g. ADD COLUMN) does not change what this method computes.
     */
    private PartitionViewCacheKey partitionViewCacheKey(PaimonTableHandle paimonHandle) {
        Identifier identifier = Identifier.create(paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        long snapshotId = latestSnapshotCache.getOrLoad(identifier,
                () -> catalogOps.latestSnapshotId(resolveTable(paimonHandle)).orElse(-1L));
        return new PartitionViewCacheKey(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName(), snapshotId, -1L);
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
     * {@link #listPartitionValues}. Replicates the fe-core display-name logic
     * ({@code PaimonUtil.generatePartitionInfo} + {@code isLegacyPartitionName}) so the rendered
     * partition names stay byte-identical to fe-core — including #65904, which drives value order from
     * the partition columns and escapes path-special characters in the name via the Paimon SDK.
     */
    private List<ConnectorPartitionInfo> collectPartitions(PaimonTableHandle paimonHandle) {
        List<String> partitionKeys = paimonHandle.getPartitionKeys();
        // Legacy never lists partitions for unpartitioned tables: PaimonPartitionInfoLoader.load
        // returns EMPTY when partitionColumns is empty, so guard before touching the seam.
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return Collections.emptyList();
        }

        // Partition enumeration is intentionally BASE-only: branch / time-travel reads carry EMPTY
        // partition info (legacy PaimonPartitionInfo.EMPTY) and never reach this path, so for the
        // (non-branch) handles that do, resolveTable returns the base table and the base-Identifier
        // listing below is consistent. (A branch handle would otherwise mix branch schema metadata
        // here with the base partition list — but that combination does not occur by design.)
        Table table = resolveTable(paimonHandle);
        Identifier identifier = Identifier.create(
                paimonHandle.getDatabaseName(), paimonHandle.getTableName());
        // M-11: wrap the remote listPartitions in executeAuthenticated (D-052), mirroring legacy
        // PaimonExternalCatalog.getPaimonPartitions which ran it inside executionAuthenticator.execute
        // and swallowed TableNotExistException INSIDE the wrap (Kerberos UGI.doAs would otherwise wrap
        // the checked exception, so it must be caught inside).
        List<Partition> paimonPartitions;
        try {
            paimonPartitions = context.executeAuthenticated(() -> {
                try {
                    return catalogOps.listPartitions(identifier);
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
            // collide (one partition item silently lost). Parity with fe-core #65904.
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
                    rendered = ConnectorPartitionValues.HIVE_DEFAULT_PARTITION;
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
            // generatePartitionPath returns "k1=v1/k2=v2/" (escaped values, trailing separator); drop it.
            String partitionPath = PartitionPathUtils.generatePartitionPath(renderedSpec);
            String partitionName = partitionPath.substring(0, partitionPath.length() - 1);
            if (!seenPartitionNames.add(partitionName)) {
                throw new IllegalStateException("Duplicate Paimon partition name: " + partitionName);
            }
            // partitionValues = RAW spec (un-rendered): downstream indexes by raw remote keys.
            result.add(new ConnectorPartitionInfo(
                    partitionName,
                    spec,
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
            rowCount = catalogOps.rowCount(resolveTable(paimonHandle));
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

    private static PaimonTypeMapping.Options buildTypeMappingOptions(Map<String, String> props) {
        boolean binaryAsVarbinary = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_VARBINARY,
                        "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ,
                        "false"));
        return new PaimonTypeMapping.Options(binaryAsVarbinary, timestampTz);
    }
}
