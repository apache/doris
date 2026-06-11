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
        // resolveTable branches on isSystemTable() to pick the 4-arg sys Identifier vs the 2-arg
        // base Identifier on a transient-table-null reload, so a sys handle reads its OWN rowType.
        Table table = resolveTable(paimonHandle);
        // The LATEST path keys partition_columns off the HANDLE's partitionKeys (the handle was
        // built from the latest table.partitionKeys()); fields + primaryKeys come from the live
        // table. Sharing buildTableSchema with the at-snapshot path keeps the two from drifting.
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
        Table table = resolveTable(paimonHandle);
        PaimonCatalogOps.PaimonSchemaSnapshot schema =
                catalogOps.schemaAt(table, snapshot.getSchemaId());
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
            schemaProps.put("partition_columns", String.join(",", partitionKeys));
        }
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            schemaProps.put("primary_keys", String.join(",", primaryKeys));
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
        Table sysTable;
        try {
            sysTable = catalogOps.getTable(sysId);
        } catch (Catalog.TableNotExistException e) {
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
        Table table = resolveTable(paimonHandle);
        long id = catalogOps.latestSnapshotId(table).orElse(-1L);
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
                // (the legacy null-valued scan.snapshot-id/scan.mode resets are stripped — see that
                // class's javadoc for why that's byte-parity on a freshly-loaded base table).
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

        // Partition enumeration is intentionally BASE-only: branch / time-travel reads carry EMPTY
        // partition info (legacy PaimonPartitionInfo.EMPTY) and never reach this path, so for the
        // (non-branch) handles that do, resolveTable returns the base table and the base-Identifier
        // listing below is consistent. (A branch handle would otherwise mix branch schema metadata
        // here with the base partition list — but that combination does not occur by design.)
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
                    partition.lastFileCreationTime(),
                    partition.fileCount()));
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
        try {
            return PaimonTableResolver.resolve(catalogOps, paimonHandle);
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
                        PaimonConnectorProperties.ENABLE_MAPPING_VARBINARY,
                        "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(
                        PaimonConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ,
                        "false"));
        return new PaimonTypeMapping.Options(binaryAsVarbinary, timestampTz);
    }
}
