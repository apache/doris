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
import org.apache.doris.connector.api.ConnectorDatabaseMetadata;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.ConnectorViewDefinition;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorColumnPath;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.ConnectorSortField;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.PartitionFieldChange;
import org.apache.doris.connector.api.ddl.TagChange;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.cache.ConnectorPartitionViewCache;
import org.apache.doris.connector.cache.PartitionViewCacheKey;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.THiveTable;
import org.apache.doris.thrift.TIcebergTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;

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

    // Iceberg v3 row-lineage hidden columns. Local literal copies of the Doris-side constants — the
    // connector cannot import fe-core. Column names mirror IcebergUtils.ICEBERG_ROW_ID_COL /
    // ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL; the reserved field ids and the min format version mirror
    // IcebergUtils.appendRowLineageColumnsForV3 / ICEBERG_ROW_LINEAGE_MIN_VERSION. A fe-core contract test
    // (IcebergUtilsTest / PluginDrivenScanNodeClassifyColumnTest) pins these values so a change there fails
    // loud, flagging that these duplicates must change too.
    private static final String ICEBERG_ROW_ID_COL = "_row_id";
    private static final String ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL = "_last_updated_sequence_number";
    private static final int ICEBERG_ROW_ID_FIELD_ID = 2147483540;
    private static final int ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID = 2147483539;
    private static final int ICEBERG_ROW_LINEAGE_MIN_VERSION = 3;

    // Snapshot-summary keys for table-level row count (getTableStatistics). Local literal copies of the
    // spec-stable iceberg strings — byte-identical to legacy IcebergUtils.TOTAL_* and to the COUNT(*)
    // pushdown copies in IcebergScanPlanProvider (themselves deliberately NOT org.apache.iceberg
    // .SnapshotSummary.* per that file's note). Duplicated rather than shared so this fix does not touch
    // the unrelated scan provider. All THREE keys are read: legacy getIcebergRowCount (via
    // getCountFromSummary, upstream 32a2651f66b / #64648) nets out position deletes AND gates the count to
    // UNKNOWN on any equality delete — see computeRowCount.
    private static final String TOTAL_RECORDS = "total-records";
    private static final String TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String TOTAL_EQUALITY_DELETES = "total-equality-deletes";

    // Doris-level table property carrying a user comment. Local literal copy of the fe-core constant
    // IcebergExternalTable.TABLE_COMMENT_PROP ("comment") — the connector cannot import fe-core. Read by
    // getTableComment (F9/F12) so the flipped iceberg table's COMMENT clause is non-empty.
    private static final String TABLE_COMMENT_PROP = "comment";

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
    // PERF-01: cross-query RAW-table cache (null = no cross-query layer). The 3-arg direct-construction tests
    // and the credential-gated catalogs pass null; the query-scoped fat handle (IcebergTableHandle) works
    // regardless. Consumed only by resolveTableForRead.
    private final IcebergTableCache tableCache;
    // PERF-02: cross-query partition-view cache (null = no cross-query layer; the convenience ctors used by
    // direct-construction tests pass null). Consumed by getMvccPartitionView / listPartitions / listPartitionNames.
    private final IcebergPartitionCache partitionCache;
    // PERF-05: cross-query table-comment cache (null = no cross-query layer). Non-null only when the owning
    // connector is a REST vended-credentials, non-session catalog (see IcebergConnector); the convenience ctors
    // used by direct-construction tests pass null. Consumed only by getTableComment.
    private final IcebergCommentCache commentCache;
    // PERF-06: cross-query DERIVED partition-view cache A (generic ConnectorPartitionViewCache), injected by the
    // owning IcebergConnector; null = no cross-query derived layer (the convenience ctors used by
    // direct-construction tests pass null; a session=user catalog also passes null). Layered ABOVE partitionCache
    // (raw rows): a hit skips the derived-view BUILD, keyed by (db, table, snapshotId, schemaId). Two typed fields
    // because getMvccPartitionView and listPartitions return different derived types. Consumed by
    // getMvccPartitionView / listPartitions respectively.
    private final ConnectorPartitionViewCache<ConnectorMvccPartitionView> mvccPartitionViewCache;
    private final ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> listPartitionsViewCache;

    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context) {
        this(catalogOps, properties, context, new IcebergLatestSnapshotCache(0L, 1), null, null);
    }

    /** Convenience ctor without a cross-query table cache (tableCache null); used by MVCC/statistics tests. */
    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, IcebergLatestSnapshotCache latestSnapshotCache) {
        this(catalogOps, properties, context, latestSnapshotCache, null, null);
    }

    /** Convenience ctor without a partition-view cache (partitionCache null). */
    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, IcebergLatestSnapshotCache latestSnapshotCache,
            IcebergTableCache tableCache) {
        this(catalogOps, properties, context, latestSnapshotCache, tableCache, null);
    }

    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, IcebergLatestSnapshotCache latestSnapshotCache,
            IcebergTableCache tableCache, IcebergPartitionCache partitionCache) {
        this(catalogOps, properties, context, latestSnapshotCache, tableCache, partitionCache, null);
    }

    /** Convenience ctor without the PERF-06 derived partition-view caches (both null). */
    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, IcebergLatestSnapshotCache latestSnapshotCache,
            IcebergTableCache tableCache, IcebergPartitionCache partitionCache,
            IcebergCommentCache commentCache) {
        this(catalogOps, properties, context, latestSnapshotCache, tableCache, partitionCache, commentCache,
                null, null);
    }

    /**
     * Full ctor used by {@link IcebergConnector#getMetadata}, adding the PERF-06 derived partition-view caches
     * (cache A): {@code mvccPartitionViewCache} memoizes {@link #getMvccPartitionView}'s built RANGE view and
     * {@code listPartitionsViewCache} memoizes {@link #listPartitions}'s built partition-info list, each keyed by
     * {@code (db, table, snapshotId, schemaId)}. Both {@code null} for a session=user catalog / the convenience
     * ctors (no cross-query derived layer -> compute directly every call).
     */
    public IcebergConnectorMetadata(IcebergCatalogOps catalogOps, Map<String, String> properties,
            ConnectorContext context, IcebergLatestSnapshotCache latestSnapshotCache,
            IcebergTableCache tableCache, IcebergPartitionCache partitionCache,
            IcebergCommentCache commentCache,
            ConnectorPartitionViewCache<ConnectorMvccPartitionView> mvccPartitionViewCache,
            ConnectorPartitionViewCache<List<ConnectorPartitionInfo>> listPartitionsViewCache) {
        this.catalogOps = catalogOps;
        this.properties = properties;
        this.context = context;
        this.latestSnapshotCache = latestSnapshotCache;
        this.tableCache = tableCache;
        this.partitionCache = partitionCache;
        this.commentCache = commentCache;
        this.mvccPartitionViewCache = mvccPartitionViewCache;
        this.listPartitionsViewCache = listPartitionsViewCache;
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

    @Override
    public ConnectorDatabaseMetadata getDatabase(ConnectorSession session, String dbName) {
        // Surface the namespace base location for SHOW CREATE DATABASE under the neutral "location"
        // property key (Trino-aligned properties-map model). Mirrors legacy IcebergExternalDatabase
        // .getLocation (SupportsNamespaces.loadNamespaceMetadata -> "location"), wrapped in the auth
        // context like the sibling reads. The location key is omitted when blank, so SHOW CREATE
        // DATABASE renders no LOCATION clause rather than LOCATION '' for a location-less namespace.
        try {
            Optional<String> location =
                    context.executeAuthenticated(() -> catalogOps.loadNamespaceLocation(dbName));
            Map<String, String> props = new HashMap<>();
            location.ifPresent(loc -> props.put(ConnectorDatabaseMetadata.LOCATION_PROPERTY, loc));
            return new ConnectorDatabaseMetadata(dbName, props);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get database metadata, error message is:" + e.getMessage(), e);
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
    public List<String> listViewNames(ConnectorSession session, String dbName) {
        // Mirror legacy IcebergMetadataOps.listViewNames: wrap in the auth context; a RuntimeException
        // (e.g. NoSuchNamespaceException) is rethrown verbatim, other failures are wrapped.
        try {
            return context.executeAuthenticated(() -> catalogOps.listViewNames(dbName));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list view names, error message is: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean viewExists(ConnectorSession session, String dbName, String viewName) {
        // Mirror legacy IcebergMetadataOps.viewExists (an existence check, like databaseExists / the
        // getTableHandle tableExists wrapper): wrap the remote check in the auth context and normalize EVERY
        // failure into a RuntimeException — unlike the listing methods (listTableNames / listViewNames), which
        // rethrow a RuntimeException verbatim so NoSuchNamespaceException surfaces unwrapped.
        try {
            return context.executeAuthenticated(() -> catalogOps.viewExists(dbName, viewName));
        } catch (Exception e) {
            throw new RuntimeException("Failed to check view exist, error message is: " + e.getMessage(), e);
        }
    }

    @Override
    public ConnectorViewDefinition getViewDefinition(ConnectorSession session, String dbName, String viewName) {
        // Mirror viewExists: wrap the remote load in the auth context and normalize EVERY failure into a
        // RuntimeException (the seam's loadView already fails loud on a non-view catalog with a
        // DorisConnectorException, which is a RuntimeException and surfaces wrapped here). ONE remote load
        // yields both the sql/dialect (mirroring legacy IcebergExternalTable.getViewText + getSqlDialect: the
        // dialect is the view-version summary's "engine-name", the SQL is that dialect's representation) AND
        // the columns (parseSchema(view.schema()), mirroring legacy IcebergUtils.loadViewSchemaCacheValue — a
        // view has NO partition columns and NO row-lineage). The sql/dialect/column extraction lives HERE,
        // not in the SDK-only seam, because parseSchema reads the enable.mapping.* flags that only exist in
        // this layer's properties (mirrors the table path: seam loadTable -> metadata buildTableSchema).
        try {
            return context.executeAuthenticated(() -> {
                View icebergView = catalogOps.loadView(dbName, viewName);
                ViewVersion viewVersion = icebergView.currentVersion();
                if (viewVersion == null) {
                    throw new DorisConnectorException(
                            String.format("Cannot get view version for view '%s'", icebergView));
                }
                Map<String, String> summary = viewVersion.summary();
                if (summary == null) {
                    throw new DorisConnectorException(String.format("Cannot get summary for view '%s'", icebergView));
                }
                // "engine-name" is the iceberg view-version summary key the writing engine (e.g. spark) records.
                String engineName = summary.get("engine-name");
                if (engineName == null || engineName.isEmpty()) {
                    throw new DorisConnectorException(
                            String.format("Cannot get engine-name for view '%s'", icebergView));
                }
                String dialect = engineName.toLowerCase(Locale.ROOT);
                SQLViewRepresentation sqlViewRepresentation = icebergView.sqlFor(dialect);
                if (sqlViewRepresentation == null) {
                    throw new DorisConnectorException("Cannot get view text from iceberg view");
                }
                List<ConnectorColumn> columns = parseSchema(icebergView.schema());
                return new ConnectorViewDefinition(sqlViewRepresentation.sql(), dialect, columns);
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to load view definition, error message is: " + e.getMessage(), e);
        }
    }

    @Override
    public void dropView(ConnectorSession session, String dbName, String viewName) {
        // Mirror legacy IcebergMetadataOps.performDropView (routed from dropTableImpl): drop the view inside
        // the auth context. Like the other write ops (dropTable / dropDatabase), normalize EVERY failure into a
        // DorisConnectorException so PluginDrivenExternalCatalog.dropTable rewraps it as a DdlException.
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropView(dbName, viewName);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop Iceberg view "
                    + dbName + "." + viewName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public String getTableComment(ConnectorSession session, String dbName, String tableName) {
        // Mirror legacy IcebergExternalTable.getComment: return the native iceberg table's "comment"
        // property (default ""). Wrap the remote load in the auth context like the other metadata reads.
        // Without this override the SPI default (ConnectorTableOps.getTableComment) returns "", so a flipped
        // iceberg table's COMMENT clause, information_schema.tables.TABLE_COMMENT, and SHOW TABLE STATUS
        // Comment column would all be blank even though the raw comment key still appears in the SHOW CREATE
        // PROPERTIES(...) block (F9/F12). Views render their comment through getViewDefinition / the view
        // SHOW CREATE arm, so a view handle here (loadTable throws) falls back to "" via the caller's catch.
        // PERF-05: on a vended-credentials (non-session) catalog, memoize the comment per table across queries so
        // the per-table loadTable that information_schema.tables / SHOW TABLE STATUS pays for EVERY table collapses
        // on repeats. commentCache is null for every other flavor (plain catalogs reuse tableCache via loadTable;
        // session=user must stay live to preserve per-user authorization) -> resolve directly. A thrown load (view
        // handle) is not cached and propagates to the caller's catch (still ""), so behavior is unchanged.
        if (commentCache != null) {
            return commentCache.getOrLoad(TableIdentifier.of(dbName, tableName),
                    () -> loadTableComment(session, dbName, tableName));
        }
        return loadTableComment(session, dbName, tableName);
    }

    private String loadTableComment(ConnectorSession session, String dbName, String tableName) {
        Table table = loadTable(session, new IcebergTableHandle(dbName, tableName));
        return table.properties().getOrDefault(TABLE_COMMENT_PROP, "");
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
        Table table = loadTable(session, iceHandle);
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
        Table table = loadTable(session, iceHandle);
        Schema schema;
        if (table.currentSnapshot() == null) {
            // Empty table: legacy getSchema falls back to the latest schema (NEWEST_SCHEMA_ID path).
            schema = table.schema();
        } else {
            schema = table.schemas().get((int) snapshot.getSchemaId());
            if (schema == null) {
                // Defensive: a pinned id absent from table.schemas() (legacy would NPE) -> latest.
                // INVARIANT: this SLOT-schema fallback MUST stay identical to the DICT-schema fallback in
                // IcebergScanPlanProvider.pinnedSchema (same getSchemaId() lookup + same silent -> table.schema()).
                // If the two diverge, the field-id dict names and the BE scan-slot names resolve DIFFERENT
                // schemas -> BE children.at() std::out_of_range-SIGABRT on a schema-evolved time-travel read
                // (reverify #65185 L16). Do not harden ONE side to throw without the other.
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

        // Append the iceberg v3 row-lineage hidden columns (_row_id / _last_updated_sequence_number) for
        // format-version >= 3 tables, mirroring legacy IcebergUtils.appendRowLineageColumnsForV3 — invoked
        // unconditionally (format-gated) from IcebergExternalTable.getFullSchema. They are BIGINT, nullable,
        // non-key, hidden, and carry a reserved Doris field id (matched BE-side); convertColumn re-applies
        // setIsVisible(false)/setUniqueId. Appended AFTER the data columns (legacy append order). Metadata
        // (system) tables report format-version 2 (BaseMetadataTable.properties() is empty), so the gate
        // naturally excludes them — matching legacy, which injects lineage only for data tables.
        if (getFormatVersion(table) >= ICEBERG_ROW_LINEAGE_MIN_VERSION) {
            // reservedPassthrough() marks these as engine-recognized passthrough columns so fe-core MERGE/UPDATE
            // and sink binding pass them through generically (via Column.isReservedPassthrough()) instead of
            // string-matching the iceberg names — the engine no longer knows _row_id / _last_updated_sequence_number.
            columns.add(new ConnectorColumn(ICEBERG_ROW_ID_COL, ConnectorType.of("BIGINT"),
                    "", true, null, false).invisible().withUniqueId(ICEBERG_ROW_ID_FIELD_ID).reservedPassthrough());
            columns.add(new ConnectorColumn(ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL, ConnectorType.of("BIGINT"),
                    "", true, null, false).invisible().withUniqueId(ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID)
                    .reservedPassthrough());
        }

        Map<String, String> tableProps = new HashMap<>();
        tableProps.putAll(table.properties());
        // SHOW CREATE TABLE render hints under neutral reserved keys (fe-core strips them from the
        // rendered PROPERTIES and emits them as LOCATION / PARTITION BY / ORDER BY). They replace the
        // previously-injected location / iceberg.format-version / iceberg.partition-spec keys: those were
        // never read by fe-core and would leak into the rendered PROPERTIES(...) (legacy iceberg SHOW
        // CREATE dumped only the raw table.properties()). format-version stays available via the
        // getFormatVersion(table) gate above for the row-lineage columns; it is not a user property.
        if (table.location() != null) {
            tableProps.put(ConnectorTableSchema.SHOW_LOCATION_KEY, table.location());
        }
        String partitionClause = buildShowPartitionClause(table);
        if (!partitionClause.isEmpty()) {
            tableProps.put(ConnectorTableSchema.SHOW_PARTITION_CLAUSE_KEY, partitionClause);
        }
        String sortClause = buildShowSortClause(table);
        if (!sortClause.isEmpty()) {
            tableProps.put(ConnectorTableSchema.SHOW_SORT_CLAUSE_KEY, sortClause);
        }
        if (!table.spec().isUnpartitioned()) {
            // Generic FE partition-column contract: post-cutover, PluginDrivenExternalTable derives the
            // table's partition columns SOLELY from a "partition_columns" CSV property (toSchemaCacheValue),
            // the same key MaxCompute/paimon emit. Mirror legacy IcebergUtils.loadTableSchemaCacheValue:
            // walk the CURRENT spec, resolve each partition field's SOURCE column name (NO identity filter,
            // NO dedupe), case-preserved to match parseSchema's case-preserved column names (#65094 read-path
            // alignment; fromRemoteColumnName is identity for iceberg, so the FE consumer looks the names up
            // case-sensitively).
            List<String> partitionColumns = new ArrayList<>();
            for (PartitionField field : table.spec().fields()) {
                Types.NestedField source = table.schema().findField(field.sourceId());
                if (source != null) {
                    partitionColumns.add(source.name());
                }
            }
            if (!partitionColumns.isEmpty()) {
                tableProps.put(ConnectorTableSchema.PARTITION_COLUMNS_KEY, String.join(",", partitionColumns));
            }
        }

        return new ConnectorTableSchema(tableName, columns, "ICEBERG", tableProps);
    }

    /**
     * Pre-renders the Doris {@code PARTITION BY LIST (...) ()} clause from the iceberg {@link PartitionSpec}
     * for SHOW CREATE TABLE (the FE plugin-driven path has no live iceberg API). Mirrors legacy
     * {@code IcebergExternalTable.getPartitionSpecSql}: void -> skipped, identity -> bare column,
     * {@code bucket[N]}/{@code truncate[W]}/{@code year}/{@code month}/{@code day}/{@code hour} -> the
     * matching Doris partition function. Returns "" for an unpartitioned table or no renderable field.
     */
    private String buildShowPartitionClause(Table table) {
        PartitionSpec spec = table.spec();
        if (spec == null || spec.isUnpartitioned()) {
            return "";
        }
        List<String> fields = new ArrayList<>();
        for (PartitionField field : spec.fields()) {
            String colName = table.schema().findColumnName(field.sourceId());
            if (colName == null) {
                continue;
            }
            org.apache.iceberg.transforms.Transform<?, ?> t = field.transform();
            if (t.isVoid()) {
                continue;
            }
            String quotedCol = "`" + colName + "`";
            if (t.isIdentity()) {
                fields.add(quotedCol);
            } else {
                String transformStr = t.toString();
                if (transformStr.startsWith("bucket[")) {
                    int n = Integer.parseInt(transformStr.substring(7, transformStr.length() - 1));
                    fields.add("BUCKET(" + n + ", " + quotedCol + ")");
                } else if (transformStr.startsWith("truncate[")) {
                    int w = Integer.parseInt(transformStr.substring(9, transformStr.length() - 1));
                    fields.add("TRUNCATE(" + w + ", " + quotedCol + ")");
                } else if ("year".equals(transformStr)) {
                    fields.add("YEAR(" + quotedCol + ")");
                } else if ("month".equals(transformStr)) {
                    fields.add("MONTH(" + quotedCol + ")");
                } else if ("day".equals(transformStr)) {
                    fields.add("DAY(" + quotedCol + ")");
                } else if ("hour".equals(transformStr)) {
                    fields.add("HOUR(" + quotedCol + ")");
                } else {
                    LOG.warn("Unsupported Iceberg partition transform '{}' on column '{}', "
                            + "skipped in SHOW CREATE TABLE.", transformStr, colName);
                }
            }
        }
        if (fields.isEmpty()) {
            return "";
        }
        return "PARTITION BY LIST (" + String.join(", ", fields) + ") ()";
    }

    /**
     * Pre-renders the Doris {@code ORDER BY (...)} clause from the iceberg {@link SortOrder} for SHOW
     * CREATE TABLE. Mirrors legacy {@code IcebergExternalTable.getSortOrderSql} + {@code SortFieldInfo.toSql}
     * ({@code `col` ASC|DESC NULLS FIRST|LAST}). Returns "" when the table is unsorted.
     */
    static String buildShowSortClause(Table table) {
        SortOrder sortOrder = table.sortOrder();
        if (sortOrder == null || sortOrder.isUnsorted() || sortOrder.fields().isEmpty()) {
            return "";
        }
        List<String> sortItems = new ArrayList<>();
        for (org.apache.iceberg.SortField sortField : sortOrder.fields()) {
            String columnName = table.schema().findColumnName(sortField.sourceId());
            if (columnName != null) {
                boolean isAscending = sortField.direction() != org.apache.iceberg.SortDirection.DESC;
                boolean isNullFirst = sortField.nullOrder() == org.apache.iceberg.NullOrder.NULLS_FIRST;
                sortItems.add("`" + columnName + "`"
                        + (isAscending ? " ASC" : " DESC")
                        + " NULLS " + (isNullFirst ? "FIRST" : "LAST"));
            }
        }
        return "ORDER BY (" + String.join(", ", sortItems) + ")";
    }

    /**
     * Loads the iceberg {@link Table} for {@code handle}, wrapped in the FE-injected auth context (Kerberos
     * UGI). Resolution goes through {@link #resolveTableForRead} (fat handle -&gt; cross-query cache -&gt;
     * remote), so the many reads sharing one handle in a planning/analysis pass collapse onto a single remote
     * {@code loadTable} (PERF-01); a fat-handle hit returns without any remote call.
     */
    private Table loadTable(ConnectorSession session, IcebergTableHandle handle) {
        try {
            return context.executeAuthenticated(() -> resolveTableForRead(session, handle));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table, error message is:" + e.getMessage(), e);
        }
    }

    /**
     * Resolves the RAW iceberg {@link Table} for {@code handle}, WITHOUT opening an auth scope or wrapping
     * exceptions — callers own both. The per-statement scope ({@link IcebergStatementScope#sharedTable}) comes
     * first, so the statement's read metadata, scan planning and write all resolve the SAME one loaded object;
     * on a scope miss the loader consults the cross-query {@link IcebergTableCache} when enabled (else a direct
     * remote load). The remote loader's exception propagates verbatim (the cache re-throws it unwrapped), so a
     * caller's own {@code NoSuchTableException} degradation (the partition-view readers) still fires. Callers
     * needing the auth scope wrap the call in {@code executeAuthenticated} (see {@link #loadTable}). NOT used by
     * the sys-table path ({@link #loadSysTable}), which takes a fresh remote base by design.
     */
    private Table resolveTableForRead(ConnectorSession session, IcebergTableHandle handle) {
        return IcebergStatementScope.sharedTable(session, handle.getDbName(), handle.getTableName(),
                () -> tableCache != null
                        ? tableCache.getOrLoad(TableIdentifier.of(handle.getDbName(), handle.getTableName()),
                                () -> catalogOps.loadTable(handle.getDbName(), handle.getTableName()))
                        : catalogOps.loadTable(handle.getDbName(), handle.getTableName()));
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
     * Column handles keyed by (case-preserved) column name, mirroring {@code PaimonConnectorMetadata}. The generic
     * {@code PluginDrivenScanNode.buildColumnHandles} looks each query slot up here by name, so the provider
     * receives the PRUNED set of requested columns — which the T06 field-id schema dictionary keys its
     * {@code current_schema_id = -1} entry off (the CI #969249 fix: the dict's top-level names == the BE
     * scan-slot names BY CONSTRUCTION). The field id is the iceberg {@code NestedField.fieldId()} (a permanent
     * invariant). The name is case-preserved (byte-matching {@link #parseSchema}) so the handle key == the
     * Doris slot name (#65094 read-path alignment: top-level names keep their remote case).
     */
    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        // Mirror getTableSchema: wrap the remote load in the auth context. A sys handle resolves the
        // metadata-table columns (t$snapshots -> committed_at/...) so the generic scan node can look up
        // its pruned sys-table slots by name; a data handle resolves the base table's columns.
        Table table = iceHandle.isSystemTable() ? loadSysTable(iceHandle) : loadTable(session, iceHandle);
        List<Types.NestedField> fields = table.schema().columns();
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>(fields.size());
        for (Types.NestedField field : fields) {
            String name = field.name();
            handles.put(name, new IcebergColumnHandle(name, field.fieldId()));
        }
        return handles;
    }

    /**
     * Table-level row count, surfaced to the FE optimizer via {@code PluginDrivenExternalTable.fetchRowCount}
     * (without this override the connector inherits {@code ConnectorStatisticsOps}'s {@code Optional.empty()},
     * so every iceberg table reports rowCount -1 -> CBO collapses cardinality to 1 and disables join reorder).
     * Mirrors {@code PaimonConnectorMetadata.getTableStatistics} in STRUCTURE, but uses the legacy iceberg
     * FORMULA ({@code IcebergUtils.getIcebergRowCount} -> {@code getCountFromSummary(summary, true)}:
     * {@code total-records - total-position-deletes}, gated to UNKNOWN when equality deletes are present).
     * Parity decisions:
     * <ul>
     *   <li>System tables -> empty: legacy {@code IcebergSysExternalTable.fetchRowCount} is unconditionally
     *       UNKNOWN; a sys handle would otherwise load the BASE table and misreport its data row count for a
     *       metadata table. (This is a deliberate divergence from paimon, which reports sys-table counts.)</li>
     *   <li>{@code rowCount > 0} gate: legacy data-table consumer is {@code rowCount > 0 ? rowCount : UNKNOWN},
     *       but the NEW consumer takes the value whenever {@code >= 0}, so a 0-row table would wrongly report 0.
     *       Collapsing {@code <= 0} to empty pins the "0 -> UNKNOWN" semantics here (matches paimon).</li>
     *   <li>Any failure degrades to empty (best effort): a statistics miss must never break query planning.</li>
     * </ul>
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        if (iceHandle.isSystemTable()) {
            return Optional.empty();
        }
        long rowCount;
        try {
            rowCount = computeRowCount(loadTable(session, iceHandle));
        } catch (Exception e) {
            LOG.warn("Failed to compute Iceberg row count for {}.{}",
                    iceHandle.getDbName(), iceHandle.getTableName(), e);
            return Optional.empty();
        }
        if (rowCount > 0) {
            return Optional.of(new ConnectorTableStatistics(rowCount, -1));
        }
        return Optional.empty();
    }

    /**
     * Table-level row count AS OF the pinned snapshot, for a time-travel read. Same formula as the latest
     * path but reads the pinned snapshot's summary (via {@code table.snapshot(snapshotId)}) instead of
     * {@code currentSnapshot()}, so the CBO estimate matches the rows the scan actually reads. Falls back
     * to the latest path for a system table or a missing/negative snapshot id.
     */
    @Override
    public Optional<ConnectorTableStatistics> getTableStatistics(
            ConnectorSession session, ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        if (iceHandle.isSystemTable() || snapshot == null || snapshot.getSnapshotId() < 0) {
            return getTableStatistics(session, handle);
        }
        long rowCount;
        try {
            rowCount = computeRowCount(loadTable(session, iceHandle).snapshot(snapshot.getSnapshotId()));
        } catch (Exception e) {
            LOG.warn("Failed to compute Iceberg row count at snapshot {} for {}.{}",
                    snapshot.getSnapshotId(), iceHandle.getDbName(), iceHandle.getTableName(), e);
            return Optional.empty();
        }
        if (rowCount > 0) {
            return Optional.of(new ConnectorTableStatistics(rowCount, -1));
        }
        return Optional.empty();
    }

    /**
     * Row count from the current snapshot summary, a faithful port of legacy {@code IcebergUtils
     * .getIcebergRowCount} (which calls {@code getCountFromSummary(summary, true)}, upstream 32a2651f66b /
     * #64648): any equality delete ({@code total-equality-deletes} absent or {@code != "0"}) -> -1 (UNKNOWN),
     * since equality deletes re-project at read time and the summary cannot net them out; otherwise
     * {@code total-records - total-position-deletes}. Shares the equality-delete gate with the COUNT(*)
     * pushdown {@code IcebergScanPlanProvider.getCountFromSummary}, differing only in dangling-delete handling
     * (table statistics always net out position deletes; the pushdown honors the dangling-delete session var).
     * Empty table (no current snapshot) -> -1, which the caller maps to UNKNOWN.
     */
    private static long computeRowCount(Table table) {
        return computeRowCount(table.currentSnapshot());
    }

    /** Row count from a specific snapshot's summary (shared by the latest and at-snapshot paths). */
    private static long computeRowCount(Snapshot snapshot) {
        if (snapshot == null) {
            return -1;
        }
        Map<String, String> summary = snapshot.summary();
        // Equality-delete gate + null-guard, a faithful port of legacy IcebergUtils.getCountFromSummary(
        // summary, true) (upstream 32a2651f66b, #64648): an absent total-* counter (compaction / replace /
        // overwrite snapshots may omit one — the pre-fix Long.parseLong(null) NPE-d), or any equality delete
        // (total-equality-deletes != "0"), makes the summary row count unsafe -> -1 (caller maps to UNKNOWN),
        // because equality deletes re-project at read time and the summary cannot net them out. Same gate as
        // the COUNT(*) pushdown IcebergScanPlanProvider.getCountFromSummary.
        String equalityDeletes = summary.get(TOTAL_EQUALITY_DELETES);
        String totalRecords = summary.get(TOTAL_RECORDS);
        String positionDeletes = summary.get(TOTAL_POSITION_DELETES);
        if (equalityDeletes == null || totalRecords == null || positionDeletes == null) {
            return -1;
        }
        if (!equalityDeletes.equals("0")) {
            return -1;
        }
        return Long.parseLong(totalRecords) - Long.parseLong(positionDeletes);
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

    // ========== DDL writes (B1): create/drop database + table ==========

    /**
     * Iceberg supports CREATE DATABASE (namespace). Declaring it lets {@code PluginDrivenExternalCatalog.createDb}
     * consult the remote namespace existence for IF NOT EXISTS (the SPI default {@code false} would skip that
     * check). Mirrors paimon.
     */
    @Override
    public boolean supportsCreateDatabase() {
        return true;
    }

    /**
     * Creates an iceberg namespace, mirroring legacy {@code IcebergMetadataOps.performCreateDb}. Namespace
     * properties are only honored by an HMS catalog; for every other flavor a non-empty property map fails
     * loud (legacy parity) — the gate is a pure local check run BEFORE the auth context, like paimon.
     * Existence / IF NOT EXISTS is resolved upstream by {@code PluginDrivenExternalCatalog.createDb}.
     */
    @Override
    public void createDatabase(ConnectorSession session, String dbName, Map<String, String> properties) {
        if (!properties.isEmpty() && !isHmsCatalog()) {
            throw new DorisConnectorException(
                    "Not supported: create database with properties for iceberg catalog type: " + catalogType());
        }
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createDatabase(dbName, properties);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to create Iceberg database " + dbName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Drops an iceberg namespace, mirroring legacy {@code IcebergMetadataOps.performDropDb}. With
     * {@code force} the contained tables are dropped (purged) first so a non-empty namespace can be removed;
     * the namespace location is captured BEFORE the drop and its empty directory shell pruned afterwards
     * (HMS only). Existence / IF EXISTS is resolved upstream by {@code PluginDrivenExternalCatalog.dropDb}, so
     * {@code ifExists} is accepted for SPI parity but not re-checked here.
     *
     * <p>A {@code force} drop cascades the contained iceberg VIEWS as well (they live in their own namespace,
     * so the table cascade alone would leave them behind and {@code dropNamespace} would fail "not empty").
     */
    @Override
    public void dropDatabase(ConnectorSession session, String dbName, boolean ifExists, boolean force) {
        Optional<String> namespaceLocation;
        try {
            namespaceLocation = context.executeAuthenticated(() -> {
                Optional<String> location;
                try {
                    location = isHmsCatalog()
                            ? catalogOps.loadNamespaceLocation(dbName) : Optional.empty();
                    if (force) {
                        for (String table : catalogOps.listTableNames(dbName)) {
                            catalogOps.dropTable(dbName, table, true);
                        }
                        // Cascade the views too, mirroring legacy IcebergMetadataOps.performDropDb: iceberg
                        // VIEWS live in their own namespace (listTableNames subtracts them), so without this the
                        // dropDatabase below would fail loud ("namespace not empty") when the db still has views.
                        for (String view : catalogOps.listViewNames(dbName)) {
                            catalogOps.dropView(dbName, view);
                        }
                    }
                } catch (NoSuchNamespaceException e) {
                    // FORCE drop of a namespace whose remote side is already gone: tolerate it as a silent
                    // success, mirroring legacy IcebergMetadataOps.performDropDb (which swallowed
                    // NoSuchNamespaceException during the force cascade). The FE cache still holds the db but
                    // the remote namespace vanished (e.g. dropped out-of-band) -> nothing left to drop or
                    // clean up. The location probe (HMS only) runs before the cascade, so the tolerant region
                    // covers it too. A non-force drop keeps failing loud (legacy parity: only FORCE tolerates
                    // a missing namespace).
                    if (!force) {
                        throw e;
                    }
                    LOG.info("drop database[{}] force which does not exist", dbName);
                    return Optional.<String>empty();
                }
                catalogOps.dropDatabase(dbName);
                return location;
            });
        } catch (Exception e) {
            throw new DorisConnectorException(
                    "Failed to drop Iceberg database " + dbName + ": " + e.getMessage(), e);
        }
        // Cleanup runs OUTSIDE the iceberg auth scope: it is engine-side (its own storage creds) and
        // best-effort (failures are swallowed by the engine), so it must never fail the completed drop.
        namespaceLocation.ifPresent(location ->
                context.cleanupEmptyManagedLocation(location, Collections.emptyList()));
    }

    /**
     * Creates an iceberg table, mirroring legacy {@code IcebergMetadataOps.performCreateTable}: the neutral
     * request is turned into an iceberg Schema / PartitionSpec / SortOrder / properties (with the Doris
     * merge-on-read defaults) by {@link IcebergSchemaBuilder}, then created through the seam. The artifact
     * build is pure (no remote call) and runs outside the auth context. Existence / IF NOT EXISTS is resolved
     * upstream by {@code PluginDrivenExternalCatalog.createTable}.
     */
    @Override
    public void createTable(ConnectorSession session, ConnectorCreateTableRequest request) {
        rejectDistribution(request);
        rejectReservedRowLineageColumns(request);
        validateSortOrder(request);
        Schema schema = IcebergSchemaBuilder.buildSchema(request.getColumns());
        PartitionSpec partitionSpec = IcebergSchemaBuilder.buildPartitionSpec(request.getPartitionSpec(), schema);
        SortOrder sortOrder = IcebergSchemaBuilder.buildSortOrder(request.getSortOrder(), schema);
        // Pass the catalog properties so a catalog-level table-default/override.format-version is respected
        // instead of being forced to v2 (upstream 25f291673f1, #63825). `properties` holds the raw catalog
        // CREATE properties (ConnectorFactory.createConnector(catalogProperty.getProperties())).
        Map<String, String> tableProperties =
                IcebergSchemaBuilder.buildTableProperties(request.getProperties(), properties);
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createTable(request.getDbName(), request.getTableName(),
                        schema, partitionSpec, sortOrder, tableProperties);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to create Iceberg table "
                    + request.getDbName() + "." + request.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Rejects a user-defined column whose name collides with an iceberg v3 reserved row-lineage column
     * ({@code _row_id} / {@code _last_updated_sequence_number}) on a format-version &ge; 3 table. Moved off
     * fe-core {@code CreateTableInfo.validateIcebergRowLineageColumns} — the connector owns the iceberg
     * column-name convention. Uses the full effective-format-version precedence (catalog
     * {@code table-override} &gt; table request &gt; catalog {@code table-default}). Behavior differs from the
     * former fe-core analysis-time check: it runs during {@code createTable} (later, and NOT reached when an
     * {@code IF NOT EXISTS} hits an existing table — accepted relaxation) and throws
     * {@link DorisConnectorException} rather than an engine {@code AnalysisException}; the message is unchanged.
     */
    private void rejectReservedRowLineageColumns(ConnectorCreateTableRequest request) {
        int formatVersion = IcebergSchemaBuilder.getEffectiveFormatVersion(request.getProperties(), properties);
        if (formatVersion < ICEBERG_ROW_LINEAGE_MIN_VERSION) {
            return;
        }
        for (ConnectorColumn column : request.getColumns()) {
            String name = column.getName();
            if (ICEBERG_ROW_ID_COL.equalsIgnoreCase(name)
                    || ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL.equalsIgnoreCase(name)) {
                throw new DorisConnectorException("Cannot create Iceberg v" + formatVersion
                        + " table with reserved row lineage column: " + name);
            }
        }
    }

    /**
     * Rejects a {@code DISTRIBUTE BY} clause: iceberg has no hash/random distribution, buckets are expressed via
     * {@code bucket(num, column)} inside {@code PARTITIONED BY}. Moved off fe-core {@code CreateTableInfo.validate}
     * — the connector owns the iceberg DDL rule. {@code request.getBucketSpec() != null} iff the user wrote
     * {@code DISTRIBUTE BY}. Message kept byte-identical to the former fe-core wording.
     */
    // package-private for unit test; reached only via createTable() in production.
    void rejectDistribution(ConnectorCreateTableRequest request) {
        if (request.getBucketSpec() != null) {
            throw new DorisConnectorException("Iceberg doesn't support 'DISTRIBUTE BY', "
                    + "and you can use 'bucket(num, column)' in 'PARTITIONED BY'.");
        }
    }

    /**
     * Validates the create-time write sort order ({@code CREATE TABLE ... ORDER BY (...)}) against the request
     * columns: every sort column must exist, be a sortable (non metric-only) type, and appear at most once. Moved
     * off fe-core {@code CreateTableInfo.validateIcebergSortOrder} — the iceberg connector owns this now that it is
     * the sole {@code SUPPORTS_SORT_ORDER} declarer (fe-core rejects a sort order on any non-declaring engine up
     * front). Runs BEFORE {@link IcebergSchemaBuilder#buildSchema} so a bad sort column surfaces this message
     * rather than a downstream schema-build error. Existence + duplicate checks are case-insensitive, mirroring the
     * former fe-core {@code CASE_INSENSITIVE_ORDER} maps.
     */
    // package-private for unit test; reached only via createTable() in production.
    void validateSortOrder(ConnectorCreateTableRequest request) {
        List<ConnectorSortField> sortOrder = request.getSortOrder();
        if (sortOrder == null || sortOrder.isEmpty()) {
            return;
        }
        Map<String, ConnectorColumn> columnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ConnectorColumn column : request.getColumns()) {
            columnMap.put(column.getName(), column);
        }
        for (ConnectorSortField field : sortOrder) {
            String sortCol = field.getColumnName();
            ConnectorColumn column = columnMap.get(sortCol);
            if (column == null) {
                throw new DorisConnectorException("Sort order column '" + sortCol + "' does not exist in table");
            }
            String typeName = column.getType().getTypeName();
            if (isMetricOnlyType(typeName)) {
                throw new DorisConnectorException("Sort order column '" + sortCol
                        + "' has unsupported type: " + typeName);
            }
        }
        Set<String> sortColSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ConnectorSortField field : sortOrder) {
            if (!sortColSet.add(field.getColumnName())) {
                throw new DorisConnectorException("Duplicate sort order column: " + field.getColumnName());
            }
        }
    }

    // Mirrors fe-core DataType.isOnlyMetricType(): HLL / BITMAP / QUANTILE_STATE cannot be sorted. These types are
    // not representable in an iceberg table, so this is a defensive parity check reached only if such a column ever
    // reaches createTable ahead of the schema builder.
    private static boolean isMetricOnlyType(String typeName) {
        return "HLL".equalsIgnoreCase(typeName)
                || "BITMAP".equalsIgnoreCase(typeName)
                || "QUANTILE_STATE".equalsIgnoreCase(typeName);
    }

    /**
     * Drops an iceberg table, mirroring legacy {@code IcebergMetadataOps.performDropTable}: the table location
     * is captured BEFORE the drop (HMS only), the table is dropped with {@code purge=true} (iceberg deletes the
     * data + metadata files), then the empty directory shell is pruned. {@code PluginDrivenExternalCatalog}
     * has already resolved the handle / IF EXISTS upstream.
     *
     * <p>This handles TABLES only: a DROP on an iceberg view is routed to {@link #dropView} by
     * {@code PluginDrivenExternalCatalog.dropTable} (via {@link #viewExists}) BEFORE the handle is resolved,
     * mirroring legacy {@code IcebergMetadataOps.dropTableImpl}'s viewExists -> performDropView dispatch.
     */
    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Optional<String> tableLocation;
        try {
            tableLocation = context.executeAuthenticated(() -> {
                Optional<String> location = isHmsCatalog()
                        ? catalogOps.loadTableLocation(iceHandle.getDbName(), iceHandle.getTableName())
                        : Optional.empty();
                catalogOps.dropTable(iceHandle.getDbName(), iceHandle.getTableName(), true);
                return location;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
        tableLocation.ifPresent(location ->
                context.cleanupEmptyManagedLocation(location, IcebergSchemaBuilder.tableLocationChildDirs()));
    }

    /**
     * Renames a table, mirroring legacy {@code IcebergMetadataOps.renameTableImpl}: a thin seam delegation
     * ({@code catalog.renameTable}) inside the auth context. {@code newName} is the rename target's name in
     * the same (remote) database — kept as-is as the new remote name, mirroring how {@code createTable} names
     * a new table (iceberg has no separate remote-name mapping).
     */
    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle handle, String newName) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.renameTable(iceHandle.getDbName(), iceHandle.getTableName(), newName);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to rename Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + " to " + newName
                    + ": " + e.getMessage(), e);
        }
    }

    // ========== Column evolution (B2) — mirror legacy IcebergMetadataOps add/drop/rename/modify/reorder ==========

    /**
     * Adds a column, mirroring legacy {@code IcebergMetadataOps.addColumn}/{@code addOneColumn}: the neutral
     * column is turned into an iceberg type + parsed DEFAULT literal PURELY (outside auth), then committed
     * through the seam at {@code position} ({@code null} = append at the end). A non-nullable column cannot be
     * added to an existing iceberg table (legacy parity).
     */
    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        IcebergColumnChange change = toAddColumnChange(column);
        try {
            context.executeAuthenticated(() -> {
                catalogOps.addColumn(iceHandle.getDbName(), iceHandle.getTableName(), change, position);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to add column " + column.getName() + " to Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /** Adds columns in one schema update, mirroring legacy {@code IcebergMetadataOps.addColumns}. */
    @Override
    public void addColumns(ConnectorSession session, ConnectorTableHandle handle, List<ConnectorColumn> columns) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        List<IcebergColumnChange> changes = new ArrayList<>(columns.size());
        for (ConnectorColumn column : columns) {
            changes.add(toAddColumnChange(column));
        }
        try {
            context.executeAuthenticated(() -> {
                catalogOps.addColumns(iceHandle.getDbName(), iceHandle.getTableName(), changes);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to add columns to Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /** Drops a column, mirroring legacy {@code IcebergMetadataOps.dropColumn}. */
    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle handle, String columnName) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropColumn(iceHandle.getDbName(), iceHandle.getTableName(), columnName);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop column " + columnName + " from Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /** Renames a column, mirroring legacy {@code IcebergMetadataOps.renameColumn}. */
    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle handle, String oldName,
            String newName) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.renameColumn(iceHandle.getDbName(), iceHandle.getTableName(), oldName, newName);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to rename column " + oldName + " to " + newName
                    + " in Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Modifies a column, mirroring legacy {@code IcebergMetadataOps.modifyColumn}: the neutral column is turned
     * into the full iceberg type PURELY (scalar leaf or the whole {@code STRUCT}/{@code ARRAY}/{@code MAP} tree,
     * carrying nested nullability + per-field comments), then the seam validates the current column
     * (exists / not optional&rarr;required) and either commits a scalar {@code updateColumn} or diffs the new
     * complex type against the current one field-by-field ({@link IcebergComplexTypeDiff}), plus make-optional +
     * reposition.
     *
     * <p>A complex-type modify may only carry a {@code NULL} default (legacy
     * {@code validateForModifyComplexColumn} parity), checked here before the remote call.</p>
     */
    @Override
    public void modifyColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumn column, ConnectorColumnPosition position) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        validateCommonColumnInfo(column);
        if (isComplexType(column.getType()) && column.getDefaultValue() != null) {
            throw new DorisConnectorException("Complex type default value only supports NULL: " + column.getName());
        }
        Type icebergType;
        try {
            icebergType = IcebergSchemaBuilder.buildColumnType(column.getType());
        } catch (DorisConnectorException buildError) {
            // A nested narrowing to an iceberg-unrepresentable type (e.g. ARRAY<INT> -> ARRAY<SMALLINT>) throws a
            // generic "Unsupported type for Iceberg: SMALLINT" here. Restore the legacy parity message ("Cannot
            // change int to smallint in nested types") by validating the requested nested type against the
            // CURRENT type — legacy validated in Doris type space, where the narrow target still exists.
            throw upgradeNestedModifyError(iceHandle, column, buildError);
        }
        IcebergColumnChange change = new IcebergColumnChange(column.getName(), icebergType,
                column.getComment(), null, column.isNullable());
        try {
            context.executeAuthenticated(() -> {
                catalogOps.modifyColumn(iceHandle.getDbName(), iceHandle.getTableName(), change, position);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to modify column " + column.getName()
                    + " in Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Upgrades the generic "Unsupported type for Iceberg" error from a failed complex-type build into the legacy
     * "Cannot change &lt;old&gt; to &lt;new&gt; in nested types" message, by walking the requested nested type
     * against the CURRENT column type. Best-effort: a scalar modify, a load failure, or no offending nested leaf
     * keeps the original build error — so no other modify path changes.
     */
    private DorisConnectorException upgradeNestedModifyError(IcebergTableHandle handle, ConnectorColumn column,
            DorisConnectorException buildError) {
        if (!isComplexType(column.getType())) {
            return buildError;
        }
        try {
            Types.NestedField current = context.executeAuthenticated(() ->
                    catalogOps.loadTable(handle.getDbName(), handle.getTableName())
                            .schema().findField(column.getName()));
            if (current != null && !current.type().isPrimitiveType()) {
                IcebergComplexTypeDiff.validateNestedModifyRepresentable(current.type(), column.getType());
            }
        } catch (DorisConnectorException parityError) {
            return parityError;
        } catch (Exception ignored) {
            // load failed / column missing -> keep the original build error
        }
        return buildError;
    }

    /** Reorders columns, mirroring legacy {@code IcebergMetadataOps.reorderColumns}. */
    @Override
    public void reorderColumns(ConnectorSession session, ConnectorTableHandle handle, List<String> newOrder) {
        if (newOrder == null || newOrder.isEmpty()) {
            throw new DorisConnectorException("Reorder columns failed: the new order is empty");
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.reorderColumns(iceHandle.getDbName(), iceHandle.getTableName(), newOrder);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to reorder columns in Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    // ===== Nested (dotted-path) column evolution (#65329) — mirror legacy IcebergMetadataOps ColumnPath ops =====
    // The fe-core bridge routes ONLY nested paths to add/drop/rename/modify (top-level still flows through the
    // flat ops above); modifyColumnComment is the sole entrypoint for MODIFY COLUMN ... COMMENT and receives both
    // flat and nested paths. The neutral column is turned into an iceberg type PURELY (outside auth), then the
    // whole resolve + UpdateSchema commit runs through the seam inside ONE auth context (no partial commit).
    // NOTE (parity gap, deliberate): like the connector's existing flat column ops, these do NOT enforce the
    // legacy row-lineage-column mutation guard (validateRowLineageColumnMutation) — the connector guards v3
    // reserved columns only at CREATE (rejectReservedRowLineageColumns) and on the schema read path.

    /**
     * Adds a nested field at {@code path}, mirroring legacy {@code IcebergMetadataOps.addColumn(ColumnPath,...)}:
     * a new nested field must be nullable; the parent must resolve to a struct and the leaf must not collide with
     * an existing sibling (checked in the seam against the loaded schema). A single-part path degrades to the flat
     * {@link #addColumn(ConnectorSession, ConnectorTableHandle, ConnectorColumn, ConnectorColumnPosition)}.
     */
    @Override
    public void addNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        if (!path.isNested()) {
            addColumn(session, handle, column, position);
            return;
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        validateCommonColumnInfo(column);
        if (column.getDefaultValue() != null) {
            throw new DorisConnectorException(
                    "DEFAULT and ON UPDATE are not supported for Iceberg nested ADD COLUMN: " + path.getFullPath());
        }
        if (!column.isNullable()) {
            throw new DorisConnectorException("New nested field '" + path.getFullPath() + "' must be nullable");
        }
        Type icebergType = IcebergSchemaBuilder.buildColumnType(column.getType());
        IcebergColumnChange change = new IcebergColumnChange(path.getLeafName(), icebergType,
                column.getComment(), null, column.isNullable());
        try {
            context.executeAuthenticated(() -> {
                catalogOps.addNestedColumn(iceHandle.getDbName(), iceHandle.getTableName(), path, change, position);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to add nested column " + path.getFullPath()
                    + " to Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Drops the nested field at {@code path}, mirroring legacy {@code IcebergMetadataOps.dropColumn(ColumnPath,...)}.
     */
    @Override
    public void dropNestedColumn(ConnectorSession session, ConnectorTableHandle handle, ConnectorColumnPath path) {
        if (!path.isNested()) {
            dropColumn(session, handle, path.getTopLevelName());
            return;
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropNestedColumn(iceHandle.getDbName(), iceHandle.getTableName(), path);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop nested column " + path.getFullPath()
                    + " from Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Renames the nested field at {@code path} to {@code newName}, mirroring legacy
     * {@code IcebergMetadataOps.renameColumn(ColumnPath,...)} (with the iceberg identifier-field path fixup).
     */
    @Override
    public void renameNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String newName) {
        if (!path.isNested()) {
            renameColumn(session, handle, path.getTopLevelName(), newName);
            return;
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.renameNestedColumn(iceHandle.getDbName(), iceHandle.getTableName(), path, newName);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to rename nested column " + path.getFullPath()
                    + " to " + newName + " in Iceberg table " + iceHandle.getDbName() + "."
                    + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Modifies the nested field at {@code path}, mirroring legacy {@code IcebergMetadataOps.modifyColumn(
     * ColumnPath,...)}: a primitive change is an iceberg promotion, a complex change is diffed field-by-field
     * ({@link IcebergComplexTypeDiff}); a complex modify may only carry a NULL default. The
     * {@code nullableSpecified}/{@code commentSpecified} #65329 flags (threaded from the fe-catalog Column via
     * {@code ConnectorColumnConverter.toConnectorColumn}) drive the omit-preserves-metadata behavior in the seam.
     */
    @Override
    public void modifyNestedColumn(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, ConnectorColumn column, ConnectorColumnPosition position) {
        if (!path.isNested()) {
            modifyColumn(session, handle, column, position);
            return;
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        validateCommonColumnInfo(column);
        if (column.getDefaultValue() != null) {
            if (isComplexType(column.getType())) {
                throw new DorisConnectorException(
                        "Complex type default value only supports NULL: " + path.getFullPath());
            }
            throw new DorisConnectorException(
                    "Modifying default values is not supported for Iceberg columns: " + path.getFullPath());
        }
        Type icebergType;
        try {
            icebergType = IcebergSchemaBuilder.buildColumnType(column.getType());
        } catch (DorisConnectorException buildError) {
            throw upgradeNestedModifyError(iceHandle, column, buildError);
        }
        IcebergColumnChange change = new IcebergColumnChange(path.getLeafName(), icebergType,
                column.getComment(), null, column.isNullable());
        boolean nullableSpecified = column.isNullableSpecified();
        boolean commentSpecified = column.isCommentSpecified();
        try {
            context.executeAuthenticated(() -> {
                catalogOps.modifyNestedColumn(iceHandle.getDbName(), iceHandle.getTableName(), path, change,
                        nullableSpecified, commentSpecified, position);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to modify nested column " + path.getFullPath()
                    + " in Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /**
     * Sets (or clears) the comment/doc of the field at {@code path}, mirroring legacy
     * {@code IcebergMetadataOps.modifyColumnComment}. This is the sole entrypoint for {@code MODIFY COLUMN ...
     * COMMENT} (no flat SPI equivalent) and handles BOTH a single-part (flat column) and a nested path; a comment
     * on a list-element / map-value pseudo-field is rejected in the seam.
     */
    @Override
    public void modifyColumnComment(ConnectorSession session, ConnectorTableHandle handle,
            ConnectorColumnPath path, String comment) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.modifyColumnComment(iceHandle.getDbName(), iceHandle.getTableName(), path, comment);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to modify comment for column " + path.getFullPath()
                    + " in Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    // ========== Branch / tag refs (B4) — mirror legacy IcebergMetadataOps createOrReplace/drop Branch/Tag ==========

    /**
     * Creates or replaces a branch, mirroring legacy {@code IcebergMetadataOps.createOrReplaceBranchImpl}: the
     * whole {@code ManageSnapshots} build + commit (which reads the live table's current snapshot / refs) runs
     * through the seam inside the auth context. The neutral {@link BranchChange} carries the SQL options; the
     * iceberg {@code ManageSnapshots} logic stays in the seam.
     */
    @Override
    public void createOrReplaceBranch(ConnectorSession session, ConnectorTableHandle handle, BranchChange branch) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createOrReplaceBranch(iceHandle.getDbName(), iceHandle.getTableName(), branch);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to create or replace branch " + branch.getName()
                    + " on Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /** Creates or replaces a tag, mirroring legacy {@code IcebergMetadataOps.createOrReplaceTagImpl}. */
    @Override
    public void createOrReplaceTag(ConnectorSession session, ConnectorTableHandle handle, TagChange tag) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.createOrReplaceTag(iceHandle.getDbName(), iceHandle.getTableName(), tag);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to create or replace tag " + tag.getName()
                    + " on Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /** Drops a branch, mirroring legacy {@code IcebergMetadataOps.dropBranchImpl}. */
    @Override
    public void dropBranch(ConnectorSession session, ConnectorTableHandle handle, DropRefChange branch) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropBranch(iceHandle.getDbName(), iceHandle.getTableName(), branch);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop branch " + branch.getName()
                    + " from Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    /** Drops a tag, mirroring legacy {@code IcebergMetadataOps.dropTagImpl}. */
    @Override
    public void dropTag(ConnectorSession session, ConnectorTableHandle handle, DropRefChange tag) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropTag(iceHandle.getDbName(), iceHandle.getTableName(), tag);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop tag " + tag.getName()
                    + " from Iceberg table " + iceHandle.getDbName() + "." + iceHandle.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    // ===== Partition evolution (B5) — mirror legacy IcebergMetadataOps add/drop/replace PartitionField =====

    /**
     * Adds a partition field, mirroring legacy {@code IcebergMetadataOps.addPartitionField}: the whole
     * {@code UpdatePartitionSpec} build + commit (which reads the live table) runs through the seam inside the
     * auth context. The neutral {@link PartitionFieldChange} carries the SQL transform; the iceberg
     * {@code Term}/{@code UpdatePartitionSpec} logic stays in the seam.
     */
    @Override
    public void addPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.addPartitionField(iceHandle.getDbName(), iceHandle.getTableName(), change);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to add partition field to Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /** Drops a partition field, mirroring legacy {@code IcebergMetadataOps.dropPartitionField}. */
    @Override
    public void dropPartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.dropPartitionField(iceHandle.getDbName(), iceHandle.getTableName(), change);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to drop partition field from Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /** Replaces a partition field, mirroring legacy {@code IcebergMetadataOps.replacePartitionField}. */
    @Override
    public void replacePartitionField(ConnectorSession session, ConnectorTableHandle handle,
            PartitionFieldChange change) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            context.executeAuthenticated(() -> {
                catalogOps.replacePartitionField(iceHandle.getDbName(), iceHandle.getTableName(), change);
                return null;
            });
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to replace partition field in Iceberg table "
                    + iceHandle.getDbName() + "." + iceHandle.getTableName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Builds the iceberg {@code ADD COLUMN} artifacts from a neutral column, mirroring legacy
     * {@code addOneColumn}: reject aggregated / auto-inc columns and a non-nullable add, build the iceberg
     * type, parse the DEFAULT literal. Pure (no remote call); runs outside the auth context.
     */
    private IcebergColumnChange toAddColumnChange(ConnectorColumn column) {
        validateCommonColumnInfo(column);
        if (!column.isNullable()) {
            throw new DorisConnectorException("can't add a non-nullable column to an Iceberg table: "
                    + column.getName());
        }
        Type icebergType = IcebergSchemaBuilder.buildColumnType(column.getType());
        return new IcebergColumnChange(column.getName(), icebergType, column.getComment(),
                IcebergSchemaBuilder.parseDefaultLiteral(column.getDefaultValue(), icebergType),
                column.isNullable());
    }

    /** Rejects aggregated / auto-increment columns on iceberg, mirroring legacy {@code validateCommonColumnInfo}. */
    private static void validateCommonColumnInfo(ConnectorColumn column) {
        if (column.isAggregated()) {
            throw new DorisConnectorException("Can not specify aggregation method for iceberg table column: "
                    + column.getName());
        }
        if (column.isAutoInc()) {
            throw new DorisConnectorException("Can not specify auto incremental iceberg table column: "
                    + column.getName());
        }
    }

    /** Whether a neutral type is a complex (STRUCT / ARRAY / MAP) type, by its type name. */
    private static boolean isComplexType(ConnectorType type) {
        String name = type.getTypeName().toUpperCase(Locale.ROOT);
        return "ARRAY".equals(name) || "MAP".equals(name) || "STRUCT".equals(name);
    }

    /** The configured {@code iceberg.catalog.type}, or {@code null} when unset. */
    private String catalogType() {
        return properties.get(IcebergConnectorProperties.ICEBERG_CATALOG_TYPE);
    }

    /** Whether this is an HMS-backed iceberg catalog (case-insensitive, matching the read-path fork). */
    private boolean isHmsCatalog() {
        return IcebergConnectorProperties.TYPE_HMS.equalsIgnoreCase(catalogType());
    }

    // ========== E7: System Tables (P6.5) ==========

    /**
     * Lists the system-table names iceberg exposes. Connector-global: {@code IcebergSysTable.SUPPORTED_SYS_TABLES}
     * is built once from {@code MetadataTableType.values()} and applies to every iceberg table, so this returns
     * the same lower-cased names for any base handle — a defensive unmodifiable copy.
     *
     * <p>{@code POSITION_DELETES} IS exposed. The former Q2 exclusion mirrored legacy, which rejected it with
     * "SysTable position_deletes is not supported yet"; upstream #65135 then implemented it natively, so
     * excluding it here would be a capability regression vs master. Unlike every other entry, its scan takes
     * BE's native reader rather than the JNI serialized-split path (see
     * {@code IcebergScanPlanProvider.doPlanPositionDeletesSystemTableScan}).
     */
    @Override
    public List<String> listSupportedSysTables(ConnectorSession session,
            ConnectorTableHandle baseTableHandle) {
        List<String> names = new ArrayList<>();
        for (MetadataTableType type : MetadataTableType.values()) {
            names.add(type.name().toLowerCase(Locale.ROOT));
        }
        return Collections.unmodifiableList(names);
    }

    /**
     * Resolves a handle for the named system table of {@code baseTableHandle}, or empty when iceberg does
     * not expose {@code sysName} (case-insensitive; a {@code null} name and an unknown name).
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
        // Null-safe: a null / unknown sysName is "this connector does not expose that sys table"
        // (Optional.empty per the contract), NOT an NPE/exception.
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
     * Whether iceberg exposes a system table named {@code sysName} (case-insensitive). Mirrors
     * {@code IcebergSysTable.SUPPORTED_SYS_TABLES}: every {@code MetadataTableType}, {@code POSITION_DELETES}
     * included (see {@link #listSupportedSysTables}). A {@code null} name is simply not exposed (returns
     * false, not NPE).
     */
    private static boolean isSupportedSysTable(String sysName) {
        if (sysName == null) {
            return false;
        }
        for (MetadataTableType type : MetadataTableType.values()) {
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
     * Rejects row-level DML on iceberg copy-on-write tables (Doris only supports merge-on-read deletes /
     * deletion vectors). Reads the per-operation write-mode property ({@code write.delete.mode} /
     * {@code write.update.mode} / {@code write.merge.mode}, defaulting to {@code merge-on-read}) and throws if
     * it resolves to {@code copy-on-write}. Mirrors the legacy fe-resident
     * {@code IcebergDmlCommandUtils.checkNotCopyOnWrite}, but the message — and the iceberg property knowledge —
     * now lives in the connector. {@code op} values other than DELETE/UPDATE/MERGE are not row-level DML and
     * return without loading the table.
     */
    @Override
    public void validateRowLevelDmlMode(ConnectorSession session, ConnectorTableHandle handle, WriteOperation op) {
        String modeProperty;
        String defaultMode;
        String operationLabel;
        switch (op) {
            case DELETE:
                modeProperty = TableProperties.DELETE_MODE;
                defaultMode = TableProperties.DELETE_MODE_DEFAULT;
                operationLabel = "DELETE";
                break;
            case UPDATE:
                modeProperty = TableProperties.UPDATE_MODE;
                defaultMode = TableProperties.UPDATE_MODE_DEFAULT;
                operationLabel = "UPDATE";
                break;
            case MERGE:
                modeProperty = TableProperties.MERGE_MODE;
                defaultMode = TableProperties.MERGE_MODE_DEFAULT;
                operationLabel = "MERGE INTO";
                break;
            default:
                return;
        }
        Table table = loadTable(session, (IcebergTableHandle) handle);
        String mode = table.properties().getOrDefault(modeProperty, defaultMode);
        if (RowLevelOperationMode.COPY_ON_WRITE.modeName().equalsIgnoreCase(mode)) {
            throw new DorisConnectorException(String.format(
                    "Doris does not support %s on Iceberg copy-on-write tables. "
                            + "Set table property '%s' to 'merge-on-read'.",
                    operationLabel, modeProperty));
        }
    }

    /**
     * Rejects an illegal static-partition column on {@code INSERT [OVERWRITE] ... PARTITION (col=val)}: the
     * column must be an <em>identity</em> partition field of this (partitioned) table. Mirrors the legacy
     * fe-resident {@code BindSink.validateStaticPartition} (now dead on the connector-driven path), but the
     * iceberg {@link PartitionSpec} knowledge and the messages live in the connector. The lookup is keyed by
     * partition <em>field</em> name (e.g. {@code category_bucket} for {@code bucket(4, category)}), matching
     * legacy — so a source-column name that is not itself an identity field reads as "Unknown partition column".
     */
    @Override
    public void validateStaticPartitionColumns(ConnectorSession session, ConnectorTableHandle handle,
            List<String> staticPartitionColumnNames) {
        if (staticPartitionColumnNames == null || staticPartitionColumnNames.isEmpty()) {
            return;
        }
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = loadTable(session, iceHandle);
        PartitionSpec spec = table.spec();
        String tableName = iceHandle.getTableName();
        if (!spec.isPartitioned()) {
            throw new DorisConnectorException(String.format(
                    "Table %s is not partitioned, cannot use static partition syntax", tableName));
        }
        Map<String, PartitionField> partitionFieldMap = new HashMap<>();
        for (PartitionField field : spec.fields()) {
            partitionFieldMap.put(field.name(), field);
        }
        for (String colName : staticPartitionColumnNames) {
            PartitionField field = partitionFieldMap.get(colName);
            if (field == null) {
                throw new DorisConnectorException(String.format(
                        "Unknown partition column '%s' in table '%s'. Available partition columns: %s",
                        colName, tableName, partitionFieldMap.keySet()));
            }
            if (!field.transform().isIdentity()) {
                throw new DorisConnectorException(String.format(
                        "Cannot use static partition syntax for non-identity partition field '%s'"
                                + " (transform: %s).", colName, field.transform().toString()));
            }
        }
    }

    // ========== B-2: partition enumeration (MTMV RANGE view + SHOW PARTITIONS) ==========

    /**
     * The connector-supplied RANGE partition view for an iceberg table acting as an MTMV related (base) table.
     * Overrides the SPI default (empty) so the generic {@code PluginDrivenMvccExternalTable} never degrades to
     * the LIST/timestamp path: iceberg time-partitioned tables are intrinsically RANGE with snapshot-id
     * freshness. All connector-specific math (eligibility gate, transform-to-range, partition-evolution overlap
     * merge, snapshot-id resolution) happens in {@link IcebergPartitionUtils#buildMvccPartitionView}; the
     * remote PARTITIONS scan runs inside the FE-injected auth context.
     *
     * <p>The partition set + freshness are enumerated at the handle's pinned snapshot when present
     * ({@code iceHandle.getSnapshotId() >= 0}), else the table's latest snapshot. The generic model (3/3) must
     * thread the query's pin onto the handle (via {@code applySnapshot} with {@code beginQuerySnapshot}'s
     * snapshot) before calling this, so the MTMV partition/freshness view stays consistent with the data-scan
     * pin — mirroring master, which routes enumeration, freshness and the scan through ONE snapshot cache value.</p>
     *
     * <p>Fail-loud parity (NOT a degrade): a {@code NoSuchTableException} is allowed to propagate. The common
     * dropped-table case is already absorbed by the generic model at handle resolution (no handle -&gt; empty
     * pin); a not-found HERE is the narrow post-resolution concurrent-drop race, where master fails the refresh
     * rather than masking a vanished base table as "unpartitioned/fresh".</p>
     */
    @Override
    public Optional<ConnectorMvccPartitionView> getMvccPartitionView(
            ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            // PERF-06 cache A: memoize the BUILT derived view keyed by (db, table, snapshotId, schemaId) -- pure
            // function of the pinned MVCC coordinate (a new snapshot/schema yields a new key, never a stale hit).
            // The lookup sits INSIDE executeAuthenticated so a miss runs the loader (resolveTableForRead + the
            // remote PARTITIONS build) under the FE-injected auth scope; a hit returns without any remote call. A
            // null cache (session=user / no-cache catalog) computes directly every call. -1 (empty table / unpinned)
            // enumerates the current snapshot and caches a trivially-empty view (harmless; REFRESH re-pins).
            return context.executeAuthenticated(() -> {
                if (mvccPartitionViewCache == null) {
                    return Optional.of(buildMvccPartitionViewUncached(session, iceHandle));
                }
                PartitionViewCacheKey key = new PartitionViewCacheKey(iceHandle.getDbName(),
                        iceHandle.getTableName(), iceHandle.getSnapshotId(), iceHandle.getSchemaId());
                return Optional.of(mvccPartitionViewCache.get(key,
                        () -> buildMvccPartitionViewUncached(session, iceHandle)));
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to build iceberg MVCC partition view, error message is:"
                    + e.getMessage(), e);
        }
    }

    /**
     * Builds the derived MVCC partition view live (no cache-A layer): the same resolveTableForRead + remote
     * PARTITIONS build the pre-cache code ran. Extracted so it can serve as cache A's per-miss loader. MUST run
     * inside {@code context.executeAuthenticated} (the caller wraps it). A {@code NoSuchTableException} propagates
     * (fail-loud parity) and, running through the framework loader, is never cached.
     */
    private ConnectorMvccPartitionView buildMvccPartitionViewUncached(
            ConnectorSession session, IcebergTableHandle iceHandle) {
        Table table = resolveTableForRead(session, iceHandle);
        return IcebergPartitionUtils.buildMvccPartitionView(table, iceHandle.getSnapshotId(),
                TableIdentifier.of(iceHandle.getDbName(), iceHandle.getTableName()), partitionCache);
    }

    /**
     * The physical iceberg partition display names ({@code "f1=v1/f2=v2"}) for SHOW PARTITIONS (single-column
     * form — iceberg does not declare {@code SUPPORTS_PARTITION_STATS}). Post-cutover this restores real rows
     * for partitioned tables (master rejected iceberg SHOW PARTITIONS outright, so the SPI default empty list
     * would otherwise return silently zero rows). The remote PARTITIONS scan runs inside the auth context; a
     * concurrent drop yields an empty list.
     */
    @Override
    public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            return context.executeAuthenticated(() -> {
                Table table;
                try {
                    table = resolveTableForRead(session, iceHandle);
                } catch (NoSuchTableException e) {
                    LOG.warn("Iceberg table not found while listing partitions: {}.{}",
                            iceHandle.getDbName(), iceHandle.getTableName(), e);
                    return Collections.<String>emptyList();
                }
                return IcebergPartitionUtils.listPartitionNames(table,
                        TableIdentifier.of(iceHandle.getDbName(), iceHandle.getTableName()), partitionCache);
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list iceberg partition names, error message is:"
                    + e.getMessage(), e);
        }
    }

    /**
     * The physical iceberg partitions of {@code handle} with per-partition value maps, so the generic
     * {@code PluginDrivenExternalTable.getNameToPartitionItems} can populate {@code selectedPartitionNum}
     * (EXPLAIN {@code partition=N/M} + SQL-block-rule {@code partition_num} enforcement). Mirrors
     * {@link #listPartitionNames}: the remote PARTITIONS scan runs inside the auth context; a concurrent drop
     * yields an empty list. The {@code filter} is ignored (iceberg planScan is predicate-driven; the reported
     * partition count is display/enforcement metadata only, never the read set). Unpartitioned tables map to
     * the SPI default empty list via {@link IcebergPartitionUtils#listPartitions}.
     */
    @Override
    public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
            ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        try {
            // PERF-06 cache A: memoize the BUILT partition-info list keyed by (db, table, snapshotId, schemaId).
            // The lookup sits INSIDE executeAuthenticated (a miss runs the remote build under the auth scope; a hit
            // returns without a remote call). BYPASS the cache when the filter is present -- that is not the
            // pruning path (which always passes Optional.empty()) and is not keyed by (snapshot, schema) alone -- or
            // when the cache is null (session=user / no-cache catalog): compute directly every call.
            return context.executeAuthenticated(() -> {
                if (listPartitionsViewCache == null || filter.isPresent()) {
                    return listPartitionsUncached(session, iceHandle);
                }
                PartitionViewCacheKey key = new PartitionViewCacheKey(iceHandle.getDbName(),
                        iceHandle.getTableName(), iceHandle.getSnapshotId(), iceHandle.getSchemaId());
                return listPartitionsViewCache.get(key, () -> listPartitionsUncached(session, iceHandle));
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list iceberg partitions, error message is:"
                    + e.getMessage(), e);
        }
    }

    /**
     * Builds the derived partition-info list live (no cache-A layer): the same resolveTableForRead + remote
     * PARTITIONS build the pre-cache code ran, including the concurrent-drop degrade (a {@code NoSuchTableException}
     * yields an empty list). Extracted so it can serve as cache A's per-miss loader. MUST run inside
     * {@code context.executeAuthenticated} (the caller wraps it).
     */
    private List<ConnectorPartitionInfo> listPartitionsUncached(
            ConnectorSession session, IcebergTableHandle iceHandle) {
        Table table;
        try {
            table = resolveTableForRead(session, iceHandle);
        } catch (NoSuchTableException e) {
            LOG.warn("Iceberg table not found while listing partitions: {}.{}",
                    iceHandle.getDbName(), iceHandle.getTableName(), e);
            return Collections.<ConnectorPartitionInfo>emptyList();
        }
        return IcebergPartitionUtils.listPartitions(table,
                TableIdentifier.of(iceHandle.getDbName(), iceHandle.getTableName()), partitionCache);
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
        // A null latestSnapshotCache (iceberg.rest.session=user, where a shared table-keyed hit would bypass the
        // per-user loadTable authorization) reads live per-user every call, so authorization runs every time (no
        // stale-authz window); mirrors resolveTableForRead's tableCache null-fallback. A disabled cache (ttl<=0)
        // is a non-null cache that reads live internally.
        IcebergLatestSnapshotCache.CachedSnapshot pin = latestSnapshotCache != null
                ? latestSnapshotCache.getOrLoad(id, () -> loadLatestSnapshotPin(session, iceHandle))
                : loadLatestSnapshotPin(session, iceHandle);
        return Optional.of(
                ConnectorMvccSnapshot.builder().snapshotId(pin.snapshotId).schemaId(pin.schemaId).build());
    }

    /**
     * Loads the table live and pins its latest snapshot id (or {@code -1} for an empty table) plus its LATEST
     * schema id — the {@link #beginQuerySnapshot} loader, extracted so the per-user (null-cache) path can reuse
     * the exact same pin logic without a cache round-trip.
     */
    private IcebergLatestSnapshotCache.CachedSnapshot loadLatestSnapshotPin(
            ConnectorSession session, IcebergTableHandle iceHandle) {
        Table table = loadTable(session, iceHandle);
        Snapshot current = table.currentSnapshot();
        return new IcebergLatestSnapshotCache.CachedSnapshot(
                current == null ? -1L : current.snapshotId(), table.schema().schemaId());
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
     *   <li>{@code VERSION_REF} — non-numeric {@code FOR VERSION AS OF '<name>'}: resolves ANY ref (branch OR
     *       tag), mirroring legacy {@code IcebergUtils.getQuerySpecSnapshot}'s {@code table.refs().containsKey}.
     *       Unlike {@code TAG} ({@code @tag}, tag-only) it does not require the ref to be a tag.</li>
     *   <li>{@code INCREMENTAL} — unsupported for iceberg (legacy {@code getQuerySpecSnapshot} never dispatches
     *       {@code @incr}); fail loud rather than silently read latest.</li>
     * </ul>
     */
    @Override
    public Optional<ConnectorMvccSnapshot> resolveTimeTravel(
            ConnectorSession session, ConnectorTableHandle handle, ConnectorTimeTravelSpec spec) {
        Table table = loadTable(session, (IcebergTableHandle) handle);
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
                return resolveRef(table, spec.getStringValue(), SnapshotRef::isTag);
            case BRANCH:
                return resolveRef(table, spec.getStringValue(), SnapshotRef::isBranch);
            case VERSION_REF:
                // Non-numeric FOR VERSION AS OF: accept ANY ref (branch or tag), matching legacy
                // getQuerySpecSnapshot's table.refs().containsKey(value). Unlike @tag/@branch it does
                // not constrain the ref kind.
                return resolveRef(table, spec.getStringValue(), ref -> true);
            case INCREMENTAL:
            default:
                throw new DorisConnectorException(
                        "incremental read (@incr) is not supported for Iceberg tables");
        }
    }

    /**
     * Resolves a named ref to a pinned snapshot, accepting it only when {@code accept} passes
     * ({@code SnapshotRef::isTag} for {@code @tag}, {@code SnapshotRef::isBranch} for {@code @branch},
     * {@code ref -> true} for non-numeric {@code FOR VERSION AS OF}, which takes any ref). Empty when the
     * ref is absent or rejected (legacy threw "Table X does not have branch/tag named Y"; the SPI returns
     * empty so fe-core renders the user-facing message). Pins the ref NAME, not its current snapshot id —
     * {@code applySnapshot} routes it to {@code scan.useRef(name)} (legacy parity).
     */
    private Optional<ConnectorMvccSnapshot> resolveRef(Table table, String refName, Predicate<SnapshotRef> accept) {
        SnapshotRef ref = table.refs().get(refName);
        if (ref == null || !accept.test(ref)) {
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

    /**
     * Scopes a per-group rewrite scan to {@code rawDataFilePaths} by threading them onto an immutable handle
     * copy ({@link IcebergTableHandle#withRewriteFileScope}); the scan provider ({@code
     * IcebergScanPlanProvider.planScanInternal}) keeps only the re-enumerated tasks whose RAW {@code
     * dataFile.path().toString()} is in the scope, matching the SAME raw paths {@code planRewrite} emitted. A
     * {@code null}/empty set is a no-op (read the full table) — never scope to an empty set (that would scan
     * nothing); {@link IcebergTableHandle#withRewriteFileScope} also rejects null elements via {@code
     * ImmutableSet.copyOf}, so the empty/null guard here keeps it from being reached with a bad set.
     */
    @Override
    public ConnectorTableHandle applyRewriteFileScope(ConnectorSession session,
            ConnectorTableHandle handle, Set<String> rawDataFilePaths) {
        if (rawDataFilePaths == null || rawDataFilePaths.isEmpty()) {
            return handle;
        }
        return ((IcebergTableHandle) handle).withRewriteFileScope(rawDataFilePaths);
    }

    /**
     * Marks the handle as a Top-N lazy-materialization scan so {@code IcebergScanPlanProvider} builds the
     * field-id schema dictionary over the FULL schema (BE re-fetches non-projected columns by row-id). The
     * generic {@code PluginDrivenScanNode} calls this when the scan carries the synthesized
     * {@code __DORIS_GLOBAL_ROWID_COL__} column — legacy {@code IcebergScanNode.createScanRangeLocations} →
     * {@code initSchemaInfoForAllColumn} parity. Threads the flag onto an immutable handle copy, mirroring
     * {@link #applySnapshot} / {@link #applyRewriteFileScope}.
     */
    @Override
    public ConnectorTableHandle applyTopnLazyMaterialization(ConnectorSession session,
            ConnectorTableHandle handle) {
        return ((IcebergTableHandle) handle).withTopnLazyMaterialize(true);
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
            // case-preserved (#65094 read-path alignment; top-level slot names keep their remote case),
            // isKey is always true (external-table semantics: DESC shows Key=true),
            // and isAllowNull is always true regardless of the Iceberg required/optional flag (rows can
            // still read NULL under schema-evolution default-fill; do NOT propagate the NOT NULL constraint).
            ConnectorColumn column = new ConnectorColumn(
                    field.name(),
                    IcebergTypeMapping.fromIcebergType(
                            field.type(), enableVarbinary, enableTimestampTz),
                    field.doc() != null ? field.doc() : "",
                    true,
                    null,
                    true);
            // Carry the stable iceberg field-id as the column's uniqueId (legacy
            // IcebergUtils.updateIcebergColumnUniqueId set the top-level Column.uniqueId = field.fieldId()).
            // fe-core's ConnectorColumnConverter re-applies it (>= 0); the BE field-id scan path keys the
            // read projection / nested matching off it, so without it a renamed-or-evolved column would
            // mis-match. Nested children carry their ids via the ConnectorType (IcebergTypeMapping).
            column = column.withUniqueId(field.fieldId());
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
