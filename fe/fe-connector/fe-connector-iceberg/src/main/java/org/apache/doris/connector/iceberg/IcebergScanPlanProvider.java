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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorColumnCategory;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanProfile;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorSplitSource;
import org.apache.doris.connector.cache.CacheSpec;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.properties.StorageProperties;
import org.apache.doris.kerberos.HadoopAuthenticator;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndex;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SplittableScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ScanTaskUtil;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * {@link ConnectorScanPlanProvider} for Iceberg tables, mirroring the paimon connector's
 * {@code PaimonScanPlanProvider}. The generic, engine-neutral {@code PluginDrivenScanNode} drives split
 * generation through this provider once iceberg is in {@code SPI_READY_TYPES} (P6.6 cutover).
 *
 * <p>P6.2-T01 (this task) is the skeleton: it wires the collaborators ({@code properties} /
 * {@link IcebergCatalogOps} seam / {@link ConnectorContext}) and pins the predicate-driven semantics
 * ({@link #ignorePartitionPruneShortCircuit()} = {@code true}). The real split planning — self-contained
 * predicate pushdown, {@code FileScanTask} enumeration, native-vs-JNI classification, merge-on-read
 * delete files (T04), COUNT(*) pushdown (T05; batch mode deferred, mirrors paimon), the field-id
 * history-schema dictionary (T06), and vended credentials (T09) — lands across P6.2-T02..T09. Iceberg is NOT
 * yet in {@code SPI_READY_TYPES}, so {@link #planScan} is not exercised at
 * runtime this phase (iceberg queries still route to the legacy {@code IcebergScanNode}); the parity is
 * verified by offline unit tests until the P6.6 cutover.</p>
 */
public class IcebergScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(IcebergScanPlanProvider.class);

    // Split-size session variables, read via ConnectorSession.getSessionProperties() (the VariableMgr.toMap
    // channel) since the connector cannot import fe-core SessionVariable. Keys + defaults are byte-identical to
    // SessionVariable and to the paimon connector's constants.
    private static final String FILE_SPLIT_SIZE = "file_split_size";
    private static final String MAX_INITIAL_FILE_SPLIT_SIZE = "max_initial_file_split_size";
    private static final String MAX_FILE_SPLIT_SIZE = "max_file_split_size";
    private static final String MAX_INITIAL_FILE_SPLIT_NUM = "max_initial_file_split_num";
    private static final String MAX_FILE_SPLIT_NUM = "max_file_split_num";
    private static final long DEFAULT_MAX_INITIAL_FILE_SPLIT_SIZE = 32L * 1024 * 1024;
    private static final long DEFAULT_MAX_FILE_SPLIT_SIZE = 64L * 1024 * 1024;
    private static final long DEFAULT_MAX_INITIAL_FILE_SPLIT_NUM = 200L;
    private static final long DEFAULT_MAX_FILE_SPLIT_NUM = 100000L;
    // FIX-M3 streaming (file-count) batch gate — keys byte-identical to fe-core SessionVariable.
    private static final String ENABLE_EXTERNAL_TABLE_BATCH_MODE = "enable_external_table_batch_mode";
    private static final String NUM_FILES_IN_BATCH_MODE = "num_files_in_batch_mode";
    private static final long DEFAULT_NUM_FILES_IN_BATCH_MODE = 1024L;

    // COUNT(*) pushdown (T05). The snapshot-summary keys are the stable iceberg spec strings — byte-identical
    // to legacy IcebergUtils.TOTAL_* (themselves local constants, not org.apache.iceberg.SnapshotSummary.*).
    private static final String TOTAL_RECORDS = "total-records";
    private static final String TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String TOTAL_EQUALITY_DELETES = "total-equality-deletes";
    // Session var: when a table has only (dangling) position deletes, ignore them and still push count down.
    private static final String IGNORE_ICEBERG_DANGLING_DELETE = "ignore_iceberg_dangling_delete";

    // System-table (P6.5-T05) JNI split: a placeholder path matching legacy IcebergSplit.DUMMY_PATH. A sys split
    // carries no real file (BE reads the serialized FileScanTask), so the path is never opened — it only keeps
    // the generic split framework's non-null-path contract.
    private static final String SYS_TABLE_DUMMY_PATH = "/dummyPath";

    // Special-column names for classifyColumn (C2 WS-SYNTH-READ). The connector cannot import fe-core
    // (org.apache.doris.catalog.Column / IcebergUtils are forbidden), so these literals are duplicated here
    // and pinned to the fe-core constants by IcebergScanPlanProviderClassifyColumnTest (DORIS_ICEBERG_ROWID_COL
    // == Column.ICEBERG_ROWID_COL) and the row-lineage names == IcebergUtils.ICEBERG_ROW_ID_COL /
    // ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL. The hidden row-id column is SYNTHESIZED (never in the data
    // file, materialized by IcebergParquet/OrcReader); the v3 row-lineage columns are GENERATED (read from the
    // file when present, otherwise backfilled). The engine-wide __DORIS_GLOBAL_ROWID_COL__ is NOT handled here
    // (a generic Doris lazy-materialization mechanism owned by the generic node).
    private static final String DORIS_ICEBERG_ROWID_COL = "__DORIS_ICEBERG_ROWID_COL__";
    private static final String ICEBERG_ROW_ID_COL = "_row_id";
    private static final String ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL = "_last_updated_sequence_number";

    // #65784: version marker (TFileScanRangeParams.iceberg_scan_semantics_version) advertising that this plan
    // was produced by an FE honoring authoritative iceberg name mappings + logical initial-default
    // materialization. BE gates the result-changing v1 semantics on it (supports_iceberg_scan_semantics_v1), so
    // an OLD-FE plan (marker absent) keeps legacy behavior on a NEW BE during a rolling upgrade. Mirrors legacy
    // IcebergScanNode.ICEBERG_SCAN_SEMANTICS_VERSION / enableCurrentIcebergScanSemantics().
    static final int ICEBERG_SCAN_SEMANTICS_VERSION = 1;

    // FIX-SCHEMA-EVOLUTION (T06): scan-level prop carrying the base64 TBinaryProtocol-serialized schema
    // dictionary (current_schema_id + the single history_schema_info entry). getScanNodeProperties builds it
    // from the live table + requested columns; populateScanLevelParams applies it to the real params.
    // Transport via the props map because getScanPlanProvider() returns a fresh provider per call (no shared
    // instance state between the two SPI methods). Mirrors paimon's paimon.schema_evolution.
    private static final String SCHEMA_EVOLUTION_PROP = "iceberg.schema_evolution";

    // FIX (explain gap): scan-level prop carrying the newline-joined iceberg Expression.toString() of each
    // pushed-down conjunct. getScanNodeProperties serializes it (same IcebergPredicateConverter buildScan uses);
    // appendExplainInfo renders it as the legacy IcebergScanNode `icebergPredicatePushdown=` EXPLAIN block.
    // Transport via the props map for the same reason as SCHEMA_EVOLUTION_PROP above (the two SPI methods share
    // no provider instance state). Mirrors paimon's paimon.predicate. Iceberg expression toStrings are
    // single-line, so the newline is an unambiguous record separator.
    private static final String PUSHDOWN_PREDICATES_PROP = "iceberg.pushdown_predicates";

    // FE-only EXPLAIN prop carrying the statement's queryId, emitted by getScanNodeProperties ONLY when the
    // manifest cache is enabled. appendExplainInfo uses it to drain THIS scan's manifest-cache
    // hits/misses/failures from the shared per-catalog IcebergManifestCache and render the legacy
    // IcebergScanNode `manifest cache:` VERBOSE line. Same transport rationale as PUSHDOWN_PREDICATES_PROP (the
    // planning + EXPLAIN SPI methods share no provider instance); like it, this key is never consumed by
    // populateScanLevelParams, so it never reaches BE. Its presence also gates the line (absent when the cache
    // is disabled -> no line, matching legacy notContains).
    private static final String MANIFEST_CACHE_QUERYID_PROP = "iceberg.manifest_cache_query_id";

    // T08 manifest cache gate (ported from fe-core IcebergExternalCatalog + IcebergUtils.isManifestCacheEnabled
    // + CacheSpec.isCacheEnabled). Default OFF: the default scan path stays the iceberg SDK planFiles()
    // (splitFiles). When enabled, planScan re-plans at the manifest level so the per-manifest data/delete-file
    // reads hit the connector-owned IcebergManifestCache. The .ttl-second/.capacity properties feed ONLY this
    // enable formula (legacy quirk); the cache itself is fixed no-TTL / capacity 100000.
    private static final String MANIFEST_CACHE_ENABLE = "meta.cache.iceberg.manifest.enable";
    private static final String MANIFEST_CACHE_TTL_SECOND = "meta.cache.iceberg.manifest.ttl-second";
    private static final String MANIFEST_CACHE_CAPACITY = "meta.cache.iceberg.manifest.capacity";
    private static final boolean DEFAULT_MANIFEST_CACHE_ENABLE = false;
    private static final long DEFAULT_MANIFEST_CACHE_TTL_SECOND = 48L * 60 * 60;
    private static final long DEFAULT_MANIFEST_CACHE_CAPACITY = 1024L;

    private final Map<String, String> properties;
    // Per-request catalog-ops resolver: applied with the current ConnectorSession to obtain the IcebergCatalogOps
    // for that request. For a iceberg.rest.session=user catalog the connector passes this::newCatalogBackedOps so
    // scan planning loads tables through the querying user's per-request delegated REST catalog (fail-closed — a
    // tokenless request is rejected, #63068 parity). Every other catalog (and the offline-test ctors) resolves the
    // single shared ops regardless of session (constant s -> catalogOps).
    private final Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver;
    // Engine seam: executeAuthenticated (Kerberos UGI), storage properties, vended credentials. Nullable —
    // null in offline unit tests via the 2-arg ctor, in which case resolveTable resolves directly.
    private final ConnectorContext context;
    // T08: per-catalog manifest cache, owned by the long-lived IcebergConnector and injected via getScanPlanProvider.
    // Nullable — null via the 2-/3-arg ctors (offline tests, default-disabled gate); when null the gate is
    // forced off and planScan uses the SDK splitFiles path.
    private final IcebergManifestCache manifestCache;
    // PERF-01: cross-query RAW-table cache shared with the metadata layer, owned by the long-lived
    // IcebergConnector and injected via getScanPlanProvider. Nullable — null via the offline-test ctors and
    // when the connector's credential gate disables the cross-query layer; when null resolveTable still uses the
    // per-statement scope and falls back to a direct remote load.
    private final IcebergTableCache tableCache;
    // PERF-03: cross-query inferred-file-format cache shared with the connector, owned by the long-lived
    // IcebergConnector and injected via getScanPlanProvider. Nullable — null via the offline-test ctors; when null
    // getScanNodeProperties resolves file_format_type live (matching pre-PERF-03 behaviour, node-memoized per query).
    private final IcebergFormatCache formatCache;

    // FIX-SCAN-METRICS: per-query stash of the iceberg SDK scan diagnostics captured by the attached
    // IcebergScanProfileReporter during planScan, keyed by session queryId. fe-core drains it
    // (collectScanProfiles) right after planScan on the same thread; releaseReadTransaction reclaims any entry
    // a thrown planScan left behind. Attached only on the synchronous data/count path (never streaming or
    // system-table, which fe-core never drains), so the value list is appended single-threaded.
    private final ConcurrentHashMap<String, List<ConnectorScanProfile>> scanProfileStash = new ConcurrentHashMap<>();

    // Test-only gate for the PERF-11 per-file memo: how many times computePerFileInvariants actually ran across
    // this provider's scans. A file split into k byte-slices must increment it ONCE (not k times), proving the
    // per-slice recompute collapsed to per-file. Not reset per scan — a test uses a fresh provider.
    @VisibleForTesting
    int perFileInvariantComputeCount;

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps) {
        this(properties, catalogOps, null, null);
    }

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this(properties, catalogOps, context, null);
    }

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context, IcebergManifestCache manifestCache) {
        // Constant resolver: these ctors (offline tests + the pre-session connector paths) bind a single ops that
        // ignores the session, so existing behaviour/tests are byte-identical. No cross-query cache (tableCache
        // null) — the per-statement scope still dedups within a statement.
        this(properties, session -> catalogOps, context, manifestCache, null);
    }

    /**
     * Session-aware convenience ctor without a cross-query table cache (tableCache null); used by the offline
     * session-routing tests. The per-statement scope still dedups within a statement.
     */
    public IcebergScanPlanProvider(Map<String, String> properties,
            Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver,
            ConnectorContext context, IcebergManifestCache manifestCache) {
        this(properties, catalogOpsResolver, context, manifestCache, null);
    }

    /**
     * Session-aware ctor used by {@link IcebergConnector#getScanPlanProvider()}: {@code catalogOpsResolver} is
     * applied per request with the current {@link ConnectorSession} so a {@code iceberg.rest.session=user} catalog
     * resolves the querying user's per-request delegated catalog (the connector passes
     * {@code this::newCatalogBackedOps}); every other catalog resolves the single shared ops.
     */
    public IcebergScanPlanProvider(Map<String, String> properties,
            Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver,
            ConnectorContext context, IcebergManifestCache manifestCache, IcebergTableCache tableCache) {
        this(properties, catalogOpsResolver, context, manifestCache, tableCache, null);
    }

    /**
     * Full ctor used by {@link IcebergConnector#getScanPlanProvider()}, adding the PERF-03 cross-query
     * inferred-file-format cache ({@code formatCache}). The 5-arg ctor delegates here with a null format cache
     * (offline tests + pre-cache paths resolve {@code file_format_type} live).
     */
    public IcebergScanPlanProvider(Map<String, String> properties,
            Function<ConnectorSession, IcebergCatalogOps> catalogOpsResolver,
            ConnectorContext context, IcebergManifestCache manifestCache, IcebergTableCache tableCache,
            IcebergFormatCache formatCache) {
        this.properties = properties;
        this.catalogOpsResolver = catalogOpsResolver;
        this.context = context;
        this.manifestCache = manifestCache;
        this.tableCache = tableCache;
        this.formatCache = formatCache;
    }

    /**
     * Iceberg is predicate-driven: it re-plans through its own SDK from the pushed predicate and never
     * consults {@code requiredPartitions} (mirrors the legacy {@code IcebergScanNode} and paimon). The engine
     * must therefore map a genuine FE prune-to-zero to scan-all instead of short-circuiting to zero rows —
     * otherwise {@code WHERE col IS NULL} on a genuine-null partition rendered as a non-null sentinel would
     * drop rows once T02 wires the predicate path.
     */
    @Override
    public boolean ignorePartitionPruneShortCircuit() {
        return true;
    }

    /**
     * The distinct scanned partitions among the just-planned ranges (FIX-L12) — restores legacy
     * {@code IcebergScanNode}'s {@code selectedPartitionNum = partitionMapInfos.size()} (keyed by
     * {@code (PartitionData) file().partition()}) so EXPLAIN {@code partition=N/M} and
     * {@code sql_block_rule} reflect the partitions iceberg's manifest/residual evaluation actually
     * resolved — including hidden/transform partitioning ({@code days(ts)}, {@code bucket(n,id)}) that the
     * engine's declared-column Nereids pruning cannot see. The identity is
     * {@link IcebergScanRange#getScannedPartitionKey()} ({@code specId|partitionDataJson}), which is
     * distinct-faithful for a single spec's transform partitions. Returns empty when no range carries a
     * partition key (unpartitioned table), so the engine keeps its own count. Only counts this provider's
     * own {@link IcebergScanRange} instances.
     */
    @Override
    public OptionalLong scannedPartitionCount(List<ConnectorScanRange> scanRanges) {
        Set<String> distinctPartitions = new HashSet<>();
        for (ConnectorScanRange range : scanRanges) {
            if (range instanceof IcebergScanRange) {
                String key = ((IcebergScanRange) range).getScannedPartitionKey();
                if (key != null) {
                    distinctPartitions.add(key);
                }
            }
        }
        return distinctPartitions.isEmpty()
                ? OptionalLong.empty() : OptionalLong.of(distinctPartitions.size());
    }

    @Override
    public List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
        String queryId = session.getQueryId();
        if (queryId == null || queryId.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorScanProfile> profiles = scanProfileStash.remove(queryId);
        return profiles == null ? Collections.emptyList() : profiles;
    }

    @Override
    public void releaseReadTransaction(String queryId) {
        // Iceberg opens no metastore read transaction (it inherits the SPI no-op); this override only reclaims
        // the scan-metrics stash for a query whose planScan threw AFTER the reporter fired (the normal path
        // drains it via collectScanProfiles). Same queryId fe-core registered the query-finish callback with.
        if (queryId != null && !queryId.isEmpty()) {
            scanProfileStash.remove(queryId);
        }
    }

    /**
     * Iceberg metadata tables legally time-travel ({@code t$snapshots FOR TIME/VERSION AS OF ...},
     * {@code t$files@branch('b')}): legacy {@code IcebergScanNode.createTableScan} honors the pin via
     * {@code useRef}/{@code useSnapshot} with no isSystemTable gate, and this provider retains
     * ({@code getSysTableHandle}) + applies ({@code planSystemTableScan} -> {@code buildScan}) it. So the
     * generic {@code PluginDrivenScanNode} sys-table guard must let pinned iceberg sys reads through
     * (unlike paimon, whose binlog/audit_log sys tables keep the default {@code false} rejection).
     */
    @Override
    public boolean supportsSystemTableTimeTravel() {
        return true;
    }

    /**
     * Classifies iceberg's special columns for the generic {@code PluginDrivenScanNode} (C2 WS-SYNTH-READ),
     * porting the legacy {@code IcebergScanNode.classifyColumn} mapping minus the engine-wide
     * {@code __DORIS_GLOBAL_ROWID_COL__} prefix (which the generic node handles itself): the hidden row-id
     * column is SYNTHESIZED (a debug/DML metadata column never present in the data file), and the v3
     * row-lineage columns are GENERATED (read from the file when present, otherwise backfilled). Every other
     * column returns {@code DEFAULT} so the generic node applies its own partition-key / regular classification.
     */
    @Override
    public ConnectorColumnCategory classifyColumn(String columnName) {
        if (DORIS_ICEBERG_ROWID_COL.equalsIgnoreCase(columnName)) {
            return ConnectorColumnCategory.SYNTHESIZED;
        }
        if (ICEBERG_ROW_ID_COL.equalsIgnoreCase(columnName)
                || ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL.equalsIgnoreCase(columnName)) {
            return ConnectorColumnCategory.GENERATED;
        }
        return ConnectorColumnCategory.DEFAULT;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        return planScanInternal(session, handle, columns, filter, false);
    }

    /**
     * COUNT(*)-pushdown-aware scan entry (FIX-COUNT-PUSHDOWN). The generic {@code PluginDrivenScanNode}
     * forwards the no-grouping {@code COUNT(*)} signal here. {@code limit}/{@code requiredPartitions} are not
     * consumed by the iceberg read path (it is predicate-driven; mirrors paimon, whose other overloads fold
     * down to the 4-arg planScan).
     */
    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit,
            List<String> requiredPartitions,
            boolean countPushdown) {
        return planScanInternal(session, handle, columns, filter, countPushdown);
    }

    /**
     * Streaming-split decision + estimate (FIX-M3), a faithful port of legacy {@code IcebergScanNode.isBatchMode}:
     * stream when the matched-manifest file count reaches {@code num_files_in_batch_mode} with
     * {@code enable_external_table_batch_mode} on. Returns that file count (the BE concurrency hint) or -1 to stay
     * on the synchronous {@link #planScan} path. Cheap: sums manifest metadata counts, never enumerates splits.
     *
     * <p>Excluded from streaming (return -1): system tables (JNI serialized-split path); batch mode disabled;
     * empty table (no snapshot); a servable {@code COUNT(*)} pushdown (collapsed to one range); and
     * format-version &ge; 3 — v3 carries the commit-bridge rewritable-delete stash that the write side reads at
     * write-plan time, which streaming would fill too late (at BE-pull time), resurrecting deleted rows. See the
     * design doc §5.</p>
     */
    @Override
    public long streamingSplitEstimate(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter, boolean countPushdown) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        if (iceHandle.isSystemTable() || !sessionBool(session, ENABLE_EXTERNAL_TABLE_BATCH_MODE, true)) {
            return -1;
        }
        Table table = resolveTable(session, iceHandle);
        TableScan scan = buildScan(table, iceHandle, filter, session);
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return -1;
        }
        if (getFormatVersion(table) >= 3) {
            return -1;
        }
        if (countPushdown && getCountFromSnapshot(scan, session) >= 0) {
            return -1;
        }
        long threshold = sessionLong(session, NUM_FILES_IN_BATCH_MODE, DEFAULT_NUM_FILES_IN_BATCH_MODE);
        long fileCount = 0;
        try (CloseableIterable<ManifestFile> matching = getMatchingManifest(
                snapshot.dataManifests(table.io()), table.specs(), scan.filter())) {
            for (ManifestFile manifest : matching) {
                // Manifest metadata counts (cheap — no per-file read). Null guard for ancient manifests that
                // omit the counts (legacy summed them unguarded; 0 is the safe under-count, never over-streams).
                Integer added = manifest.addedFilesCount();
                Integer existing = manifest.existingFilesCount();
                fileCount += (added == null ? 0 : added) + (existing == null ? 0 : existing);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to count iceberg manifest files for batch decision, error message is:"
                    + e.getMessage(), e);
        }
        return fileCount >= threshold ? fileCount : -1;
    }

    /**
     * Lazy streaming split source (FIX-M3), mirroring legacy {@code IcebergScanNode.doStartSplit}: slice files at
     * a FIXED size ({@code file_split_size} if set, else {@code max_split_size} — NOT the per-table
     * {@link #determineTargetFileSplitSize} heuristic, which would force materializing every task), so
     * {@code planFiles()} streams without holding the full task list — the OOM protection. Bypasses the manifest
     * cache (its planning materializes; legacy's lazy batch path only ran with the manifest cache off). Only
     * called after {@link #streamingSplitEstimate} returned &ge; 0, so the snapshot/non-sys/v&lt;3 gates already hold.
     */
    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter, long limit) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = resolveTable(session, iceHandle);
        TableScan scan = buildScan(table, iceHandle, filter, session);
        int formatVersion = getFormatVersion(table);
        List<String> orderedPartitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
        ZoneId zone = resolveSessionZone(session);
        boolean partitioned = table.spec().isPartitioned();
        Map<String, String> vendedToken = context != null
                ? extractVendedToken(table, restVendedCredentialsEnabled()) : Collections.emptyMap();
        UnaryOperator<String> uriNormalizer = newUriNormalizer(vendedToken);
        long fileSplitSize = sessionLong(session, FILE_SPLIT_SIZE, 0L);
        long sliceSize = fileSplitSize > 0 ? fileSplitSize
                : sessionLong(session, MAX_FILE_SPLIT_SIZE, DEFAULT_MAX_FILE_SPLIT_SIZE);
        CloseableIterable<FileScanTask> tasks = streamingFileScanTasks(scan, session, table, filter, sliceSize);
        return new IcebergStreamingSplitSource(tasks, table, formatVersion, partitioned,
                orderedPartitionKeys, zone, uriNormalizer, sliceSize, iceHandle.getRewriteFileScope());
    }

    /**
     * The streaming source's whole-file enumeration, byte-offset-split at {@code sliceSize}. PERF-04 (C17): when
     * the manifest cache is enabled, read manifests THROUGH THE CACHE via the lazy {@link #cacheBackedFileScanTasks}
     * (no-stats overload — Phase 2 iterates on the engine pump thread, see there) so a big streaming scan finally
     * hits the cache while staying lazy (OOM-safe). An eager Phase-1 cache failure records it and falls back to the
     * SDK {@code planFiles()} path (mirrors {@link #planFileScanTask}); a later lazy failure surfaces through the
     * streaming source's {@code hasNext}. Cache disabled -> the SDK path, byte-unchanged.
     */
    private CloseableIterable<FileScanTask> streamingFileScanTasks(TableScan scan, ConnectorSession session,
            Table table, Optional<ConnectorExpression> filter, long sliceSize) {
        if (isManifestCacheEnabled()) {
            try {
                return TableScanUtil.splitFiles(
                        cacheBackedFileScanTasks(scan, session, table, filter, null), sliceSize);
            } catch (Exception e) {
                LOG.warn("Iceberg streaming plan with manifest cache failed, falling back to SDK scan: {}",
                        e.getMessage(), e);
                manifestCache.recordFailure(session.getQueryId());
            }
        }
        return TableScanUtil.splitFiles(scan.planFiles(), sliceSize);
    }

    /**
     * Lazy {@link ConnectorSplitSource} over an iceberg scan's byte-offset-split {@link FileScanTask}s: maps each
     * task to an {@link IcebergScanRange} on demand (via {@link #buildRangeForTask}) so the engine can pump them
     * into its split queue with backpressure, keeping FE heap bounded for million-file scans. v3 is gated off the
     * streaming path, so the stash side-effect is inert here ({@code stashRewritableDeletes=false}). Single-pass,
     * not thread-safe (the engine drives it from one background task).
     */
    private final class IcebergStreamingSplitSource implements ConnectorSplitSource {
        private final CloseableIterable<FileScanTask> tasks;
        private final Table table;
        private final int formatVersion;
        private final boolean partitioned;
        private final List<String> orderedPartitionKeys;
        private final ZoneId zone;
        private final UnaryOperator<String> uriNormalizer;
        private final long sliceSize;
        private final Set<String> rewriteScope;
        // Lazily opened on first hasNext() so the ctor never throws — iceberg's ParallelIterable submits
        // manifest readers in tasks.iterator(), which can fail; opening it eagerly here would throw out of
        // streamSplits() BEFORE the source is returned, leaking the planFiles() iterable (the engine pump's
        // close() never receives it). Lazy open instead routes any failure through hasNext()->the engine's
        // setException + finally-close, with tasks still closed. null until first use.
        private CloseableIterator<FileScanTask> iterator;
        // Look-ahead buffer so hasNext() can skip data files filtered out by the rewrite scope.
        private IcebergScanRange buffered;
        // Per-file invariant cache (PERF-11): per split-source = per scan; the pump is single-threaded, so the
        // 1-entry cache stays O(1) memory (never accumulates), preserving the streaming path's OOM safety.
        private final PerFileScratch scratch = new PerFileScratch();

        IcebergStreamingSplitSource(CloseableIterable<FileScanTask> tasks, Table table, int formatVersion,
                boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone,
                UnaryOperator<String> uriNormalizer, long sliceSize, Set<String> rewriteScope) {
            this.tasks = tasks;
            this.table = table;
            this.formatVersion = formatVersion;
            this.partitioned = partitioned;
            this.orderedPartitionKeys = orderedPartitionKeys;
            this.zone = zone;
            this.uriNormalizer = uriNormalizer;
            this.sliceSize = sliceSize;
            this.rewriteScope = rewriteScope;
        }

        @Override
        public boolean hasNext() {
            if (buffered != null) {
                return true;
            }
            if (iterator == null) {
                iterator = tasks.iterator();
            }
            while (iterator.hasNext()) {
                IcebergScanRange range = buildRangeForTask(iterator.next(), table, formatVersion, partitioned,
                        orderedPartitionKeys, zone, uriNormalizer, sliceSize, rewriteScope, null, scratch);
                if (range != null) {
                    buffered = range;
                    return true;
                }
            }
            return false;
        }

        @Override
        public ConnectorScanRange next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            IcebergScanRange range = buffered;
            buffered = null;
            return range;
        }

        @Override
        public void close() throws IOException {
            try {
                if (iterator != null) {
                    iterator.close();
                }
            } finally {
                tasks.close();
            }
        }
    }

    private List<ConnectorScanRange> planScanInternal(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            boolean countPushdown) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        if (iceHandle.isSystemTable()) {
            // System tables take a metadata-table path, never the data-file path below (no count pushdown, no
            // data-file ranges) — mirrors legacy IcebergScanNode branching on isSystemTable. $position_deletes
            // then splits off again inside, onto BE's NATIVE reader; every other sys table stays on JNI.
            return planSystemTableScan(iceHandle, columns, filter, session);
        }
        Table table = resolveTable(session, iceHandle);
        TableScan scan = buildScan(table, iceHandle, filter, session);
        // FIX-SCAN-METRICS: attach a per-scan metrics reporter so the iceberg SDK's ScanReport (planning time,
        // data/delete files, scanned vs skipped manifests) is captured into the query profile — restores the
        // legacy IcebergScanNode scan-metrics profile the migration dropped. Attached HERE (the synchronous
        // data/count path), NOT in buildScan, which is also reached by streamSplits/planSystemTableScan whose
        // report would stash a queryId entry fe-core never drains (leak). The reporter fires on close of the
        // planFiles iterable, which the data (try-with-resources) and count paths close on this thread.
        // Guard a null session (offline unit tests) — production planScan always carries one.
        if (session != null) {
            scan = scan.metricsReporter(new IcebergScanProfileReporter(session.getQueryId(), scanProfileStash));
        }

        int formatVersion = getFormatVersion(table);
        List<String> orderedPartitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
        ZoneId zone = resolveSessionZone(session);
        boolean partitioned = table.spec().isPartitioned();

        // Vended credentials (T09): extract the per-table REST vended token ONCE per scan (gated on the catalog
        // flag iceberg.rest.vended-credentials-enabled, mirroring legacy IcebergVendedCredentialsProvider), then
        // thread it into the 2-arg URI normalization below so REST object-store data/delete paths normalize via
        // the vended map (a REST catalog's static storage map is empty by design). Empty for non-vended catalogs
        // / no context -> the 2-arg normalize folds to the static-map path (non-REST reads byte-unchanged). The
        // BE-credential overlay is emitted separately by getScanNodeProperties.
        Map<String, String> vendedToken = context != null
                ? extractVendedToken(table, restVendedCredentialsEnabled()) : Collections.emptyMap();
        // Derive the vended storage config ONCE per scan (the token is scan-invariant) and reuse it for every
        // per-file path normalization below, instead of rebuilding it per data/delete file (C3).
        UnaryOperator<String> uriNormalizer = newUriNormalizer(vendedToken);

        // COUNT(*) pushdown (T05): when the count is servable from the snapshot summary, collapse the scan to
        // a single whole-file range carrying the full count (mirrors paimon's collapse + legacy's <=10000
        // case; the legacy >10000 parallel multi-split trim is a perf-only divergence, dropped). A -1 (equality
        // deletes, or dangling position deletes without the ignore flag) falls through to the normal scan so
        // BE reads and counts.
        if (countPushdown) {
            long realCount = getCountFromSnapshot(scan, session);
            if (realCount >= 0) {
                return planCountPushdown(table, scan, realCount, formatVersion, partitioned,
                        orderedPartitionKeys, zone, uriNormalizer, session, filter);
            }
        }

        // Enumerate FileScanTasks via the iceberg SDK (split byte-offsets come from TableScanUtil.splitFiles)
        // and emit one BE-ready IcebergScanRange per task, populating the typed iceberg carriers — incl. the
        // merge-on-read delete files (T04) — mirroring legacy IcebergScanNode.createIcebergSplit. The field-id
        // history dict (T06, scan-level), MVCC pin, and vended credentials (T09) land later.
        // commit-bridge supply (S4 part 2): for a format-version>=3 scan, accumulate each data file's non-equality
        // delete supply (old DVs + old position deletes) into the per-statement scope, so a DELETE/MERGE write on
        // the same statement can fill rewritable_delete_file_sets and the BE OR-merges those old deletes into the
        // new deletion vector — a missing supply silently resurrects previously-deleted rows. The scope is keyed by
        // catalog id + queryId (shared across this statement's scan and write, isolated per catalog for a
        // cross-catalog MERGE). Skipped pre-v3; a non-DML scan just leaves an entry GC'd with the statement, and an
        // absent scope (offline) yields a throwaway map that the write seam guards against (fail loud on v3 DML).
        Map<String, List<TIcebergDeleteFileDesc>> rewritableDeleteSupply = formatVersion >= 3
                ? IcebergStatementScope.rewritableDeleteSupply(session) : null;

        // WS-REWRITE R2 per-group scope: when the handle carries a rewrite file scope (the engine
        // rewrite_data_files driver sets it before each group's INSERT-SELECT), keep ONLY the data files in
        // that scope so the group rewrites exactly its bin-packed files. Match on the RAW iceberg path
        // (dataFile.path(), the SAME value the rewrite planner records into the scope), NOT the
        // scheme-normalized BE path (the range's .path()) — a normalization difference would silently scope to
        // the wrong files (over-read -> a RewriteFiles commit replacing more than the group -> duplicate rows).
        // null = no scope = full scan (every non-rewrite scan). Each kept task keeps its merge-on-read deletes
        // (buildRange re-attaches task.deletes()), so scoping never drops a delete binding.
        Set<String> rewriteScope = iceHandle.getRewriteFileScope();

        // Per-file invariant cache (PERF-11): ONE instance per scan, never shared across scans. Reused across
        // a data file's consecutive byte-slices so partition JSON / identity map / delete carriers are computed
        // once per file, not per slice. The streaming source below holds its own.
        PerFileScratch scratch = new PerFileScratch();
        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (SplitPlan plan = planFileScanTask(scan, session, table, filter)) {
            for (FileScanTask task : plan.tasks) {
                // Shared per-task mapping (rewrite-scope skip + M-2 weight denominator + v3 stash side-effect),
                // identical to the streaming path's IcebergStreamingSplitSource so both produce the same ranges.
                IcebergScanRange range = buildRangeForTask(task, table, formatVersion, partitioned,
                        orderedPartitionKeys, zone, uriNormalizer, plan.targetSplitSize, rewriteScope,
                        rewritableDeleteSupply, scratch);
                if (range != null) {
                    ranges.add(range);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to enumerate iceberg file scan tasks, error message is:"
                    + e.getMessage(), e);
        } catch (ValidationException e) {
            // Port of legacy IcebergScanNode.checkNotSupportedException: a table with a partition spec whose
            // SOURCE column was later DROPPED cannot be planned — iceberg resolves the orphaned partition field
            // while building the delete-file index / partition projection during planFiles() and fails. The
            // legacy stack was a raw NullPointerException on iceberg 1.4.x ("Type cannot be null"); the iceberg
            // version this connector links guards the null source explicitly and throws ValidationException
            // ("Cannot find source column for partition field: ..."). Surface the stable legacy user-facing
            // message (the partition-evolution regression asserts this substring) instead of a raw internal
            // failure. Only the dropped-source-column signature is reclassified; any UNRELATED ValidationException
            // (a genuine query-validation error) is rethrown untouched. NullPointerException is deliberately NOT
            // caught here — on this iceberg version the dropped-column case is a ValidationException, so catching
            // NPE would only mask unrelated bugs (fail loud instead).
            if (!IcebergPartitionUtils.isDroppedPartitionSourceColumn(e)) {
                throw e;
            }
            LOG.warn("Unable to plan for iceberg table {}", table.name(), e);
            throw new DorisConnectorException("Unable to plan for this table. "
                    + "Maybe read Iceberg table with dropped old partition column. Cause: " + rootCauseMessage(e));
        }
        LOG.debug("Iceberg planScan produced {} ranges for table {}", ranges.size(), table.name());
        return ranges;
    }

    /**
     * The class-qualified message of the ROOT cause, a self-contained port of legacy
     * {@code Util.getRootCauseMessage} (the connector cannot import fe-core), used to fill the legacy
     * "Cause: ..." suffix of the "Unable to plan for this table" error.
     */
    private static String rootCauseMessage(Throwable t) {
        if (t == null) {
            return "unknown";
        }
        Throwable p = t;
        while (p.getCause() != null) {
            p = p.getCause();
        }
        String message = p.getMessage();
        return message == null ? p.getClass().getName() : p.getClass().getName() + ": " + message;
    }

    /**
     * Map one {@link FileScanTask} to its BE-ready {@link IcebergScanRange}, applying the rewrite-scope filter
     * (returns {@code null} to skip a data file outside the scope) and the v3 commit-bridge rewritable-delete
     * accumulation. Shared by the synchronous {@link #planScanInternal} loop and the streaming
     * {@code IcebergStreamingSplitSource} so both paths produce byte-identical ranges and never drop a
     * side-effect. The streaming path passes {@code rewritableDeleteSupply=null} (v3 is gated onto the eager
     * path — see {@link #streamingSplitEstimate}), so the accumulation is inert there.
     */
    private IcebergScanRange buildRangeForTask(FileScanTask task, Table table, int formatVersion,
            boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone,
            UnaryOperator<String> uriNormalizer, long targetSplitSize, Set<String> rewriteScope,
            Map<String, List<TIcebergDeleteFileDesc>> rewritableDeleteSupply, PerFileScratch scratch) {
        DataFile dataFile = task.file();
        if (rewriteScope != null && !rewriteScope.contains(dataFile.path().toString())) {
            return null;
        }
        // First byte-slice of a new data file? (scratch still holds the previous file until buildRange refreshes
        // it below — so capture this BEFORE the buildRange call.) The v3 rewritable-delete supply is identical
        // for every slice of a file, so record it exactly ONCE per file — on the file's first slice, keyed to
        // the new file, so the LAST file in the stream is never dropped.
        boolean firstSliceOfFile = scratch.file != dataFile;
        // targetSplitSize is the scan-level weight denominator (M-2): each data-file range carries a
        // size-proportional BE scheduling weight (selfSplitWeight computed inside buildRange).
        IcebergScanRange range = buildRange(table, dataFile, task, formatVersion, partitioned,
                orderedPartitionKeys, zone, uriNormalizer, -1, targetSplitSize, scratch);
        if (rewritableDeleteSupply != null && firstSliceOfFile) {
            // Record this data file's non-equality delete supply keyed on its RAW path (the exact string the BE
            // matches a rewritable set against). An empty list (no old non-eq deletes) contributes nothing.
            List<TIcebergDeleteFileDesc> descs = range.rewritableDeleteDescs();
            if (range.getOriginalPath() != null && descs != null && !descs.isEmpty()) {
                rewritableDeleteSupply.put(range.getOriginalPath(), descs);
            }
        }
        return range;
    }

    /**
     * Plan the system-table (JNI) scan for a {@code $sys} handle, mirroring legacy
     * {@code IcebergScanNode.doGetSystemTableSplits} + {@code createIcebergSysSplit} + {@code setIcebergParams}:
     * resolve the metadata table ({@link #resolveSysTable}), apply the time-travel pin + predicate through the
     * shared {@link #buildScan} (legacy {@code createTableScan} honors {@code useSnapshot}/{@code useRef} on the
     * metadata-table scan too — iceberg system tables are legal time-travel targets), then serialize each
     * metadata {@code FileScanTask} ({@code SerializationUtil.serializeToBase64}) into a JNI split carrying ONLY
     * {@code serialized_split} + {@code FORMAT_JNI} (see {@link IcebergScanRange#populateRangeParams}). COUNT(*)
     * pushdown does not apply (a metadata table has no snapshot-summary count). The serialized {@code
     * FileScanTask} bytes are consumed verbatim by BE's {@code IcebergSysTableJniScanner}
     * ({@code deserializeFromBase64(...).asDataTask().rows()}); FE unit tests cannot reach the BE classloader, so
     * the cross-version byte compatibility is covered by the P6.8 docker e2e. Live since the iceberg SPI cutover.
     */
    private List<ConnectorScanRange> planSystemTableScan(IcebergTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter,
            ConnectorSession session) {
        // Thread-level auth wrap (legacy parity: preExecutionAuthenticator.execute around doGetSplits), ONE
        // scope spanning the base-table load (resolveSysTable) plus the metadata-table planFiles — whose
        // manifest-list read for the $files family happens on THIS thread. Deliberately NOT the
        // wrapTableForScan object-level wrap — the planned FileScanTasks are Java-serialized to the BE JNI
        // reader and the authenticator-bearing FileIO wrapper is not serializable.
        if (context == null) {
            return doPlanSystemTableScan(handle, columns, filter, session);
        }
        try {
            return context.executeAuthenticated(() -> doPlanSystemTableScan(handle, columns, filter, session));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to plan iceberg system-table scan, error message is:"
                    + e.getMessage(), e);
        }
    }

    private List<ConnectorScanRange> doPlanSystemTableScan(IcebergTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter,
            ConnectorSession session) {
        Table metadataTable = resolveSysTable(session, handle);
        if (isPositionDeletesSysTable(handle)) {
            return doPlanPositionDeletesSystemTableScan(handle, metadataTable, columns, filter, session);
        }
        TableScan scan = buildScan(metadataTable, handle, filter, session);
        // Project the metadata-table scan to ONLY the requested columns, in the SAME order BE lists them in
        // required_fields (== the scan slot order: buildColumnHandles iterates desc.getSlots(), BE builds
        // required_fields from the matching file_slot_descs). This mirrors legacy IcebergScanNode
        // .getSystemTableProjectedSchema() + scan.project(...). Two reasons the projection is mandatory AND must
        // be order-preserving:
        //   1. Without it the iceberg SDK materialises EVERY metadata column per row for the $files/$data_files
        //      family -- including readable_metrics, whose bound conversion (MetricsUtil.readableMetricsStruct ->
        //      Conversions.fromByteBuffer) throws BufferUnderflowException on boolean/complex bound columns --
        //      even when the query selects only a scalar such as file_size_in_bytes.
        //   2. BE's IcebergSysTableJniScanner reads the projected StaticDataTask rows POSITIONALLY (row.get(i)),
        //      relying on the i-th projected field being the i-th required field. scan.select(names) would route
        //      the ids through a Set and re-emit them in TABLE-schema order (TypeUtil.project), breaking that
        //      contract; scan.project(orderedSchema) preserves the requested order, so we build the schema by
        //      hand from the requested columns.
        List<String> projectedColumns = requestedLowerNames(columns);
        if (!projectedColumns.isEmpty()) {
            Schema metadataSchema = metadataTable.schema();
            List<NestedField> projectedFields = new ArrayList<>(projectedColumns.size());
            Set<Integer> seenFieldIds = new HashSet<>();
            for (String columnName : projectedColumns) {
                NestedField field = metadataSchema.findField(columnName);
                if (field == null) {
                    throw new RuntimeException("Column " + columnName
                            + " not found in iceberg system table schema " + metadataTable.name());
                }
                if (seenFieldIds.add(field.fieldId())) {
                    projectedFields.add(field);
                }
            }
            scan = scan.project(new Schema(projectedFields));
        }
        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                ranges.add(new IcebergScanRange.Builder()
                        .path(SYS_TABLE_DUMMY_PATH)
                        .serializedSplit(SerializationUtil.serializeToBase64(task))
                        .build());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to enumerate iceberg system-table scan tasks, error message is:"
                    + e.getMessage(), e);
        }
        LOG.debug("Iceberg planScan produced {} system-table splits for {}.{}${}", ranges.size(),
                handle.getDbName(), handle.getTableName(), handle.getSysTableName());
        return ranges;
    }

    /** Whether {@code handle} is the {@code $position_deletes} metadata table (the one native-reader sys table). */
    private static boolean isPositionDeletesSysTable(IcebergTableHandle handle) {
        return handle.isSystemTable()
                && MetadataTableType.POSITION_DELETES.name().equalsIgnoreCase(handle.getSysTableName());
    }

    /**
     * Plan a {@code $position_deletes} scan. Port of legacy
     * {@code IcebergScanNode.doGetPositionDeletesSystemTableSplits} (upstream #65135).
     *
     * <p>This is the ONE system table that does not ride the JNI serialized-split path: BE reads it with a
     * native parquet/orc/puffin reader, so FE must emit real file ranges (see
     * {@link IcebergScanRange#populateRangeParams} for the wire shape).
     *
     * <p>Deviations from the JNI sys path above, each forced:
     * <ul>
     *   <li>{@code newBatchScan()}, not {@link #buildScan}'s {@code newScan()} —
     *       {@code PositionDeletesTable.newScan()} THROWS {@code UnsupportedOperationException}. The
     *       time-travel pin and predicate conversion are therefore replicated onto the BatchScan here.</li>
     *   <li>Predicates convert against the METADATA table's schema (file_path/pos/partition/...), best-effort:
     *       an unconvertible conjunct is dropped to a BE residual, mirroring legacy (NOT the fail-loud
     *       all-or-nothing of the write path's WHERE lowering).</li>
     * </ul>
     *
     * <p>Legacy's {@code metricsReporter} + {@code planWith(threadPool)} are dropped, the same documented
     * deviation {@link #buildScan} already makes for every other scan (profile-only; identical file set).
     * Legacy's smooth-upgrade backend guard is deliberately NOT ported (design doc D1: implement the final
     * form; the engine owns BE-compat and the SPI exposes no backends).
     */
    private List<ConnectorScanRange> doPlanPositionDeletesSystemTableScan(IcebergTableHandle handle,
            Table metadataTable, List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter,
            ConnectorSession session) {
        BatchScan scan = metadataTable.newBatchScan();
        if (handle.hasSnapshotPin()) {
            if (handle.getRef() != null) {
                scan = scan.useRef(handle.getRef());
            } else {
                scan = scan.useSnapshot(handle.getSnapshotId());
            }
        }
        if (filter.isPresent()) {
            List<Expression> predicates = new IcebergPredicateConverter(
                    metadataTable.schema(), resolveSessionZone(session)).convert(filter.get());
            for (Expression predicate : predicates) {
                scan = scan.filter(predicate);
            }
        }

        List<PositionDeletesScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<ScanTask> scanTasks = scan.planFiles()) {
            for (ScanTask task : scanTasks) {
                if (!(task instanceof PositionDeletesScanTask)) {
                    throw new DorisConnectorException(
                            "Unexpected Iceberg position_deletes scan task: " + task);
                }
                tasks.add((PositionDeletesScanTask) task);
            }
        } catch (IOException e) {
            throw new DorisConnectorException(
                    "Failed to enumerate iceberg position_deletes scan tasks, error message is:"
                            + e.getMessage(), e);
        }

        // Split sizing mirrors legacy determinePositionDeleteTargetSplitSize: an explicit file_split_size wins,
        // else the shared per-table heuristic. PUFFIN is non-splittable in iceberg 1.10.1, so
        // BaseContentScanTask.split() hands a DV task straight back — DVs are never fragmented.
        long fileSplitSize = sessionLong(session, FILE_SPLIT_SIZE, 0L);
        long targetSplitSize = fileSplitSize > 0 ? fileSplitSize
                : determineTargetFileSplitSize(tasks, session);

        boolean partitionRequested = isPositionDeletesPartitionColumnRequested(columns);
        List<NestedField> outputPartitionFields = partitionRequested
                ? getPositionDeletesOutputPartitionFields(metadataTable) : Collections.emptyList();
        boolean enableMappingVarbinary = Boolean.parseBoolean(
                properties.getOrDefault(IcebergConnectorProperties.ENABLE_MAPPING_VARBINARY, "false"));
        ZoneId zone = resolveSessionZone(session);
        Map<String, String> vendedToken = context != null
                ? extractVendedToken(metadataTable, restVendedCredentialsEnabled()) : Collections.emptyMap();
        UnaryOperator<String> uriNormalizer = newUriNormalizer(vendedToken);

        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (PositionDeletesScanTask task : tasks) {
            for (PositionDeletesScanTask splitTask : splitPositionDeleteScanTask(task, targetSplitSize)) {
                ranges.add(buildPositionDeleteRange(splitTask, metadataTable, outputPartitionFields,
                        enableMappingVarbinary, zone, uriNormalizer));
            }
        }
        LOG.debug("Iceberg planScan produced {} position_deletes splits for {}.{}", ranges.size(),
                handle.getDbName(), handle.getTableName());
        return ranges;
    }

    @SuppressWarnings("unchecked")
    private static Iterable<PositionDeletesScanTask> splitPositionDeleteScanTask(PositionDeletesScanTask task,
            long targetSplitSize) {
        return ((SplittableScanTask<PositionDeletesScanTask>) task).split(targetSplitSize);
    }

    /**
     * One native range per position-delete split. Port of legacy
     * {@code IcebergScanNode.createIcebergPositionDeleteSysSplit}.
     */
    private IcebergScanRange buildPositionDeleteRange(PositionDeletesScanTask task, Table metadataTable,
            List<NestedField> outputPartitionFields, boolean enableMappingVarbinary, ZoneId zone,
            UnaryOperator<String> uriNormalizer) {
        DeleteFile deleteFile = task.file();
        String originalPath = deleteFile.path().toString();
        IcebergScanRange.Builder builder = new IcebergScanRange.Builder()
                .path(uriNormalizer.apply(originalPath))
                .start(task.start())
                .length(task.length())
                .fileSize(deleteFile.fileSizeInBytes())
                // The split's own scheduling weight, mirroring legacy newPositionDeleteSysTableSplit
                // (selfSplitWeight = max(length, 1)).
                .selfSplitWeight(Math.max(task.length(), 1L))
                .targetSplitSize(task.length());

        // DV vs plain position-delete file is decided by content ALONE (BE never looks at the file format):
        // a puffin DV still travels as FORMAT_PARQUET, exactly as legacy getNativePositionDeleteFileFormat does.
        TFileFormatType fileFormat = getNativePositionDeleteFileFormat(deleteFile.format());
        if (deleteFile.format() == FileFormat.PUFFIN) {
            Long contentOffset = deleteFile.contentOffset();
            Long contentLength = deleteFile.contentSizeInBytes();
            validateDeletionVectorMetadata(
                    originalPath, deleteFile.fileSizeInBytes(), contentOffset, contentLength);
            builder.positionDeleteSysTableSplit(
                    IcebergScanRange.DeleteFile.CONTENT_DELETION_VECTOR, fileFormat, originalPath);
            builder.positionDeleteDeletionVector(deleteFile.referencedDataFile(),
                    contentOffset, contentLength);
        } else {
            builder.positionDeleteSysTableSplit(
                    IcebergScanRange.DeleteFile.CONTENT_POSITION_DELETE, fileFormat, originalPath);
        }

        builder.partitionSpecId(deleteFile.specId());
        PartitionSpec partitionSpec = metadataTable.specs().get(deleteFile.specId());
        if (partitionSpec == null) {
            throw new DorisConnectorException("Partition spec with specId " + deleteFile.specId()
                    + " not found for table " + metadataTable.name());
        }
        // Only render the partition struct when the query actually projects `partition` — legacy parity, and it
        // keeps the (throwing) binary guard off queries that never asked for it.
        if (partitionSpec.isPartitioned() && deleteFile.partition() != null && !outputPartitionFields.isEmpty()) {
            builder.partitionDataJson(IcebergPartitionUtils.getPartitionDataObjectJson(
                    (PartitionData) deleteFile.partition(), partitionSpec, outputPartitionFields,
                    enableMappingVarbinary, zone));
        }
        return builder.build();
    }

    /**
     * PARQUET and PUFFIN both read through BE's parquet path; ORC through the orc one. AVRO position-delete
     * files have no native reader — fail loud rather than mis-route. Message text mirrors legacy exactly.
     */
    private static TFileFormatType getNativePositionDeleteFileFormat(FileFormat fileFormat) {
        if (fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.PUFFIN) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (fileFormat == FileFormat.ORC) {
            return TFileFormatType.FORMAT_ORC;
        }
        throw new UnsupportedOperationException(
                "Unsupported Iceberg position delete file format: " + fileFormat);
    }

    /**
     * Validate an Iceberg deletion-vector blob descriptor before it reaches BE cache lookup / memory allocation.
     * Port of legacy {@code IcebergDeleteFileFilter.validateDeletionVectorMetadata} (upstream #65676): a puffin DV
     * carries {@code content_offset}/{@code content_size_in_bytes} that address a blob inside the puffin file, so a
     * missing, negative, overflowing, or out-of-file-bounds range is malformed metadata we must reject on the FE
     * (fail loud) rather than hand BE an invalid offset/length. Shared by both DV emitters: the normal-scan
     * merge-on-read path ({@link #convertDelete}) and the {@code $position_deletes} split path
     * ({@link #buildPositionDeleteRange}).
     */
    static void validateDeletionVectorMetadata(
            String deleteFilePath, long fileSize, Long contentOffset, Long contentLength) {
        if (contentOffset == null || contentLength == null) {
            throw new IllegalArgumentException(String.format(
                    "Iceberg deletion vector metadata misses content offset or length: %s", deleteFilePath));
        }
        if (fileSize < 0 || contentOffset < 0 || contentLength < 0) {
            throw new IllegalArgumentException(String.format(
                    "Iceberg deletion vector metadata must be non-negative, file: %s, file size: %d, "
                            + "content offset: %d, content length: %d",
                    deleteFilePath, fileSize, contentOffset, contentLength));
        }
        if (contentOffset > Long.MAX_VALUE - contentLength) {
            throw new IllegalArgumentException(String.format(
                    "Iceberg deletion vector metadata range overflows, file: %s, content offset: %d, "
                            + "content length: %d",
                    deleteFilePath, contentOffset, contentLength));
        }
        if (contentOffset + contentLength > fileSize) {
            throw new IllegalArgumentException(String.format(
                    "Iceberg deletion vector metadata range exceeds file size, file: %s, file size: %d, "
                            + "content offset: %d, content length: %d",
                    deleteFilePath, fileSize, contentOffset, contentLength));
        }
    }

    /**
     * The {@code partition} struct's fields on the {@code $position_deletes} metadata table, in output order.
     * These carry the metadata table's REASSIGNED field ids (BaseMetadataTable.transformSpec), which is why
     * {@code getPartitionDataObjectJson} must map values by field id and not by spec position.
     */
    private static List<NestedField> getPositionDeletesOutputPartitionFields(Table metadataTable) {
        NestedField partitionField = metadataTable.schema().findField("partition");
        if (partitionField == null) {
            throw new DorisConnectorException(
                    "Partition field not found in Iceberg position_deletes metadata table schema");
        }
        return partitionField.type().asNestedType().fields();
    }

    /** Whether the query projects the {@code partition} column (legacy read the tuple's slots). */
    private static boolean isPositionDeletesPartitionColumnRequested(List<ConnectorColumnHandle> columns) {
        return requestedLowerNames(columns).stream().anyMatch("partition"::equalsIgnoreCase);
    }

    /**
     * Build the predicate-filtered {@link TableScan}, mirroring legacy {@code createTableScan}: translate the
     * engine-neutral predicate into iceberg {@code Expression}s (self-contained, mirrors legacy
     * {@code IcebergUtils.convertToIcebergExpr}; unpushable conjuncts are dropped → BE residual) and apply
     * each as a separate filter (iceberg ANDs them internally). The fe-core-only {@code metricsReporter}
     * (profile) and {@code planWith(threadPool)} are intentionally dropped — the iceberg SDK default worker
     * pool plans, and the file set is identical (see design deviations). The MVCC / time-travel pin (T07) is
     * applied here ({@code useRef} for a tag/branch, else {@code useSnapshot}), mirroring legacy
     * {@code createTableScan}; {@code getCountFromSnapshot} reads {@code scan.snapshot()} so the count follows.
     */
    private TableScan buildScan(Table table, IcebergTableHandle handle, Optional<ConnectorExpression> filter,
            ConnectorSession session) {
        TableScan scan = table.newScan();
        // MVCC / time-travel pin: a tag/branch pins by REF (so a later commit to the ref is honored, legacy
        // parity), else by snapshot id (legacy createTableScan: useRef when info.getRef()!=null else useSnapshot).
        if (handle.hasSnapshotPin()) {
            if (handle.getRef() != null) {
                scan = scan.useRef(handle.getRef());
            } else {
                scan = scan.useSnapshot(handle.getSnapshotId());
            }
        }
        if (filter.isPresent()) {
            // Predicate conversion uses the table's CURRENT schema, matching legacy createTableScan:589
            // (convertToIcebergExpr(conjunct, icebergTable.schema())) — NOT the pinned schema. A predicate on a
            // column renamed since the pinned snapshot then resolves to no field and drops to BE residual,
            // exactly like legacy; the common no-rename case is identical (the pinned name == the current name),
            // and the unbound expression still binds against the pinned snapshot's schema at plan time.
            List<Expression> predicates =
                    new IcebergPredicateConverter(table.schema(), resolveSessionZone(session)).convert(filter.get());
            for (Expression predicate : predicates) {
                scan = scan.filter(predicate);
            }
        }
        return scan;
    }

    /**
     * The schema AS OF the handle's pinned schema id (for time-travel reads under schema evolution); the latest
     * schema when there is no pinned id or it is absent from {@code table.schemas()} (defensive — legacy
     * {@code IcebergUtils.getSchema} falls back to {@code table.schema()}).
     *
     * <p><b>INVARIANT (do not break):</b> this dict-schema selector MUST stay byte-identical — same
     * {@code getSchemaId()} lookup, same silent fallback to {@code table.schema()} — to the SLOT-schema
     * selector in {@code IcebergConnectorMetadata.getTableSchema(session, handle, snapshot)}. The field-id
     * dict's top-level names must equal the BE StructNode scan-slot names; a divergence (e.g. hardening ONE
     * side to throw-loud on a missing schemaId while the other silently falls back) would make them resolve
     * DIFFERENT schemas → BE's unconditional {@code children.at(name)} std::out_of_range-SIGABRTs the whole BE
     * on a schema-evolved time-travel read. Because {@code schemas()} is append-only and the {@code schemaId}
     * is the atomic pin threaded into both sides, they resolve the same schema by construction TODAY — keep it
     * that way (reverify #65185 L16).</p>
     */
    private static Schema pinnedSchema(Table table, IcebergTableHandle handle) {
        long schemaId = handle.getSchemaId();
        if (schemaId >= 0) {
            Schema pinned = table.schemas().get((int) schemaId);
            if (pinned != null) {
                return pinned;
            }
        }
        return table.schema();
    }

    /**
     * Emit the single collapsed COUNT(*)-pushdown range: the first whole-file {@link FileScanTask} from
     * {@code scan.planFiles()} carrying the full {@code realCount} via {@code table_level_row_count} → BE's
     * count reader serves it without opening the data file. Mirrors paimon's {@code buildCountRange} (one
     * range bearing the summed total). Result-identical to legacy's count short-circuit even though legacy
     * takes a different shape: legacy byte-splits the count file ({@code planFileScanTask} →
     * {@code splitFiles} → {@code TableScanUtil.splitFiles}), keeps the first split task's byte-range for
     * {@code count < 10000}, and {@code assignCountToSplits} distributes the same total — but under count
     * pushdown BE's count reader never reads the file (the range's start/length are irrelevant) and sums
     * {@code table_level_row_count} across ranges, so one whole-file range yields the identical total (and
     * legacy's {@code >10000} parallel multi-split trim is the perf-only divergence we drop). An empty table
     * (no files) yields no range, so BE gets 0 ranges and COUNT returns 0 (legacy returns empty splits too).
     */
    private List<ConnectorScanRange> planCountPushdown(Table table, TableScan scan, long realCount,
            int formatVersion, boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone,
            UnaryOperator<String> uriNormalizer, ConnectorSession session, Optional<ConnectorExpression> filter) {
        try (CloseableIterable<FileScanTask> tasks = countPushdownFileScanTasks(scan, session, table, filter)) {
            for (FileScanTask task : tasks) {
                // targetSplitSize = -1: the count-pushdown collapse emits a single range, so its scheduling
                // weight is irrelevant → PluginDrivenSplit keeps SplitWeight.standard().
                return Collections.singletonList(buildRange(table, task.file(), task, formatVersion,
                        partitioned, orderedPartitionKeys, zone, uriNormalizer, realCount, -1, null));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan iceberg count-pushdown file, error message is:"
                    + e.getMessage(), e);
        }
        return Collections.emptyList();
    }

    /**
     * The COUNT(*)-pushdown placeholder enumeration: only the FIRST surviving file is consumed (BE serves the
     * count from {@code table_level_row_count} and never reads the file). PERF-04 (C18): when the manifest cache is
     * enabled, read through the lazy {@link #cacheBackedFileScanTasks} (stats overload — this runs on the single
     * planning thread) so the manifest reads are cache hits and, being lazy, stop at the first file's manifest
     * instead of the SDK {@code planFiles()}'s {@code ParallelIterable} eagerly submitting every manifest reader.
     * An eager cache failure falls back to the SDK path (mirrors {@link #planFileScanTask}). The first surviving
     * (pruned) file may differ from the SDK path's first file (its {@code ParallelIterable} order is
     * non-deterministic), but the count is identical (from the snapshot summary) and BE ignores the file. Cache
     * disabled -> the SDK path, byte-unchanged.
     */
    private CloseableIterable<FileScanTask> countPushdownFileScanTasks(TableScan scan, ConnectorSession session,
            Table table, Optional<ConnectorExpression> filter) {
        if (isManifestCacheEnabled()) {
            try {
                return cacheBackedFileScanTasks(scan, session, table, filter, session.getQueryId());
            } catch (Exception e) {
                LOG.warn("Iceberg count-pushdown plan with manifest cache failed, falling back to SDK scan: {}",
                        e.getMessage(), e);
                manifestCache.recordFailure(session.getQueryId());
            }
        }
        return scan.planFiles();
    }

    /**
     * Per-file scratch for {@link #buildRange}: the values identical for every byte-slice of one data file
     * ({@code TableScanUtil.splitFiles} cuts a file into k slices whose {@code FileScanTask}s all return the
     * SAME {@code DataFile} instance from {@code file()}, and emits them consecutively). Computed once on a
     * file change and reused across the file's slices, collapsing the per-slice partition-JSON / identity-map /
     * delete-carrier recompute to per-file (PERF-11 / C12); the k ranges then share the same immutable
     * {@code partitionValues} / {@code deleteCarriers} instances (C15a). The eager loop and the streaming
     * source each own ONE instance, never shared across scans. {@code file == null} = fresh / unused.
     */
    private static final class PerFileScratch {
        private DataFile file;
        private Integer partitionSpecId;
        private String partitionDataJson;
        private Map<String, String> partitionValues = Collections.emptyMap();
        private List<IcebergScanRange.DeleteFile> deleteCarriers = Collections.emptyList();
        private String fileFormat;
        private Long firstRowId;
        private Long lastUpdatedSequenceNumber;
        private String rawDataPath;
        private String normalizedPath;
    }

    /**
     * Build the BE-ready {@link IcebergScanRange} for one {@link FileScanTask}, mirroring legacy
     * {@code IcebergScanNode.createIcebergSplit} + {@code setIcebergParams}: the file path/offset/size, the
     * per-file format (native parquet/orc), the table format version, the v3 row-lineage fields, and — for a
     * partitioned table — the partition spec-id, the all-fields {@code partition_data_json}, and the ordered
     * identity {@code partitionValues} that become columns-from-path.
     *
     * <p>The per-file-invariant work is memoized through {@code scratch} (keyed by the shared {@code DataFile}
     * instance) and reused across the file's byte-slices; only {@code start} / {@code length} / the
     * size-proportional {@code selfSplitWeight} are per-slice. A {@code null} scratch (the single
     * count-pushdown range) computes without caching. Byte-identical to the former per-slice recompute.</p>
     */
    private IcebergScanRange buildRange(Table table, DataFile dataFile, FileScanTask task, int formatVersion,
            boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone,
            UnaryOperator<String> uriNormalizer, long pushDownRowCount, long targetSplitSize,
            PerFileScratch scratch) {
        PerFileScratch file = (scratch != null && scratch.file == dataFile)
                ? scratch
                : computePerFileInvariants(table, dataFile, task, formatVersion, partitioned,
                        orderedPartitionKeys, zone, uriNormalizer, scratch);
        // M-2 size-proportional weight numerator = this split's byte length + the byte size of every
        // merge-on-read delete file applying to it, mirroring legacy IcebergSplit.selfSplitWeight (ctor sets
        // = length; setDeleteFileFilters adds Σ delete fileSizeInBytes). The delete-size sum is file-invariant
        // but the task.length() term is per byte-slice, so the whole weight stays per-slice (NEVER memoized).
        // The denominator (targetSplitSize) is passed in; for the single count-pushdown range it is -1 →
        // PluginDrivenSplit keeps SplitWeight.standard() (one range, weight irrelevant).
        long selfSplitWeight = task.length();
        if (task.deletes() != null) {
            for (DeleteFile delete : task.deletes()) {
                selfSplitWeight += delete.fileSizeInBytes();
            }
        }
        return new IcebergScanRange.Builder()
                .path(file.normalizedPath)
                .originalPath(file.rawDataPath)
                .start(task.start())
                .length(task.length())
                .fileSize(dataFile.fileSizeInBytes())
                .fileFormat(file.fileFormat)
                .formatVersion(formatVersion)
                .partitionSpecId(file.partitionSpecId)
                .partitionDataJson(file.partitionDataJson)
                .firstRowId(file.firstRowId)
                .lastUpdatedSequenceNumber(file.lastUpdatedSequenceNumber)
                .partitionValues(file.partitionValues)
                .deleteFiles(file.deleteCarriers)
                .pushDownRowCount(pushDownRowCount)
                .selfSplitWeight(selfSplitWeight)
                .targetSplitSize(targetSplitSize)
                .build();
    }

    /**
     * Compute {@link #buildRange}'s per-file invariants and, when {@code scratch} is non-null, store them into
     * it so the file's remaining byte-slices reuse them. Uses {@code task.deletes()} (identical across a
     * file's slices) for the delete carriers. Mirrors the former inline per-slice computation exactly — same
     * partition-JSON / identity-map ordering, same fail-loud on a non-parquet/orc file, same v3 row-lineage —
     * so the memoized range is byte-identical to the per-slice recompute. The scratch fields are assigned only
     * after the fail-loud check, so a rejected file never leaves a half-populated scratch.
     */
    private PerFileScratch computePerFileInvariants(Table table, DataFile dataFile, FileScanTask task,
            int formatVersion, boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone,
            UnaryOperator<String> uriNormalizer, PerFileScratch scratch) {
        perFileInvariantComputeCount++;
        Integer partitionSpecId = null;
        String partitionDataJson = null;
        Map<String, String> partitionValues = Collections.emptyMap();
        if (partitioned && dataFile.partition() instanceof PartitionData) {
            PartitionData partitionData = (PartitionData) dataFile.partition();
            int specId = dataFile.specId();
            PartitionSpec spec = table.specs().get(specId);
            partitionSpecId = specId;
            partitionDataJson = IcebergPartitionUtils.getPartitionDataJson(partitionData, spec, zone);
            // Order the identity values as the path_partition_keys list (legacy getOrderedPathPartitionKeys),
            // filtered to keys this file carries — so columns-from-path matches legacy ordering exactly.
            Map<String, String> identityMap =
                    IcebergPartitionUtils.getIdentityPartitionInfoMap(partitionData, spec, table, zone);
            Map<String, String> ordered = new LinkedHashMap<>();
            for (String key : orderedPartitionKeys) {
                if (identityMap.containsKey(key)) {
                    ordered.put(key, identityMap.get(key));
                }
            }
            partitionValues = ordered;
        }
        // Fail loud on a non-orc/parquet data file, mirroring legacy IcebergScanNode.getFileFormatType() (which
        // throws DdlException at plan start). Without this guard the per-range format would silently stay the
        // node default FORMAT_JNI and BE would route the file to its system-table JNI reader. (System tables,
        // which legitimately use JNI, are the separate P6.5 path, not this normal-read path.)
        String fileFormat = dataFile.format().name().toLowerCase(Locale.ROOT);
        if (!"parquet".equals(fileFormat) && !"orc".equals(fileFormat)) {
            throw new IllegalStateException(
                    String.format("Unsupported format name: %s for iceberg table.", fileFormat));
        }
        Long firstRowId = null;
        Long lastUpdatedSequenceNumber = null;
        if (formatVersion >= 3) {
            // -1 means a file carried over from a v2->v3 upgrade (no row lineage). The sequence-number guard is
            // asymmetric (legacy): it also requires first_row_id to be present.
            firstRowId = dataFile.firstRowId() != null ? dataFile.firstRowId() : -1L;
            lastUpdatedSequenceNumber =
                    dataFile.fileSequenceNumber() != null && dataFile.firstRowId() != null
                            ? dataFile.fileSequenceNumber() : -1L;
        }
        // The range path BE opens is scheme-normalized (legacy createIcebergSplit:852 normalizes via the
        // 2-arg LocationPath.of(path, storagePropertiesMap)); original_file_path stays raw so BE can match
        // position-delete entries against the raw iceberg path (legacy setOriginalFilePath:304).
        String rawDataPath = dataFile.path().toString();
        PerFileScratch file = scratch != null ? scratch : new PerFileScratch();
        file.file = dataFile;
        file.partitionSpecId = partitionSpecId;
        file.partitionDataJson = partitionDataJson;
        file.partitionValues = partitionValues;
        file.fileFormat = fileFormat;
        file.firstRowId = firstRowId;
        file.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
        file.rawDataPath = rawDataPath;
        file.normalizedPath = uriNormalizer.apply(rawDataPath);
        file.deleteCarriers = buildDeleteFiles(task, uriNormalizer);
        return file;
    }

    /**
     * Translate a scan task's merge-on-read deletes ({@code task.deletes()}) into the typed delete carriers,
     * mirroring legacy {@code IcebergScanNode.getDeleteFileFilters} + {@code IcebergDeleteFileFilter}. Empty
     * for v1 / no-delete files (v1 has no delete files, so {@code task.deletes()} is always empty there).
     */
    private List<IcebergScanRange.DeleteFile> buildDeleteFiles(FileScanTask task,
            UnaryOperator<String> uriNormalizer) {
        List<DeleteFile> deletes = task.deletes();
        if (deletes == null || deletes.isEmpty()) {
            return Collections.emptyList();
        }
        List<IcebergScanRange.DeleteFile> result = new ArrayList<>(deletes.size());
        for (DeleteFile delete : deletes) {
            result.add(convertDelete(delete, uriNormalizer));
        }
        return result;
    }

    /**
     * Convert one iceberg {@link DeleteFile} into a BE-facing carrier, a faithful port of legacy
     * {@code getDeleteFileFilters} + {@code IcebergDeleteFileFilter.create*} + {@code setIcebergParams}:
     * <ul>
     *   <li>{@code POSITION_DELETES} whose format is {@code PUFFIN} → a deletion vector (content 3) carrying
     *       the blob {@code content_offset}/{@code content_size_in_bytes} (plus any position bounds);</li>
     *   <li>other {@code POSITION_DELETES} → a position delete (content 1) with the [lower,upper] bounds;</li>
     *   <li>{@code EQUALITY_DELETES} → an equality delete (content 2) with the delete-file's equality
     *       field-ids (read straight from delete metadata — correct independent of the T06 data dictionary).</li>
     * </ul>
     * The delete path is normalized through the scan-scoped {@code uriNormalizer} (legacy
     * {@code LocationPath.of(path,config).toStorageLocation()}), which bakes in the per-table vended token
     * (empty for non-REST) so a REST object-store deletion path normalizes via the vended map (T09).
     * Package-private for direct unit testing.
     */
    IcebergScanRange.DeleteFile convertDelete(DeleteFile delete, UnaryOperator<String> uriNormalizer) {
        String path = uriNormalizer.apply(delete.path().toString());
        FileContent content = delete.content();
        if (content == FileContent.POSITION_DELETES) {
            Long lowerBound = readPositionBound(delete.lowerBounds());
            Long upperBound = readPositionBound(delete.upperBounds());
            if (delete.format() == FileFormat.PUFFIN) {
                Long contentOffset = delete.contentOffset();
                Long contentLength = delete.contentSizeInBytes();
                validateDeletionVectorMetadata(
                        delete.path().toString(), delete.fileSizeInBytes(), contentOffset, contentLength);
                return IcebergScanRange.DeleteFile.deletionVector(path, lowerBound, upperBound,
                        contentOffset, contentLength);
            }
            return IcebergScanRange.DeleteFile.positionDelete(path, deleteFileFormat(delete.format()),
                    lowerBound, upperBound);
        } else if (content == FileContent.EQUALITY_DELETES) {
            return IcebergScanRange.DeleteFile.equalityDelete(path, deleteFileFormat(delete.format()),
                    delete.equalityFieldIds());
        }
        // Defensive (legacy parity): delete files are only position or equality; DATA content here is a bug.
        throw new IllegalStateException("Unknown delete content: " + content);
    }

    /**
     * Decode the position [lower|upper] bound from a delete file's bounds map, mirroring legacy
     * {@code IcebergDeleteFileFilter.createPositionDelete}: read the {@code DELETE_FILE_POS} field's bytes and
     * decode them. Returns {@code null} when the bound is absent, or is the {@code -1} sentinel (legacy stores
     * {@code orElse(-1L)} and emits the thrift bound only when present), so {@link #convertDelete} sets the
     * thrift bound only when a real one exists.
     */
    private static Long readPositionBound(Map<Integer, ByteBuffer> bounds) {
        if (bounds == null) {
            return null;
        }
        ByteBuffer buf = bounds.get(MetadataColumns.DELETE_FILE_POS.fieldId());
        if (buf == null) {
            return null;
        }
        Long value = Conversions.fromByteBuffer(MetadataColumns.DELETE_FILE_POS.type(), buf);
        return (value == null || value == -1L) ? null : value;
    }

    /**
     * Map an iceberg delete-file format to BE's file-format type, mirroring legacy {@code setDeleteFileFormat}:
     * only parquet/orc are emitted; any other format (notably {@code PUFFIN} deletion vectors) leaves the
     * thrift {@code file_format} unset ({@code null} here).
     */
    private static TFileFormatType deleteFileFormat(FileFormat format) {
        if (format == FileFormat.PARQUET) {
            return TFileFormatType.FORMAT_PARQUET;
        } else if (format == FileFormat.ORC) {
            return TFileFormatType.FORMAT_ORC;
        }
        return null;
    }

    /**
     * Build the scan-scoped URI normalizer once (where the per-table vended token is extracted) and thread it
     * through the per-file range builders, instead of re-deriving the vended storage config per data/delete
     * file. Each application normalizes a raw iceberg storage path (the data file BE opens, or a delete file)
     * to BE's canonical scheme via the engine seam (legacy goes through {@code LocationPath.of(path,
     * storagePropertiesMap).toStorageLocation()}; the connector cannot import fe-core's {@code LocationPath}).
     * BE's scheme-dispatched S3 factory only opens {@code s3://}, so an un-normalized {@code oss://}/{@code
     * cos://}/{@code obs://}/{@code s3a://} path fails the native read (data file) or silently drops the deletes
     * (merge-on-read wrong rows). Mirrors paimon's {@code normalizeUri} (FIX-URI-NORMALIZE), which normalizes
     * both the data-file and deletion-vector paths. The {@code vendedToken} (empty for non-REST / no context)
     * is the per-table vended credential map, baked into the normalizer so a REST object-store path normalizes
     * via the vended map (T09); when empty the seam folds to the catalog's static storage map, byte-equivalent
     * to legacy for non-vended catalogs. A {@code null} context (offline unit tests) yields an identity
     * normalizer that preserves the raw path (paimon parity).
     */
    UnaryOperator<String> newUriNormalizer(Map<String, String> vendedToken) {
        return context != null ? context.newStorageUriNormalizer(vendedToken) : UnaryOperator.identity();
    }

    /**
     * Whether this catalog requests REST vended credentials, gating {@link #extractVendedToken}. Faithfully
     * reproduces legacy {@code IcebergVendedCredentialsProvider.isVendedCredentialsEnabled}, which is TWO-part:
     * the metastore is REST ({@code metastoreProperties instanceof IcebergRestProperties}) AND the flag
     * {@code iceberg.rest.vended-credentials-enabled} is true. The {@code instanceof} is mirrored by the flavor
     * check (the flag is declared only on {@code IcebergRestProperties}, so on a non-REST flavor legacy ignores
     * it and never vends) — without it a non-REST catalog that erroneously carries the flag would extract vended
     * creds that legacy suppresses. Same flag T05 uses to inject the REST delegation header.
     */
    private boolean restVendedCredentialsEnabled() {
        return restVendedCredentialsEnabled(properties);
    }

    /**
     * Package-static form shared with {@link IcebergWritePlanProvider} (the write sink applies the same
     * vended-credentials gate to its hadoop config / output path). Pure function of the catalog properties.
     */
    static boolean restVendedCredentialsEnabled(Map<String, String> properties) {
        return IcebergConnectorProperties.TYPE_REST.equals(IcebergCatalogFactory.resolveFlavor(properties))
                && Boolean.parseBoolean(properties.get(IcebergConnectorProperties.REST_VENDED_CREDENTIALS_ENABLED));
    }

    /**
     * Extracts the raw per-table vended credential token from a REST catalog table's {@link FileIO}, a faithful
     * port of legacy {@code IcebergVendedCredentialsProvider.extractRawVendedCredentials} (iceberg SDK only, no
     * fe-core import): the FileIO's own {@code properties()} plus, when the FileIO
     * {@link SupportsStorageCredentials}, every server-vended {@link StorageCredential}'s {@code config()}. The
     * gate is the catalog flag ({@code vendedEnabled}) checked BEFORE extraction, equivalent to legacy's
     * "metastore is REST and vended enabled" guard; returns empty when disabled, the table/FileIO is null, so
     * the downstream {@code vendStorageCredentials} / {@code normalizeStorageUri} overlays are no-ops for
     * non-REST reads. Iceberg (unlike paimon's {@code RESTTokenFileIO.validToken()}) has no explicit token
     * refresh — the credentials are fresh because the REST catalog reloads the table per query.
     */
    static Map<String, String> extractVendedToken(Table table, boolean vendedEnabled) {
        if (!vendedEnabled || table == null || table.io() == null) {
            return Collections.emptyMap();
        }
        FileIO fileIO = table.io();
        Map<String, String> ioProps = new HashMap<>(fileIO.properties());
        if (fileIO instanceof SupportsStorageCredentials) {
            for (StorageCredential storageCredential : ((SupportsStorageCredentials) fileIO).credentials()) {
                ioProps.putAll(storageCredential.config());
            }
        }
        return ioProps;
    }

    /**
     * Scan-node-level (not per-range) properties consumed by the generic {@code PluginDrivenScanNode}:
     * <ul>
     *   <li>{@code file_format_type=jni} — makes the parent default the per-range format to {@code FORMAT_JNI},
     *       which each native range overrides to parquet/orc in {@code populateRangeParams} (mirrors paimon).</li>
     *   <li>{@code path_partition_keys} — the lowercased, comma-joined identity partition columns, so FE marks
     *       those slots as partition columns and excludes them from the file-decode set; without it BE
     *       double-fills the partition columns (decode-from-file AND append-from-path) and DCHECKs (CI #968880).
     *       Emitted only when the table is partitioned (an empty value would split into a single "" key).</li>
     *   <li>{@code iceberg.schema_evolution} (T06) — the base64 field-id schema dictionary
     *       ({@code current_schema_id = -1} + one {@code history_schema_info} entry), built from the requested
     *       columns so BE field-id-matches file&harr;table columns across rename/reorder (see
     *       {@link IcebergSchemaUtils}). Emitted unconditionally (legacy {@code createScanRangeLocations}
     *       always sets the dict). {@link #populateScanLevelParams} applies it.</li>
     *   <li>{@code location.*} (T09) — the BE-canonical storage credentials: the catalog's static creds
     *       (all flavors) plus, for a REST vended catalog, the per-table vended overlay (legacy precedence).
     *       Without these BE opens the object store with no creds (403). See {@link #extractVendedToken}.</li>
     * </ul>
     * The serialized-table key (JNI system-table path) lands in a later task.
     */
    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = resolveTable(session, iceHandle);
        Map<String, String> props = new LinkedHashMap<>();
        boolean systemTable = iceHandle.isSystemTable();
        // Scan-level file_format_type is NOT merely a per-range default: BE selects FileScannerV2 vs the V1
        // FileScanner from it (FileScanLocalState::_should_use_file_scanner_v2), and that selector runs before
        // any split is fetched -> FORMAT_JNI here pins the ENTIRE scan to V1 whatever IcebergScanRange sets per
        // range. V1 is the only tree carrying TableSchemaChangeHelper, so pinning there also re-exposes every
        // V1-only reader bug. Emit the table's real data format (legacy IcebergScanNode.getFileFormatType
        // parity, same IcebergUtils.getFileFormat resolution, which throws on a non-parquet/orc table); the
        // per-file format still travels per range, since one table may mix parquet and orc data files.
        // PERF-03: the non-system format resolution falls back to an unfiltered whole-table planFiles() when the
        // table sets neither write-format nor write.format.default; memoize that inference per (table, snapshot)
        // across queries via formatCache (pure metadata, no credential gate). Null cache (offline) resolves live.
        props.put("file_format_type",
                systemTable ? "jni"
                        : IcebergWriterHelper.getFileFormat(table,
                                TableIdentifier.of(iceHandle.getDbName(), iceHandle.getTableName()), formatCache)
                        .name().toLowerCase(Locale.ROOT));
        // [D-065] System (metadata) tables ($snapshots/$files/...) read via the JNI serialized-split path
        // (planSystemTableScan): the metadata-table schema travels INSIDE the serialized FileScanTask, so BE
        // needs neither the base-table path_partition_keys (a metadata table is not base-spec partitioned ->
        // emitting them would double-fill/DCHECK) nor the field-id schema-evolution dict. Worse, building the
        // dict for a sys handle uses the BASE schema keyed off the requested META columns ->
        // IcebergSchemaUtils throws ("requested column not found"). So skip BOTH for a sys handle, keeping
        // file_format_type=jni and the location.* credential overlay (BE still needs creds to read the
        // metadata files). Mirrors paimon, whose getScanNodeProperties skips both for a metadata table
        // (empty partitionKeys + null schema-dict table). resolveTable still loads the base table here for
        // the credential overlay below (the metadata table shares the base table's FileIO).
        if (!systemTable) {
            List<String> partitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
            if (!partitionKeys.isEmpty()) {
                props.put("path_partition_keys", String.join(",", partitionKeys));
            }
        }
        // Static storage credentials (T09, all flavors): the catalog's bound fe-filesystem StorageProperties,
        // normalized to BE-canonical scan keys (AWS_* for object stores, hadoop/dfs for HDFS) and shipped under
        // location.*. BLOCKER: BE's native (FILE_S3) reader understands ONLY the canonical keys, so the raw
        // catalog aliases (s3.access_key, oss.access_key, …) must be translated before they leave FE — copying
        // them verbatim gives BE no usable creds (403 on a private bucket). Mirrors paimon getScanNodeProperties
        // + legacy IcebergScanNode.getLocationProperties (backendStorageProperties). Empty for no context
        // (offline tests) or a REST-vended catalog (whose static storage map is empty by design) -> no static
        // overlay, just the vended one below.
        if (context != null) {
            Map<String, String> backendStorageProps = new HashMap<>();
            for (StorageProperties sp : context.getStorageProperties()) {
                sp.toBackendProperties().ifPresent(b -> backendStorageProps.putAll(b.toMap()));
            }
            backendStorageProps.forEach((k, v) -> props.put("location." + k, v));
        }
        // Vended-credential overlay (T09, REST per-table token): the raw token is extracted from the live,
        // snapshot-pinned table's FileIO (gated on the catalog flag iceberg.rest.vended-credentials-enabled,
        // legacy IcebergVendedCredentialsProvider parity), then normalized to BE-facing AWS_* keys by the engine
        // (the connector cannot import fe-core's StorageProperties). Vended overlays static (legacy precedence —
        // a colliding location.* key takes the vended value). Skipped when no context (offline tests) or the
        // table yields no vended token (flag off / non-REST -> empty -> no-op).
        if (context != null) {
            Map<String, String> vendedBeProps =
                    context.vendStorageCredentials(extractVendedToken(table, restVendedCredentialsEnabled()));
            vendedBeProps.forEach((k, v) -> props.put("location." + k, v));
        }
        // Field-id schema dictionary (T06). Under a time-travel pin (T07, Option A): the query slots carry the
        // PINNED schema's names, but the generic node builds the column handles from the LATEST schema (the pin
        // lands after buildColumnHandles), so a column renamed between the pinned snapshot and now would be
        // dropped from `columns` -> the dict would miss that BE scan slot -> BE StructNode DCHECK crash. Build
        // the dict from the FULL pinned schema (a guaranteed superset of the BE slots — iceberg projection is
        // BE-tuple-driven, so `columns` only feeds the dict). See P6-T07 design §6. Without a pin, keep T06's
        // pruned-by-requested-columns dict (CI #969249).
        //
        // Row-lineage (format-version >= 3): _row_id / _last_updated_sequence_number are GENERATED BE scan slots
        // (they reach BE column_names) but are NOT in schema.columns(), so requestedLowerNames — keyed off the
        // iceberg column handles — never carries them. encodeSchemaEvolutionProp(appendRowLineage=true) appends
        // them to the dict root so BE's StructNode children map contains them; else the ParquetReader's
        // unconditional children.at("_row_id") std::out_of_range-SIGABRTs the whole BE.
        if (!systemTable) {
            String dict;
            boolean appendRowLineage = getFormatVersion(table) >= 3;
            // #65502: the catalog's enable.mapping.timestamp_tz flag controls whether a TIMESTAMPTZ column's
            // iceberg initial default keeps its trailing offset (mapping on) or is rendered as UTC wall time
            // (mapping off, DATETIMEV2). Thread it into every dict branch so the default matches BE's read.
            boolean enableTimestampTz = Boolean.parseBoolean(
                    properties.getOrDefault(IcebergConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
            if (iceHandle.hasSnapshotPin()) {
                dict = IcebergSchemaUtils.encodeSchemaEvolutionProp(
                        table, pinnedSchema(table, iceHandle), Collections.emptyList(), appendRowLineage,
                        enableTimestampTz);
            } else if (iceHandle.isTopnLazyMaterialize()) {
                // Top-N lazy materialization (M-4): BE re-fetches the non-projected columns of the surviving
                // rows by the synthesized row-id, so the dict must span the FULL latest schema, not just the
                // pruned slots — otherwise a lazily re-fetched column on a schema-evolved table has no
                // field-id entry and the native read drops/mis-reads it. An empty requested list builds the
                // dict over every top-level column (legacy initSchemaInfoForAllColumn parity).
                dict = IcebergSchemaUtils.encodeSchemaEvolutionProp(
                        table, table.schema(), Collections.emptyList(), appendRowLineage, enableTimestampTz);
            } else {
                dict = IcebergSchemaUtils.encodeSchemaEvolutionProp(
                        table, table.schema(),
                        withEqualityDeleteKeyColumns(table, requestedLowerNames(columns)),
                        appendRowLineage, enableTimestampTz);
            }
            props.put(SCHEMA_EVOLUTION_PROP, dict);
        } else if (isPositionDeletesSysTable(iceHandle)) {
            // [D-065] narrowed: $position_deletes is the ONE system table BE reads with a NATIVE reader, so
            // the "schema rides inside the serialized FileScanTask" rationale above does not hold for it — no
            // FileScanTask is serialized on this path. Both native readers resolve the `row` column through
            // params.history_schema_info: without the dict, scanner v1 hard-errors ("Iceberg position delete
            // system table row schema is missing") and scanner v2 SILENTLY degrades to name matching, which
            // mis-reads a renamed column under schema evolution. Skipping the dict here would be silent wrong
            // data, so emit it.
            //
            // Built from the METADATA table (its own schema keyed by its own requested columns) — feeding the
            // BASE schema keyed off META columns is exactly what makes IcebergSchemaUtils throw "requested
            // column not found", per the comment above. The metadata table delegates properties() to the base
            // table, so the name mapping resolved inside is the base table's, matching legacy.
            //
            // appendRowLineage=false: that flag exists for the DATA-file ParquetReader's unconditional
            // children.at("_row_id") lookup; the position-delete reader opens the DELETE file, whose schema
            // carries no row-lineage columns.
            //
            // Built from the base table ALREADY resolved above, not via resolveSysTable: that helper does a
            // second loadTable and — by its own contract — carries NO auth wrap, because its only other caller
            // (planSystemTableScan) supplies one. This method has no such scope, so calling it here would fail
            // a kerberized catalog at plan time. createMetadataTableInstance is a pure local construction over
            // an already-loaded table, so this is also one fewer remote round-trip.
            Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
                    table, MetadataTableType.POSITION_DELETES);
            props.put(SCHEMA_EVOLUTION_PROP, IcebergSchemaUtils.encodeSchemaEvolutionProp(
                    metadataTable, metadataTable.schema(), requestedLowerNames(columns), false));
        }
        // Pushed-predicate EXPLAIN prop (explain gap): serialize the iceberg Expression form of each pushed
        // conjunct so appendExplainInfo can re-emit the legacy `icebergPredicatePushdown=` block. Same converter
        // as buildScan (current schema + session zone) → byte-identical strings. A system table has no
        // base-spec pushdown path (its converter would key off the wrong schema), so skip it — mirroring the
        // schema-dict gate above. Absent / no convertible conjunct → no prop → no EXPLAIN line (legacy parity:
        // IcebergScanNode `if (!pushdownIcebergPredicates.isEmpty())`).
        if (!systemTable && filter.isPresent()) {
            List<Expression> pushed =
                    new IcebergPredicateConverter(table.schema(), resolveSessionZone(session)).convert(filter.get());
            if (!pushed.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (Expression predicate : pushed) {
                    if (sb.length() > 0) {
                        sb.append('\n');
                    }
                    sb.append(predicate);
                }
                props.put(PUSHDOWN_PREDICATES_PROP, sb.toString());
            }
        }
        // Carry the queryId for the per-scan `manifest cache:` VERBOSE line ONLY when the cache is enabled
        // (omitted otherwise -> appendExplainInfo prints no line, legacy notContains parity). Skip system tables:
        // they have no manifest-level planning path. FE-only, like PUSHDOWN_PREDICATES_PROP.
        if (!systemTable && isManifestCacheEnabled()) {
            props.put(MANIFEST_CACHE_QUERYID_PROP, session.getQueryId());
        }
        return props;
    }

    /**
     * The lowercased names of the requested (pruned) columns — the authoritative Doris scan slots the field-id
     * dictionary keys its {@code -1} entry off (so its top-level names == the BE scan-slot names BY
     * CONSTRUCTION; CI #969249). The names come straight from the {@link IcebergColumnHandle}s
     * {@code IcebergConnectorMetadata.getColumnHandles} produced (already lowercased). An empty list (count-only
     * scan / no column handles) makes the dictionary fall back to all top-level columns.
     */
    private static List<String> requestedLowerNames(List<ConnectorColumnHandle> columns) {
        if (columns == null || columns.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>(columns.size());
        for (ConnectorColumnHandle column : columns) {
            names.add(((IcebergColumnHandle) column).getName());
        }
        return names;
    }

    /**
     * Ensure the schema-evolution dict carries the table's equality-delete KEY columns even when the query
     * does not project them (#65502). Equality-delete keys are hidden scan dependencies: BE resolves a key
     * that is missing from an OLD data file by looking its field id up in this dict to get the column type +
     * iceberg initial default; without the entry BE materializes the key as NULL and mis-applies the delete.
     * The keys are the table's declared identifier fields (what equality-delete writers key on) -> a few
     * columns, DCHECK-safe superset (BE looks up only its own scan slots; the pin/top-N branches already ship
     * the full schema). If the table declares NO identifier yet the scan carries equality deletes (whose
     * equality_ids we cannot cheaply enumerate here), fall back to the full schema. Non-identifier /
     * append-only / position-delete-only tables are unaffected (the pruned dict is returned verbatim).
     */
    private List<String> withEqualityDeleteKeyColumns(Table table, List<String> requested) {
        if (requested.isEmpty()) {
            // An empty requested list already makes buildCurrentSchema fall back to the FULL schema (every
            // top-level column) — a superset that covers every equality-delete key — so there is nothing to
            // force-include. Returning early also preserves that all-columns fallback (a non-empty identifier
            // set would otherwise prune it to identifier-only) and skips the table.schema()/currentSnapshot()
            // probe when it cannot change the result.
            return requested;
        }
        Schema schema = table.schema();
        Set<Integer> identifierFieldIds = schema.identifierFieldIds();
        if (identifierFieldIds.isEmpty()) {
            return hasEqualityDeletes(table) ? Collections.emptyList() : requested;
        }
        Set<String> present = new HashSet<>();
        for (String name : requested) {
            present.add(name.toLowerCase(Locale.ROOT));
        }
        List<String> result = new ArrayList<>(requested);
        for (int fieldId : identifierFieldIds) {
            Types.NestedField field = schema.findField(fieldId);
            if (field == null) {
                continue;
            }
            String lower = field.name().toLowerCase(Locale.ROOT);
            if (present.add(lower)) {
                result.add(lower);
            }
        }
        return result;
    }

    private static boolean hasEqualityDeletes(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return false;
        }
        String equalityDeletes = snapshot.summary().get(TOTAL_EQUALITY_DELETES);
        // Absent (compaction/replace snapshots omit the counter) -> unknown -> assume present (safe superset).
        return equalityDeletes == null || !equalityDeletes.equals("0");
    }

    /**
     * Apply the scan-level field-id schema dictionary (T06) built in {@link #getScanNodeProperties} to the real
     * {@link TFileScanRangeParams} (the same props map is round-tripped by the generic
     * {@code PluginDrivenScanNode}). Delegates to {@link IcebergSchemaUtils#applySchemaEvolution}, which fails
     * loud on a decode error (the prop is produced by us — silently dropping it would re-introduce the silent
     * wrong-rows BLOCKER on schema-evolved native reads).
     */
    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        // #65784: advertise v1 iceberg scan semantics for every iceberg scan this connector plans (data AND
        // system tables), mirroring legacy IcebergScanNode.createScanRangeLocations ->
        // enableCurrentIcebergScanSemantics(). BE reads it via supports_iceberg_scan_semantics_v1 to opt into
        // authoritative name mapping + logical initial-default materialization.
        params.setIcebergScanSemanticsVersion(ICEBERG_SCAN_SEMANTICS_VERSION);
        IcebergSchemaUtils.applySchemaEvolution(params, nodeProperties.get(SCHEMA_EVOLUTION_PROP));
    }

    /**
     * Re-emit the legacy {@code IcebergScanNode} {@code icebergPredicatePushdown=} EXPLAIN block from the
     * {@link #PUSHDOWN_PREDICATES_PROP} prop (the pushed iceberg {@code Expression.toString()}s, newline-joined,
     * serialized by {@link #getScanNodeProperties}). Connector-specific EXPLAIN delegated by the generic
     * {@code PluginDrivenScanNode} (which owns the source-agnostic FileScanNode body). Byte-faithful to legacy
     * {@code IcebergScanNode.getNodeExplainString}: each predicate double-prefix indented under the header; an
     * absent prop (no pushed predicate, or another connector's props map) prints nothing.
     */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        // Per-scan manifest cache stats (VERBOSE only — the generic node calls this at VERBOSE). Emitted iff the
        // FE-only queryId prop is present (i.e. the cache is enabled), draining THIS scan's tally from the shared
        // per-catalog cache. Rendered BEFORE the pushdown block AND its early-return below, so a cache-enabled
        // scan with no pushed predicate still prints the line — legacy IcebergScanNode emitted it independently
        // of the `icebergPredicatePushdown=` block.
        String manifestCacheQueryId = nodeProperties.get(MANIFEST_CACHE_QUERYID_PROP);
        if (manifestCacheQueryId != null && manifestCache != null) {
            long[] stats = manifestCache.takeStats(manifestCacheQueryId);
            output.append(prefix).append("manifest cache: hits=").append(stats[0])
                    .append(", misses=").append(stats[1])
                    .append(", failures=").append(stats[2]).append("\n");
        }
        String encoded = nodeProperties.get(PUSHDOWN_PREDICATES_PROP);
        if (encoded == null || encoded.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (String predicate : encoded.split("\n")) {
            sb.append(prefix).append(prefix).append(predicate).append("\n");
        }
        output.append(String.format("%sicebergPredicatePushdown=\n%s\n", prefix, sb));
    }

    /**
     * Read back the delete-file paths carried by one range's {@code iceberg_params}, for the VERBOSE
     * per-backend EXPLAIN block ({@code deleteFileNum}/{@code deleteSplitNum}). Verbatim port of legacy
     * {@code IcebergScanNode.getDeleteFiles} (and the shape of paimon's {@code getDeleteFiles}): every
     * delete file's path, including equality deletes (the equality-vs-non-equality split legacy keeps in
     * {@code deleteFilesByReferencedDataFile} is only for the write/rewrite path, not this count). Returns
     * empty when the range carries no iceberg params or no delete files (v1 / no-delete table).
     */
    @Override
    public List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) {
        List<String> deleteFiles = new ArrayList<>();
        if (tableFormatParams == null || !tableFormatParams.isSetIcebergParams()) {
            return deleteFiles;
        }
        TIcebergFileDesc icebergParams = tableFormatParams.getIcebergParams();
        if (icebergParams == null || !icebergParams.isSetDeleteFiles()) {
            return deleteFiles;
        }
        List<TIcebergDeleteFileDesc> icebergDeleteFiles = icebergParams.getDeleteFiles();
        if (icebergDeleteFiles == null) {
            return deleteFiles;
        }
        for (TIcebergDeleteFileDesc deleteFile : icebergDeleteFiles) {
            if (deleteFile != null && deleteFile.isSetPath()) {
                deleteFiles.add(deleteFile.getPath());
            }
        }
        return deleteFiles;
    }

    /**
     * Reads the real table format version, mirroring legacy {@code IcebergUtils.getFormatVersion} (and the
     * connector's {@code IcebergConnectorMetadata.getFormatVersion}): from a {@link BaseTable}'s current
     * metadata when available, else the {@code format-version} table property, defaulting to 2.
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

    /**
     * The byte-offset-split {@link FileScanTask}s of one scan plus the scan-level weight denominator (legacy
     * {@code IcebergScanNode.targetSplitSize}). The denominator is threaded into each normal data-file range's
     * {@link IcebergScanRange#getTargetSplitSize()} so {@code FederationBackendPolicy} schedules splits by byte
     * size, not uniformly by count (M-2). Immutable holder (provider may be reused → no mutable field);
     * {@link #close()} closes the underlying iterable so {@code planScanInternal}'s try-with-resources frees it.
     */
    private static final class SplitPlan implements Closeable {
        private final CloseableIterable<FileScanTask> tasks;
        private final long targetSplitSize;

        SplitPlan(CloseableIterable<FileScanTask> tasks, long targetSplitSize) {
            this.tasks = tasks;
            this.targetSplitSize = targetSplitSize;
        }

        @Override
        public void close() throws IOException {
            tasks.close();
        }
    }

    /**
     * Enumerate + byte-offset-split the data files of a built scan via the iceberg SDK
     * {@code TableScanUtil.splitFiles} (the legacy {@code IcebergScanNode.splitFiles} algorithm — NOT fe-core
     * {@code FileSplitter}). A positive {@code file_split_size} session var forces that granularity directly;
     * otherwise the tasks are materialized once and split at the {@link #determineTargetFileSplitSize}
     * heuristic. Batch mode is deferred (paimon parity). Returns the split tasks plus the weight denominator
     * (M-2): the forced {@code file_split_size} (that path slices to it) or the heuristic {@code targetSplitSize}
     * (legacy {@code IcebergScanNode.targetSplitSize}, the same value used to slice — legacy reuses it for both).
     */
    private SplitPlan splitFiles(TableScan scan, ConnectorSession session) {
        long fileSplitSize = sessionLong(session, FILE_SPLIT_SIZE, 0L);
        if (fileSplitSize > 0) {
            // The split granularity IS fileSplitSize, so it is the correct weight denominator. (Legacy left its
            // targetSplitSize field at 0 here → divide-by-zero → weight clamped to 1.0; the generic
            // PluginDrivenSplit guards target>0, so reproducing that is impossible without un-guarding division
            // for all connectors. Using fileSplitSize gives proper proportional weighting — strictly better.)
            return new SplitPlan(TableScanUtil.splitFiles(scan.planFiles(), fileSplitSize), fileSplitSize);
        }
        List<FileScanTask> fileScanTasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> planned = scan.planFiles()) {
            for (FileScanTask task : planned) {
                fileScanTasks.add(task);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to materialize iceberg file scan tasks, error message is:"
                    + e.getMessage(), e);
        }
        long targetSplitSize = determineTargetFileSplitSize(fileScanTasks, session);
        return new SplitPlan(
                TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks), targetSplitSize),
                targetSplitSize);
    }

    /**
     * Gate between the two file-enumeration paths (port of legacy {@code IcebergScanNode.planFileScanTask}). By
     * default ({@code meta.cache.iceberg.manifest.enable} off) and whenever no manifest cache is wired, this is
     * the iceberg SDK {@code splitFiles} path — byte-identical to T02. When the manifest cache is enabled it
     * re-plans at the manifest level so the per-manifest data/delete reads hit {@link IcebergManifestCache};
     * any failure falls back to {@code splitFiles} (legacy parity), so a cache bug can never break a query.
     */
    private SplitPlan planFileScanTask(TableScan scan, ConnectorSession session, Table table,
            Optional<ConnectorExpression> filter) {
        if (!isManifestCacheEnabled()) {
            return splitFiles(scan, session);
        }
        try {
            return planFileScanTaskWithManifestCache(scan, session, table, filter);
        } catch (Exception e) {
            LOG.warn("Iceberg plan with manifest cache failed, falling back to SDK scan: {}", e.getMessage(), e);
            // Mirror the legacy manifestCacheFailures bump so VERBOSE EXPLAIN can report the fallback.
            manifestCache.recordFailure(session.getQueryId());
            return splitFiles(scan, session);
        }
    }

    /**
     * Synchronous (below-batch-threshold) manifest-cache planning: materialize the shared lazy
     * {@link #cacheBackedFileScanTasks} enumeration into a list, because {@link #determineTargetFileSplitSize}
     * (the per-table heuristic that gives small tables good BE parallelism) needs the whole task list. Byte
     * identical to the pre-PERF-04 body; the streaming ({@link #streamSplits}) and COUNT(*)
     * ({@link #planCountPushdown}) paths consume the same enumeration LAZILY instead (bounded FE heap). Uses the
     * stats-tallying cache overload — this path runs on the single planning thread. The predicate / metrics /
     * schema use the table's CURRENT schema (legacy parity).
     */
    private SplitPlan planFileScanTaskWithManifestCache(TableScan scan,
            ConnectorSession session, Table table, Optional<ConnectorExpression> filter) throws IOException {
        // Null-safe queryId (offline tests pass a null session): a null id selects the no-stats cache overload,
        // matching the pre-PERF-04 body, which read scan.snapshot() and returned early for an empty table BEFORE
        // ever calling session.getQueryId().
        String statsQueryId = session != null ? session.getQueryId() : null;
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> whole =
                cacheBackedFileScanTasks(scan, session, table, filter, statsQueryId)) {
            for (FileScanTask task : whole) {
                tasks.add(task);
            }
        }
        long targetSplitSize = determineTargetFileSplitSize(tasks, session);
        return new SplitPlan(
                TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks), targetSplitSize), targetSplitSize);
    }

    /**
     * PERF-04: the lazy, cache-backed {@link FileScanTask} enumeration shared by the synchronous
     * ({@link #planFileScanTaskWithManifestCache}), streaming ({@link #streamSplits}), and COUNT(*)
     * ({@link #planCountPushdown}) paths whenever {@link #isManifestCacheEnabled()}. Ported faithfully from legacy
     * {@code IcebergScanNode.planFileScanTaskWithManifestCache}: partition-prune manifests with a
     * {@link ManifestEvaluator}, read each surviving manifest's data/delete files THROUGH THE CACHE, then per data
     * file apply the {@link InclusiveMetricsEvaluator} (file-stats prune) + {@link ResidualEvaluator} (partition
     * residual) and attach its deletes via a {@link DeleteFileIndex}. Produces WHOLE-FILE
     * {@link BaseFileScanTask}s (callers byte-offset-split via {@link TableScanUtil#splitFiles}).
     *
     * <p><b>Phase 1 is eager</b> (delete-manifest reads + {@link DeleteFileIndex} build): a data file's deletes
     * need the full index before any data task can be produced, and running it here (on the caller's thread) lets
     * the streaming/count callers catch a cache failure and fall back to the SDK path. Bounded like the SDK
     * {@code planFiles()} (which also reads all delete manifests up front); delete manifests are far fewer than
     * data manifests. <b>Phase 2 is lazy</b>: the returned iterable's iterator flat-maps the matching data
     * manifests, yielding one surviving task at a time WITHOUT materializing the list, so a million-file streaming
     * scan keeps FE heap bounded (peak = the delete index + one manifest's files + the split queue).
     *
     * <p>{@code statsQueryId} is nullable: a non-null id tallies cache hits/misses under that query (the
     * single-threaded synchronous + COUNT paths); {@code null} selects the no-stats overload for the STREAMING
     * path, whose Phase 2 runs on the engine pump thread while Phase 1 ran on the calling thread — tallying the
     * per-query {@code ScanStats} counters from two threads would race (the cache documents them as
     * single-thread-per-query), and streaming reports no manifest-cache stats today anyway.
     */
    private CloseableIterable<FileScanTask> cacheBackedFileScanTasks(TableScan scan,
            ConnectorSession session, Table table, Optional<ConnectorExpression> filter, String statsQueryId) {
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return CloseableIterable.withNoopClose(Collections.emptyList());
        }
        Expression filterExpr = combineFilter(filter, table, session);
        Map<Integer, PartitionSpec> specsById = table.specs();
        boolean caseSensitive = true;

        Map<Integer, ResidualEvaluator> residualEvaluators = new HashMap<>();
        specsById.forEach((id, spec) -> residualEvaluators.put(id,
                ResidualEvaluator.of(spec, filterExpr, caseSensitive)));
        InclusiveMetricsEvaluator metricsEvaluator =
                new InclusiveMetricsEvaluator(table.schema(), filterExpr, caseSensitive);
        String schemaJson = SchemaParser.toJson(table.schema());

        // Phase 1 (eager): partition-prune + cache-load delete manifests into the delete-file index.
        List<DeleteFile> deleteFiles = new ArrayList<>();
        for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
            if (manifest.content() != ManifestContent.DELETES) {
                continue;
            }
            PartitionSpec spec = specsById.get(manifest.partitionSpecId());
            if (spec == null) {
                continue;
            }
            if (!ManifestEvaluator.forPartitionFilter(filterExpr, spec, caseSensitive).eval(manifest)) {
                continue;
            }
            deleteFiles.addAll(manifestCacheGet(manifest, table, statsQueryId).getDeleteFiles());
        }
        DeleteFileIndex deleteIndex = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(specsById)
                .caseSensitive(caseSensitive)
                .build();

        // Phase 2 (lazy): flat-map the matching data manifests, read through the cache on demand.
        CloseableIterable<ManifestFile> dataManifests =
                getMatchingManifest(snapshot.dataManifests(table.io()), specsById, filterExpr);
        return new CloseableIterable<FileScanTask>() {
            @Override
            public CloseableIterator<FileScanTask> iterator() {
                return new ManifestCacheFileScanTaskIterator(dataManifests.iterator(), table, specsById,
                        residualEvaluators, metricsEvaluator, deleteIndex, schemaJson, statsQueryId);
            }

            @Override
            public void close() throws IOException {
                dataManifests.close();
            }
        };
    }

    /** Dispatch a manifest read to the stats-tallying or no-stats cache overload (see cacheBackedFileScanTasks). */
    private ManifestCacheValue manifestCacheGet(ManifestFile manifest, Table table, String statsQueryId) {
        return statsQueryId != null
                ? manifestCache.getManifestCacheValue(manifest, table, statsQueryId)
                : manifestCache.getManifestCacheValue(manifest, table);
    }

    /**
     * Lazy flat-map iterator behind {@link #cacheBackedFileScanTasks}: walks the matching data manifests, reads
     * each through the manifest cache on demand, and yields the surviving (metrics + residual pruned)
     * {@link BaseFileScanTask}s one file at a time so the consumer never holds the whole table's task list. Not
     * thread-safe: single-pass, driven by ONE consumer (the sync materialize loop, the streaming pump, or the
     * COUNT take-first). {@code schemaJson}/{@code currentSpecJson} are hoisted loop invariants (constant per
     * table / per manifest spec), byte-identical to the pre-PERF-04 per-file computation.
     */
    private final class ManifestCacheFileScanTaskIterator implements CloseableIterator<FileScanTask> {
        private final CloseableIterator<ManifestFile> manifestIt;
        private final Table table;
        private final Map<Integer, PartitionSpec> specsById;
        private final Map<Integer, ResidualEvaluator> residualEvaluators;
        private final InclusiveMetricsEvaluator metricsEvaluator;
        private final DeleteFileIndex deleteIndex;
        private final String schemaJson;
        private final String statsQueryId;

        private Iterator<DataFile> dataFileIt;
        private ResidualEvaluator currentResidual;
        private String currentSpecJson;
        private FileScanTask next;

        ManifestCacheFileScanTaskIterator(CloseableIterator<ManifestFile> manifestIt, Table table,
                Map<Integer, PartitionSpec> specsById, Map<Integer, ResidualEvaluator> residualEvaluators,
                InclusiveMetricsEvaluator metricsEvaluator, DeleteFileIndex deleteIndex, String schemaJson,
                String statsQueryId) {
            this.manifestIt = manifestIt;
            this.table = table;
            this.specsById = specsById;
            this.residualEvaluators = residualEvaluators;
            this.metricsEvaluator = metricsEvaluator;
            this.deleteIndex = deleteIndex;
            this.schemaJson = schemaJson;
            this.statsQueryId = statsQueryId;
        }

        @Override
        public boolean hasNext() {
            advance();
            return next != null;
        }

        @Override
        public FileScanTask next() {
            advance();
            if (next == null) {
                throw new NoSuchElementException();
            }
            FileScanTask result = next;
            next = null;
            return result;
        }

        // Fill `next` with the next surviving task, draining the current data manifest's files and advancing to
        // further data manifests as needed. Per-file logic mirrors the pre-PERF-04 materialized Phase 2 exactly.
        private void advance() {
            while (next == null) {
                if (dataFileIt != null && dataFileIt.hasNext()) {
                    DataFile dataFile = dataFileIt.next();
                    if (!metricsEvaluator.eval(dataFile)) {
                        continue;
                    }
                    if (currentResidual.residualFor(dataFile.partition()).equals(Expressions.alwaysFalse())) {
                        continue;
                    }
                    DeleteFile[] deletes = deleteIndex.forDataFile(dataFile.dataSequenceNumber(), dataFile);
                    next = new BaseFileScanTask(dataFile, deletes, schemaJson, currentSpecJson, currentResidual);
                    return;
                }
                if (!manifestIt.hasNext()) {
                    return;
                }
                ManifestFile manifest = manifestIt.next();
                if (manifest.content() != ManifestContent.DATA) {
                    dataFileIt = null;
                    continue;
                }
                PartitionSpec spec = specsById.get(manifest.partitionSpecId());
                ResidualEvaluator residual = residualEvaluators.get(manifest.partitionSpecId());
                if (spec == null || residual == null) {
                    dataFileIt = null;
                    continue;
                }
                currentResidual = residual;
                currentSpecJson = PartitionSpecParser.toJson(spec);
                dataFileIt = manifestCacheGet(manifest, table, statsQueryId).getDataFiles().iterator();
            }
        }

        @Override
        public void close() throws IOException {
            manifestIt.close();
        }
    }

    /**
     * Combine the pushed predicate into one iceberg {@link Expression} for manifest-level pruning, mirroring
     * legacy {@code conjuncts.stream().map(convertToIcebergExpr).filter(nonNull).reduce(alwaysTrue, and)}. Reuses
     * the T02 {@link IcebergPredicateConverter} on the table's CURRENT schema; an absent filter is
     * {@code alwaysTrue()} (scan everything).
     */
    private Expression combineFilter(Optional<ConnectorExpression> filter, Table table, ConnectorSession session) {
        if (!filter.isPresent()) {
            return Expressions.alwaysTrue();
        }
        List<Expression> predicates =
                new IcebergPredicateConverter(table.schema(), resolveSessionZone(session)).convert(filter.get());
        Expression combined = Expressions.alwaysTrue();
        for (Expression predicate : predicates) {
            combined = Expressions.and(combined, predicate);
        }
        return combined;
    }

    /**
     * Port of legacy {@code IcebergUtils.getMatchingManifest}: keep only the data manifests whose partition
     * summaries can match {@code dataFilter} (a {@link ManifestEvaluator} over the spec-projected filter) and
     * that still hold added/existing files. Uses a per-call {@link HashMap} evaluator memo (single-threaded
     * iteration) in place of legacy's Caffeine {@code LoadingCache} — semantically identical.
     */
    private static CloseableIterable<ManifestFile> getMatchingManifest(List<ManifestFile> dataManifests,
            Map<Integer, PartitionSpec> specsById, Expression dataFilter) {
        Map<Integer, ManifestEvaluator> evalCache = new HashMap<>();
        CloseableIterable<ManifestFile> matching = CloseableIterable.filter(
                CloseableIterable.withNoopClose(dataManifests),
                manifest -> evalCache.computeIfAbsent(manifest.partitionSpecId(), specId -> {
                    PartitionSpec spec = specsById.get(specId);
                    return ManifestEvaluator.forPartitionFilter(
                            Expressions.and(Expressions.alwaysTrue(),
                                    Projections.inclusive(spec, true).project(dataFilter)),
                            spec, true);
                }).eval(manifest));
        return CloseableIterable.filter(matching,
                manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles());
    }

    /**
     * Port of legacy {@code IcebergUtils.isManifestCacheEnabled}: the manifest-level path is used iff the
     * manifest cache is wired AND the spec is enabled ({@code enable && ttl-second != 0 && capacity != 0}).
     * The {@code .ttl-second}/{@code .capacity} properties feed ONLY this formula (legacy quirk); the cache
     * itself is fixed no-TTL / capacity 100000. Parsing is best-effort (blank/unparseable falls back to the
     * default) via the shared {@link CacheSpec}, matching the legacy fe-core behavior.
     */
    private boolean isManifestCacheEnabled() {
        if (manifestCache == null) {
            return false;
        }
        CacheSpec spec = CacheSpec.fromProperties(properties,
                MANIFEST_CACHE_ENABLE, DEFAULT_MANIFEST_CACHE_ENABLE,
                MANIFEST_CACHE_TTL_SECOND, DEFAULT_MANIFEST_CACHE_TTL_SECOND,
                MANIFEST_CACHE_CAPACITY, DEFAULT_MANIFEST_CACHE_CAPACITY);
        return CacheSpec.isCacheEnabled(spec.isEnable(), spec.getTtlSecond(), spec.getCapacity());
    }

    /**
     * Port of legacy {@code IcebergScanNode.determineTargetFileSplitSize} + {@code applyMaxFileSplitNumLimit}
     * (non-batch path), reading the split-size knobs from the session-property channel. Start at
     * {@code max_initial_file_split_size}, escalate to {@code max_file_split_size} once total content exceeds
     * {@code max_file_split_size * max_initial_file_split_num}, then raise the size so the split count stays
     * under {@code max_file_split_num}.
     *
     * <p>Accumulates {@code ScanTaskUtil.contentSizeInBytes(task.file())}, matching legacy. For a data file
     * that is just {@code fileSizeInBytes()} — but for a PUFFIN deletion vector the two differ sharply
     * ({@code fileSizeInBytes()} is the whole puffin file, which packs many DV blobs, so summing it would
     * over-count ~N-fold and prematurely escalate the target size). Widened to {@code ContentScanTask} so the
     * {@code $position_deletes} path can share it, exactly as legacy widened the same method.
     */
    private long determineTargetFileSplitSize(List<? extends ContentScanTask<?>> tasks,
            ConnectorSession session) {
        long maxInitialSplitSize = sessionLong(session, MAX_INITIAL_FILE_SPLIT_SIZE,
                DEFAULT_MAX_INITIAL_FILE_SPLIT_SIZE);
        long maxSplitSize = sessionLong(session, MAX_FILE_SPLIT_SIZE, DEFAULT_MAX_FILE_SPLIT_SIZE);
        long maxInitialSplitNum = sessionLong(session, MAX_INITIAL_FILE_SPLIT_NUM,
                DEFAULT_MAX_INITIAL_FILE_SPLIT_NUM);
        long maxFileSplitNum = sessionLong(session, MAX_FILE_SPLIT_NUM, DEFAULT_MAX_FILE_SPLIT_NUM);
        long total = 0;
        boolean exceedInitialThreshold = false;
        for (ContentScanTask<?> task : tasks) {
            total += ScanTaskUtil.contentSizeInBytes(task.file());
            if (!exceedInitialThreshold && total >= maxSplitSize * maxInitialSplitNum) {
                exceedInitialThreshold = true;
            }
        }
        long result = exceedInitialThreshold ? maxSplitSize : maxInitialSplitSize;
        if (maxFileSplitNum > 0 && total > 0) {
            long minSplitSizeForMaxNum = (total + maxFileSplitNum - 1) / maxFileSplitNum;
            result = Math.max(result, minSplitSizeForMaxNum);
        }
        return result;
    }

    private static long sessionLong(ConnectorSession session, String key, long defaultValue) {
        if (session == null) {
            return defaultValue;
        }
        String raw = session.getSessionProperties().get(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static boolean sessionBool(ConnectorSession session, String key, boolean defaultValue) {
        if (session == null) {
            return defaultValue;
        }
        String raw = session.getSessionProperties().get(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(raw.trim());
    }

    /**
     * Compute the COUNT(*)-pushdown row count from the scan's snapshot summary, a faithful port of legacy
     * {@code IcebergScanNode.getCountFromSnapshot}. No snapshot (empty table) &rarr; {@code 0}; otherwise
     * delegates to {@link #getCountFromSummary}. Reads the scan's snapshot ({@code scan.snapshot()}) so the
     * count tracks the scan automatically (the current snapshot today; the pinned snapshot once MVCC
     * time-travel lands) — equivalent to legacy's {@code currentSnapshot()} for every non-time-travel query.
     */
    private static long getCountFromSnapshot(TableScan scan, ConnectorSession session) {
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return 0;
        }
        return getCountFromSummary(snapshot.summary(), ignoreIcebergDanglingDelete(session));
    }

    /**
     * Null-safe port of fe-core {@code IcebergUtils.getCountFromSummary} (upstream 32a2651f66b, #64648).
     * Returns {@code -1} — this module's "count not pushable / unknown" sentinel; the {@code planScan} gate
     * and count-collapse callers both test {@code >= 0} — in two cases:
     * <ul>
     *   <li>any required {@code total-*} counter is ABSENT: compaction / replace / overwrite snapshots may
     *       omit {@code total-records} / {@code total-position-deletes} / {@code total-equality-deletes}, and
     *       the pre-fix code NPE-d on {@code summary.get(...).equals(...)} / {@code Long.parseLong(null)};</li>
     *   <li>any equality delete ({@code total-equality-deletes != "0"}) — not pushable, since equality
     *       deletes re-project at read time and the summary cannot net them out.</li>
     * </ul>
     * Otherwise: no position deletes &rarr; {@code total-records}; position deletes present and
     * {@code ignoreDanglingDelete} &rarr; {@code total-records - total-position-deletes}; else {@code -1}.
     */
    static long getCountFromSummary(Map<String, String> summary, boolean ignoreDanglingDelete) {
        String equalityDeletes = summary.get(TOTAL_EQUALITY_DELETES);
        String positionDeletes = summary.get(TOTAL_POSITION_DELETES);
        String totalRecords = summary.get(TOTAL_RECORDS);
        if (equalityDeletes == null || positionDeletes == null || totalRecords == null) {
            // a summary that omits any total-* counter can't be netted safely -> fall back to a real scan
            return -1;
        }
        if (!equalityDeletes.equals("0")) {
            // has equality delete files, can not push down count
            return -1;
        }
        long deleteCount = Long.parseLong(positionDeletes);
        if (deleteCount == 0) {
            // no delete files, can push down count directly
            return Long.parseLong(totalRecords);
        }
        if (ignoreDanglingDelete) {
            // has position delete files; if we ignore dangling deletes, the netted count can be pushed down
            return Long.parseLong(totalRecords) - deleteCount;
        }
        // otherwise, can not push down count
        return -1;
    }

    private static boolean ignoreIcebergDanglingDelete(ConnectorSession session) {
        if (session == null) {
            return false;
        }
        String raw = session.getSessionProperties().get(IGNORE_ICEBERG_DANGLING_DELETE);
        return raw != null && Boolean.parseBoolean(raw.trim());
    }

    // The session time zone drives zone-adjusted (timestamptz) literal pushdown. Delegates to the shared
    // IcebergTimeUtils (Doris alias map, mirrors fe-core TimeUtils.getTimeZone()) so aliases like CST/PRC/EST
    // match legacy instead of throwing; null/blank/genuinely-invalid -> UTC. Package-private for unit testing.
    static ZoneId resolveSessionZone(ConnectorSession session) {
        return IcebergTimeUtils.resolveSessionZone(session);
    }

    /**
     * Loads the live Iceberg {@link Table} through the {@link IcebergCatalogOps} seam, wrapped in the
     * FE-injected authentication context when present — so the Kerberos UGI applies (mirrors
     * {@code IcebergConnectorMetadata} and paimon's {@code PaimonScanPlanProvider.resolveTable}). A
     * {@code null} context (offline unit tests / simple-auth) resolves directly.
     */
    private Table resolveTable(ConnectorSession session, IcebergTableHandle handle) {
        // Per-statement scope (PERF-07): the statement's read metadata, scan planning and write all resolve the
        // SAME one loaded RAW table. The scope holds the RAW table; wrapTableForScan (the Kerberos doAs FileIO) is
        // re-applied per call below so no per-request authenticator is ever frozen into the shared object.
        // Resolve the per-request ops before the auth scope so a session=user fail-closed surfaces verbatim (it
        // re-validates the credential even on a scope hit).
        IcebergCatalogOps ops = catalogOpsResolver.apply(session);
        Table raw = IcebergStatementScope.sharedTable(session, handle.getDbName(), handle.getTableName(), () -> {
            if (context == null) {
                return loadRawTable(ops, handle);
            }
            try {
                return context.executeAuthenticated(() -> loadRawTable(ops, handle));
            } catch (Exception e) {
                throw new RuntimeException("Failed to load table for scan, error message is:" + e.getMessage(), e);
            }
        });
        return wrapTableForScan(raw);
    }

    /**
     * Loads the RAW iceberg table for {@code handle} through the cross-query {@link IcebergTableCache} when
     * enabled (the connector disables it for credential-dependent catalogs), else a direct remote
     * {@code loadTable}. No wrap and no auth scope here — {@link #resolveTable} owns both.
     */
    private Table loadRawTable(IcebergCatalogOps ops, IcebergTableHandle handle) {
        if (tableCache != null) {
            return tableCache.getOrLoad(TableIdentifier.of(handle.getDbName(), handle.getTableName()),
                    () -> ops.loadTable(handle.getDbName(), handle.getTableName()));
        }
        return ops.loadTable(handle.getDbName(), handle.getTableName());
    }

    /**
     * Routes a resolved data table's {@code io()} through the plugin-side Kerberos {@code doAs}
     * ({@link IcebergAuthenticatedFileIO} via {@link IcebergAuthenticatedTableOperations}) — the scan-side
     * mirror of the write path ({@code IcebergConnectorTransaction.openTransaction}). Scan planning reads the
     * manifest list and manifests through {@code table.io()} ({@code SnapshotScan.planFiles},
     * {@code streamingSplitEstimate}'s {@code dataManifests}, the streaming source's lazy iteration) — on the
     * CALLING thread for small tables and fanned onto iceberg's shared worker pool ({@code ParallelIterable})
     * for multi-manifest tables, which never inherits a caller-thread {@code doAs}. Wrapping at the FileIO
     * seam is thread-agnostic: the factory-time {@code doAs} captures the secured FileSystem, so later
     * {@code newStream()} on ANY thread stays authenticated (see {@link IcebergAuthenticatedFileIO}). Legacy
     * parity: {@code IcebergScanNode} wrapped {@code doGetSplits} AND its streaming callbacks in
     * {@code preExecutionAuthenticator.execute}; single-UGI fe-core made thread-level cover enough there,
     * while the plugin's child-first UGI copy does not (CI: SELECT after INSERT on
     * test_iceberg_hadoop_catalog_kerberos failed SASL reading snap-*.avro at plan time).
     *
     * <p>Non-Kerberos catalogs ({@code getPluginAuthenticator() == null}) and offline tests (plain context /
     * non-{@link BaseTable} fakes) pass through unchanged. Kerberos and REST vended credentials are disjoint
     * (the authenticator is gated on hadoop.security.authentication=kerberos), so
     * {@code extractVendedToken}'s {@code instanceof SupportsStorageCredentials} probe never sees the wrapper.
     * The system-table path deliberately does NOT use this wrap: its {@code FileScanTask}s are Java-serialized
     * to the BE JNI reader and the wrapper (authenticator-bearing) is not serializable — it is covered by the
     * thread-level wrap in {@link #planSystemTableScan} instead. Package-private for unit testing.
     */
    Table wrapTableForScan(Table table) {
        if (!(context instanceof TcclPinningConnectorContext) || !(table instanceof BaseTable)) {
            return table;
        }
        HadoopAuthenticator auth = ((TcclPinningConnectorContext) context).getPluginAuthenticator();
        if (auth == null) {
            return table;
        }
        TableOperations rawOps = ((BaseTable) table).operations();
        return new BaseTable(new IcebergAuthenticatedTableOperations(
                rawOps, new IcebergAuthenticatedFileIO(rawOps.io(), auth)), table.name());
    }

    /**
     * Resolve the metadata table for a system-table handle, mirroring
     * {@code IcebergConnectorMetadata.loadSysTable} (and legacy {@code IcebergSysExternalTable.getSysIcebergTable}):
     * load the BASE table and build the metadata-table instance ({@code MetadataTableUtils}).
     * {@code getSysTableName()} is the already-validated lowercase name, so {@code MetadataTableType.from}
     * never returns null. The auth scope is owned by the SOLE caller {@link #planSystemTableScan}, whose
     * thread-level {@code executeAuthenticated} spans the whole sys planning (this load + {@code planFiles} +
     * task serialization) in ONE scope — no nested wrap here.
     */
    private Table resolveSysTable(ConnectorSession session, IcebergTableHandle handle) {
        return MetadataTableUtils.createMetadataTableInstance(
                catalogOpsResolver.apply(session).loadTable(handle.getDbName(), handle.getTableName()),
                MetadataTableType.from(handle.getSysTableName()));
    }
}
