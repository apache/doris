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
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndex;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

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

    // COUNT(*) pushdown (T05). The snapshot-summary keys are the stable iceberg spec strings — byte-identical
    // to legacy IcebergUtils.TOTAL_* (themselves local constants, not org.apache.iceberg.SnapshotSummary.*).
    private static final String TOTAL_RECORDS = "total-records";
    private static final String TOTAL_POSITION_DELETES = "total-position-deletes";
    private static final String TOTAL_EQUALITY_DELETES = "total-equality-deletes";
    // Session var: when a table has only (dangling) position deletes, ignore them and still push count down.
    private static final String IGNORE_ICEBERG_DANGLING_DELETE = "ignore_iceberg_dangling_delete";

    // FIX-SCHEMA-EVOLUTION (T06): scan-level prop carrying the base64 TBinaryProtocol-serialized schema
    // dictionary (current_schema_id + the single history_schema_info entry). getScanNodeProperties builds it
    // from the live table + requested columns; populateScanLevelParams applies it to the real params.
    // Transport via the props map because getScanPlanProvider() returns a fresh provider per call (no shared
    // instance state between the two SPI methods). Mirrors paimon's paimon.schema_evolution.
    private static final String SCHEMA_EVOLUTION_PROP = "iceberg.schema_evolution";

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
    private final IcebergCatalogOps catalogOps;
    // Engine seam: executeAuthenticated (Kerberos UGI), storage properties, vended credentials. Nullable —
    // null in offline unit tests via the 2-arg ctor, in which case resolveTable resolves directly.
    private final ConnectorContext context;
    // T08: per-catalog manifest cache, owned by the long-lived IcebergConnector and injected via getScanPlanProvider.
    // Nullable — null via the 2-/3-arg ctors (offline tests, default-disabled gate); when null the gate is
    // forced off and planScan uses the SDK splitFiles path.
    private final IcebergManifestCache manifestCache;

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps) {
        this(properties, catalogOps, null, null);
    }

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this(properties, catalogOps, context, null);
    }

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context, IcebergManifestCache manifestCache) {
        this.properties = properties;
        this.catalogOps = catalogOps;
        this.context = context;
        this.manifestCache = manifestCache;
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

    private List<ConnectorScanRange> planScanInternal(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            boolean countPushdown) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = resolveTable(iceHandle);
        TableScan scan = buildScan(table, iceHandle, filter, session);

        int formatVersion = getFormatVersion(table);
        List<String> orderedPartitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
        ZoneId zone = resolveSessionZone(session);
        boolean partitioned = table.spec().isPartitioned();

        // COUNT(*) pushdown (T05): when the count is servable from the snapshot summary, collapse the scan to
        // a single whole-file range carrying the full count (mirrors paimon's collapse + legacy's <=10000
        // case; the legacy >10000 parallel multi-split trim is a perf-only divergence, dropped). A -1 (equality
        // deletes, or dangling position deletes without the ignore flag) falls through to the normal scan so
        // BE reads and counts.
        if (countPushdown) {
            long realCount = getCountFromSnapshot(scan, session);
            if (realCount >= 0) {
                return planCountPushdown(table, scan, realCount, formatVersion, partitioned,
                        orderedPartitionKeys, zone);
            }
        }

        // Enumerate FileScanTasks via the iceberg SDK (split byte-offsets come from TableScanUtil.splitFiles)
        // and emit one BE-ready IcebergScanRange per task, populating the typed iceberg carriers — incl. the
        // merge-on-read delete files (T04) — mirroring legacy IcebergScanNode.createIcebergSplit. The field-id
        // history dict (T06, scan-level), MVCC pin, and vended credentials (T09) land later.
        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = planFileScanTask(scan, session, table, filter)) {
            for (FileScanTask task : tasks) {
                DataFile dataFile = task.file();
                ranges.add(buildRange(table, dataFile, task, formatVersion, partitioned, orderedPartitionKeys,
                        zone, -1));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to enumerate iceberg file scan tasks, error message is:"
                    + e.getMessage(), e);
        }
        LOG.debug("Iceberg planScan produced {} ranges for table {}", ranges.size(), table.name());
        return ranges;
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
            int formatVersion, boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone) {
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            for (FileScanTask task : tasks) {
                return Collections.singletonList(buildRange(table, task.file(), task, formatVersion,
                        partitioned, orderedPartitionKeys, zone, realCount));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to plan iceberg count-pushdown file, error message is:"
                    + e.getMessage(), e);
        }
        return Collections.emptyList();
    }

    /**
     * Build the BE-ready {@link IcebergScanRange} for one {@link FileScanTask}, mirroring legacy
     * {@code IcebergScanNode.createIcebergSplit} + {@code setIcebergParams}: the file path/offset/size, the
     * per-file format (native parquet/orc), the table format version, the v3 row-lineage fields, and — for a
     * partitioned table — the partition spec-id, the all-fields {@code partition_data_json}, and the ordered
     * identity {@code partitionValues} that become columns-from-path.
     */
    private IcebergScanRange buildRange(Table table, DataFile dataFile, FileScanTask task, int formatVersion,
            boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone, long pushDownRowCount) {
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
        return new IcebergScanRange.Builder()
                .path(normalizePath(rawDataPath))
                .originalPath(rawDataPath)
                .start(task.start())
                .length(task.length())
                .fileSize(dataFile.fileSizeInBytes())
                .fileFormat(fileFormat)
                .formatVersion(formatVersion)
                .partitionSpecId(partitionSpecId)
                .partitionDataJson(partitionDataJson)
                .firstRowId(firstRowId)
                .lastUpdatedSequenceNumber(lastUpdatedSequenceNumber)
                .partitionValues(partitionValues)
                .deleteFiles(buildDeleteFiles(task))
                .pushDownRowCount(pushDownRowCount)
                .build();
    }

    /**
     * Translate a scan task's merge-on-read deletes ({@code task.deletes()}) into the typed delete carriers,
     * mirroring legacy {@code IcebergScanNode.getDeleteFileFilters} + {@code IcebergDeleteFileFilter}. Empty
     * for v1 / no-delete files (v1 has no delete files, so {@code task.deletes()} is always empty there).
     */
    private List<IcebergScanRange.DeleteFile> buildDeleteFiles(FileScanTask task) {
        List<DeleteFile> deletes = task.deletes();
        if (deletes == null || deletes.isEmpty()) {
            return Collections.emptyList();
        }
        List<IcebergScanRange.DeleteFile> result = new ArrayList<>(deletes.size());
        for (DeleteFile delete : deletes) {
            result.add(convertDelete(delete));
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
     * The delete path is normalized through the engine seam (legacy
     * {@code LocationPath.of(path,config).toStorageLocation()}). Package-private for direct unit testing.
     */
    IcebergScanRange.DeleteFile convertDelete(DeleteFile delete) {
        String path = normalizePath(delete.path().toString());
        FileContent content = delete.content();
        if (content == FileContent.POSITION_DELETES) {
            Long lowerBound = readPositionBound(delete.lowerBounds());
            Long upperBound = readPositionBound(delete.upperBounds());
            if (delete.format() == FileFormat.PUFFIN) {
                return IcebergScanRange.DeleteFile.deletionVector(path, lowerBound, upperBound,
                        delete.contentOffset(), delete.contentSizeInBytes());
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
     * Normalize a raw iceberg storage path (the data file BE opens, or a delete file) to BE's canonical
     * scheme via the engine seam (legacy goes through {@code LocationPath.of(path, storagePropertiesMap)
     * .toStorageLocation()}; the connector cannot import fe-core's {@code LocationPath}). BE's
     * scheme-dispatched S3 factory only opens {@code s3://}, so an un-normalized {@code oss://}/{@code cos://}
     * /{@code obs://}/{@code s3a://} path fails the native read (data file) or silently drops the deletes
     * (merge-on-read wrong rows). Mirrors paimon's {@code normalizeUri} (FIX-URI-NORMALIZE), which normalizes
     * both the data-file and deletion-vector paths. The static-map form is byte-equivalent to legacy for
     * non-vended catalogs; the vended-credential (2-arg) form is T09. A {@code null} context (offline unit
     * tests) preserves the raw path (paimon parity).
     */
    private String normalizePath(String rawPath) {
        return context != null ? context.normalizeStorageUri(rawPath) : rawPath;
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
     * </ul>
     * Location / serialized-table keys land in later tasks.
     */
    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = resolveTable(iceHandle);
        Map<String, String> props = new LinkedHashMap<>();
        props.put("file_format_type", "jni");
        List<String> partitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
        if (!partitionKeys.isEmpty()) {
            props.put("path_partition_keys", String.join(",", partitionKeys));
        }
        // Field-id schema dictionary (T06). Under a time-travel pin (T07, Option A): the query slots carry the
        // PINNED schema's names, but the generic node builds the column handles from the LATEST schema (the pin
        // lands after buildColumnHandles), so a column renamed between the pinned snapshot and now would be
        // dropped from `columns` -> the dict would miss that BE scan slot -> BE StructNode DCHECK crash. Build
        // the dict from the FULL pinned schema (a guaranteed superset of the BE slots — iceberg projection is
        // BE-tuple-driven, so `columns` only feeds the dict). See P6-T07 design §6. Without a pin, keep T06's
        // pruned-by-requested-columns dict (CI #969249).
        String dict;
        if (iceHandle.hasSnapshotPin()) {
            dict = IcebergSchemaUtils.encodeSchemaEvolutionProp(
                    table, pinnedSchema(table, iceHandle), Collections.emptyList());
        } else {
            dict = IcebergSchemaUtils.encodeSchemaEvolutionProp(
                    table, table.schema(), requestedLowerNames(columns));
        }
        props.put(SCHEMA_EVOLUTION_PROP, dict);
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
     * Apply the scan-level field-id schema dictionary (T06) built in {@link #getScanNodeProperties} to the real
     * {@link TFileScanRangeParams} (the same props map is round-tripped by the generic
     * {@code PluginDrivenScanNode}). Delegates to {@link IcebergSchemaUtils#applySchemaEvolution}, which fails
     * loud on a decode error (the prop is produced by us — silently dropping it would re-introduce the silent
     * wrong-rows BLOCKER on schema-evolved native reads).
     */
    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        IcebergSchemaUtils.applySchemaEvolution(params, nodeProperties.get(SCHEMA_EVOLUTION_PROP));
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
     * Enumerate + byte-offset-split the data files of a built scan via the iceberg SDK
     * {@code TableScanUtil.splitFiles} (the legacy {@code IcebergScanNode.splitFiles} algorithm — NOT fe-core
     * {@code FileSplitter}). A positive {@code file_split_size} session var forces that granularity directly;
     * otherwise the tasks are materialized once and split at the {@link #determineTargetFileSplitSize}
     * heuristic. Batch mode is deferred (paimon parity).
     */
    private CloseableIterable<FileScanTask> splitFiles(TableScan scan, ConnectorSession session) {
        long fileSplitSize = sessionLong(session, FILE_SPLIT_SIZE, 0L);
        if (fileSplitSize > 0) {
            return TableScanUtil.splitFiles(scan.planFiles(), fileSplitSize);
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
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks), targetSplitSize);
    }

    /**
     * Gate between the two file-enumeration paths (port of legacy {@code IcebergScanNode.planFileScanTask}). By
     * default ({@code meta.cache.iceberg.manifest.enable} off) and whenever no manifest cache is wired, this is
     * the iceberg SDK {@code splitFiles} path — byte-identical to T02. When the manifest cache is enabled it
     * re-plans at the manifest level so the per-manifest data/delete reads hit {@link IcebergManifestCache};
     * any failure falls back to {@code splitFiles} (legacy parity), so a cache bug can never break a query.
     */
    private CloseableIterable<FileScanTask> planFileScanTask(TableScan scan, ConnectorSession session, Table table,
            Optional<ConnectorExpression> filter) {
        if (!isManifestCacheEnabled()) {
            return splitFiles(scan, session);
        }
        try {
            return planFileScanTaskWithManifestCache(scan, session, table, filter);
        } catch (Exception e) {
            LOG.warn("Iceberg plan with manifest cache failed, falling back to SDK scan: {}", e.getMessage(), e);
            return splitFiles(scan, session);
        }
    }

    /**
     * Manifest-level planning that consumes {@link IcebergManifestCache}, ported faithfully from legacy
     * {@code IcebergScanNode.planFileScanTaskWithManifestCache}. It reconstructs iceberg's own planning:
     * partition-prune manifests with a {@link ManifestEvaluator}, read each surviving manifest's data/delete
     * files THROUGH THE CACHE, then per data file apply the {@link InclusiveMetricsEvaluator} (file-stats
     * pruning) + {@link ResidualEvaluator} (partition residual) and attach its deletes via a
     * {@link DeleteFileIndex}. The resulting {@link FileScanTask}s are byte-offset-split exactly like
     * {@link #splitFiles}, so the downstream {@code buildRange} (T03-T07) is unchanged. The predicate /
     * metrics / schema use the table's CURRENT schema (legacy parity).
     */
    private CloseableIterable<FileScanTask> planFileScanTaskWithManifestCache(TableScan scan,
            ConnectorSession session, Table table, Optional<ConnectorExpression> filter) throws IOException {
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

        // Phase 1: partition-prune + cache-load delete manifests into a flat delete-file list.
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
            deleteFiles.addAll(manifestCache.getManifestCacheValue(manifest, table).getDeleteFiles());
        }
        DeleteFileIndex deleteIndex = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(specsById)
                .caseSensitive(caseSensitive)
                .build();

        // Phase 2: partition-prune + cache-load data manifests, then file-level prune + attach deletes.
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<ManifestFile> dataManifests =
                getMatchingManifest(snapshot.dataManifests(table.io()), specsById, filterExpr)) {
            for (ManifestFile manifest : dataManifests) {
                if (manifest.content() != ManifestContent.DATA) {
                    continue;
                }
                PartitionSpec spec = specsById.get(manifest.partitionSpecId());
                if (spec == null) {
                    continue;
                }
                ResidualEvaluator residualEvaluator = residualEvaluators.get(manifest.partitionSpecId());
                if (residualEvaluator == null) {
                    continue;
                }
                ManifestCacheValue value = manifestCache.getManifestCacheValue(manifest, table);
                for (DataFile dataFile : value.getDataFiles()) {
                    if (!metricsEvaluator.eval(dataFile)) {
                        continue;
                    }
                    if (residualEvaluator.residualFor(dataFile.partition()).equals(Expressions.alwaysFalse())) {
                        continue;
                    }
                    DeleteFile[] deletes = deleteIndex.forDataFile(dataFile.dataSequenceNumber(), dataFile);
                    tasks.add(new BaseFileScanTask(
                            dataFile,
                            deletes,
                            SchemaParser.toJson(table.schema()),
                            PartitionSpecParser.toJson(spec),
                            residualEvaluator));
                }
            }
        }
        long targetSplitSize = determineTargetFileSplitSize(tasks, session);
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks), targetSplitSize);
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
     * Port of legacy {@code IcebergUtils.isManifestCacheEnabled} + {@code CacheSpec.isCacheEnabled}: the
     * manifest-level path is used iff the manifest cache is wired AND
     * {@code enable && ttl-second != 0 && capacity != 0}. The {@code .ttl-second}/{@code .capacity} properties
     * feed ONLY this formula (legacy quirk); the cache itself is fixed no-TTL / capacity 100000. A blank or
     * unparseable numeric value falls back to its default rather than failing the scan.
     */
    private boolean isManifestCacheEnabled() {
        if (manifestCache == null) {
            return false;
        }
        boolean enable = Boolean.parseBoolean(
                properties.getOrDefault(MANIFEST_CACHE_ENABLE, String.valueOf(DEFAULT_MANIFEST_CACHE_ENABLE)));
        long ttl = propLong(MANIFEST_CACHE_TTL_SECOND, DEFAULT_MANIFEST_CACHE_TTL_SECOND);
        long capacity = propLong(MANIFEST_CACHE_CAPACITY, DEFAULT_MANIFEST_CACHE_CAPACITY);
        return enable && ttl != 0 && capacity != 0;
    }

    private long propLong(String key, long defaultValue) {
        String raw = properties == null ? null : properties.get(key);
        if (raw == null || raw.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Port of legacy {@code IcebergScanNode.determineTargetFileSplitSize} + {@code applyMaxFileSplitNumLimit}
     * (non-batch path), reading the split-size knobs from the session-property channel. Start at
     * {@code max_initial_file_split_size}, escalate to {@code max_file_split_size} once total content exceeds
     * {@code max_file_split_size * max_initial_file_split_num}, then raise the size so the split count stays
     * under {@code max_file_split_num}. Uses {@code DataFile.fileSizeInBytes()} (== content size for data
     * files; T02 is the no-delete path) since iceberg 1.10.1 omits {@code ScanTaskUtil.contentSizeInBytes}.
     */
    private long determineTargetFileSplitSize(List<FileScanTask> tasks, ConnectorSession session) {
        long maxInitialSplitSize = sessionLong(session, MAX_INITIAL_FILE_SPLIT_SIZE,
                DEFAULT_MAX_INITIAL_FILE_SPLIT_SIZE);
        long maxSplitSize = sessionLong(session, MAX_FILE_SPLIT_SIZE, DEFAULT_MAX_FILE_SPLIT_SIZE);
        long maxInitialSplitNum = sessionLong(session, MAX_INITIAL_FILE_SPLIT_NUM,
                DEFAULT_MAX_INITIAL_FILE_SPLIT_NUM);
        long maxFileSplitNum = sessionLong(session, MAX_FILE_SPLIT_NUM, DEFAULT_MAX_FILE_SPLIT_NUM);
        long total = 0;
        boolean exceedInitialThreshold = false;
        for (FileScanTask task : tasks) {
            total += task.file().fileSizeInBytes();
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

    /**
     * Compute the COUNT(*)-pushdown row count from the scan's snapshot summary, a faithful port of legacy
     * {@code IcebergScanNode.getCountFromSnapshot} (:1142-1171):
     * <ul>
     *   <li>no snapshot (empty table) → {@code 0};</li>
     *   <li>any equality delete ({@code total-equality-deletes != "0"}) → {@code -1} — not pushable, since
     *       equality deletes re-project at read time and the summary cannot net them out;</li>
     *   <li>no position deletes → {@code total-records};</li>
     *   <li>position deletes present and session {@code ignore_iceberg_dangling_delete=true} →
     *       {@code total-records - total-position-deletes};</li>
     *   <li>otherwise {@code -1}.</li>
     * </ul>
     * Reads the scan's snapshot ({@code scan.snapshot()}) so the count tracks the scan automatically (the
     * current snapshot today; the pinned snapshot once MVCC time-travel lands) — equivalent to legacy's
     * {@code currentSnapshot()} for every non-time-travel query. The {@code summary.get(...)} calls are
     * null-unsafe by design (legacy parity — real iceberg snapshots always carry these totals, "0" when none).
     */
    private static long getCountFromSnapshot(TableScan scan, ConnectorSession session) {
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return 0;
        }
        Map<String, String> summary = snapshot.summary();
        if (!summary.get(TOTAL_EQUALITY_DELETES).equals("0")) {
            // has equality delete files, can not push down count
            return -1;
        }
        long deleteCount = Long.parseLong(summary.get(TOTAL_POSITION_DELETES));
        if (deleteCount == 0) {
            // no delete files, can push down count directly
            return Long.parseLong(summary.get(TOTAL_RECORDS));
        }
        if (ignoreIcebergDanglingDelete(session)) {
            // has position delete files; if we ignore dangling deletes, the netted count can be pushed down
            return Long.parseLong(summary.get(TOTAL_RECORDS)) - deleteCount;
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
    private Table resolveTable(IcebergTableHandle handle) {
        if (context == null) {
            return catalogOps.loadTable(handle.getDbName(), handle.getTableName());
        }
        try {
            return context.executeAuthenticated(
                    () -> catalogOps.loadTable(handle.getDbName(), handle.getTableName()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to load table for scan, error message is:" + e.getMessage(), e);
        }
    }
}
