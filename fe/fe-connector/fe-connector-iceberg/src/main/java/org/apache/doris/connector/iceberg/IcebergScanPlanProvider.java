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
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * {@link ConnectorScanPlanProvider} for Iceberg tables, mirroring the paimon connector's
 * {@code PaimonScanPlanProvider}. The generic, engine-neutral {@code PluginDrivenScanNode} drives split
 * generation through this provider once iceberg is in {@code SPI_READY_TYPES} (P6.6 cutover).
 *
 * <p>P6.2-T01 (this task) is the skeleton: it wires the collaborators ({@code properties} /
 * {@link IcebergCatalogOps} seam / {@link ConnectorContext}) and pins the predicate-driven semantics
 * ({@link #ignorePartitionPruneShortCircuit()} = {@code true}). The real split planning — self-contained
 * predicate pushdown, {@code FileScanTask} enumeration, native-vs-JNI classification, merge-on-read
 * delete files, the field-id history-schema dictionary, COUNT/batch, and vended credentials — lands in
 * P6.2-T02..T09. Iceberg is NOT yet in {@code SPI_READY_TYPES}, so {@link #planScan} is not exercised at
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

    // Self-contained mirror of fe-core TimeUtils.timeZoneAliasMap (cannot import TimeUtils). Doris stores the
    // session time_zone un-canonicalized (e.g. SET time_zone='CST' keeps "CST"), and legacy resolves it via
    // ZoneId.of(tz, timeZoneAliasMap). Without these aliases a plain ZoneId.of("CST") throws (CST is a
    // SHORT_ID) and falls back to UTC, shifting timestamptz literal pushdown by hours -> wrong file pruning.
    private static final Map<String, String> TIME_ZONE_ALIAS_MAP;

    static {
        Map<String, String> aliases = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        aliases.putAll(ZoneId.SHORT_IDS);
        // CST/PRC -> Asia/Shanghai (NOT America/Chicago), UTC/GMT -> UTC (= TimeUtils DEFAULT/UTC_TIME_ZONE).
        aliases.put("CST", "Asia/Shanghai");
        aliases.put("PRC", "Asia/Shanghai");
        aliases.put("UTC", "UTC");
        aliases.put("GMT", "UTC");
        TIME_ZONE_ALIAS_MAP = Collections.unmodifiableMap(aliases);
    }

    private final Map<String, String> properties;
    private final IcebergCatalogOps catalogOps;
    // Engine seam: executeAuthenticated (Kerberos UGI), storage properties, vended credentials. Nullable —
    // null in offline unit tests via the 2-arg ctor, in which case resolveTable resolves directly.
    private final ConnectorContext context;

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps) {
        this(properties, catalogOps, null);
    }

    public IcebergScanPlanProvider(Map<String, String> properties, IcebergCatalogOps catalogOps,
            ConnectorContext context) {
        this.properties = properties;
        this.catalogOps = catalogOps;
        this.context = context;
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
        IcebergTableHandle iceHandle = (IcebergTableHandle) handle;
        Table table = resolveTable(iceHandle);

        // Predicate pushdown: translate the engine-neutral predicate into iceberg Expressions (self-contained,
        // mirrors legacy IcebergUtils.convertToIcebergExpr); unpushable conjuncts are dropped (BE residual).
        List<Expression> predicates = Collections.emptyList();
        if (filter.isPresent()) {
            predicates = new IcebergPredicateConverter(table.schema(), resolveSessionZone(session))
                    .convert(filter.get());
        }

        // Build the TableScan, applying each pushed predicate as a separate filter (iceberg ANDs them
        // internally), mirroring legacy createTableScan. The fe-core-only metricsReporter (profile) and
        // planWith(threadPool) are intentionally dropped — the iceberg SDK default worker pool plans, and the
        // file set is identical (see design deviations). Snapshot pinning (time-travel/MVCC) is P6.2-T07.
        TableScan scan = table.newScan();
        for (Expression predicate : predicates) {
            scan = scan.filter(predicate);
        }

        // Enumerate FileScanTasks via the iceberg SDK (split byte-offsets come from TableScanUtil.splitFiles)
        // and emit one BE-ready IcebergScanRange per task, populating the typed iceberg carriers — incl. the
        // merge-on-read delete files (T04) — mirroring legacy IcebergScanNode.createIcebergSplit. COUNT (T05),
        // the field-id history dict (T06, scan-level), MVCC pin (T07), and vended credentials (T09) land later.
        int formatVersion = getFormatVersion(table);
        List<String> orderedPartitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
        ZoneId zone = resolveSessionZone(session);
        boolean partitioned = table.spec().isPartitioned();
        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = splitFiles(scan, session)) {
            for (FileScanTask task : tasks) {
                DataFile dataFile = task.file();
                ranges.add(buildRange(table, dataFile, task, formatVersion, partitioned, orderedPartitionKeys, zone));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to enumerate iceberg file scan tasks, error message is:"
                    + e.getMessage(), e);
        }
        LOG.debug("Iceberg planScan produced {} ranges for table {}", ranges.size(), table.name());
        return ranges;
    }

    /**
     * Build the BE-ready {@link IcebergScanRange} for one {@link FileScanTask}, mirroring legacy
     * {@code IcebergScanNode.createIcebergSplit} + {@code setIcebergParams}: the file path/offset/size, the
     * per-file format (native parquet/orc), the table format version, the v3 row-lineage fields, and — for a
     * partitioned table — the partition spec-id, the all-fields {@code partition_data_json}, and the ordered
     * identity {@code partitionValues} that become columns-from-path.
     */
    private IcebergScanRange buildRange(Table table, DataFile dataFile, FileScanTask task, int formatVersion,
            boolean partitioned, List<String> orderedPartitionKeys, ZoneId zone) {
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
     * </ul>
     * Location / serialized-table / schema-evolution keys land in later tasks.
     */
    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        Table table = resolveTable((IcebergTableHandle) handle);
        Map<String, String> props = new LinkedHashMap<>();
        props.put("file_format_type", "jni");
        List<String> partitionKeys = IcebergPartitionUtils.getIdentityPartitionColumns(table);
        if (!partitionKeys.isEmpty()) {
            props.put("path_partition_keys", String.join(",", partitionKeys));
        }
        return props;
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

    // The session time zone drives zone-adjusted (timestamptz) literal pushdown. Resolved through the Doris
    // alias map (mirrors fe-core TimeUtils.getTimeZone()) so aliases like CST/PRC/EST match legacy instead of
    // throwing; null/blank/genuinely-invalid -> UTC. Package-private for direct unit testing.
    static ZoneId resolveSessionZone(ConnectorSession session) {
        if (session == null) {
            return ZoneOffset.UTC;
        }
        String tz = session.getTimeZone();
        if (tz == null || tz.trim().isEmpty()) {
            return ZoneOffset.UTC;
        }
        try {
            return ZoneId.of(tz.trim(), TIME_ZONE_ALIAS_MAP);
        } catch (Exception e) {
            return ZoneOffset.UTC;
        }
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
