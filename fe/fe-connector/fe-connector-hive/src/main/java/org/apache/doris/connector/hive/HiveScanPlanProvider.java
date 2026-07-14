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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRangeType;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Scan plan provider for Hive tables.
 *
 * <p>Implements the core scan planning pipeline:</p>
 * <ol>
 *   <li>Resolve partitions from HMS (using pruned list if available)</li>
 *   <li>List files in each partition directory via Hadoop FileSystem</li>
 *   <li>Split large files based on target split size</li>
 *   <li>Return {@link HiveScanRange} for each file split</li>
 * </ol>
 *
 * <p>Current limitations (Phase 2 initial):</p>
 * <ul>
 *   <li>No file listing cache (lists files directly each time)</li>
 *   <li>No ACID transaction support (non-transactional tables only)</li>
 *   <li>No file-count streaming split mode; partition-batch mode is limited to partitioned
 *       non-transactional tables (see {@link #supportsBatchScan})</li>
 * </ul>
 */
public class HiveScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(HiveScanPlanProvider.class);

    /** Default target split size: 256 MB. */
    private static final long DEFAULT_TARGET_SPLIT_SIZE = 256 * 1024 * 1024L;

    /** Maximum number of partitions to list from HMS. */
    private static final int MAX_PARTITIONS = 100000;

    /** Scan node property keys. */
    public static final String PROP_FILE_FORMAT_TYPE = "file_format_type";
    public static final String PROP_PATH_PARTITION_KEYS = "path_partition_keys";
    public static final String PROP_LOCATION_PREFIX = "location.";
    /**
     * Connector-internal signal (consumed by {@link #populateScanLevelParams}) marking a full-ACID
     * (transactional) Hive scan; drives the scan-level {@code table_format_type} stamp. Not a BE-facing key.
     */
    public static final String PROP_TRANSACTIONAL_HIVE = "transactional_hive";

    /** Input format of a full-ACID (ORC) transactional Hive table; other formats are rejected. */
    private static final String ORC_ACID_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";

    private final HmsClient hmsClient;
    private final Map<String, String> catalogProperties;
    // Engine-owned, per-catalog filesystem accessor. The non-ACID listing path borrows the Doris FileSystem via
    // context.getFileSystem(session) (never closes it — the engine owns its lifecycle) to list partition
    // directories, replacing bare Hadoop FileSystem.get (FIX-HIVEFS: the hive plugin bundles no HDFS impl).
    private final ConnectorContext context;
    private final HiveReadTransactionManager readTxnManager;
    // Connector-owned directory-listing cache (the SAME instance HiveConnector shares with HiveConnectorMetadata),
    // so a repeated scan of the same partition directory is served from the cache instead of re-listing. Only the
    // plain (non-ACID) path uses it; the ACID path lists via HiveAcidUtil and is uncached (legacy parity).
    private final HiveFileListingCache fileListingCache;

    public HiveScanPlanProvider(HmsClient hmsClient, Map<String, String> catalogProperties,
            ConnectorContext context, HiveReadTransactionManager readTxnManager,
            HiveFileListingCache fileListingCache) {
        this.hmsClient = hmsClient;
        this.catalogProperties = catalogProperties;
        this.context = context;
        this.readTxnManager = readTxnManager;
        this.fileListingCache = fileListingCache;
    }

    @Override
    public ConnectorScanRangeType getScanRangeType() {
        return ConnectorScanRangeType.FILE_SCAN;
    }

    @Override
    public List<ConnectorScanRange> planScan(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();

        List<PartitionScanInfo> partitions = resolvePartitions(hiveHandle);
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        HiveFileFormat fileFormat = HiveFileFormat.detect(
                hiveHandle.getInputFormat(), hiveHandle.getSerializationLib(),
                readHiveJsonInOneColumn(session), hiveHandle.isFirstColumnString());
        long targetSplitSize = getTargetSplitSize(session);
        boolean isLzo = isLzoInputFormat(hiveHandle.getInputFormat());
        // LZO text is NOT splittable: a .lzo stream cannot be decompressed from an arbitrary byte offset.
        // Legacy HiveUtil.isSplittable returned false for LZO; HiveFileFormat maps LZO text to TEXT (which
        // reports splittable), so the LZO case must be masked out here. ACID is never LZO, so this leaves the
        // transactional branch unchanged.
        boolean splittable = fileFormat.isSplittable() && !isLzo;

        List<ConnectorScanRange> ranges = new ArrayList<>();

        if (hiveHandle.isTransactional()) {
            // Transactional (ACID) table: descend into base/delta directories under the query's write-id
            // snapshot and emit ACID-annotated ranges. Borrows the engine's per-catalog Doris FileSystem to
            // list (same source as the non-ACID branch below; the engine owns its lifecycle — never closed here).
            planAcidScan(session, hiveHandle, partitions, context.getFileSystem(session), fileFormat,
                    splittable, targetSplitSize, ranges);
        } else {
            // Borrow the engine's per-catalog Doris FileSystem to list partition directories (see field javadoc).
            FileSystem fs = context.getFileSystem(session);
            for (PartitionScanInfo partition : partitions) {
                HiveFileFormat partFormat = partition.fileFormat != null
                        ? partition.fileFormat : fileFormat;
                listAndSplitFiles(dbName, tableName, partition, partFormat,
                        splittable, isLzo, targetSplitSize, fs, ranges);
            }
        }

        LOG.info("Hive scan plan: table={}.{}, partitions={}, splits={}",
                dbName, tableName, partitions.size(), ranges.size());
        return ranges;
    }

    /**
     * Whether this hive table supports batched split generation (see
     * {@link ConnectorScanPlanProvider#supportsBatchScan}): {@code true} iff the table is PARTITIONED (has
     * partition keys) and NOT transactional. A large partitioned scan then streams its splits per partition
     * batch via {@link #planScanForPartitionBatch} on a background pool instead of materializing every
     * partition's files synchronously (legacy {@code HiveScanNode} went async at
     * {@code num_partitions_in_batch_mode}).
     *
     * <p>Transactional/ACID tables are excluded DELIBERATELY: their scan opens one metastore read transaction
     * (see {@link #planAcidScan}); running the per-batch resolution on background threads would open — and leak
     * — a read transaction per batch. ACID partitioned tables keep the synchronous {@link #planScan} path
     * (correct, just not streamed). The transactional test uses the SAME {@code isTransactional()} accessor
     * {@link #planScan} branches on.</p>
     */
    @Override
    public boolean supportsBatchScan(ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        return partKeyNames != null && !partKeyNames.isEmpty() && !hiveHandle.isTransactional();
    }

    /**
     * Hive scan ranges (base/insert files) carry positive byte lengths, so the engine can apply
     * {@code TABLESAMPLE} by size-weighted split selection — restoring the legacy
     * {@code HiveScanNode.selectFiles} behavior lost at the SPI cutover. Only Hive ever sampled; the
     * other connectors keep the default {@code false} (their ranges do not carry byte-proportional lengths).
     */
    @Override
    public boolean supportsTableSample() {
        return true;
    }

    /**
     * Remaps {@code LZ4FRAME -> LZ4BLOCK} for hive splits, restoring legacy {@code HiveScanNode.getFileCompressType}
     * parity lost at the SPI cutover. Hadoop/hive write {@code .lz4} files with the LZ4 <em>block</em> codec, but
     * the generic node infers {@code LZ4FRAME} from the {@code .lz4} extension; sending frame to BE makes its
     * text/CSV reader fail with {@code LZ4F_getFrameInfo ERROR_frameType_unknown} on block-format bytes. Only
     * {@code LZ4FRAME} is remapped — every other codec (incl. an actual frame-format non-hive file, which hive
     * never produces) passes through, so this is byte-identical to legacy in every reachable case.
     */
    @Override
    public TFileCompressType adjustFileCompressType(TFileCompressType inferred) {
        return inferred == TFileCompressType.LZ4FRAME ? TFileCompressType.LZ4BLOCK : inferred;
    }

    /**
     * Plans the scan for a SINGLE partition batch (see
     * {@link ConnectorScanPlanProvider#planScanForPartitionBatch}). Unlike {@link #planScan} — which resolves
     * partitions from {@code handle.getPrunedPartitions()} and IGNORES the passed partition set — this MUST scope
     * resolution to {@code partitionBatch}: {@code PluginDrivenScanNode} slices the pruned partition NAMES into
     * batches and calls this once per batch, so inheriting the SPI default (which re-runs the whole-pruned-set
     * {@link #planScan} per batch) would emit every partition's files once per batch — duplicate rows. The batch
     * strings are the metastore-rendered {@code key=value/...} partition names (the keys of the Nereids
     * selected-partition map), exactly the form {@code hmsClient.getPartitions} accepts, so batch-scoped
     * resolution is a clean {@code getPartitions(batch)} round-trip. Only the non-ACID path is reachable here
     * ({@link #supportsBatchScan} excludes transactional tables). Reuses the SAME helpers as {@link #planScan}:
     * format detect, split size, hadoop conf, {@link #convertPartitions}, {@link #listAndSplitFiles}.
     */
    @Override
    public List<ConnectorScanRange> planScanForPartitionBatch(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter,
            long limit,
            List<String> partitionBatch) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();

        // Resolve ONLY this batch's partitions (scoped to partitionBatch), NOT handle.getPrunedPartitions().
        List<HmsPartitionInfo> hmsPartitions = hmsClient.getPartitions(dbName, tableName, partitionBatch);
        List<PartitionScanInfo> partitions = convertPartitions(
                hmsPartitions, hiveHandle.getPartitionKeyNames());
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }

        HiveFileFormat fileFormat = HiveFileFormat.detect(
                hiveHandle.getInputFormat(), hiveHandle.getSerializationLib(),
                readHiveJsonInOneColumn(session), hiveHandle.isFirstColumnString());
        long targetSplitSize = getTargetSplitSize(session);
        boolean isLzo = isLzoInputFormat(hiveHandle.getInputFormat());
        // LZO text is not splittable (see planScan); mask it out of the TEXT-derived splittable flag.
        boolean splittable = fileFormat.isSplittable() && !isLzo;
        // Only the non-ACID path is reachable here (supportsBatchScan excludes transactional tables), so this
        // borrows the engine's per-catalog Doris FileSystem to list — no Hadoop Configuration is needed.
        FileSystem fs = context.getFileSystem(session);

        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (PartitionScanInfo partition : partitions) {
            HiveFileFormat partFormat = partition.fileFormat != null
                    ? partition.fileFormat : fileFormat;
            listAndSplitFiles(dbName, tableName, partition, partFormat,
                    splittable, isLzo, targetSplitSize, fs, ranges);
        }
        return ranges;
    }

    /**
     * Plans a scan of a transactional (ACID) Hive table.
     *
     * <p>Opens (or reuses) the query's read transaction — which acquires a shared read lock and pins a
     * write-id snapshot — then, per partition, runs {@link HiveAcidUtil#getAcidState} to resolve the
     * surviving base/delta data files and delete-delta directories, and emits one ACID-annotated
     * {@link HiveScanRange} per data-file split. The BE subtracts the delete deltas on read.</p>
     *
     * <p><b>Live path.</b> Post-cutover {@code hms} is in {@code SPI_READY_TYPES}, so an hms-catalog
     * transactional table is a {@code PluginDrivenExternalTable} routed through {@code PluginDrivenScanNode}
     * straight into this method on a live query (the only read gate is the full-ACID ORC-format check below).
     * It opens a real metastore transaction/lock; the matching commit (lock release) is driven by
     * {@link HiveReadTransactionManager#deregister} at query finish (via {@link #releaseReadTransaction}),
     * without which the shared read lock would leak for the metastore's lifetime.</p>
     */
    private void planAcidScan(ConnectorSession session, HiveTableHandle handle,
            List<PartitionScanInfo> partitions, FileSystem fs, HiveFileFormat fileFormat,
            boolean splittable, long targetSplitSize, List<ConnectorScanRange> ranges) {
        boolean isFullAcid = handle.isFullAcid();
        if (isFullAcid && !ORC_ACID_INPUT_FORMAT.equals(handle.getInputFormat())) {
            // Full-ACID data is only stored in the ORC ACID layout; reject other formats loudly
            // (legacy parity: HMSExternalTable.isFullAcidTable throws "no Orc Format").
            throw new DorisConnectorException("This table is full Acid Table, but no Orc Format.");
        }

        // One transaction per transactional-table scan, pinning this table's write-id snapshot (mirrors
        // fe-core, which opens a HiveTransaction per scan node). The manager holds it for commit at finish.
        HiveReadTransaction txn = new HiveReadTransaction(session.getQueryId(), session.getUser(),
                handle.getDbName(), handle.getTableName(), isFullAcid, hmsClient);
        readTxnManager.register(txn);
        for (PartitionScanInfo partition : partitions) {
            if (!partition.partitionValues.isEmpty()) {
                txn.addPartition(buildPartitionName(partition.partitionValues));
            }
        }
        Map<String, String> txnValidIds = txn.getValidWriteIds();

        for (PartitionScanInfo partition : partitions) {
            HiveFileFormat partFormat = partition.fileFormat != null
                    ? partition.fileFormat : fileFormat;
            try {
                // Descend the ACID directory tree through the engine-injected Doris FileSystem. The hive plugin
                // bundles no HDFS impl, so a bare Hadoop FileSystem.get here would throw "No FileSystem for
                // scheme hdfs" (FIX-HIVEFS); the engine owns this FileSystem's lifecycle — never closed here.
                HiveAcidUtil.AcidState state = HiveAcidUtil.getAcidState(
                        fs, partition.location, txnValidIds, isFullAcid);
                // Only full-ACID reads are marked transactional_hive (delete deltas applied by the BE
                // merge-on-read reader). Insert-only tables use the write-id snapshot to pick files but
                // store plain data files, so their splits stay plain hive — matching legacy AcidUtil,
                // which sets acidInfo only when isFullAcid.
                // BE-facing ACID paths (delete-delta dir + partition location) also carry the raw HMS scheme; the
                // BE transactional reader opens them via the same native S3 factory, so normalize s3a://->s3://.
                String acidLocation = isFullAcid ? normalizeNativeUri(partition.location) : null;
                List<String> encodedDeltas = isFullAcid
                        ? encodeDeleteDeltas(state.getDeleteDeltas(), this::normalizeNativeUri) : null;
                for (FileEntry dataFile : state.getDataFiles()) {
                    splitFile(dataFile.location().uri(), dataFile.length(),
                            dataFile.modificationTime(), partition, partFormat, splittable,
                            targetSplitSize, acidLocation, encodedDeltas, ranges);
                }
            } catch (IOException e) {
                throw new DorisConnectorException(
                        "Failed to list ACID files for partition: " + partition.location, e);
            }
        }
    }

    /**
     * Commits and deregisters this query's read transaction (opened by {@link #planAcidScan} via
     * {@link HiveReadTransactionManager#register}), releasing the metastore shared read lock. Driven by the
     * generic fe-core query-finish callback at query end; without it a transactional-hive read leaks the shared
     * read lock for the metastore's lifetime. {@code readTxnManager} is the same per-connector manager that
     * {@code register} used (both injected by {@code HiveConnector}), so the {@code queryId} keys match.
     * {@code deregister} is idempotent (a no-op for a query that opened no transaction) and swallows a commit
     * failure, matching the best-effort SPI contract.
     */
    @Override
    public void releaseReadTransaction(String queryId) {
        readTxnManager.deregister(queryId);
    }

    /** Encodes each delete-delta as {@code "dir|file1,file2"} for {@link HiveScanRange.Builder#acidInfo}. */
    private static List<String> encodeDeleteDeltas(List<HiveAcidUtil.DeleteDelta> deltas,
            UnaryOperator<String> nativePathNormalizer) {
        List<String> encoded = new ArrayList<>(deltas.size());
        for (HiveAcidUtil.DeleteDelta delta : deltas) {
            // Normalize the delete-delta directory scheme (s3a://->s3://) for BE's native reader; the file names
            // are bare names appended after '|'.
            encoded.add(nativePathNormalizer.apply(delta.getDirectoryLocation()) + "|"
                    + String.join(",", delta.getFileNames()));
        }
        return encoded;
    }

    /** Builds the Hive partition name {@code k1=v1/k2=v2} used for the shared read lock components. */
    private static String buildPartitionName(Map<String, String> partitionValues) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(entry.getKey()).append('=').append(entry.getValue());
        }
        return sb.toString();
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        Map<String, String> props = new HashMap<>();

        // File format type
        HiveFileFormat fileFormat = HiveFileFormat.detect(
                hiveHandle.getInputFormat(), hiveHandle.getSerializationLib(),
                readHiveJsonInOneColumn(session), hiveHandle.isFirstColumnString());
        props.put(PROP_FILE_FORMAT_TYPE, fileFormat.getFormatName());

        // Partition key column names
        List<String> partKeys = hiveHandle.getPartitionKeyNames();
        if (partKeys != null && !partKeys.isEmpty()) {
            props.put(PROP_PATH_PARTITION_KEYS, String.join(",", partKeys));
        }

        // Location properties (Hadoop/S3 config for BE file access).
        //  (1) BE-canonical static credentials (AWS_* for object stores, resolved hadoop.*/dfs.* for HDFS): BE's
        //      native (FILE_S3) reader understands ONLY these canonical keys — it reads AWS_ACCESS_KEY /
        //      AWS_SECRET_KEY / AWS_ENDPOINT (s3_util.cpp), NOT the raw s3.access_key/... aliases — so without
        //      them a private bucket 403s. Legacy HiveScanNode.getLocationProperties() emitted exactly this
        //      (hmsTable.getBackendStorageProperties()); the new path had dropped it. Empty for a null context
        //      (offline tests) or a credential-less warehouse.
        if (context != null) {
            context.getBackendStorageProperties().forEach((k, v) -> props.put(PROP_LOCATION_PREFIX + k, v));
        }
        //  (2) Raw catalog aliases + inline fs./hadoop./dfs. keys. Emitted AFTER the canonical set so a user-inline
        //      fs./hadoop. key wins; the s3./oss./cos./obs. aliases are harmless to BE (ignored by the native
        //      reader) but kept so no configured key is dropped.
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            String key = entry.getKey();
            if (isLocationProperty(key)) {
                props.put(PROP_LOCATION_PREFIX + key, entry.getValue());
            }
        }

        // Text format properties (delimiters / enclose / escape / null_format / is_json) for every non-columnar
        // format — TEXT (hive text serde), CSV (OpenCSV serde) and JSON. BE reads these from the hive.text.*
        // props regardless of the resolved TFileFormatType, so all three families must emit them.
        if (fileFormat == HiveFileFormat.TEXT || fileFormat == HiveFileFormat.CSV
                || fileFormat == HiveFileFormat.JSON) {
            Map<String, String> textProps = HiveTextProperties.extract(
                    hiveHandle.getSerializationLib(),
                    hiveHandle.getSdParameters(),
                    hiveHandle.getTableParameters());
            props.putAll(textProps);
        }

        // #65437: BE selects the file scanner from SCAN-LEVEL params before the per-range splits arrive, and its
        // FileScannerV2 gate excludes scans whose scan-level table_format_type is "transactional_hive" (V2 does
        // not apply ACID delete deltas). Signal a full-ACID scan so populateScanLevelParams stamps it at scan
        // level; gate on isFullAcid() to match the per-range marker (insert-only tables keep the V2 fast path).
        if (hiveHandle.isFullAcid()) {
            props.put(PROP_TRANSACTIONAL_HIVE, "true");
        }

        return props;
    }

    /**
     * Stamps the SCAN-LEVEL table format for a full-ACID Hive scan (signalled by {@link #PROP_TRANSACTIONAL_HIVE}
     * from {@link #getScanNodeProperties}). BE's FileScannerV2 selection reads {@code table_format_type} from the
     * scan-level params (before per-range splits are fetched); without this stamp a transactional Hive read would
     * wrongly use FileScannerV2, which does not apply ACID delete deltas — deleted rows would reappear. Mirrors the
     * legacy {@code HiveScanNode.markTransactionalHiveScanParams} (#65437); the generic {@code PluginDrivenScanNode}
     * invokes this hook after building the scan-level params.
     */
    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        if ("true".equals(nodeProperties.get(PROP_TRANSACTIONAL_HIVE))) {
            markTransactionalHiveScanParams(params);
        }
    }

    static void markTransactionalHiveScanParams(TFileScanRangeParams params) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        // Same literal the per-range marker uses (HiveScanRange); BE matches table_format_type == "transactional_hive".
        tableFormatFileDesc.setTableFormatType("transactional_hive");
        params.setTableFormatParams(tableFormatFileDesc);
    }

    /**
     * Reads the {@code read_hive_json_in_one_column} session flag (default false) from the connector session —
     * the same channel the write path uses for {@code hive_text_compression}. The engine dumps every visible
     * session variable into {@code getSessionProperties()}, so no extra plumbing is needed.
     */
    private static boolean readHiveJsonInOneColumn(ConnectorSession session) {
        return Boolean.parseBoolean(session.getSessionProperties()
                .getOrDefault(HiveConnectorProperties.SESSION_READ_HIVE_JSON_IN_ONE_COLUMN, "false"));
    }

    /**
     * Resolves the partitions to scan, using pruned partitions from the handle
     * if available, or listing all partitions from HMS.
     */
    private List<PartitionScanInfo> resolvePartitions(HiveTableHandle handle) {
        List<String> partKeyNames = handle.getPartitionKeyNames();

        if (partKeyNames == null || partKeyNames.isEmpty()) {
            // Unpartitioned table: single partition using table location
            return Collections.singletonList(new PartitionScanInfo(
                    handle.getLocation(), Collections.emptyMap(), null));
        }

        // Check for pruned partitions in handle (set by applyFilter)
        List<HmsPartitionInfo> prunedPartitions = handle.getPrunedPartitions();
        if (prunedPartitions != null) {
            return convertPartitions(prunedPartitions, partKeyNames);
        }

        // No pruning: list all partitions from HMS
        List<String> partNames = hmsClient.listPartitionNames(
                handle.getDbName(), handle.getTableName(), MAX_PARTITIONS);
        if (partNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<HmsPartitionInfo> hmsPartitions = hmsClient.getPartitions(
                handle.getDbName(), handle.getTableName(), partNames);
        return convertPartitions(hmsPartitions, partKeyNames);
    }

    private List<PartitionScanInfo> convertPartitions(
            List<HmsPartitionInfo> hmsPartitions, List<String> partKeyNames) {
        List<PartitionScanInfo> result = new ArrayList<>(hmsPartitions.size());
        for (HmsPartitionInfo part : hmsPartitions) {
            Map<String, String> partValues = new LinkedHashMap<>();
            List<String> values = part.getValues();
            for (int i = 0; i < partKeyNames.size() && i < values.size(); i++) {
                partValues.put(partKeyNames.get(i), values.get(i));
            }
            // Per-partition file format is not carried to BE (the whole scan node reads with the single
            // table-level format resolved in planScan/getScanNodeProperties); leave it null so planScan falls
            // back to the table format, matching the unpartitioned case. Detecting it per partition here would
            // also fail-loud spuriously if one partition carried an unusual serde the table itself does not.
            result.add(new PartitionScanInfo(
                    part.getLocation(), partValues, null));
        }
        return result;
    }

    /**
     * Lists the data files of a partition directory (through the connector's shared {@link HiveFileListingCache},
     * which filters directories and {@code _}/{@code .}-prefixed hidden files) and splits them into scan ranges.
     * A LOCAL per-directory listing failure ({@link HiveDirectoryListingException}) is tolerated — the partition is
     * skipped with a warning, the same resilience the pre-cache code gave a missing/unreadable partition directory.
     * A SYSTEMIC filesystem-resolution failure (a plain {@link DorisConnectorException} from {@code FileSystem.get},
     * which affects every partition) is NOT caught here: it propagates to fail the query loud, matching the pre-cache
     * behavior where a {@code FileSystem.get} failure aborted the query instead of silently returning an empty scan.
     * Failures are never cached (the cache loader throws).
     */
    private void listAndSplitFiles(String dbName, String tableName,
            PartitionScanInfo partition, HiveFileFormat fileFormat,
            boolean splittable, boolean isLzo, long targetSplitSize, FileSystem fs,
            List<ConnectorScanRange> ranges) {
        List<HiveFileStatus> files;
        try {
            files = fileListingCache.listDataFiles(dbName, tableName, partition.location,
                    new ArrayList<>(partition.partitionValues.values()), fs);
        } catch (HiveDirectoryListingException e) {
            // hive.ignore_absent_partitions=false (non-default): a partition whose LOCATION does not exist
            // must fail the query loud with the legacy message rather than be silently skipped. Only a
            // not-found cause counts as "absent"; a transient/unreadable listing failure still follows the
            // tolerant skip-with-warning path below. Default true preserves the skip behavior.
            if (isLocationNotFound(e) && !HiveConnectorProperties.getBoolean(
                    catalogProperties, HiveConnectorProperties.IGNORE_ABSENT_PARTITIONS, true)) {
                throw new DorisConnectorException(
                        "Partition location does not exist: " + partition.location, e);
            }
            LOG.warn("Cannot list files in partition: {}", partition.location, e);
            return;
        }
        for (HiveFileStatus file : files) {
            // LZO text tables: only *.lzo files are data. Exclude *.lzo.index sidecars (and any other
            // non-*.lzo entry), mirroring Hive's LzoTextInputFormat.listStatus() and legacy
            // HiveExternalMetaCache's HiveUtil.isLzoDataFile filter. HiveFileListingCache strips only
            // _/.-prefixed hidden files, so without this a *.lzo.index sidecar is read as an extra text row.
            if (isLzo && !isLzoDataFile(file.getPath())) {
                continue;
            }
            splitFile(file.getPath(), file.getLength(), file.getModificationTime(),
                    partition, fileFormat, splittable, targetSplitSize, null, null, ranges);
        }
    }

    /**
     * Whether a listing failure was caused by the directory not existing (a {@link FileNotFoundException}
     * anywhere in the cause chain), as opposed to a transient or unreadable-permission failure. Used to
     * decide whether {@code hive.ignore_absent_partitions=false} should turn a skipped partition into a
     * loud "Partition location does not exist" error.
     */
    private static boolean isLocationNotFound(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof FileNotFoundException) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code inputFormat} is one of the hadoop-lzo text InputFormat variants (the twitter
     * {@code com.hadoop.compression.lzo.LzoTextInputFormat}, the anarres {@code com.hadoop.mapreduce.
     * LzoTextInputFormat}, and the legacy {@code com.hadoop.mapred.DeprecatedLzoTextInputFormat}). Such tables
     * read only {@code *.lzo} data files and must exclude {@code *.lzo.index} sidecars. Ports legacy
     * {@code HiveUtil.isLzoInputFormat}; the {@code contains("lzo")} match (rather than exact class names)
     * mirrors legacy and covers all variants alike. The connector cannot import fe-core, hence the local copy.
     * Package-private for unit testing.
     */
    static boolean isLzoInputFormat(String inputFormat) {
        return inputFormat != null && inputFormat.toLowerCase(Locale.ROOT).contains("lzo");
    }

    /**
     * For an LZO text InputFormat, only {@code *.lzo} files are data files; {@code *.lzo.index} sidecars and any
     * other extension are metadata to exclude, mirroring Hive's {@code LzoTextInputFormat.listStatus()}. Ports
     * legacy {@code HiveUtil.isLzoDataFile}. Package-private for unit testing.
     */
    static boolean isLzoDataFile(String filePath) {
        // Strip any object-store query string/fragment before matching the extension.
        String path = filePath;
        int q = path.indexOf('?');
        if (q >= 0) {
            path = path.substring(0, q);
        }
        return path.toLowerCase(Locale.ROOT).endsWith(".lzo");
    }

    /**
     * Normalizes a raw HMS storage URI into BE's canonical scheme for a BE-facing native reader path
     * (e.g. {@code s3a://}/{@code oss://}/{@code cos://} &rarr; {@code s3://}), delegating to the engine seam
     * {@link ConnectorContext#normalizeStorageUri(String)} — the connector cannot import fe-core's
     * {@code LocationPath}. BE's native S3 file factory (S3URI) accepts ONLY {@code s3://}, so an un-normalized
     * {@code s3a://} scan path fails the native read with "Invalid S3 URI". Mirrors iceberg/paimon/hudi and hive's
     * OWN write path ({@code HiveWritePlanProvider}); legacy {@code HiveScanNode} normalized via the 2-arg
     * {@code LocationPath.of(path, storagePropertiesMap)}. Non-object-store schemes (hdfs://, local) pass through
     * unchanged. A null context (offline unit tests) preserves the raw URI.
     */
    private String normalizeNativeUri(String rawUri) {
        return context != null ? context.normalizeStorageUri(rawUri) : rawUri;
    }

    /**
     * Splits a file into scan ranges based on target split size.
     *
     * <p>When {@code acidPartitionLocation} is non-null the ranges carry ACID delete-delta info
     * (marking them {@code transactional_hive}); otherwise they are plain Hive ranges.</p>
     */
    private void splitFile(String filePath, long fileSize, long modTime, PartitionScanInfo partition,
            HiveFileFormat fileFormat, boolean splittable, long targetSplitSize,
            String acidPartitionLocation, List<String> acidDeltas,
            List<ConnectorScanRange> ranges) {
        if (fileSize == 0) {
            return;
        }
        // Normalize the BE-facing native data-file path scheme (s3a://->s3://): the connector lists files via the
        // engine FileSystem with the raw scheme (Hadoop wants s3a), but BE's native S3 reader rejects s3a. ACID
        // delete-delta / partition paths are normalized separately at their emit sites.
        filePath = normalizeNativeUri(filePath);

        if (!splittable || targetSplitSize <= 0 || fileSize <= targetSplitSize) {
            // Single range for the whole file
            ranges.add(newRangeBuilder(filePath, 0, fileSize, fileSize, modTime, fileFormat,
                    partition, acidPartitionLocation, acidDeltas).build());
            return;
        }

        // Split file into ranges
        long offset = 0;
        while (offset < fileSize) {
            long splitSize = Math.min(targetSplitSize, fileSize - offset);
            ranges.add(newRangeBuilder(filePath, offset, splitSize, fileSize, modTime, fileFormat,
                    partition, acidPartitionLocation, acidDeltas).build());
            offset += splitSize;
        }
    }

    private static HiveScanRange.Builder newRangeBuilder(String filePath, long start, long length,
            long fileSize, long modTime, HiveFileFormat fileFormat, PartitionScanInfo partition,
            String acidPartitionLocation, List<String> acidDeltas) {
        HiveScanRange.Builder builder = HiveScanRange.builder()
                .path(filePath)
                .start(start)
                .length(length)
                .fileSize(fileSize)
                .modificationTime(modTime)
                .fileFormat(fileFormat.getFormatName())
                .tableFormatType("hive")
                .partitionValues(partition.partitionValues);
        if (acidPartitionLocation != null) {
            // Sets tableFormatType="transactional_hive" and attaches the delete-delta descriptors.
            builder.acidInfo(acidPartitionLocation, acidDeltas);
        }
        return builder;
    }

    private long getTargetSplitSize(ConnectorSession session) {
        String splitSizeStr = session.getProperty(
                "file_split_size", String.class);
        if (splitSizeStr != null && !splitSizeStr.isEmpty()) {
            try {
                long val = Long.parseLong(splitSizeStr);
                if (val > 0) {
                    return val;
                }
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return DEFAULT_TARGET_SPLIT_SIZE;
    }

    private boolean isLocationProperty(String key) {
        return key.startsWith("fs.")
                || key.startsWith("hadoop.")
                || key.startsWith("dfs.")
                || key.startsWith("s3.")
                || key.startsWith("s3a.")
                || key.startsWith("cos.")
                || key.startsWith("obs.")
                || key.startsWith("oss.")
                || key.equals("uri");
    }

    /**
     * Internal representation of a partition to scan.
     */
    private static final class PartitionScanInfo {
        final String location;
        final Map<String, String> partitionValues;
        final HiveFileFormat fileFormat;

        PartitionScanInfo(String location, Map<String, String> partitionValues,
                HiveFileFormat fileFormat) {
            this.location = location;
            this.partitionValues = partitionValues;
            this.fileFormat = fileFormat;
        }
    }
}
