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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsPartitionBatchResult;
import org.apache.doris.connector.hms.HmsPartitionBatchStats;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.spi.ConnectorContext;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStorageContext;
import org.apache.doris.connector.spi.ConnectorTaskScheduler;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanProfile;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ConnectorSplitSource;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.foundation.format.FileCompression;
import org.apache.doris.thrift.TFileCompressType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p>Tables can stream file splits without materializing a partition's ranges.</p>
 */

public class HiveScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(HiveScanPlanProvider.class);

    /** Default target split size: 256 MB. */
    private static final long DEFAULT_TARGET_SPLIT_SIZE = 256 * 1024 * 1024L;

    /** Maximum number of partitions to list from HMS. */
    private static final int MAX_PARTITIONS = 100000;
    private static final int DEFAULT_BATCH_THRESHOLD = 1024;
    private static final int DEFAULT_PARTITION_BATCH_SIZE = 16;

    /**
     * Session variable gating the OpenX-JSON "read the whole JSON row into one CSV column" mode (legacy
     * SessionVariable.read_hive_json_in_one_column). Byte-identical to the fe-core session-var name; it is
     * surfaced through ConnectorSession.getSessionProperties() (VariableMgr dumps all visible vars).
     */
    private static final String SESSION_READ_HIVE_JSON_IN_ONE_COLUMN = "read_hive_json_in_one_column";
    static final String SCAN_REUSE_NAMESPACE = "hms.scan-reuse";

    private final HmsClient hmsClient;
    private final HiveCatalogProperties catalogProperties;
    // Engine-owned, per-catalog filesystem accessor. The listing path borrows the Doris FileSystem via
    // storage().getFileSystem(session) (never closes it — the engine owns its lifecycle) to list partition
    // directories, replacing bare Hadoop FileSystem.get (FIX-HIVEFS: the hive plugin bundles no HDFS impl).
    private final ConnectorContext context;
    // Connector-owned directory-listing cache (the SAME instance HiveConnector shares with HiveConnectorMetadata),
    // so a repeated scan of the same partition directory is served from the cache instead of re-listing.
    private final HiveFileListingCache fileListingCache;
    private final PartitionBatchProfile partitionBatchProfile = new PartitionBatchProfile();

    public HiveScanPlanProvider(HmsClient hmsClient, HiveCatalogProperties catalogProperties,
            ConnectorContext context, HiveFileListingCache fileListingCache) {
        this.hmsClient = hmsClient;
        this.catalogProperties = catalogProperties;
        this.context = context;
        this.fileListingCache = fileListingCache;
    }

    @Override
    public boolean supportsFileCache() {
        // hive tables are read by BE's native parquet/orc/text readers, so the BE file cache applies.
        return true;
    }

    @Override
    public boolean usesHiveParquetInt96TimeZone() {
        return true;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        HiveTableHandle hiveHandle = (HiveTableHandle) request.getTableHandle();
        rejectTransactionalRead(hiveHandle);
        if (request.isPartitionsPrunedToEmpty()) {
            return Collections.emptyList();
        }
        if (session == null || !session.isExternalScanTaskReuseEnabled()) {
            return doPlanScan(session, request);
        }
        // Statement-scoped reuse: within one statement the identical scan (same table, same
        // partition set, same formats) plans once and every duplicated relation shares the result.
        // The scope is NONE for offline planning and tests, in which case the loader runs on every
        // call. Session variables are constant within a statement and deliberately absent. Every scan of a
        // statement is planned within one plan, so the memo is dropped when planning ends.
        String memoKey = SCAN_REUSE_NAMESPACE + ":" + session.getCatalogId() + ":" + session.getQueryId();
        Map<HiveScanReuseKey, List<ConnectorScanRange>> scanReuse =
                session.getStatementScope().computeIfAbsentForPlanning(memoKey, () -> new ConcurrentHashMap<>());
        HiveScanReuseKey reuseKey = new HiveScanReuseKey(hiveHandle, request.getRequiredPartitions());
        AtomicReference<List<ConnectorScanRange>> uncached = new AtomicReference<>();
        List<ConnectorScanRange> cached = scanReuse.computeIfAbsent(reuseKey, key -> {
            PlanCompleteness completeness = new PlanCompleteness();
            List<ConnectorScanRange> planned = Collections.unmodifiableList(
                    doPlanScan(session, request, completeness));
            if (!completeness.isComplete()) {
                // ConcurrentHashMap does not install a mapping when the loader returns null. Return this
                // caller's partial result below, but let the next identical alias retry the failed directory.
                uncached.set(planned);
                return null;
            }
            return planned;
        });
        return cached != null ? cached : Objects.requireNonNull(uncached.get());
    }

    private List<ConnectorScanRange> doPlanScan(ConnectorSession session, ConnectorScanRequest request) {
        return doPlanScan(session, request, null);
    }

    private List<ConnectorScanRange> doPlanScan(ConnectorSession session, ConnectorScanRequest request,
            PlanCompleteness completeness) {
        HiveTableHandle hiveHandle = (HiveTableHandle) request.getTableHandle();
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();
        recordPruningProfile(hiveHandle);

        // Typed FE pruning can leave an unfiltered handle when its full view was preloaded before planner
        // locks. Honor those names directly: do not enumerate or reselect the full partition universe here.
        List<String> requiredPartitions = request.getRequiredPartitions();
        List<PartitionScanInfo> partitions = requiredPartitions.isEmpty()
                ? resolvePartitions(hiveHandle)
                : convertPartitions(resolveBatchPartitions(hiveHandle, requiredPartitions, request.getScanContext()),
                        hiveHandle.getPartitionKeyNames());
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
        // reports splittable), so the LZO case must be masked out here.
        boolean splittable = fileFormat.isSplittable() && !isLzo;

        List<ConnectorScanRange> ranges = new ArrayList<>();

        // Borrow the engine's per-catalog Doris FileSystem to list partition directories (see field javadoc).
        FileSystem fs = storage().getFileSystem(session);
        for (PartitionScanInfo partition : partitions) {
            HiveFileFormat partFormat = partition.fileFormat != null
                    ? partition.fileFormat : fileFormat;
            listAndSplitFiles(dbName, tableName, partition, partFormat,
                    splittable, isLzo, targetSplitSize, fs, ranges, completeness);
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
     */
    @Override
    public boolean supportsBatchScan(ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        rejectTransactionalRead(hiveHandle);
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        return partKeyNames != null && !partKeyNames.isEmpty();
    }

    @Override
    public int partitionBatchSize(ConnectorSession session, int partitionCount, int parallelism) {
        int configured = sessionInt(session, "split_partition_batch_size", DEFAULT_PARTITION_BATCH_SIZE);
        return Math.min(configured, (partitionCount - 1) / parallelism + 1);
    }

    @Override
    public long streamingSplitEstimate(ConnectorSession session, ConnectorTableHandle handle,
            Optional<ConnectorExpression> filter, boolean countPushdown) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        rejectTransactionalRead(hiveHandle);
        if (Boolean.FALSE.equals(session.getProperty("enable_external_table_batch_mode", Boolean.class))) {
            return -1;
        }
        int fileThreshold = sessionInt(session, "num_files_in_batch_mode", DEFAULT_BATCH_THRESHOLD);
        long fileCount = cachedFileCount(hiveHandle);
        // Unknown file counts must not trigger a full listing on the planning thread. Use a
        // nonzero concurrency hint and let the lazy source perform the listing asynchronously.
        if (fileCount < 0) {
            return Math.max(DEFAULT_BATCH_THRESHOLD, fileThreshold);
        }
        return fileCount >= fileThreshold ? fileCount : -1;
    }

    private long cachedFileCount(HiveTableHandle handle) {
        List<String> partitionKeys = handle.getPartitionKeyNames();
        if (partitionKeys == null || partitionKeys.isEmpty()) {
            return fileListingCache.cachedFileCount(handle.getDbName(), handle.getTableName(),
                    handle.getLocation(), Collections.emptyList());
        }
        List<HmsPartitionInfo> partitions = handle.getPrunedPartitions();
        if (partitions == null) {
            return -1;
        }
        long fileCount = 0;
        for (HmsPartitionInfo partition : partitions) {
            long count = fileListingCache.cachedFileCount(handle.getDbName(), handle.getTableName(),
                    partition.getLocation(), partition.getValues());
            if (count < 0) {
                return -1;
            }
            fileCount += count;
        }
        return fileCount;
    }

    @Override
    public ConnectorSplitSource streamSplits(ConnectorSession session, ConnectorScanRequest request) {
        HiveTableHandle hiveHandle = (HiveTableHandle) request.getTableHandle();
        rejectTransactionalRead(hiveHandle);
        return new HiveSplitSource(session, hiveHandle, request.getRequiredPartitions(), request.getScanContext());
    }

    private static void rejectTransactionalRead(HiveTableHandle handle) {
        // Both full ACID and insert-only layouts need write-id visibility; plain file scans
        // (including metadata-only counts) cannot return a transactionally correct result.
        if (handle.isTransactional()) {
            throw new DorisConnectorException("Hive transactional table reads are no longer supported");
        }
    }

    private static int sessionInt(ConnectorSession session, String name, int defaultValue) {
        Integer value = session.getProperty(name, Integer.class);
        return value == null ? defaultValue : value;
    }

    private final class HiveSplitSource implements ConnectorSplitSource {
        private final HiveTableHandle handle;
        private final ConnectorScanContext scanContext;
        private final ConnectorTaskScheduler scheduler;
        private final FileSystem fs;
        private final HiveFileFormat tableFormat;
        private final boolean isLzo;
        private final boolean splittable;
        private final long targetSplitSize;
        private final int partitionBatchSize;
        private Iterator<PartitionScanInfo> partitions = Collections.emptyIterator();
        private Iterator<HiveFileStatus> files = Collections.emptyIterator();
        private Iterator<ConnectorScanRange> ranges = Collections.emptyIterator();
        private List<String> partitionNames;
        private PartitionScanInfo partition;
        private int nextPartition;
        private boolean unpartitionedInitialized;
        private volatile boolean closed;

        HiveSplitSource(ConnectorSession session, HiveTableHandle handle, List<String> requiredPartitions,
                ConnectorScanContext scanContext) {
            rejectTransactionalRead(handle);
            this.handle = handle;
            this.scanContext = scanContext;
            scanContext.checkCancelled();
            this.scheduler = session.getTaskScheduler(ConnectorTaskScheduler.Stage.SPLIT);
            this.partitionNames = requiredPartitions.isEmpty() ? null : requiredPartitions;
            this.fs = storage().getFileSystem(session);
            this.tableFormat = HiveFileFormat.detect(handle.getInputFormat(), handle.getSerializationLib(),
                    readHiveJsonInOneColumn(session), handle.isFirstColumnString());
            this.isLzo = isLzoInputFormat(handle.getInputFormat());
            this.splittable = tableFormat.isSplittable() && !isLzo;
            this.targetSplitSize = getTargetSplitSize(session);
            this.partitionBatchSize = Math.max(1,
                    sessionInt(session, "split_partition_batch_size", DEFAULT_PARTITION_BATCH_SIZE));
            recordPruningProfile(handle);
        }

        @Override
        public boolean hasNext() {
            // Empty partitions can keep hasNext() inside this source across many directory requests.
            while (!closed && !scheduler.isClosed()) {
                scanContext.checkCancelled();
                if (ranges.hasNext()) {
                    return true;
                }
                if (files.hasNext()) {
                    loadNextFileRanges();
                    continue;
                }
                if (partitions.hasNext()) {
                    loadNextPartitionFiles();
                    continue;
                }
                if (!loadNextPartitions()) {
                    return false;
                }
            }
            return false;
        }

        private void loadNextFileRanges() {
            HiveFileStatus file = files.next();
            if (isLzo && !isLzoDataFile(file.getPath())) {
                return;
            }
            HiveFileFormat format = partition.fileFormat == null ? tableFormat : partition.fileFormat;
            ranges = splitFileRanges(file.getPath(), file.getLength(), file.getModificationTime(),
                    partition, format, splittable, targetSplitSize);
        }

        private void loadNextPartitionFiles() {
            partition = partitions.next();
            files = listPartitionFiles(handle.getDbName(), handle.getTableName(),
                    partition, fs, null, scanContext).iterator();
            scanContext.partitionProcessed();
        }

        private boolean loadNextPartitions() {
            List<String> keys = handle.getPartitionKeyNames();
            if (keys == null || keys.isEmpty()) {
                if (unpartitionedInitialized) {
                    return false;
                }
                unpartitionedInitialized = true;
                partitions = Collections.singletonList(new PartitionScanInfo(
                        handle.getLocation(), Collections.emptyMap(), null)).iterator();
                return true;
            }
            List<HmsPartitionInfo> pruned = handle.getPrunedPartitions();
            if (partitionNames == null && pruned != null) {
                if (nextPartition == pruned.size()) {
                    return false;
                }
                int end = Math.min(pruned.size(), nextPartition + partitionBatchSize);
                partitions = convertPartitions(pruned.subList(nextPartition, end), keys).iterator();
                nextPartition = end;
                return true;
            }
            if (partitionNames == null) {
                scanContext.checkCancelled();
                partitionNames = hmsClient.listPartitionNames(
                        handle.getDbName(), handle.getTableName(), MAX_PARTITIONS, scanContext);
            }
            if (nextPartition == partitionNames.size()) {
                return false;
            }
            int end = Math.min(partitionNames.size(), nextPartition + partitionBatchSize);
            scanContext.checkCancelled();
            partitions = convertPartitions(resolveBatchPartitions(handle,
                    partitionNames.subList(nextPartition, end), scanContext), keys).iterator();
            nextPartition = end;
            return true;
        }

        @Override
        public ConnectorScanRange next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return ranges.next();
        }

        @Override
        public void close() {
            closed = true;
            partitions = Collections.emptyIterator();
            files = Collections.emptyIterator();
            ranges = Collections.emptyIterator();
            partitionNames = null;
            partition = null;
        }
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
     * {@link ConnectorScanPlanProvider#planScanForPartitionBatch}). While {@link #planScan} honors the request's
     * required partitions, this MUST scope resolution to {@code partitionBatch}: {@code PluginDrivenScanNode}
     * slices the pruned partition NAMES into batches and calls this once per batch. Resolving the request's whole
     * selection per batch would emit every partition's files once per batch — duplicate rows. The batch
     * strings are the metastore-rendered {@code key=value/...} partition names (the keys of the Nereids
     * selected-partition map), exactly the form {@code hmsClient.getPartitions} accepts, so batch-scoped
     * resolution is a clean {@code getPartitions(batch)} round-trip. Collects the same partition-scoped source
     * that {@link #streamSplits} consumes lazily, preserving its file filtering and split boundaries.
     */
    @Override
    public List<ConnectorScanRange> planScanForPartitionBatch(
            ConnectorSession session,
            ConnectorScanRequest request,
            List<String> partitionBatch) {
        // Batch mode deliberately does not retain completed ranges in the statement scope: its
        // purpose is to bound FE memory while splits are streamed to the coordinator. Caching every
        // batch until statement close would materialize the full scan again and defeat that bound.
        // An empty batch contains no partitions; an empty streaming request instead means scan all.
        if (partitionBatch.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorScanRange> ranges = new ArrayList<>();
        try (HiveSplitSource source = new HiveSplitSource(
                session, (HiveTableHandle) request.getTableHandle(), partitionBatch, request.getScanContext())) {
            while (source.hasNext()) {
                ranges.add(source.next());
            }
        }
        return ranges;
    }

    private List<HmsPartitionInfo> resolveBatchPartitions(HiveTableHandle handle, List<String> partitionBatch,
            ConnectorScanContext scanContext) {
        Map<String, HmsPartitionInfo> retained = handle.getPrunedPartitionsByName();
        if (retained.isEmpty()) {
            return loadPartitionsWithProfile(handle.getDbName(), handle.getTableName(), partitionBatch, scanContext);
        }
        List<HmsPartitionInfo> partitions = new ArrayList<>(partitionBatch.size());
        boolean covered = true;
        for (String partitionName : partitionBatch) {
            HmsPartitionInfo partition = retained.get(partitionName);
            partitions.add(partition);
            if (partition == null) {
                covered = false;
            }
        }
        if (covered) {
            return partitions;
        }
        // The retained map covers exactly what THIS scan's connector predicate admitted, which can be narrower
        // than the logical selection this batch came from: the connector converter declines an expression the
        // physical converter strips (e.g. CAST(p AS INT) = 1 becomes the bare p = '1' handle), so a typed
        // logical prune may legitimately select names the map does not hold, and no HMS mutation is needed for
        // that. The native map is not proven to cover the logical selection, so resolve the whole batch by name
        // exactly as the pre-cutover batch path did - one consistent generation, never a mixed one.
        return loadPartitionsWithProfile(handle.getDbName(), handle.getTableName(), partitionBatch, scanContext);
    }

    @Override
    public List<ConnectorScanProfile> collectScanProfiles(ConnectorSession session) {
        List<ConnectorScanProfile> profiles = new ArrayList<>(partitionBatchProfile.drain());
        profiles.addAll(statementPruningFailureProfile(session).drain());
        return profiles;
    }

    static void recordPruningFailure(ConnectorSession session, String dbName, String tableName,
            HmsPartitionBatchStats stats) {
        statementPruningFailureProfile(session).recordFailure(dbName, tableName, stats);
    }

    private static PartitionBatchProfile statementPruningFailureProfile(ConnectorSession session) {
        if (session == null) {
            return new PartitionBatchProfile();
        }
        String key = "hive.partition_batch_failures:" + session.getCatalogId() + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, PartitionBatchProfile::new);
    }

    @Override
    public Map<String, String> getScanNodeProperties(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        rejectTransactionalRead(hiveHandle);
        Map<String, String> props = new HashMap<>();

        // File format type
        HiveFileFormat fileFormat = HiveFileFormat.detect(
                hiveHandle.getInputFormat(), hiveHandle.getSerializationLib(),
                readHiveJsonInOneColumn(session), hiveHandle.isFirstColumnString());
        props.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, fileFormat.getFormatName());

        // Partition key column names
        List<String> partKeys = hiveHandle.getPartitionKeyNames();
        if (partKeys != null && !partKeys.isEmpty()) {
            props.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS, String.join(",", partKeys));
        }

        // Location properties (Hadoop/S3 config for BE file access).
        //  (1) BE-canonical static credentials (AWS_* for object stores, resolved hadoop.*/dfs.* for HDFS): BE's
        //      native (FILE_S3) reader understands ONLY these canonical keys — it reads AWS_ACCESS_KEY /
        //      AWS_SECRET_KEY / AWS_ENDPOINT (s3_util.cpp), NOT the raw s3.access_key/... aliases — so without
        //      them a private bucket 403s. Legacy HiveScanNode.getLocationProperties() emitted exactly this
        //      (hmsTable.getBackendStorageProperties()); the new path had dropped it. Empty for a null context
        //      (offline tests) or a credential-less warehouse.
        if (context != null) {
            storage().getBackendStorageProperties()
                    .forEach((k, v) -> props.put(ScanNodePropertyKeys.LOCATION_PREFIX + k, v));
        }
        //  (2) Raw catalog aliases + inline fs./hadoop./dfs. keys. Emitted AFTER the canonical set so a user-inline
        //      fs./hadoop. key wins; the s3./oss./cos./obs. aliases are harmless to BE (ignored by the native
        //      reader) but kept so no configured key is dropped.
        for (Map.Entry<String, String> entry : catalogProperties.getRaw().entrySet()) {
            String key = entry.getKey();
            if (isLocationProperty(key)) {
                props.put(ScanNodePropertyKeys.LOCATION_PREFIX + key, entry.getValue());
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

        return props;
    }

    /**
     * Reads the {@code read_hive_json_in_one_column} session flag (default false) from the connector session —
     * the same channel the write path uses for {@code hive_text_compression}. The engine dumps every visible
     * session variable into {@code getSessionProperties()}, so no extra plumbing is needed.
     */
    private static boolean readHiveJsonInOneColumn(ConnectorSession session) {
        return Boolean.parseBoolean(session.getSessionProperties()
                .getOrDefault(SESSION_READ_HIVE_JSON_IN_ONE_COLUMN, "false"));
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
            recordPruningProfile(handle);
            return convertPartitions(prunedPartitions, partKeyNames);
        }

        // No pruning: list all partitions from HMS
        List<String> partNames = hmsClient.listPartitionNames(
                handle.getDbName(), handle.getTableName(), MAX_PARTITIONS);
        if (partNames.isEmpty()) {
            return Collections.emptyList();
        }
        List<HmsPartitionInfo> hmsPartitions = loadPartitionsWithProfile(
                handle.getDbName(), handle.getTableName(), partNames);
        return convertPartitions(hmsPartitions, partKeyNames);
    }

    private List<HmsPartitionInfo> loadPartitionsWithProfile(
            String dbName, String tableName, List<String> partitionNames) {
        return loadPartitionsWithProfile(dbName, tableName, partitionNames, null);
    }

    private List<HmsPartitionInfo> loadPartitionsWithProfile(String dbName, String tableName,
            List<String> partitionNames, ConnectorScanContext scanContext) {
        try {
            HmsPartitionBatchResult result = scanContext == null
                    ? hmsClient.getExistingPartitionsWithStats(dbName, tableName, partitionNames)
                    : hmsClient.getExistingPartitionsWithStats(dbName, tableName, partitionNames, scanContext);
            partitionBatchProfile.record(dbName, tableName, result.getStats());
            return result.getPartitions();
        } catch (HmsClientException e) {
            if (e.getPartitionBatchStats() != null) {
                partitionBatchProfile.recordFailure(dbName, tableName, e.getPartitionBatchStats());
            }
            throw e;
        }
    }

    void recordPruningProfile(HiveTableHandle handle) {
        if (handle.getPruningBatchStats() != null) {
            partitionBatchProfile.recordOnce(
                    handle.getDbName(), handle.getTableName(), handle.getPruningBatchStats());
        }
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
            List<ConnectorScanRange> ranges, PlanCompleteness completeness) {
        for (HiveFileStatus file : listPartitionFiles(dbName, tableName, partition, fs, completeness)) {
            if (isLzo && !isLzoDataFile(file.getPath())) {
                continue;
            }
            splitFile(file.getPath(), file.getLength(), file.getModificationTime(),
                    partition, fileFormat, splittable, targetSplitSize, ranges);
        }
    }

    private List<HiveFileStatus> listPartitionFiles(String dbName, String tableName,
            PartitionScanInfo partition, FileSystem fs, PlanCompleteness completeness) {
        return listPartitionFiles(dbName, tableName, partition, fs, completeness, null);
    }

    private List<HiveFileStatus> listPartitionFiles(String dbName, String tableName,
            PartitionScanInfo partition, FileSystem fs, PlanCompleteness completeness,
            ConnectorScanContext scanContext) {
        List<HiveFileStatus> files;
        try {
            List<String> values = new ArrayList<>(partition.partitionValues.values());
            files = scanContext == null
                    ? fileListingCache.listDataFiles(dbName, tableName, partition.location, values, fs)
                    : fileListingCache.listDataFiles(dbName, tableName, partition.location, values, fs, scanContext);
        } catch (HiveDirectoryListingException e) {
            // hive.ignore_absent_partitions=false (non-default): a partition whose LOCATION does not exist
            // must fail the query loud with the legacy message rather than be silently skipped. Only a
            // not-found cause counts as "absent"; a transient/unreadable listing failure still follows the
            // tolerant skip-with-warning path below. Default true preserves the skip behavior.
            if (isLocationNotFound(e) && !catalogProperties.isIgnoreAbsentPartitions()) {
                throw new DorisConnectorException(
                        "Partition location does not exist: " + partition.location, e);
            }
            if (completeness != null) {
                completeness.markIncomplete();
            }
            LOG.warn("Cannot list files in partition: {}", partition.location, e);
            return Collections.emptyList();
        }
        return files;
    }

    private static final class PlanCompleteness {
        private boolean complete = true;

        private void markIncomplete() {
            complete = false;
        }

        private boolean isComplete() {
            return complete;
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
     * {@link ConnectorStorageContext#normalizeStorageUri(String)} — the connector cannot import fe-core's
     * {@code LocationPath}. BE's native S3 file factory (S3URI) accepts ONLY {@code s3://}, so an un-normalized
     * {@code s3a://} scan path fails the native read with "Invalid S3 URI". Mirrors iceberg/paimon/hudi and hive's
     * OWN write path ({@code HiveWritePlanProvider}); legacy {@code HiveScanNode} normalized via the 2-arg
     * {@code LocationPath.of(path, storagePropertiesMap)}. Non-object-store schemes (hdfs://, local) pass through
     * unchanged. A null context (offline unit tests) preserves the raw URI.
     */
    private String normalizeNativeUri(String rawUri) {
        return context != null ? storage().normalizeStorageUri(rawUri) : rawUri;
    }

    /**
     * Splits a file into scan ranges based on target split size.
     */
    private void splitFile(String filePath, long fileSize, long modTime, PartitionScanInfo partition,
            HiveFileFormat fileFormat, boolean splittable, long targetSplitSize,
            List<ConnectorScanRange> ranges) {
        splitFileRanges(filePath, fileSize, modTime, partition, fileFormat, splittable,
                targetSplitSize).forEachRemaining(ranges::add);
    }

    private Iterator<ConnectorScanRange> splitFileRanges(String filePath, long fileSize, long modTime,
            PartitionScanInfo partition, HiveFileFormat fileFormat, boolean splittable, long targetSplitSize) {
        if (fileSize == 0) {
            return Collections.emptyIterator();
        }
        String normalizedPath = normalizeNativeUri(filePath);
        // Native text/CSV readers cannot restart compressed streams at nonzero offsets, even for
        // codecs Hadoop considers splittable. Use the same inference as the codec sent to BE;
        // columnar readers handle range boundaries independently of filename compression suffixes.
        boolean compressedText = (fileFormat == HiveFileFormat.TEXT || fileFormat == HiveFileFormat.CSV)
                && FileCompression.fromPath(normalizedPath) != FileCompression.PLAIN;
        long splitSize = splittable && !compressedText && targetSplitSize > 0 ? targetSplitSize : fileSize;
        return new Iterator<ConnectorScanRange>() {
            private long offset;

            @Override
            public boolean hasNext() {
                return offset < fileSize;
            }

            @Override
            public ConnectorScanRange next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                long length = Math.min(splitSize, fileSize - offset);
                ConnectorScanRange range = newRangeBuilder(normalizedPath, offset, length, fileSize,
                        modTime, fileFormat, partition).build();
                offset += length;
                return range;
            }
        };
    }

    private static HiveScanRange.Builder newRangeBuilder(String filePath, long start, long length,
            long fileSize, long modTime, HiveFileFormat fileFormat, PartitionScanInfo partition) {
        HiveScanRange.Builder builder = HiveScanRange.builder()
                .path(filePath)
                .start(start)
                .length(length)
                .fileSize(fileSize)
                .modificationTime(modTime)
                .fileFormat(fileFormat.getFormatName())
                .tableFormatType("hive")
                .partitionValues(partition.partitionValues);
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

    /** This catalog's engine-owned storage services (see {@link ConnectorContext#getStorageContext()}). */
    private ConnectorStorageContext storage() {
        return context.getStorageContext();
    }

    /** Thread-safe aggregation because partition-batch scan planning runs on the shared metadata executor. */
    private static final class PartitionBatchProfile {
        private String tableLabel;
        private int logicalRequests;
        private int failedRequests;
        private long requestedItems;
        private long transportInvocations;
        private long transportItems;
        private int largestBatchSize;
        private int smallestBatchSize;
        private long fallbacks;
        private long logicalElapsedNanos;
        private long transportElapsedNanos;
        private long maxTransportElapsedNanos;
        private boolean initialRequestRecorded;

        synchronized void recordOnce(String dbName, String tableName, HmsPartitionBatchStats stats) {
            if (!initialRequestRecorded) {
                record(dbName, tableName, stats);
                initialRequestRecorded = true;
            }
        }

        synchronized void record(String dbName, String tableName, HmsPartitionBatchStats stats) {
            tableLabel = dbName + "." + tableName;
            logicalRequests++;
            requestedItems += stats.getRequestedItems();
            transportInvocations += stats.getTransportInvocations();
            transportItems += stats.getTransportItems();
            largestBatchSize = Math.max(largestBatchSize, stats.getLargestBatchSize());
            if (stats.getSmallestBatchSize() > 0) {
                smallestBatchSize = smallestBatchSize == 0
                        ? stats.getSmallestBatchSize()
                        : Math.min(smallestBatchSize, stats.getSmallestBatchSize());
            }
            fallbacks += stats.getFallbackCount();
            logicalElapsedNanos += stats.getLogicalElapsedNanos();
            transportElapsedNanos += stats.getTransportElapsedNanos();
            maxTransportElapsedNanos = Math.max(
                    maxTransportElapsedNanos, stats.getMaxTransportElapsedNanos());
        }

        synchronized void recordFailure(String dbName, String tableName, HmsPartitionBatchStats stats) {
            record(dbName, tableName, stats);
            failedRequests++;
        }

        synchronized List<ConnectorScanProfile> drain() {
            if (logicalRequests == 0) {
                return Collections.emptyList();
            }
            Map<String, String> metrics = new LinkedHashMap<>();
            metrics.put("LogicalRequests", String.valueOf(logicalRequests));
            metrics.put("FailedRequests", String.valueOf(failedRequests));
            metrics.put("RequestedItems", String.valueOf(requestedItems));
            metrics.put("TransportInvocations", String.valueOf(transportInvocations));
            metrics.put("TransportItems", String.valueOf(transportItems));
            metrics.put("LargestBatchSize", String.valueOf(largestBatchSize));
            metrics.put("SmallestBatchSize", String.valueOf(smallestBatchSize));
            metrics.put("Fallbacks", String.valueOf(fallbacks));
            metrics.put("LogicalElapsedTime", formatNanos(logicalElapsedNanos));
            metrics.put("TransportElapsedTime", formatNanos(transportElapsedNanos));
            metrics.put("MaxTransportElapsedTime", formatNanos(maxTransportElapsedNanos));
            ConnectorScanProfile profile = new ConnectorScanProfile(
                    "Connector Metadata Access", "hms.get_partitions_by_names [QUERY] (" + tableLabel + ")",
                    metrics);
            logicalRequests = 0;
            failedRequests = 0;
            requestedItems = 0;
            transportInvocations = 0;
            transportItems = 0;
            largestBatchSize = 0;
            smallestBatchSize = 0;
            fallbacks = 0;
            logicalElapsedNanos = 0;
            transportElapsedNanos = 0;
            maxTransportElapsedNanos = 0;
            initialRequestRecorded = false;
            return Collections.singletonList(profile);
        }

        private static String formatNanos(long nanos) {
            return TimeUnit.NANOSECONDS.toMillis(nanos) + "ms";
        }
    }

    /**
     * Statement-scoped cache key for one Hive scan.
     *
     * <p>Includes every input that changes the planned split list: table identity, the file formats
     * (input format / serialization lib / JSON single-column gate), the partition keys and the
     * pruned partition set (each partition's location and values). ACID tables are excluded
     * upstream, and session variables are statement-constant, so both stay out of the key.
     */
    private static final class HiveScanReuseKey {
        private final String dbName;
        private final String tableName;
        private final String location;
        private final String inputFormat;
        private final String serializationLib;
        private final boolean firstColumnIsString;
        private final List<String> partitionKeyNames;
        private final List<HmsPartitionInfo> prunedPartitions;
        private final List<String> requiredPartitions;

        private HiveScanReuseKey(HiveTableHandle handle, List<String> requiredPartitions) {
            // Catalog and query isolation are provided by the statement-scope memo key. The table
            // location identifies the data source of unpartitioned tables, whose prunedPartitions
            // is null.
            this.dbName = handle.getDbName();
            this.tableName = handle.getTableName();
            this.location = handle.getLocation();
            this.inputFormat = handle.getInputFormat();
            this.serializationLib = handle.getSerializationLib();
            this.firstColumnIsString = handle.isFirstColumnString();
            this.partitionKeyNames = handle.getPartitionKeyNames() == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(new ArrayList<>(handle.getPartitionKeyNames()));
            this.prunedPartitions = handle.getPrunedPartitions() == null
                    ? null
                    : Collections.unmodifiableList(new ArrayList<>(handle.getPrunedPartitions()));
            this.requiredPartitions = Collections.unmodifiableList(new ArrayList<>(requiredPartitions));
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof HiveScanReuseKey)) {
                return false;
            }
            HiveScanReuseKey that = (HiveScanReuseKey) object;
            return firstColumnIsString == that.firstColumnIsString
                    && Objects.equals(dbName, that.dbName)
                    && Objects.equals(tableName, that.tableName)
                    && Objects.equals(location, that.location)
                    && Objects.equals(inputFormat, that.inputFormat)
                    && Objects.equals(serializationLib, that.serializationLib)
                    && Objects.equals(partitionKeyNames, that.partitionKeyNames)
                    && Objects.equals(prunedPartitions, that.prunedPartitions)
                    && Objects.equals(requiredPartitions, that.requiredPartitions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dbName, tableName, location,
                    inputFormat, serializationLib, firstColumnIsString,
                    partitionKeyNames, prunedPartitions, requiredPartitions);
        }

        @Override
        public String toString() {
            return "HiveScanReuseKey{table=" + dbName + "." + tableName + "}";
        }
    }
}
