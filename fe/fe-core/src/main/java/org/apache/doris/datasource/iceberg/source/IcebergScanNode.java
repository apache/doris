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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.ExternalUtil;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.datasource.iceberg.cache.IcebergManifestCache;
import org.apache.doris.datasource.iceberg.cache.IcebergManifestCacheLoader;
import org.apache.doris.datasource.iceberg.cache.ManifestCacheValue;
import org.apache.doris.datasource.iceberg.profile.IcebergMetricsReporter;
import org.apache.doris.datasource.iceberg.source.IcebergDeleteFileFilter.EqualityDelete;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergDeleteFileDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFileIndex;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.ScanTaskUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergScanNode extends FileQueryScanNode {

    public static final int MIN_DELETE_FILE_SUPPORT_VERSION = 2;
    private static final Logger LOG = LogManager.getLogger(IcebergScanNode.class);

    private IcebergSource source;
    private Table icebergTable;
    private List<String> pushdownIcebergPredicates = Lists.newArrayList();
    // If tableLevelPushDownCount is true, means we can do count push down opt at table level.
    // which means all splits have no position/equality delete files,
    // so for query like "select count(*) from ice_tbl", we can get count from snapshot row count info directly.
    // If tableLevelPushDownCount is false, means we can't do count push down opt at table level,
    // But for part of splits which have no position/equality delete files, we can still do count push down opt.
    // And for split level count push down opt, the flag is set in each split.
    private boolean tableLevelPushDownCount = false;
    private long countFromSnapshot;
    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;
    private long targetSplitSize = 0;
    // This is used to avoid repeatedly calculating partition info map for the same partition data.
    private Map<PartitionData, Map<String, String>> partitionMapInfos;
    private boolean isPartitionedTable;
    private int formatVersion;
    private ExecutionAuthenticator preExecutionAuthenticator;
    private TableScan icebergTableScan;
    // Store PropertiesMap, including vended credentials or static credentials
    // get them in doInitialize() to ensure internal consistency of ScanNode
    private Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;
    private Map<String, String> backendStorageProperties;
    private long manifestCacheHits;
    private long manifestCacheMisses;
    private long manifestCacheFailures;

    // Cached values for LocationPath creation optimization
    // These are lazily initialized on first use to avoid parsing overhead for each file
    private boolean locationPathCacheInitialized = false;
    private StorageProperties cachedStorageProperties;
    private String cachedSchema;
    private String cachedFsIdPrefix;
    // Cache for path prefix transformation to avoid repeated S3URI parsing
    // Maps original path prefix (e.g., "https://bucket.s3.amazonaws.com/") to normalized prefix (e.g., "s3://bucket/")
    private String cachedOriginalPathPrefix;
    private String cachedNormalizedPathPrefix;
    private String cachedFsIdentifier;

    private Boolean isBatchMode = null;

    // for test
    @VisibleForTesting
    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        super(id, desc, "ICEBERG_SCAN_NODE", false, sv);
    }

    /**
     * External file scan node for Query iceberg table
     * needCheckColumnPriv: Some of ExternalFileScanNode do not need to check column priv
     * eg: s3 tvf
     * These scan nodes do not have corresponding catalog/database/table info, so no need to do priv check
     */
    public IcebergScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv, SessionVariable sv) {
        super(id, desc, "ICEBERG_SCAN_NODE", needCheckColumnPriv, sv);

        ExternalTable table = (ExternalTable) desc.getTable();
        if (table instanceof HMSExternalTable) {
            source = new IcebergHMSSource((HMSExternalTable) table, desc);
        } else if (table instanceof IcebergExternalTable) {
            String catalogType = ((IcebergExternalTable) table).getIcebergCatalogType();
            switch (catalogType) {
                case IcebergExternalCatalog.ICEBERG_HMS:
                case IcebergExternalCatalog.ICEBERG_REST:
                case IcebergExternalCatalog.ICEBERG_DLF:
                case IcebergExternalCatalog.ICEBERG_GLUE:
                case IcebergExternalCatalog.ICEBERG_HADOOP:
                case IcebergExternalCatalog.ICEBERG_JDBC:
                case IcebergExternalCatalog.ICEBERG_S3_TABLES:
                    source = new IcebergApiSource((IcebergExternalTable) table, desc, columnNameToRange);
                    break;
                default:
                    Preconditions.checkState(false, "Unknown iceberg catalog type: " + catalogType);
                    break;
            }
        }
        Preconditions.checkNotNull(source);
    }

    @Override
    protected void doInitialize() throws UserException {
        icebergTable = source.getIcebergTable();
        partitionMapInfos = new HashMap<>();
        isPartitionedTable = icebergTable.spec().isPartitioned();
        // Metadata tables (system tables) are not BaseTable instances, so we need to handle this case
        if (icebergTable instanceof BaseTable) {
            formatVersion = ((BaseTable) icebergTable).operations().current().formatVersion();
        } else {
            // For metadata tables (e.g., snapshots, history), use a default format version
            // These tables are always readable regardless of format version
            formatVersion = MIN_DELETE_FILE_SUPPORT_VERSION;
        }
        preExecutionAuthenticator = source.getCatalog().getExecutionAuthenticator();
        storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                source.getCatalog().getCatalogProperty().getMetastoreProperties(),
                source.getCatalog().getCatalogProperty().getStoragePropertiesMap(),
                icebergTable
        );
        backendStorageProperties = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);
        super.doInitialize();
        ExternalUtil.initSchemaInfo(params, -1L, source.getTargetTable().getColumns());
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        if (split instanceof IcebergSplit) {
            setIcebergParams(rangeDesc, (IcebergSplit) split);
        }
    }

    private void setIcebergParams(TFileRangeDesc rangeDesc, IcebergSplit icebergSplit) {
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(icebergSplit.getTableFormatType().value());
        if (tableLevelPushDownCount) {
            tableFormatFileDesc.setTableLevelRowCount(icebergSplit.getTableLevelRowCount());
        } else {
            // MUST explicitly set to -1, to be distinct from valid row count >= 0
            tableFormatFileDesc.setTableLevelRowCount(-1);
        }
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setFormatVersion(formatVersion);
        fileDesc.setOriginalFilePath(icebergSplit.getOriginalPath());
        if (formatVersion < MIN_DELETE_FILE_SUPPORT_VERSION) {
            fileDesc.setContent(FileContent.DATA.id());
        } else {
            for (IcebergDeleteFileFilter filter : icebergSplit.getDeleteFileFilters()) {
                TIcebergDeleteFileDesc deleteFileDesc = new TIcebergDeleteFileDesc();
                String deleteFilePath = filter.getDeleteFilePath();
                LocationPath locationPath = LocationPath.of(deleteFilePath, icebergSplit.getConfig());
                deleteFileDesc.setPath(locationPath.toStorageLocation().toString());
                if (filter instanceof IcebergDeleteFileFilter.PositionDelete) {
                    IcebergDeleteFileFilter.PositionDelete positionDelete =
                            (IcebergDeleteFileFilter.PositionDelete) filter;
                    OptionalLong lowerBound = positionDelete.getPositionLowerBound();
                    OptionalLong upperBound = positionDelete.getPositionUpperBound();
                    if (lowerBound.isPresent()) {
                        deleteFileDesc.setPositionLowerBound(lowerBound.getAsLong());
                    }
                    if (upperBound.isPresent()) {
                        deleteFileDesc.setPositionUpperBound(upperBound.getAsLong());
                    }
                    deleteFileDesc.setContent(IcebergDeleteFileFilter.PositionDelete.type());

                    if (filter instanceof IcebergDeleteFileFilter.DeletionVector) {
                        IcebergDeleteFileFilter.DeletionVector dv = (IcebergDeleteFileFilter.DeletionVector) filter;
                        deleteFileDesc.setContentOffset((int) dv.getContentOffset());
                        deleteFileDesc.setContentSizeInBytes((int) dv.getContentLength());
                        deleteFileDesc.setContent(IcebergDeleteFileFilter.DeletionVector.type());
                    }
                } else {
                    IcebergDeleteFileFilter.EqualityDelete equalityDelete =
                            (IcebergDeleteFileFilter.EqualityDelete) filter;
                    deleteFileDesc.setFieldIds(equalityDelete.getFieldIds());
                    deleteFileDesc.setContent(EqualityDelete.type());
                }
                fileDesc.addToDeleteFiles(deleteFileDesc);
            }
        }
        tableFormatFileDesc.setIcebergParams(fileDesc);
        Map<String, String> partitionValues = icebergSplit.getIcebergPartitionValues();
        if (partitionValues != null) {
            List<String> fromPathKeys = new ArrayList<>();
            List<String> fromPathValues = new ArrayList<>();
            List<Boolean> fromPathIsNull = new ArrayList<>();
            for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
                fromPathKeys.add(entry.getKey());
                fromPathValues.add(entry.getValue() != null ? entry.getValue() : "");
                fromPathIsNull.add(entry.getValue() == null);
            }
            rangeDesc.setColumnsFromPathKeys(fromPathKeys);
            rangeDesc.setColumnsFromPath(fromPathValues);
            rangeDesc.setColumnsFromPathIsNull(fromPathIsNull);
        }
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc) {
        List<String> deleteFiles = new ArrayList<>();
        if (rangeDesc == null || !rangeDesc.isSetTableFormatParams()) {
            return deleteFiles;
        }
        TTableFormatFileDesc tableFormatParams = rangeDesc.getTableFormatParams();
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

    private String getDeleteFileContentType(int content) {
        // Iceberg file type: 0: data, 1: position delete, 2: equality delete, 3: deletion vector
        switch (content) {
            case 1:
                return "position_delete";
            case 2:
                return "equality_delete";
            case 3:
                return "deletion_vector";
            default:
                return "unknown";
        }
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        try {
            return preExecutionAuthenticator.execute(() -> doGetSplits(numBackends));
        } catch (Exception e) {
            Optional<NotSupportedException> opt = checkNotSupportedException(e);
            if (opt.isPresent()) {
                throw opt.get();
            } else {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
    }

    /**
     * Get FileScanTasks from StatementContext for rewrite operations.
     * This allows setting file scan tasks before the plan is generated.
     */
    private List<FileScanTask> getFileScanTasksFromContext() {
        ConnectContext ctx = ConnectContext.get();
        Preconditions.checkNotNull(ctx);
        Preconditions.checkNotNull(ctx.getStatementContext());

        // Get the rewrite file scan tasks from statement context
        List<FileScanTask> tasks = ctx.getStatementContext().getAndClearIcebergRewriteFileScanTasks();
        if (tasks != null && !tasks.isEmpty()) {
            LOG.info("Retrieved {} file scan tasks from context for table {}",
                    tasks.size(), icebergTable.name());
            return new ArrayList<>(tasks);
        }
        return null;
    }

    @Override
    public void startSplit(int numBackends) throws UserException {
        try {
            preExecutionAuthenticator.execute(() -> {
                doStartSplit();
                return null;
            });
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    public void doStartSplit() throws UserException {
        TableScan scan = createTableScan();
        CompletableFuture.runAsync(() -> {
            AtomicReference<CloseableIterable<FileScanTask>> taskRef = new AtomicReference<>();
            try {
                preExecutionAuthenticator.execute(
                        () -> {
                            CloseableIterable<FileScanTask> fileScanTasks = planFileScanTask(scan);
                            taskRef.set(fileScanTasks);

                            CloseableIterator<FileScanTask> iterator = fileScanTasks.iterator();
                            while (splitAssignment.needMoreSplit() && iterator.hasNext()) {
                                try {
                                    splitAssignment.addToQueue(Lists.newArrayList(createIcebergSplit(iterator.next())));
                                } catch (UserException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                );
                splitAssignment.finishSchedule();
                recordManifestCacheProfile();
            } catch (Exception e) {
                Optional<NotSupportedException> opt = checkNotSupportedException(e);
                if (opt.isPresent()) {
                    splitAssignment.setException(new UserException(opt.get().getMessage(), opt.get()));
                } else {
                    splitAssignment.setException(new UserException(e.getMessage(), e));
                }
            } finally {
                if (taskRef.get() != null) {
                    try {
                        taskRef.get().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }, Env.getCurrentEnv().getExtMetaCacheMgr().getScheduleExecutor());
    }

    @VisibleForTesting
    public TableScan createTableScan() throws UserException {
        if (icebergTableScan != null) {
            return icebergTableScan;
        }

        TableScan scan = icebergTable.newScan().metricsReporter(new IcebergMetricsReporter());

        // set snapshot
        IcebergTableQueryInfo info = getSpecifiedSnapshot();
        if (info != null) {
            if (info.getRef() != null) {
                scan = scan.useRef(info.getRef());
            } else {
                scan = scan.useSnapshot(info.getSnapshotId());
            }
        }

        // set filter
        List<Expression> expressions = new ArrayList<>();
        for (Expr conjunct : conjuncts) {
            Expression expression = IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema());
            if (expression != null) {
                expressions.add(expression);
            }
        }
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
            this.pushdownIcebergPredicates.add(predicate.toString());
        }

        icebergTableScan = scan.planWith(source.getCatalog().getThreadPoolWithPreAuth());

        return icebergTableScan;
    }

    private CloseableIterable<FileScanTask> planFileScanTask(TableScan scan) {
        if (!IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            return splitFiles(scan);
        }
        try {
            return planFileScanTaskWithManifestCache(scan);
        } catch (Exception e) {
            manifestCacheFailures++;
            LOG.warn("Plan with manifest cache failed, fallback to original scan: " + e.getMessage(), e);
            return splitFiles(scan);
        }
    }

    private CloseableIterable<FileScanTask> splitFiles(TableScan scan) {
        if (sessionVariable.getFileSplitSize() > 0) {
            return TableScanUtil.splitFiles(scan.planFiles(),
                    sessionVariable.getFileSplitSize());
        }
        if (isBatchMode()) {
            // Currently iceberg batch split mode will use max split size.
            // TODO: dynamic split size in batch split mode need to customize iceberg splitter.
            return TableScanUtil.splitFiles(scan.planFiles(), sessionVariable.getMaxSplitSize());
        }

        // Non Batch Mode
        // Materialize planFiles() into a list to avoid iterating the CloseableIterable twice.
        // RISK: It will cost memory if the table is large.
        List<FileScanTask> fileScanTaskList = new ArrayList<>();
        try (CloseableIterable<FileScanTask> scanTasksIter = scan.planFiles()) {
            for (FileScanTask task : scanTasksIter) {
                fileScanTaskList.add(task);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to materialize file scan tasks", e);
        }

        targetSplitSize = determineTargetFileSplitSize(fileScanTaskList);
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTaskList), targetSplitSize);
    }

    private long determineTargetFileSplitSize(Iterable<FileScanTask> tasks) {
        long result = sessionVariable.getMaxInitialSplitSize();
        long accumulatedTotalFileSize = 0;
        boolean exceedInitialThreshold = false;
        for (FileScanTask task : tasks) {
            accumulatedTotalFileSize += ScanTaskUtil.contentSizeInBytes(task.file());
            if (!exceedInitialThreshold && accumulatedTotalFileSize
                    >= sessionVariable.getMaxSplitSize() * sessionVariable.getMaxInitialSplitNum()) {
                exceedInitialThreshold = true;
            }
        }
        result = exceedInitialThreshold ? sessionVariable.getMaxSplitSize() : result;
        if (!isBatchMode()) {
            result = applyMaxFileSplitNumLimit(result, accumulatedTotalFileSize);
        }
        return result;
    }

    private CloseableIterable<FileScanTask> planFileScanTaskWithManifestCache(TableScan scan) throws IOException {
        // Get the snapshot from the scan; return empty if no snapshot exists
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return CloseableIterable.withNoopClose(Collections.emptyList());
        }

        // Initialize manifest cache for efficient manifest file access
        IcebergManifestCache cache = IcebergUtils.getManifestCache(source.getCatalog());

        // Convert query conjuncts to Iceberg filter expression
        // This combines all predicates with AND logic for partition/file pruning
        Expression filterExpr = conjuncts.stream()
                .map(conjunct -> IcebergUtils.convertToIcebergExpr(conjunct, icebergTable.schema()))
                .filter(Objects::nonNull)
                .reduce(Expressions.alwaysTrue(), Expressions::and);

        // Get all partition specs by their IDs for later use
        Map<Integer, PartitionSpec> specsById = icebergTable.specs();
        boolean caseSensitive = true;

        // Create residual evaluators for each partition spec
        // Residual evaluators compute the remaining filter expression after partition pruning
        Map<Integer, ResidualEvaluator> residualEvaluators = new HashMap<>();
        specsById.forEach((id, spec) -> residualEvaluators.put(id,
                ResidualEvaluator.of(spec, filterExpr, caseSensitive)));

        // Create metrics evaluator for file-level pruning based on column statistics
        InclusiveMetricsEvaluator metricsEvaluator =
                new InclusiveMetricsEvaluator(icebergTable.schema(), filterExpr, caseSensitive);

        // ========== Phase 1: Load delete files from delete manifests ==========
        List<DeleteFile> deleteFiles = new ArrayList<>();
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(icebergTable.io());
        for (ManifestFile manifest : deleteManifests) {
            // Skip non-delete manifests
            if (manifest.content() != ManifestContent.DELETES) {
                continue;
            }
            // Get the partition spec for this manifest
            PartitionSpec spec = specsById.get(manifest.partitionSpecId());
            if (spec == null) {
                continue;
            }
            // Create manifest evaluator for partition-level pruning
            ManifestEvaluator evaluator =
                    ManifestEvaluator.forPartitionFilter(filterExpr, spec, caseSensitive);
            // Skip manifest if it doesn't match the filter expression (partition pruning)
            if (!evaluator.eval(manifest)) {
                continue;
            }
            // Load delete files from cache (or from storage if not cached)
            ManifestCacheValue value = IcebergManifestCacheLoader.loadDeleteFilesWithCache(cache, manifest,
                    icebergTable, this::recordManifestCacheAccess);
            deleteFiles.addAll(value.getDeleteFiles());
        }

        // Build delete file index for efficient lookup of deletes applicable to each data file
        DeleteFileIndex deleteIndex = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(specsById)
                .caseSensitive(caseSensitive)
                .build();

        // ========== Phase 2: Load data files and create scan tasks ==========
        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<ManifestFile> dataManifests =
                     IcebergUtils.getMatchingManifest(snapshot.dataManifests(icebergTable.io()),
                             specsById, filterExpr)) {
            for (ManifestFile manifest : dataManifests) {
                // Skip non-data manifests
                if (manifest.content() != ManifestContent.DATA) {
                    continue;
                }
                // Get the partition spec for this manifest
                PartitionSpec spec = specsById.get(manifest.partitionSpecId());
                if (spec == null) {
                    continue;
                }
                // Get the residual evaluator for this partition spec
                ResidualEvaluator residualEvaluator = residualEvaluators.get(manifest.partitionSpecId());
                if (residualEvaluator == null) {
                    continue;
                }

                // Load data files from cache (or from storage if not cached)
                ManifestCacheValue value = IcebergManifestCacheLoader.loadDataFilesWithCache(cache, manifest,
                        icebergTable, this::recordManifestCacheAccess);

                // Process each data file in the manifest
                for (org.apache.iceberg.DataFile dataFile : value.getDataFiles()) {
                    // Skip file if column statistics indicate no matching rows (metrics-based pruning)
                    if (metricsEvaluator != null && !metricsEvaluator.eval(dataFile)) {
                        continue;
                    }
                    // Skip file if partition values don't match the residual filter
                    if (residualEvaluator.residualFor(dataFile.partition()).equals(Expressions.alwaysFalse())) {
                        continue;
                    }
                    // Find all delete files that apply to this data file based on sequence number
                    List<DeleteFile> deletes = Arrays.asList(
                            deleteIndex.forDataFile(dataFile.dataSequenceNumber(), dataFile));

                    // Create a FileScanTask containing the data file, associated deletes, and metadata
                    tasks.add(new BaseFileScanTask(
                            dataFile,
                            deletes.toArray(new DeleteFile[0]),
                            SchemaParser.toJson(icebergTable.schema()),
                            PartitionSpecParser.toJson(spec),
                            residualEvaluator));
                }
            }
        }

        // Split tasks into smaller chunks based on target split size for parallel processing
        targetSplitSize = determineTargetFileSplitSize(tasks);
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks), targetSplitSize);
    }

    /**
     * Initialize cached values for LocationPath creation on first use.
     * This avoids repeated StorageProperties lookup, scheme parsing, and S3URI regex parsing for each file.
     */
    private void initLocationPathCache(String samplePath) {
        try {
            // Create a LocationPath using the full method to get all cached values
            LocationPath sampleLocationPath = LocationPath.of(samplePath, storagePropertiesMap);
            cachedStorageProperties = sampleLocationPath.getStorageProperties();
            cachedSchema = sampleLocationPath.getSchema();
            cachedFsIdentifier = sampleLocationPath.getFsIdentifier();

            // Extract fsIdPrefix like "s3://" from fsIdentifier like "s3://bucket"
            int schemeEnd = cachedFsIdentifier.indexOf("://");
            if (schemeEnd > 0) {
                cachedFsIdPrefix = cachedFsIdentifier.substring(0, schemeEnd + 3);
            }

            // Cache path prefix mapping for fast transformation
            // This allows subsequent files to skip S3URI regex parsing entirely
            String normalizedPath = sampleLocationPath.getNormalizedLocation();

            // Find the common prefix by looking for the last '/' before the filename
            int lastSlashInOriginal = samplePath.lastIndexOf('/');
            int lastSlashInNormalized = normalizedPath.lastIndexOf('/');

            if (lastSlashInOriginal > 0 && lastSlashInNormalized > 0) {
                cachedOriginalPathPrefix = samplePath.substring(0, lastSlashInOriginal + 1);
                cachedNormalizedPathPrefix = normalizedPath.substring(0, lastSlashInNormalized + 1);
            }

            locationPathCacheInitialized = true;
        } catch (Exception e) {
            // If caching fails, try to initialize again on next use
            LOG.warn("Failed to initialize LocationPath cache, will use full parsing", e);
        }
    }

    /**
     * Create a LocationPath with cached values for better performance.
     * Uses cached path prefix mapping to completely bypass S3URI regex parsing for most files.
     * Falls back to full parsing if cache is not available or path doesn't match cached prefix.
     */
    private LocationPath createLocationPathWithCache(String path) {
        // Initialize cache on first call
        if (!locationPathCacheInitialized) {
            initLocationPathCache(path);
        }

        // Fast path: if path starts with cached original prefix, directly transform without any parsing
        if (cachedOriginalPathPrefix != null && path.startsWith(cachedOriginalPathPrefix)) {
            // Transform: replace original prefix with normalized prefix
            String normalizedPath = cachedNormalizedPathPrefix + path.substring(cachedOriginalPathPrefix.length());
            return LocationPath.ofDirect(normalizedPath, cachedSchema, cachedFsIdentifier, cachedStorageProperties);
        }

        // Medium path: use cached StorageProperties but still need validateAndNormalizeUri
        if (cachedStorageProperties != null) {
            return LocationPath.ofWithCache(path, cachedStorageProperties, cachedSchema, cachedFsIdPrefix);
        }

        // Fallback to full parsing
        return LocationPath.of(path, storagePropertiesMap);
    }

    private Split createIcebergSplit(FileScanTask fileScanTask) {
        String originalPath = fileScanTask.file().path().toString();
        LocationPath locationPath = createLocationPathWithCache(originalPath);
        IcebergSplit split = new IcebergSplit(
                locationPath,
                fileScanTask.start(),
                fileScanTask.length(),
                fileScanTask.file().fileSizeInBytes(),
                new String[0],
                formatVersion,
                storagePropertiesMap,
                new ArrayList<>(),
                originalPath);
        if (!fileScanTask.deletes().isEmpty()) {
            split.setDeleteFileFilters(getDeleteFileFilters(fileScanTask));
        }
        split.setTableFormatType(TableFormatType.ICEBERG);
        split.setTargetSplitSize(targetSplitSize);
        if (isPartitionedTable) {
            PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
            if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
                // Get specId and corresponding PartitionSpec to handle partition evolution
                int specId = fileScanTask.file().specId();
                PartitionSpec partitionSpec = icebergTable.specs().get(specId);

                Preconditions.checkNotNull(partitionSpec, "Partition spec with specId %s not found for table %s",
                        specId, icebergTable.name());
                Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(
                        partitionData, k -> {
                            return IcebergUtils.getPartitionInfoMap(partitionData, partitionSpec,
                                    sessionVariable.getTimeZone());
                        });
                // Only set partition values if all partitions are identity transform
                // For non-identity partitions, getPartitionInfoMap returns null to skip dynamic partition pruning
                if (partitionInfoMap != null) {
                    split.setIcebergPartitionValues(partitionInfoMap);
                }
            } else {
                partitionMapInfos.put(partitionData, null);
            }
        }
        return split;
    }

    private List<Split> doGetSplits(int numBackends) throws UserException {

        List<Split> splits = new ArrayList<>();

        // Use custom file scan tasks if available (for rewrite operations)
        List<FileScanTask> customFileScanTasks = getFileScanTasksFromContext();
        if (customFileScanTasks != null) {
            for (FileScanTask task : customFileScanTasks) {
                splits.add(createIcebergSplit(task));
            }
            selectedPartitionNum = partitionMapInfos.size();
            recordManifestCacheProfile();
            return splits;
        }

        // Normal table scan planning
        TableScan scan = createTableScan();

        try (CloseableIterable<FileScanTask> fileScanTasks = planFileScanTask(scan)) {
            if (tableLevelPushDownCount) {
                int needSplitCnt = countFromSnapshot < COUNT_WITH_PARALLEL_SPLITS
                        ? 1 : sessionVariable.getParallelExecInstanceNum() * numBackends;
                for (FileScanTask next : fileScanTasks) {
                    splits.add(createIcebergSplit(next));
                    if (splits.size() >= needSplitCnt) {
                        break;
                    }
                }
                setPushDownCount(countFromSnapshot);
                assignCountToSplits(splits, countFromSnapshot);
                recordManifestCacheProfile();
                return splits;
            } else {
                fileScanTasks.forEach(taskGrp -> splits.add(createIcebergSplit(taskGrp)));
            }
        } catch (IOException e) {
            throw new UserException(e.getMessage(), e.getCause());
        }

        selectedPartitionNum = partitionMapInfos.size();
        recordManifestCacheProfile();
        return splits;
    }

    @Override
    public boolean isBatchMode() {
        Boolean cached = isBatchMode;
        if (cached != null) {
            return cached;
        }
        TPushAggOp aggOp = getPushDownAggNoGroupingOp();
        if (aggOp.equals(TPushAggOp.COUNT)) {
            try {
                countFromSnapshot = getCountFromSnapshot();
            } catch (UserException e) {
                throw new RuntimeException(e);
            }
            if (countFromSnapshot >= 0) {
                tableLevelPushDownCount = true;
                isBatchMode = false;
                return false;
            }
        }

        try {
            if (createTableScan().snapshot() == null) {
                isBatchMode = false;
                return false;
            }
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

        if (!sessionVariable.getEnableExternalTableBatchMode()) {
            isBatchMode = false;
            return false;
        }

        try {
            return preExecutionAuthenticator.execute(() -> {
                try (CloseableIterator<ManifestFile> matchingManifest =
                        IcebergUtils.getMatchingManifest(
                                createTableScan().snapshot().dataManifests(icebergTable.io()),
                                icebergTable.specs(),
                                createTableScan().filter()).iterator()) {
                    int cnt = 0;
                    while (matchingManifest.hasNext()) {
                        ManifestFile next = matchingManifest.next();
                        cnt += next.addedFilesCount() + next.existingFilesCount();
                        if (cnt >= sessionVariable.getNumFilesInBatchMode()) {
                            isBatchMode = true;
                            return true;
                        }
                    }
                }
                isBatchMode = false;
                return false;
            });
        } catch (Exception e) {
            Optional<NotSupportedException> opt = checkNotSupportedException(e);
            if (opt.isPresent()) {
                throw opt.get();
            } else {
                throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
            }
        }
    }

    public IcebergTableQueryInfo getSpecifiedSnapshot() throws UserException {
        TableSnapshot tableSnapshot = getQueryTableSnapshot();
        TableScanParams scanParams = getScanParams();
        Optional<TableScanParams> params = Optional.ofNullable(scanParams);
        if (tableSnapshot != null || IcebergUtils.isIcebergBranchOrTag(params)) {
            return IcebergUtils.getQuerySpecSnapshot(
                icebergTable,
                Optional.ofNullable(tableSnapshot),
                params);
        }
        return null;
    }

    private List<IcebergDeleteFileFilter> getDeleteFileFilters(FileScanTask spitTask) {
        List<IcebergDeleteFileFilter> filters = new ArrayList<>();
        for (DeleteFile delete : spitTask.deletes()) {
            if (delete.content() == FileContent.POSITION_DELETES) {
                filters.add(IcebergDeleteFileFilter.createPositionDelete(delete));
            } else if (delete.content() == FileContent.EQUALITY_DELETES) {
                filters.add(IcebergDeleteFileFilter.createEqualityDelete(
                        delete.path().toString(), delete.equalityFieldIds(),
                        delete.fileSizeInBytes(), delete.format()));
            } else {
                throw new IllegalStateException("Unknown delete content: " + delete.content());
            }
        }
        return filters;
    }

    @Override
    public TFileFormatType getFileFormatType() throws UserException {
        TFileFormatType type;
        String icebergFormat = source.getFileFormat();
        if (icebergFormat.equalsIgnoreCase("parquet")) {
            type = TFileFormatType.FORMAT_PARQUET;
        } else if (icebergFormat.equalsIgnoreCase("orc")) {
            type = TFileFormatType.FORMAT_ORC;
        } else {
            throw new DdlException(String.format("Unsupported format name: %s for iceberg table.", icebergFormat));
        }
        return type;
    }

    @Override
    public List<String> getPathPartitionKeys() throws UserException {
        // return icebergTable.spec().fields().stream().map(PartitionField::name).map(String::toLowerCase)
        //         .collect(Collectors.toList());
        /**First, iceberg partition columns are based on existing fields, which will be stored in the actual data file.
         * Second, iceberg partition columns support Partition transforms. In this case, the path partition key is not
         * equal to the column name of the partition column, so remove this code and get all the columns you want to
         * read from the file.
         * Related code:
         *  be/src/vec/exec/scan/vfile_scanner.cpp:
         *      VFileScanner::_init_expr_ctxes()
         *          if (slot_info.is_file_slot) {
         *              xxxx
         *          }
         */
        return new ArrayList<>();
    }

    private void recordManifestCacheAccess(boolean cacheHit) {
        if (cacheHit) {
            manifestCacheHits++;
        } else {
            manifestCacheMisses++;
        }
    }

    private void recordManifestCacheProfile() {
        if (!IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            return;
        }
        SummaryProfile summaryProfile = SummaryProfile.getSummaryProfile(ConnectContext.get());
        if (summaryProfile == null || summaryProfile.getExecutionSummary() == null) {
            return;
        }
        summaryProfile.getExecutionSummary().addInfoString("Manifest Cache",
                String.format("hits=%d, misses=%d, failures=%d",
                        manifestCacheHits, manifestCacheMisses, manifestCacheFailures));
    }

    @Override
    public TableIf getTargetTable() {
        return source.getTargetTable();
    }

    @Override
    public Map<String, String> getLocationProperties() throws UserException {
        return backendStorageProperties;
    }

    @VisibleForTesting
    public long getCountFromSnapshot() throws UserException {
        IcebergTableQueryInfo info = getSpecifiedSnapshot();

        Snapshot snapshot = info == null
                ? icebergTable.currentSnapshot() : icebergTable.snapshot(info.getSnapshotId());

        // empty table
        if (snapshot == null) {
            return 0;
        }

        Map<String, String> summary = snapshot.summary();
        if (!summary.get(IcebergUtils.TOTAL_EQUALITY_DELETES).equals("0")) {
            // has equality delete files, can not push down count
            return -1;
        }

        long deleteCount = Long.parseLong(summary.get(IcebergUtils.TOTAL_POSITION_DELETES));
        if (deleteCount == 0) {
            // no delete files, can push down count directly
            return Long.parseLong(summary.get(IcebergUtils.TOTAL_RECORDS));
        }
        if (sessionVariable.ignoreIcebergDanglingDelete) {
            // has position delete files, if we ignore dangling delete, can push down count
            return Long.parseLong(summary.get(IcebergUtils.TOTAL_RECORDS)) - deleteCount;
        } else {
            // otherwise, can not push down count
            return -1;
        }
    }

    @Override
    protected void toThrift(TPlanNode planNode) {
        super.toThrift(planNode);
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        String base = super.getNodeExplainString(prefix, detailLevel);
        StringBuilder builder = new StringBuilder(base);

        if (detailLevel == TExplainLevel.VERBOSE && IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
            builder.append(prefix).append("manifest cache: hits=").append(manifestCacheHits)
                    .append(", misses=").append(manifestCacheMisses)
                    .append(", failures=").append(manifestCacheFailures).append("\n");
        }

        if (!pushdownIcebergPredicates.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String predicate : pushdownIcebergPredicates) {
                sb.append(prefix).append(prefix).append(predicate).append("\n");
            }
            builder.append(String.format("%sicebergPredicatePushdown=\n%s\n", prefix, sb));
        }
        return builder.toString();
    }

    private void assignCountToSplits(List<Split> splits, long totalCount) {
        if (splits.isEmpty()) {
            return;
        }
        int size = splits.size();
        long countPerSplit = totalCount / size;
        for (int i = 0; i < size - 1; i++) {
            ((IcebergSplit) splits.get(i)).setTableLevelRowCount(countPerSplit);
        }
        ((IcebergSplit) splits.get(size - 1)).setTableLevelRowCount(countPerSplit + totalCount % size);
    }

    @Override
    public int numApproximateSplits() {
        return NUM_SPLITS_PER_PARTITION * partitionMapInfos.size() > 0 ? partitionMapInfos.size() : 1;
    }

    private Optional<NotSupportedException> checkNotSupportedException(Exception e) {
        if (e instanceof NullPointerException) {
            /*
        Caused by: java.lang.NullPointerException: Type cannot be null
            at org.apache.iceberg.relocated.com.google.common.base.Preconditions.checkNotNull
                (Preconditions.java:921) ~[iceberg-bundled-guava-1.4.3.jar:?]
            at org.apache.iceberg.types.Types$NestedField.<init>(Types.java:447) ~[iceberg-api-1.4.3.jar:?]
            at org.apache.iceberg.types.Types$NestedField.optional(Types.java:416) ~[iceberg-api-1.4.3.jar:?]
            at org.apache.iceberg.PartitionSpec.partitionType(PartitionSpec.java:132) ~[iceberg-api-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex.lambda$new$0(DeleteFileIndex.java:97) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.relocated.com.google.common.collect.RegularImmutableMap.forEach
                (RegularImmutableMap.java:297) ~[iceberg-bundled-guava-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex.<init>(DeleteFileIndex.java:97) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex.<init>(DeleteFileIndex.java:71) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.DeleteFileIndex$Builder.build(DeleteFileIndex.java:578) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.ManifestGroup.plan(ManifestGroup.java:183) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.ManifestGroup.planFiles(ManifestGroup.java:170) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.DataTableScan.doPlanFiles(DataTableScan.java:89) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.iceberg.SnapshotScan.planFiles(SnapshotScan.java:139) ~[iceberg-core-1.4.3.jar:?]
            at org.apache.doris.datasource.iceberg.source.IcebergScanNode.doGetSplits
                (IcebergScanNode.java:209) ~[doris-fe.jar:1.2-SNAPSHOT]
        EXAMPLE:
             CREATE TABLE iceberg_tb(col1 INT,col2 STRING) USING ICEBERG PARTITIONED BY (bucket(10,col2));
             INSERT INTO iceberg_tb VALUES( ... );
             ALTER TABLE iceberg_tb DROP PARTITION FIELD bucket(10,col2);
             ALTER TABLE iceberg_tb DROP COLUMNS col2;
        Link: https://github.com/apache/iceberg/pull/10755
            */
            LOG.warn("Unable to plan for iceberg table {}", this.desc.getTable().getName(), e);
            return Optional.of(
                    new NotSupportedException("Unable to plan for this table. "
                            + "Maybe read Iceberg table with dropped old partition column. Cause: "
                            + Util.getRootCauseMessage(e)));
        }
        return Optional.empty();
    }
}
