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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.exceptions.NotSupportedException;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.iceberg.BaseFileScanTask;
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

class IcebergSplitPlanningSupport {
    private static final Logger LOG = LogManager.getLogger(IcebergSplitPlanningSupport.class);
    private static final long COUNT_WITH_PARALLEL_SPLITS = 10000;

    private final IcebergScanNode scanNode;
    private final IcebergSource source;
    private final Table icebergTable;
    private final ExecutionAuthenticator preExecutionAuthenticator;
    private final SessionVariable sessionVariable;
    private final ScanContext scanContext;
    private final Map<StorageProperties.Type, StorageProperties> storagePropertiesMap;
    private final int formatVersion;
    private final boolean isPartitionedTable;

    private final List<String> pushdownIcebergPredicates = Lists.newArrayList();
    private final Map<PartitionData, Map<String, String>> partitionMapInfos = new HashMap<>();

    private TableScan icebergTableScan;
    private Boolean isBatchMode;
    private boolean tableLevelPushDownCount;
    private long countFromSnapshot;
    private long targetSplitSize;
    private long manifestCacheHits;
    private long manifestCacheMisses;
    private long manifestCacheFailures;

    private boolean locationPathCacheInitialized;
    private StorageProperties cachedStorageProperties;
    private String cachedSchema;
    private String cachedFsIdPrefix;
    private String cachedOriginalPathPrefix;
    private String cachedNormalizedPathPrefix;
    private String cachedFsIdentifier;

    IcebergSplitPlanningSupport(
            IcebergScanNode scanNode,
            IcebergSource source,
            Table icebergTable,
            ExecutionAuthenticator preExecutionAuthenticator,
            SessionVariable sessionVariable,
            ScanContext scanContext,
            Map<StorageProperties.Type, StorageProperties> storagePropertiesMap,
            int formatVersion,
            boolean isPartitionedTable) {
        this.scanNode = scanNode;
        this.source = source;
        this.icebergTable = icebergTable;
        this.preExecutionAuthenticator = preExecutionAuthenticator;
        this.sessionVariable = sessionVariable;
        this.scanContext = scanContext;
        this.storagePropertiesMap = storagePropertiesMap;
        this.formatVersion = formatVersion;
        this.isPartitionedTable = isPartitionedTable;
    }

    TableScan createTableScan() throws UserException {
        try {
            return preExecutionAuthenticator.execute(this::createTableScanInternal);
        } catch (Exception e) {
            throw new UserException(e.getMessage(), e);
        }
    }

    CloseableIterable<FileScanTask> planFileScanTask(TableScan scan) throws Exception {
        return preExecutionAuthenticator.execute(() -> planFileScanTaskInternal(scan));
    }

    Split createSplit(FileScanTask fileScanTask) {
        return createIcebergSplit(fileScanTask);
    }

    boolean isBatchMode() {
        Boolean cached = isBatchMode;
        if (cached != null) {
            return cached;
        }
        // Rewrite tasks inject precomputed FileScanTasks into StatementContext and expect the
        // scan node to consume them on the current planning thread. Keep this path in sync mode
        // to preserve the pre-refactor behavior and avoid accessing thread-local planning context
        // from the async batch producer.
        if (hasRewriteFileScanTasksInContext()) {
            isBatchMode = false;
            return false;
        }
        // Table-level count pushdown also stays in sync mode. Once snapshot metadata can answer
        // COUNT directly, planning only needs a small bounded number of splits carrying the
        // table-level row count, so there is no benefit in exposing the lazy batch protocol.
        TPushAggOp aggOp = scanNode.getPushDownAggNoGroupingOp();
        if (aggOp.equals(TPushAggOp.COUNT)) {
            try {
                countFromSnapshot = loadCountFromSnapshot();
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
            if (createTableScanInternal().snapshot() == null) {
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

        // Ordinary query planning reaches batch mode only when the table has a real snapshot,
        // session batch mode is enabled, and the matching manifest set is large enough to justify
        // incremental split production for backends.
        try {
            return preExecutionAuthenticator.execute(() -> {
                try (CloseableIterator<ManifestFile> matchingManifest =
                        org.apache.doris.datasource.iceberg.IcebergUtils.getMatchingManifest(
                                createTableScanInternal().snapshot().dataManifests(icebergTable.io()),
                                icebergTable.specs(),
                                createTableScanInternal().filter()).iterator()) {
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
            }
            throw new RuntimeException(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    int numApproximateSplits() {
        return partitionMapInfos.isEmpty() ? 1 : partitionMapInfos.size();
    }

    int getSelectedPartitionNum() {
        return partitionMapInfos.size();
    }

    boolean hasTableLevelPushDownCount() {
        return tableLevelPushDownCount;
    }

    long getCountFromSnapshot() {
        return countFromSnapshot;
    }

    List<String> getPushdownIcebergPredicates() {
        return pushdownIcebergPredicates;
    }

    long getManifestCacheHits() {
        return manifestCacheHits;
    }

    long getManifestCacheMisses() {
        return manifestCacheMisses;
    }

    long getManifestCacheFailures() {
        return manifestCacheFailures;
    }

    int getCountPushDownSplitCount(int numBackends) {
        Preconditions.checkState(tableLevelPushDownCount, "Table-level count pushdown is not enabled");
        return countFromSnapshot < COUNT_WITH_PARALLEL_SPLITS
                ? 1
                : sessionVariable.getParallelExecInstanceNum(scanContext.getClusterName()) * numBackends;
    }

    void recordManifestCacheProfile() {
        if (!org.apache.doris.datasource.iceberg.IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
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

    UserException translatePlanningException(Exception e) {
        Optional<NotSupportedException> opt = checkNotSupportedException(e);
        if (opt.isPresent()) {
            return new UserException(opt.get().getMessage(), opt.get());
        }
        if (e instanceof UserException) {
            return (UserException) e;
        }
        return new UserException(e.getMessage(), e);
    }

    private TableScan createTableScanInternal() throws UserException {
        if (icebergTableScan != null) {
            return icebergTableScan;
        }

        TableScan scan = icebergTable.newScan().metricsReporter(
                new org.apache.doris.datasource.iceberg.profile.IcebergMetricsReporter());

        org.apache.doris.datasource.iceberg.source.IcebergTableQueryInfo info = scanNode.getSpecifiedSnapshot();
        if (info != null) {
            if (info.getRef() != null) {
                scan = scan.useRef(info.getRef());
            } else {
                scan = scan.useSnapshot(info.getSnapshotId());
            }
        }

        List<Expression> expressions = new ArrayList<>();
        for (Expr conjunct : scanNode.getConjuncts()) {
            Expression expression = org.apache.doris.datasource.iceberg.IcebergUtils.convertToIcebergExpr(
                    conjunct, icebergTable.schema());
            if (expression != null) {
                expressions.add(expression);
            }
        }
        for (Expression predicate : expressions) {
            scan = scan.filter(predicate);
            pushdownIcebergPredicates.add(predicate.toString());
        }

        icebergTableScan = scan.planWith(source.getCatalog().getThreadPoolWithPreAuth());
        return icebergTableScan;
    }

    private CloseableIterable<FileScanTask> planFileScanTaskInternal(TableScan scan) {
        if (!org.apache.doris.datasource.iceberg.IcebergUtils.isManifestCacheEnabled(source.getCatalog())) {
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
            return TableScanUtil.splitFiles(scan.planFiles(), sessionVariable.getFileSplitSize());
        }
        if (isBatchMode()) {
            return TableScanUtil.splitFiles(scan.planFiles(), sessionVariable.getMaxSplitSize());
        }

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

    private long applyMaxFileSplitNumLimit(long targetFileSplitSize, long totalFileSize) {
        int maxFileSplitNum = sessionVariable.getMaxFileSplitNum();
        if (maxFileSplitNum <= 0 || totalFileSize <= 0) {
            return targetFileSplitSize;
        }
        long minSplitSizeForMaxNum = (totalFileSize + maxFileSplitNum - 1L) / (long) maxFileSplitNum;
        return Math.max(targetFileSplitSize, minSplitSizeForMaxNum);
    }

    private CloseableIterable<FileScanTask> planFileScanTaskWithManifestCache(TableScan scan) throws IOException {
        Snapshot snapshot = scan.snapshot();
        if (snapshot == null) {
            return CloseableIterable.withNoopClose(Collections.emptyList());
        }

        org.apache.doris.datasource.iceberg.IcebergExternalMetaCache cache =
                Env.getCurrentEnv().getExtMetaCacheMgr().iceberg(source.getCatalog().getId());
        if (!(source.getTargetTable() instanceof ExternalTable)) {
            throw new RuntimeException("Iceberg scan target table is not an external table");
        }
        ExternalTable targetExternalTable = (ExternalTable) source.getTargetTable();

        Expression filterExpr = scanNode.getConjuncts().stream()
                .map(conjunct -> org.apache.doris.datasource.iceberg.IcebergUtils.convertToIcebergExpr(
                        conjunct, icebergTable.schema()))
                .filter(Objects::nonNull)
                .reduce(Expressions.alwaysTrue(), Expressions::and);

        Map<Integer, PartitionSpec> specsById = icebergTable.specs();
        boolean caseSensitive = true;

        Map<Integer, ResidualEvaluator> residualEvaluators = new HashMap<>();
        specsById.forEach((id, spec) -> residualEvaluators.put(id,
                ResidualEvaluator.of(spec, filterExpr, caseSensitive)));

        InclusiveMetricsEvaluator metricsEvaluator =
                new InclusiveMetricsEvaluator(icebergTable.schema(), filterExpr, caseSensitive);

        List<DeleteFile> deleteFiles = new ArrayList<>();
        List<ManifestFile> deleteManifests = snapshot.deleteManifests(icebergTable.io());
        for (ManifestFile manifest : deleteManifests) {
            if (manifest.content() != ManifestContent.DELETES) {
                continue;
            }
            PartitionSpec spec = specsById.get(manifest.partitionSpecId());
            if (spec == null) {
                continue;
            }
            ManifestEvaluator evaluator =
                    ManifestEvaluator.forPartitionFilter(filterExpr, spec, caseSensitive);
            if (!evaluator.eval(manifest)) {
                continue;
            }
            org.apache.doris.datasource.iceberg.cache.ManifestCacheValue value =
                    org.apache.doris.datasource.iceberg.cache.IcebergManifestCacheLoader.loadDeleteFilesWithCache(
                            cache, targetExternalTable, manifest, icebergTable, this::recordManifestCacheAccess);
            deleteFiles.addAll(value.getDeleteFiles());
        }

        DeleteFileIndex deleteIndex = DeleteFileIndex.builderFor(deleteFiles)
                .specsById(specsById)
                .caseSensitive(caseSensitive)
                .build();

        List<FileScanTask> tasks = new ArrayList<>();
        try (CloseableIterable<ManifestFile> dataManifests =
                     org.apache.doris.datasource.iceberg.IcebergUtils.getMatchingManifest(
                             snapshot.dataManifests(icebergTable.io()), specsById, filterExpr)) {
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

                org.apache.doris.datasource.iceberg.cache.ManifestCacheValue value =
                        org.apache.doris.datasource.iceberg.cache.IcebergManifestCacheLoader.loadDataFilesWithCache(
                                cache, targetExternalTable, manifest, icebergTable, this::recordManifestCacheAccess);

                for (org.apache.iceberg.DataFile dataFile : value.getDataFiles()) {
                    if (metricsEvaluator != null && !metricsEvaluator.eval(dataFile)) {
                        continue;
                    }
                    if (residualEvaluator.residualFor(dataFile.partition()).equals(Expressions.alwaysFalse())) {
                        continue;
                    }
                    List<DeleteFile> deletes = Arrays.asList(
                            deleteIndex.forDataFile(dataFile.dataSequenceNumber(), dataFile));

                    tasks.add(new BaseFileScanTask(
                            dataFile,
                            deletes.toArray(new DeleteFile[0]),
                            SchemaParser.toJson(icebergTable.schema()),
                            PartitionSpecParser.toJson(spec),
                            residualEvaluator));
                }
            }
        }

        targetSplitSize = determineTargetFileSplitSize(tasks);
        return TableScanUtil.splitFiles(CloseableIterable.withNoopClose(tasks), targetSplitSize);
    }

    private void initLocationPathCache(String samplePath) {
        try {
            LocationPath sampleLocationPath = LocationPath.of(samplePath, storagePropertiesMap);
            cachedStorageProperties = sampleLocationPath.getStorageProperties();
            cachedSchema = sampleLocationPath.getSchema();
            cachedFsIdentifier = sampleLocationPath.getFsIdentifier();

            int schemeEnd = cachedFsIdentifier.indexOf("://");
            if (schemeEnd > 0) {
                cachedFsIdPrefix = cachedFsIdentifier.substring(0, schemeEnd + 3);
            }

            String normalizedPath = sampleLocationPath.getNormalizedLocation();
            int lastSlashInOriginal = samplePath.lastIndexOf('/');
            int lastSlashInNormalized = normalizedPath.lastIndexOf('/');
            if (lastSlashInOriginal > 0 && lastSlashInNormalized > 0) {
                cachedOriginalPathPrefix = samplePath.substring(0, lastSlashInOriginal + 1);
                cachedNormalizedPathPrefix = normalizedPath.substring(0, lastSlashInNormalized + 1);
            }

            locationPathCacheInitialized = true;
        } catch (Exception e) {
            LOG.warn("Failed to initialize LocationPath cache, will use full parsing", e);
        }
    }

    private LocationPath createLocationPathWithCache(String path) {
        if (!locationPathCacheInitialized) {
            initLocationPathCache(path);
        }
        if (cachedOriginalPathPrefix != null && path.startsWith(cachedOriginalPathPrefix)) {
            String normalizedPath = cachedNormalizedPathPrefix + path.substring(cachedOriginalPathPrefix.length());
            return LocationPath.ofDirect(normalizedPath, cachedSchema, cachedFsIdentifier, cachedStorageProperties);
        }
        if (cachedStorageProperties != null) {
            return LocationPath.ofWithCache(path, cachedStorageProperties, cachedSchema, cachedFsIdPrefix);
        }
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
            populatePartitionMetadata(split, fileScanTask);
        }
        return split;
    }

    private void populatePartitionMetadata(IcebergSplit split, FileScanTask fileScanTask) {
        int specId = fileScanTask.file().specId();
        PartitionSpec partitionSpec = icebergTable.specs().get(specId);
        if (partitionSpec == null) {
            LOG.warn("Skip partition metadata serialization for split because partition spec {} is not present "
                            + "in current table metadata for table {}. This can happen when planning branch/tag "
                            + "snapshots that still reference an older spec.",
                    specId, icebergTable.name());
            return;
        }

        PartitionData partitionData = (PartitionData) fileScanTask.file().partition();
        if (partitionData != null) {
            split.setPartitionSpecId(specId);
            split.setPartitionDataJson(org.apache.doris.datasource.iceberg.IcebergUtils.getPartitionDataJson(
                    partitionData, partitionSpec, sessionVariable.getTimeZone()));
        }

        if (sessionVariable.isEnableRuntimeFilterPartitionPrune()) {
            Map<String, String> partitionInfoMap = partitionMapInfos.computeIfAbsent(partitionData, k ->
                    org.apache.doris.datasource.iceberg.IcebergUtils.getPartitionInfoMap(
                            partitionData, partitionSpec, sessionVariable.getTimeZone()));
            if (partitionInfoMap != null) {
                split.setIcebergPartitionValues(partitionInfoMap);
            }
            return;
        }
        partitionMapInfos.put(partitionData, null);
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

    void assignCountToSplits(List<Split> splits) {
        Preconditions.checkState(tableLevelPushDownCount, "Table-level count pushdown is not enabled");
        if (splits.isEmpty()) {
            return;
        }
        int size = splits.size();
        long countPerSplit = countFromSnapshot / size;
        for (int i = 0; i < size - 1; i++) {
            ((IcebergSplit) splits.get(i)).setTableLevelRowCount(countPerSplit);
        }
        ((IcebergSplit) splits.get(size - 1))
                .setTableLevelRowCount(countPerSplit + countFromSnapshot % size);
    }

    private void recordManifestCacheAccess(boolean cacheHit) {
        if (cacheHit) {
            manifestCacheHits++;
        } else {
            manifestCacheMisses++;
        }
    }

    List<FileScanTask> getFileScanTasksFromContext() {
        ConnectContext ctx = ConnectContext.get();
        Preconditions.checkNotNull(ctx);
        Preconditions.checkNotNull(ctx.getStatementContext());

        List<FileScanTask> tasks = ctx.getStatementContext().getAndClearIcebergRewriteFileScanTasks();
        if (tasks != null && !tasks.isEmpty()) {
            LOG.info("Retrieved {} file scan tasks from context for table {}",
                    tasks.size(), icebergTable.name());
            return new ArrayList<>(tasks);
        }
        return null;
    }

    private boolean hasRewriteFileScanTasksInContext() {
        ConnectContext ctx = ConnectContext.get();
        Preconditions.checkNotNull(ctx);
        Preconditions.checkNotNull(ctx.getStatementContext());
        return ctx.getStatementContext().hasIcebergRewriteFileScanTasks();
    }

    private long loadCountFromSnapshot() throws UserException {
        IcebergTableQueryInfo info = scanNode.getSpecifiedSnapshot();
        Snapshot snapshot = info == null ? icebergTable.currentSnapshot() : icebergTable.snapshot(info.getSnapshotId());
        if (snapshot == null) {
            return 0;
        }

        Map<String, String> summary = snapshot.summary();
        if (!summary.get(org.apache.doris.datasource.iceberg.IcebergUtils.TOTAL_EQUALITY_DELETES).equals("0")) {
            return -1;
        }

        long deleteCount = Long.parseLong(summary.get(org.apache.doris.datasource.iceberg.IcebergUtils
                .TOTAL_POSITION_DELETES));
        if (deleteCount == 0) {
            return Long.parseLong(summary.get(org.apache.doris.datasource.iceberg.IcebergUtils.TOTAL_RECORDS));
        }
        if (sessionVariable.ignoreIcebergDanglingDelete) {
            return Long.parseLong(summary.get(org.apache.doris.datasource.iceberg.IcebergUtils.TOTAL_RECORDS))
                    - deleteCount;
        }
        return -1;
    }

    private Optional<NotSupportedException> checkNotSupportedException(Exception e) {
        if (e instanceof NullPointerException) {
            LOG.warn("Unable to plan for iceberg table {}", source.getTargetTable().getName(), e);
            return Optional.of(new NotSupportedException(
                    "Unable to plan iceberg table due to unsupported partition evolution/drop-column pattern"));
        }
        return Optional.empty();
    }
}
