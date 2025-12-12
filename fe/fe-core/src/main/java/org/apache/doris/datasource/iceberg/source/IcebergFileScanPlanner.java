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

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.iceberg.IcebergManifestCache;
import org.apache.doris.datasource.iceberg.IcebergUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Planner that turns an Iceberg {@link TableScan} into {@link FileScanTask}s,
 * with support for using {@link IcebergManifestCache}.
 */
public class IcebergFileScanPlanner {

    private static final Logger LOG = LogManager.getLogger(IcebergFileScanPlanner.class);

    /**
     * Planning context passed from {@link IcebergScanNode}.
     */
    public static final class PlanContext {
        public final TableScan scan;
        public final Table icebergTable;
        public final Snapshot snapshot;
        public final IcebergManifestCache manifestCache;
        public final String tableName;

        public PlanContext(TableScan scan,
                           Table icebergTable,
                           Snapshot snapshot,
                           IcebergManifestCache manifestCache,
                           String tableName) {
            this.scan = scan;
            this.icebergTable = icebergTable;
            this.snapshot = snapshot;
            this.manifestCache = manifestCache;
            this.tableName = tableName;
        }
    }

    /**
     * Plan {@link FileScanTask}s with manifest cache support.
     */
    public CloseableIterable<FileScanTask> planWithCache(PlanContext ctx) throws IOException {
        // Get data manifests and delete manifests
        List<ManifestFile> dataManifests = Lists.newArrayList(
                IcebergUtils.getMatchingManifest(
                        ctx.snapshot.dataManifests(ctx.icebergTable.io()),
                        ctx.icebergTable.specs(),
                        ctx.scan.filter()));
        List<ManifestFile> deleteManifests = Lists.newArrayList(
                ctx.snapshot.deleteManifests(ctx.icebergTable.io()));

        // Process data manifests with cache
        return planDataManifestsWithCache(ctx, dataManifests, deleteManifests);
    }

    /**
     * Process data manifests with cache support.
     */
    private CloseableIterable<FileScanTask> planDataManifestsWithCache(
            PlanContext ctx,
            List<ManifestFile> dataManifests,
            List<ManifestFile> deleteManifests) throws IOException {

        List<FileScanTask> cachedTasks = new ArrayList<>();
        List<ManifestFile> uncachedManifests = new ArrayList<>();

        // Separate cached and uncached manifests
        for (ManifestFile manifestFile : dataManifests) {
            Set<DataFile> dataFiles = ctx.manifestCache.getDataFiles(manifestFile.path());
            if (dataFiles != null && !dataFiles.isEmpty()) {
                // Register mapping so invalidateTableCache can clean up
                ctx.manifestCache.prepareCache(manifestFile.path(), ctx.tableName);
                // Cache hit: create FileScanTask from cached DataFiles
                List<FileScanTask> tasks = createFileScanTasksFromCachedDataFiles(
                        ctx, manifestFile, dataFiles, deleteManifests);
                cachedTasks.addAll(tasks);
            } else {
                // Cache miss: mark for reading
                ctx.manifestCache.prepareCache(manifestFile.path(), ctx.tableName);
                uncachedManifests.add(manifestFile);
            }
        }

        // Process uncached manifests: read and fill cache
        CloseableIterable<FileScanTask> uncachedTasks = null;
        if (!uncachedManifests.isEmpty()) {
            uncachedTasks = planFileScanTaskFromManifests(
                    ctx, uncachedManifests, deleteManifests);
        }

        // Merge cached and uncached results
        return mergeFileScanTasks(cachedTasks, uncachedTasks);
    }

    /**
     * Create {@link FileScanTask}s from cached {@link DataFile}s.
     */
    private List<FileScanTask> createFileScanTasksFromCachedDataFiles(
            PlanContext ctx,
            ManifestFile manifestFile,
            Set<DataFile> cachedDataFiles,
            List<ManifestFile> deleteManifests) {

        List<FileScanTask> tasks = new ArrayList<>();

        // Get partition and file filters
        Expression dataFilter = ctx.scan.filter();
        PartitionSpec spec = ctx.icebergTable.specs().get(manifestFile.partitionSpecId());

        // Create evaluators for filtering
        // 1. Partition filter: project dataFilter to partition expression
        Expression partitionFilter = Projections.inclusive(spec, ctx.scan.isCaseSensitive())
                .project(dataFilter);
        Evaluator partitionEvaluator = new Evaluator(
                spec.partitionType(), partitionFilter, ctx.scan.isCaseSensitive());

        // 2. File filter: use InclusiveMetricsEvaluator for file-level metrics filtering
        InclusiveMetricsEvaluator fileEvaluator = new InclusiveMetricsEvaluator(
                ctx.icebergTable.schema(), dataFilter, ctx.scan.isCaseSensitive());

        // Get relevant delete files for this scan (filtered by manifest/partition/data
        // filter)
        Set<DeleteFile> cachedDeleteFiles = loadRelevantDeleteFiles(ctx, deleteManifests);
        DeleteIndex deleteIndex = DeleteIndex.build(cachedDeleteFiles);

        // Create TaskContext
        // Note: For cached files, we don't ignore residuals to maintain correctness
        boolean ignoreResiduals = false;
        // Simplified: always keep stats for cached files
        boolean dropStats = false;
        // Simplified: keep all stats
        Set<Integer> columnsToKeepStats = null;
        TaskContextHelper taskContext = createTaskContext(
                spec, ctx.scan.filter(), ctx.scan.isCaseSensitive(),
                ignoreResiduals, dropStats, columnsToKeepStats);

        for (DataFile dataFile : cachedDataFiles) {
            // 1. Apply partition filter
            if (!partitionEvaluator.eval(dataFile.partition())) {
                continue;
            }

            // 2. Apply file filter (based on metrics)
            if (!fileEvaluator.eval(dataFile)) {
                continue;
            }

            List<DeleteFile> matchingDeletes = new ArrayList<>();
            deleteIndex.match(dataFile, matchingDeletes);

            // 3. Find matching DeleteFiles
            DeleteFile[] deleteFilesArray = matchingDeletes.isEmpty()
                    ? new DeleteFile[0]
                    : matchingDeletes.toArray(new DeleteFile[0]);

            // 4. Create FileScanTask
            FileScanTask task = createFileScanTaskFromDataFile(
                    dataFile, deleteFilesArray, taskContext);
            tasks.add(task);
        }

        return tasks;
    }

    /**
     * Load delete files that are relevant to current scan filter/partition filter.
     * This mirrors Iceberg DeleteFileIndex manifest-level filtering to avoid
     * over-application.
     */
    private Set<DeleteFile> loadRelevantDeleteFiles(PlanContext ctx, List<ManifestFile> deleteManifests) {
        Set<DeleteFile> result = new HashSet<>();
        Expression dataFilter = ctx.scan.filter() == null ? Expressions.alwaysTrue() : ctx.scan.filter();
        boolean caseSensitive = ctx.scan.isCaseSensitive();

        for (ManifestFile deleteManifest : deleteManifests) {
            PartitionSpec spec = ctx.icebergTable.specs().get(deleteManifest.partitionSpecId());
            // project data filter to partition to prune manifest
            Expression projectedPartitionFilter = Projections.inclusive(spec, caseSensitive).project(dataFilter);
            ManifestEvaluator manifestEvaluator = ManifestEvaluator.forPartitionFilter(
                    projectedPartitionFilter, spec, caseSensitive);
            if (!manifestEvaluator.eval(deleteManifest)) {
                continue;
            }

            // Partition-level evaluator for per-file filtering
            Evaluator partitionEvaluator = new Evaluator(spec.partitionType(), projectedPartitionFilter, caseSensitive);

            Set<DeleteFile> cached = Env.getCurrentEnv()
                    .getExtMetaCacheMgr()
                    .getIcebergMetadataCache()
                    .getManifestCache()
                    .getDeleteFiles(deleteManifest.path());

            if (cached != null && !cached.isEmpty()) {
                for (DeleteFile df : cached) {
                    if (df.partition() == null || partitionEvaluator.eval(df.partition())) {
                        result.add(df);
                    }
                }
                continue;
            }

            // cache miss: read delete manifest with filters and fill cache
            Set<DeleteFile> deleteFiles = Sets.newHashSet();
            try {
                ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(
                        deleteManifest, ctx.icebergTable.io(), ctx.icebergTable.specs());
                try (CloseableIterable<DeleteFile> files = reader
                        .filterRows(dataFilter)
                        .filterPartitions(projectedPartitionFilter)
                        .caseSensitive(caseSensitive)) {
                    for (DeleteFile df : files) {
                        if (df.partition() == null || partitionEvaluator.eval(df.partition())) {
                            deleteFiles.add(df);
                            result.add(df);
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to read delete manifest {} for cache", deleteManifest.path(), e);
            }
            if (!deleteFiles.isEmpty()) {
                Env.getCurrentEnv()
                        .getExtMetaCacheMgr()
                        .getIcebergMetadataCache()
                        .getManifestCache()
                        .putDeleteFiles(deleteManifest.path(), deleteFiles);
            }
        }
        return result;
    }

    /**
     * Lightweight delete file index built from cached delete files.
     * Mirrors Iceberg DeleteFileIndex grouping: position deletes by referenced path
     * (preferred)
     * or partition, equality deletes by partition.
     */
    private static class DeleteIndex {
        private final Map<String, List<DeleteFile>> posDeletesByPath;
        private final Map<Integer, Map<StructLike, List<DeleteFile>>> posDeletesByPartition;
        private final Map<Integer, Map<StructLike, List<DeleteFile>>> eqDeletesByPartition;

        private DeleteIndex(Map<String, List<DeleteFile>> posDeletesByPath,
                Map<Integer, Map<StructLike, List<DeleteFile>>> posDeletesByPartition,
                Map<Integer, Map<StructLike, List<DeleteFile>>> eqDeletesByPartition) {
            this.posDeletesByPath = posDeletesByPath;
            this.posDeletesByPartition = posDeletesByPartition;
            this.eqDeletesByPartition = eqDeletesByPartition;
        }

        static DeleteIndex build(Set<DeleteFile> deleteFiles) {
            Map<String, List<DeleteFile>> posByPath = new HashMap<>();
            Map<Integer, Map<StructLike, List<DeleteFile>>> posByPartition = new HashMap<>();
            Map<Integer, Map<StructLike, List<DeleteFile>>> eqByPartition = new HashMap<>();
            for (DeleteFile deleteFile : deleteFiles) {
                if (deleteFile.content() == FileContent.POSITION_DELETES) {
                    CharSequence refPath = ContentFileUtil.referencedDataFile(deleteFile);
                    if (refPath != null) {
                        posByPath.computeIfAbsent(refPath.toString(), k -> new ArrayList<>()).add(deleteFile);
                    } else {
                        posByPartition
                                .computeIfAbsent(deleteFile.specId(), k -> new HashMap<>())
                                .computeIfAbsent(deleteFile.partition(), k -> new ArrayList<>())
                                .add(deleteFile);
                    }
                } else if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                    eqByPartition
                            .computeIfAbsent(deleteFile.specId(), k -> new HashMap<>())
                            .computeIfAbsent(deleteFile.partition(), k -> new ArrayList<>())
                            .add(deleteFile);
                }
            }
            return new DeleteIndex(posByPath, posByPartition, eqByPartition);
        }

        void match(DataFile dataFile, List<DeleteFile> out) {
            // Position deletes matched by data file path (preferred)
            List<DeleteFile> pathDeletes = posDeletesByPath.get(dataFile.path().toString());
            if (pathDeletes != null) {
                for (DeleteFile deleteFile : pathDeletes) {
                    if (deleteFile.dataSequenceNumber() >= dataFile.dataSequenceNumber()) {
                        out.add(deleteFile);
                    }
                }
            }

            // Position deletes matched by partition (when no referenced path is present)
            Map<StructLike, List<DeleteFile>> partitionPosDeletes = posDeletesByPartition
                    .getOrDefault(dataFile.specId(), Collections.emptyMap());
            List<DeleteFile> posPartitionList = partitionPosDeletes.get(dataFile.partition());
            if (posPartitionList != null) {
                for (DeleteFile deleteFile : posPartitionList) {
                    if (deleteFile.dataSequenceNumber() >= dataFile.dataSequenceNumber()) {
                        out.add(deleteFile);
                    }
                }
            }

            // Equality deletes matched by partition
            Map<StructLike, List<DeleteFile>> partitionEqDeletes = eqDeletesByPartition.getOrDefault(dataFile.specId(),
                    Collections.emptyMap());
            List<DeleteFile> eqPartitionList = partitionEqDeletes.get(dataFile.partition());
            if (eqPartitionList != null) {
                for (DeleteFile deleteFile : eqPartitionList) {
                    if (deleteFile.dataSequenceNumber() >= dataFile.dataSequenceNumber()) {
                        out.add(deleteFile);
                    }
                }
            }
        }
    }

    /**
     * Create {@link FileScanTask} from {@link DataFile}.
     */
    private FileScanTask createFileScanTaskFromDataFile(
            DataFile dataFile,
            DeleteFile[] deleteFiles,
            TaskContextHelper taskContext) {

        // Copy DataFile (defensive copy)
        // For simplicity, we always copy with stats (can be optimized later)
        DataFile copiedDataFile = taskContext.shouldKeepStats()
                ? dataFile.copy()
                : dataFile.copyWithoutStats();

        // Create BaseFileScanTask (using public constructor)
        return new BaseFileScanTask(
                copiedDataFile,
                deleteFiles != null ? deleteFiles : new DeleteFile[0],
                taskContext.schemaAsString(),
                taskContext.specAsString(),
                taskContext.residuals());
    }

    /**
     * Create TaskContext for creating FileScanTask.
     */
    private TaskContextHelper createTaskContext(
            PartitionSpec spec,
            Expression dataFilter,
            boolean caseSensitive,
            boolean ignoreResiduals,
            boolean dropStats,
            Set<Integer> columnsToKeepStats) {

        // Create ResidualEvaluator
        Expression filter = ignoreResiduals ? Expressions.alwaysTrue() : dataFilter;
        ResidualEvaluator residuals = ResidualEvaluator.of(spec, filter, caseSensitive);

        // Serialize Schema and Spec
        String schemaAsString = SchemaParser.toJson(spec.schema());
        String specAsString = org.apache.iceberg.PartitionSpecParser.toJson(spec);

        return new TaskContextHelper(
                schemaAsString,
                specAsString,
                residuals,
                dropStats,
                columnsToKeepStats,
                ScanMetrics.noop());
    }

    /**
     * Process uncached manifests: read and fill cache.
     */
    private CloseableIterable<FileScanTask> planFileScanTaskFromManifests(
            PlanContext ctx,
            List<ManifestFile> uncachedManifests,
            List<ManifestFile> deleteManifests) throws IOException {

        // For uncached manifests, use TableScan.planFiles() and fill cache
        // Note: We still use scan.planFiles() for uncached manifests to leverage
        // Iceberg's filtering and planning logic
        CloseableIterable<FileScanTask> tasks = ctx.scan.planFiles();

        // Wrap to fill cache during iteration
        return wrapTasksWithCacheFill(ctx, tasks, uncachedManifests, deleteManifests);
    }

    /**
     * Merge cached and uncached FileScanTasks.
     */
    private CloseableIterable<FileScanTask> mergeFileScanTasks(
            List<FileScanTask> cachedTasks,
            CloseableIterable<FileScanTask> uncachedTasks) {

        if (cachedTasks.isEmpty() && uncachedTasks == null) {
            return CloseableIterable.empty();
        }

        if (cachedTasks.isEmpty()) {
            return uncachedTasks;
        }

        if (uncachedTasks == null) {
            return CloseableIterable.withNoopClose(cachedTasks);
        }

        // Merge both
        return CloseableIterable.combine(
                CloseableIterable.withNoopClose(cachedTasks),
                uncachedTasks);
    }

    /**
     * Wrap FileScanTask to collect DataFile and DeleteFile for caching.
     * For uncached manifests, read them using ManifestFiles.read() and fill cache.
     */
    private CloseableIterable<FileScanTask> wrapTasksWithCacheFill(
            PlanContext ctx,
            CloseableIterable<FileScanTask> tasks,
            List<ManifestFile> uncachedManifests,
            List<ManifestFile> deleteManifests) throws IOException {

        // Read data manifests and fill cache
        // Note: We use ManifestReader's iterator() which returns ContentFile directly
        for (ManifestFile manifest : uncachedManifests) {
            try {
                ManifestReader<DataFile> reader = ManifestFiles.read(
                        manifest, ctx.icebergTable.io(), ctx.icebergTable.specs());
                Set<DataFile> dataFiles = Sets.newHashSet();
                try (CloseableIterable<DataFile> files = reader) {
                    for (DataFile file : files) {
                        dataFiles.add(file);
                    }
                }
                if (!dataFiles.isEmpty()) {
                    ctx.manifestCache.putDataFiles(manifest.path(), dataFiles);
                }
            } catch (Exception e) {
                LOG.warn("Failed to read manifest {} for cache", manifest.path(), e);
            }
        }

        // Read delete manifests and fill cache
        for (ManifestFile manifest : deleteManifests) {
            try {
                ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(
                        manifest, ctx.icebergTable.io(), ctx.icebergTable.specs());
                Set<DeleteFile> deleteFiles = Sets.newHashSet();
                try (CloseableIterable<DeleteFile> files = reader) {
                    for (DeleteFile file : files) {
                        deleteFiles.add(file);
                    }
                }
                if (!deleteFiles.isEmpty()) {
                    ctx.manifestCache.putDeleteFiles(manifest.path(), deleteFiles);
                }
            } catch (Exception e) {
                LOG.warn("Failed to read delete manifest {} for cache", manifest.path(), e);
            }
        }

        // Return the original tasks (cache is already filled)
        return tasks;
    }

    /**
     * TaskContext helper class (replacement for ManifestGroup.TaskContext).
     */
    private static class TaskContextHelper {
        private final String schemaAsString;
        private final String specAsString;
        private final ResidualEvaluator residuals;
        private final boolean dropStats;
        private final Set<Integer> columnsToKeepStats;
        private final ScanMetrics scanMetrics;

        TaskContextHelper(String schemaAsString, String specAsString,
                          ResidualEvaluator residuals,
                          boolean dropStats, Set<Integer> columnsToKeepStats,
                          ScanMetrics scanMetrics) {
            this.schemaAsString = schemaAsString;
            this.specAsString = specAsString;
            this.residuals = residuals;
            this.dropStats = dropStats;
            this.columnsToKeepStats = columnsToKeepStats;
            this.scanMetrics = scanMetrics;
        }

        String schemaAsString() {
            return schemaAsString;
        }

        String specAsString() {
            return specAsString;
        }

        ResidualEvaluator residuals() {
            return residuals;
        }

        boolean shouldKeepStats() {
            return !dropStats;
        }

        Set<Integer> columnsToKeepStats() {
            return columnsToKeepStats;
        }

        ScanMetrics scanMetrics() {
            return scanMetrics;
        }
    }
}
