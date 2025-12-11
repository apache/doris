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
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        // Get cached delete files for all delete manifests
        Set<DeleteFile> cachedDeleteFiles = new HashSet<>();
        for (ManifestFile deleteManifest : deleteManifests) {
            Set<DeleteFile> deleteFiles = Env.getCurrentEnv()
                    .getExtMetaCacheMgr()
                    .getIcebergMetadataCache()
                    .getManifestCache()
                    .getDeleteFiles(deleteManifest.path());
            if (deleteFiles != null) {
                cachedDeleteFiles.addAll(deleteFiles);
            }
        }

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

            // 3. Find matching DeleteFiles
            DeleteFile[] deleteFilesArray = findDeleteFilesForDataFile(
                    dataFile, cachedDeleteFiles);

            // 4. Create FileScanTask
            FileScanTask task = createFileScanTaskFromDataFile(
                    dataFile, deleteFilesArray, taskContext);
            tasks.add(task);
        }

        return tasks;
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
     * Find DeleteFiles for a DataFile.
     */
    private DeleteFile[] findDeleteFilesForDataFile(
            DataFile dataFile,
            Set<DeleteFile> cachedDeleteFiles) {

        if (cachedDeleteFiles == null || cachedDeleteFiles.isEmpty()) {
            return new DeleteFile[0];
        }

        List<DeleteFile> matchingDeletes = new ArrayList<>();
        long dataFileSequence = dataFile.dataSequenceNumber();

        for (DeleteFile deleteFile : cachedDeleteFiles) {
            // Check sequence number
            if (deleteFile.dataSequenceNumber() < dataFileSequence) {
                continue;
            }

            // Match based on DeleteFile type
            if (deleteFile.content() == FileContent.EQUALITY_DELETES) {
                // Equality deletes: match partition
                if (matchesPartition(dataFile, deleteFile)) {
                    matchingDeletes.add(deleteFile);
                }
            } else if (deleteFile.content() == FileContent.POSITION_DELETES) {
                // Position deletes: match file path or partition
                if (matchesFileOrPartition(dataFile, deleteFile)) {
                    matchingDeletes.add(deleteFile);
                }
            }
        }

        return matchingDeletes.toArray(new DeleteFile[0]);
    }

    /**
     * Check if DeleteFile matches DataFile's partition.
     */
    private boolean matchesPartition(DataFile dataFile, DeleteFile deleteFile) {
        StructLike dataPartition = dataFile.partition();
        StructLike deletePartition = deleteFile.partition();

        if (dataPartition == null || deletePartition == null) {
            return false;
        }

        return dataPartition.equals(deletePartition);
    }

    /**
     * Check if DeleteFile matches DataFile's file path or partition.
     */
    private boolean matchesFileOrPartition(DataFile dataFile, DeleteFile deleteFile) {
        // For position deletes, a full implementation would check the referenced data file path.
        // Here we fall back to partition matching for simplicity.
        return matchesPartition(dataFile, deleteFile);
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

