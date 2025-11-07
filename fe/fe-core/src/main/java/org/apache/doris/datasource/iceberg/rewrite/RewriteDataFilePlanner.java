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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergNereidsUtils;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.util.BinPacking;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Planner for organizing and filtering file scan tasks into rewrite groups.
 */
public class RewriteDataFilePlanner {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFilePlanner.class);

    private final Parameters parameters;

    public RewriteDataFilePlanner(Parameters parameters) {
        this.parameters = parameters;
    }

    /**
     * Plan and organize file scan tasks into rewrite groups
     */
    public List<RewriteDataGroup> planAndOrganizeTasks(Table icebergTable) throws UserException {
        try {
            // Step 1: Plan FileScanTask from Iceberg table
            Iterable<FileScanTask> allTasks = planFileScanTasks(icebergTable);

            // Step 2: First layer - Group tasks by partition (without filtering files)
            Map<StructLikeWrapper, List<FileScanTask>> filesByPartition = groupTasksByPartition(allTasks);

            // Step 3: Apply binPack grouping strategy within each partition and convert to
            // RewriteDataGroup
            Map<StructLikeWrapper, List<RewriteDataGroup>> fileGroupsByPartition = Maps.transformValues(
                    filesByPartition, this::packGroupsInPartition);

            // Step 4: Flatten all groups from all partitions
            return fileGroupsByPartition.values().stream()
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new UserException("Failed to plan file scan tasks: " + e.getMessage(), e);
        }
    }

    /**
     * Plan FileScanTask from Iceberg table
     */
    private Iterable<FileScanTask> planFileScanTasks(Table icebergTable) throws UserException {
        // Create table scan with optional filters
        TableScan tableScan = icebergTable.newScan();

        // Use current snapshot if available
        if (icebergTable.currentSnapshot() != null) {
            tableScan = tableScan.useSnapshot(icebergTable.currentSnapshot().snapshotId());
        }

        // Apply WHERE condition if specified
        if (parameters.hasWhereCondition()) {
            org.apache.iceberg.expressions.Expression icebergExpression = IcebergNereidsUtils
                    .convertNereidsToIcebergExpression(parameters.getWhereCondition().get(), icebergTable.schema());
            tableScan = tableScan.filter(icebergExpression);
        }

        // Ignore residuals to avoid reading data files unnecessarily
        tableScan = tableScan.ignoreResiduals();

        return tableScan.planFiles();
    }

    /**
     * Filter files based on rewrite criteria
     */
    private Iterable<FileScanTask> filterFiles(Iterable<FileScanTask> tasks) {
        return Iterables.filter(tasks, this::shouldRewriteFile);
    }

    /**
     * Check if a file should be rewritten
     */
    private boolean shouldRewriteFile(FileScanTask task) {
        return outsideDesiredFileSizeRange(task) || tooManyDeletes(task) || tooHighDeleteRatio(task);
    }

    /**
     * Check if file is outside desired size range
     */
    private boolean outsideDesiredFileSizeRange(FileScanTask task) {
        long fileSize = task.file().fileSizeInBytes();
        return fileSize < parameters.getMinFileSizeBytes() || fileSize > parameters.getMaxFileSizeBytes();
    }

    /**
     * Check if file has too many delete files
     */
    private boolean tooManyDeletes(FileScanTask task) {
        if (task.deletes() == null) {
            return false;
        }
        return task.deletes().size() >= parameters.getDeleteFileThreshold();
    }

    /**
     * Check if file has too high delete ratio
     */
    private boolean tooHighDeleteRatio(FileScanTask task) {
        if (task.deletes() == null || task.deletes().isEmpty()) {
            return false;
        }

        long recordCount = task.file().recordCount();
        if (recordCount == 0) {
            return false;
        }

        // Calculate known deleted record count (only file-scoped deletes)
        long knownDeletedRecordCount = task.deletes().stream()
                .filter(ContentFileUtil::isFileScoped)
                .mapToLong(ContentFile::recordCount)
                .sum();

        // Calculate delete ratio
        double deletedRecords = (double) Math.min(knownDeletedRecordCount, recordCount);
        double deleteRatio = deletedRecords / recordCount;

        return deleteRatio >= parameters.getDeleteRatioThreshold();
    }

    /**
     * Returns a map from partition to list of file scan tasks in that partition.
     */
    private Map<StructLikeWrapper, List<FileScanTask>> groupTasksByPartition(Iterable<FileScanTask> allTasks) {
        Map<StructLikeWrapper, List<FileScanTask>> filesByPartition = new HashMap<>();
        for (FileScanTask task : allTasks) {
            PartitionSpec spec = task.spec();
            StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(spec.partitionType());

            // If a task uses an incompatible partition spec, treat it as un-partitioned
            // by using an empty partition (all null values)
            StructLikeWrapper partition;
            if (task.file().specId() == spec.specId()) {
                partition = partitionWrapper.copyFor(task.file().partition());
            } else {
                // Use empty partition for incompatible spec
                // Create an empty GenericRecord with all null values
                org.apache.iceberg.StructLike emptyStruct = GenericRecord.create(spec.partitionType());
                partition = partitionWrapper.copyFor(emptyStruct);
            }

            filesByPartition.computeIfAbsent(partition, k -> Lists.newArrayList()).add(task);
        }
        return filesByPartition;
    }

    /**
     * Pack files in a partition using bin-packing strategy.
     * <p>
     * This method is used to group files in a partition using bin-packing strategy.
     * It first filters files if not rewriteAll, then uses bin-packing to group
     * files based on their size, and then converts the groups to RewriteDataGroup.
     * Finally, it filters groups if not rewriteAll.
     * </p>
     */
    private List<RewriteDataGroup> packGroupsInPartition(List<FileScanTask> tasks) {
        // Step 1: Filter files if not rewriteAll
        Iterable<FileScanTask> filteredTasks = parameters.isRewriteAll() ? tasks : filterFiles(tasks);

        // Step 2: Use bin-packing to group files
        BinPacking.ListPacker<FileScanTask> packer = new BinPacking.ListPacker<>(
                parameters.getMaxFileGroupSizeBytes(),
                1, // lookback: number of bins to look back when packing
                false // largestBinFirst: whether to prefer larger bins
        );

        // Pack files using file size as weight
        List<List<FileScanTask>> groups = packer.pack(filteredTasks, task -> task.file().fileSizeInBytes());

        // Step 3: Convert to RewriteDataGroup
        List<RewriteDataGroup> rewriteDataGroups = groups.stream()
                .map(RewriteDataGroup::new)
                .collect(Collectors.toList());

        // Step 4: Filter groups if not rewriteAll
        return parameters.isRewriteAll() ? rewriteDataGroups : filterFileGroups(rewriteDataGroups);
    }

    /**
     * Filter file groups based on rewrite parameters.
     * Only groups that meet the rewrite criteria are kept.
     */
    private List<RewriteDataGroup> filterFileGroups(List<RewriteDataGroup> groups) {
        return groups.stream()
                .filter(this::shouldRewriteFileGroup)
                .collect(Collectors.toList());
    }

    /**
     * Check if a file group should be rewritten based on parameters.
     */
    private boolean shouldRewriteFileGroup(RewriteDataGroup group) {
        return hasEnoughInputFiles(group) || hasEnoughContent(group)
                || hasTooMuchContent(group) || hasDeleteIssues(group);
    }

    /**
     * Check if group has enough input files
     */
    private boolean hasEnoughInputFiles(RewriteDataGroup group) {
        return group.getTaskCount() > 1 && group.getTaskCount() >= parameters.getMinInputFiles();
    }

    /**
     * Check if group has enough content
     */
    private boolean hasEnoughContent(RewriteDataGroup group) {
        return group.getTaskCount() > 1 && group.getTotalSize() > parameters.getTargetFileSizeBytes();
    }

    /**
     * Check if group has too much content
     */
    private boolean hasTooMuchContent(RewriteDataGroup group) {
        return group.getTotalSize() > parameters.getMaxFileGroupSizeBytes();
    }

    /**
     * Check if any file in the group has too many deletes or high delete ratio
     */
    private boolean hasDeleteIssues(RewriteDataGroup group) {
        return group.getTasks().stream()
                .anyMatch(task -> tooManyDeletes(task) || tooHighDeleteRatio(task));
    }

    /**
     * Parameters for Iceberg data file rewrite operation
     */
    public static class Parameters {
        private final long targetFileSizeBytes;
        private final long minFileSizeBytes;
        private final long maxFileSizeBytes;
        private final int minInputFiles;
        private final boolean rewriteAll;
        private final long maxFileGroupSizeBytes;
        private final int deleteFileThreshold;
        private final double deleteRatioThreshold;

        private final Optional<Expression> whereCondition;

        public Parameters(
                long targetFileSizeBytes,
                long minFileSizeBytes,
                long maxFileSizeBytes,
                int minInputFiles,
                boolean rewriteAll,
                long maxFileGroupSizeBytes,
                int deleteFileThreshold,
                double deleteRatioThreshold,
                long outputSpecId,
                Optional<Expression> whereCondition) {
            this.targetFileSizeBytes = targetFileSizeBytes;
            this.minFileSizeBytes = minFileSizeBytes;
            this.maxFileSizeBytes = maxFileSizeBytes;
            this.minInputFiles = minInputFiles;
            this.rewriteAll = rewriteAll;
            this.maxFileGroupSizeBytes = maxFileGroupSizeBytes;
            this.deleteFileThreshold = deleteFileThreshold;
            this.deleteRatioThreshold = deleteRatioThreshold;
            this.whereCondition = whereCondition;
        }

        public long getTargetFileSizeBytes() {
            return targetFileSizeBytes;
        }

        public long getMinFileSizeBytes() {
            return minFileSizeBytes;
        }

        public long getMaxFileSizeBytes() {
            return maxFileSizeBytes;
        }

        public int getMinInputFiles() {
            return minInputFiles;
        }

        public boolean isRewriteAll() {
            return rewriteAll;
        }

        public long getMaxFileGroupSizeBytes() {
            return maxFileGroupSizeBytes;
        }

        public int getDeleteFileThreshold() {
            return deleteFileThreshold;
        }

        public double getDeleteRatioThreshold() {
            return deleteRatioThreshold;
        }

        public boolean hasWhereCondition() {
            return whereCondition.isPresent();
        }

        public Optional<Expression> getWhereCondition() {
            return whereCondition;
        }

        @Override
        public String toString() {
            return "RewriteDataFilesParameters{"
                    + ", targetFileSizeBytes=" + targetFileSizeBytes
                    + ", minFileSizeBytes=" + minFileSizeBytes
                    + ", maxFileSizeBytes=" + maxFileSizeBytes
                    + ", minInputFiles=" + minInputFiles
                    + ", rewriteAll=" + rewriteAll
                    + ", maxFileGroupSizeBytes=" + maxFileGroupSizeBytes
                    + ", deleteFileThreshold=" + deleteFileThreshold
                    + ", deleteRatioThreshold=" + deleteRatioThreshold
                    + ", hasWhereCondition=" + hasWhereCondition()
                    + '}';
        }
    }
}
