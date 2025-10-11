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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Planner for organizing and filtering file scan tasks into rewrite groups.
 * This class handles:
 * 1. Planning FileScanTask from Iceberg table
 * 2. Grouping tasks by partition and size constraints
 * 3. Filtering groups based on rewrite parameters
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

            // Step 2: Filter files based on rewrite criteria (matching Iceberg logic)
            Iterable<FileScanTask> filteredTasks = parameters.isRewriteAll() ? allTasks : filterFiles(allTasks);

            // Step 3: Group tasks by partition
            Iterable<RewriteDataGroup> groupedTasks = groupTasksByPartition(filteredTasks);

            // Step 4: Filter groups based on parameters
            Iterable<RewriteDataGroup> filteredGroups = parameters.isRewriteAll() ? groupedTasks
                    : filterGroups(groupedTasks);
            return Lists.newArrayList(filteredGroups);
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

        // Apply WHERE condition if specified
        if (parameters.hasWhereCondition()) {
            org.apache.iceberg.expressions.Expression icebergExpression = IcebergNereidsUtils
                    .convertNereidsToIcebergExpression(parameters.getWhereCondition().get(), icebergTable.schema());
            tableScan = tableScan.filter(icebergExpression);
        }
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

        // Calculate known deleted record count (only file-scoped deletes)
        long knownDeletedRecordCount = task.deletes().stream()
                .filter(ContentFileUtil::isFileScoped)
                .mapToLong(ContentFile::recordCount)
                .sum();

        // Calculate delete ratio
        double deletedRecords = (double) Math.min(knownDeletedRecordCount, task.file().recordCount());
        double deleteRatio = deletedRecords / task.file().recordCount();

        return deleteRatio >= parameters.getDeleteRatioThreshold();
    }

    /**
     * Group file scan tasks by partition
     */
    private Iterable<RewriteDataGroup> groupTasksByPartition(Iterable<FileScanTask> allTasks) {
        Map<StructLikeWrapper, RewriteDataGroup> groups = new HashMap<>();
        for (FileScanTask task : allTasks) {
            PartitionSpec spec = task.spec();
            StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(spec.partitionType());
            StructLikeWrapper partition = partitionWrapper.copyFor(task.file().partition());
            groups.computeIfAbsent(partition, k -> new RewriteDataGroup()).addTask(task);
        }
        return groups.values();
    }

    /**
     * Filter groups based on rewrite parameters
     */
    private Iterable<RewriteDataGroup> filterGroups(Iterable<RewriteDataGroup> groupedTasks) {
        return Iterables.filter(groupedTasks, group -> shouldRewriteGroup(group));
    }

    /**
     * Check if a group should be rewritten based on parameters
     */
    private boolean shouldRewriteGroup(RewriteDataGroup group) {
        return enoughInputFiles(group) || enoughContent(group) || tooMuchContent(group)
                || group.getTasks().stream().anyMatch(this::tooManyDeletes)
                || group.getTasks().stream().anyMatch(this::tooHighDeleteRatio);
    }

    /**
     * Check if group has enough input files
     */
    private boolean enoughInputFiles(RewriteDataGroup group) {
        return group.getTaskCount() > 1 && group.getTaskCount() >= parameters.getMinInputFiles();
    }

    /**
     * Check if group has enough content
     */
    private boolean enoughContent(RewriteDataGroup group) {
        return group.getTaskCount() > 1 && group.getTotalSize() > parameters.getTargetFileSizeBytes();
    }

    /**
     * Check if group has too much content
     */
    private boolean tooMuchContent(RewriteDataGroup group) {
        return group.getTotalSize() > parameters.getMaxFileGroupSizeBytes();
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
