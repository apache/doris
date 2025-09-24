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

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * Manager for organizing and filtering file scan tasks into rewrite groups.
 * This class handles:
 * 1. Planning FileScanTask from Iceberg table
 * 2. Grouping tasks by partition and size constraints
 * 3. Filtering groups based on rewrite parameters
 * 4. Providing iterator interface for processing groups
 */
public class RewriteDataFileManager {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFileManager.class);

    private final Table icebergTable;
    private final Parameters parameters;

    // File scan task management
    private Map<StructLikeWrapper, List<RewriteDataGroup>> partitionedGroups;
    private Iterator<List<RewriteDataGroup>> partitionIterator;
    private Iterator<RewriteDataGroup> groupIterator = Collections.emptyIterator();

    public RewriteDataFileManager(Table icebergTable, Parameters parameters) {
        this.icebergTable = icebergTable;
        this.parameters = parameters;
        this.partitionedGroups = new HashMap<>();
    }

    /**
     * Plan and organize file scan tasks into rewrite groups
     */
    public void planAndOrganizeTasks() throws Exception {
        LOG.info("Planning and organizing file scan tasks for table");

        // Step 1: Plan FileScanTask from Iceberg table
        List<FileScanTask> allTasks = planFileScanTasks();

        // Step 2: Group tasks by partition
        groupTasksByPartition(allTasks);

        // Step 3: Filter groups based on parameters
        filterGroups();

        LOG.info("Organized {} file groups for rewrite", getTotalGroupCount());
    }

    /**
     * Plan FileScanTask from Iceberg table
     */
    private List<FileScanTask> planFileScanTasks() throws Exception {
        LOG.info("Planning file scan tasks for table");

        // Create table scan with optional filters
        TableScan tableScan = icebergTable.newScan();

        // Apply partition filters if specified
        if (parameters.hasPartitionFilter()) {
            // TODO: Convert partition filter to Iceberg expression
            LOG.info("Partition filtering is specified but not yet implemented");
        }

        // Apply WHERE condition if specified
        if (parameters.hasWhereCondition()) {
            // TODO: Convert WHERE condition to Iceberg expression
            LOG.info("Where condition filtering is specified but not yet implemented");
        }

        // Get all file scan tasks
        List<FileScanTask> allTasks = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = tableScan.planFiles()) {
            for (FileScanTask task : tasks) {
                allTasks.add(task);
            }
        }

        LOG.info("Found {} file scan tasks for table", allTasks.size());
        return allTasks;
    }

    /**
     * Group file scan tasks by partition
     */
    private void groupTasksByPartition(List<FileScanTask> allTasks) {
        LOG.info("Grouping {} tasks by partition", allTasks.size());

        for (FileScanTask task : allTasks) {
            PartitionSpec spec = task.spec();
            StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(spec.partitionType());
            StructLikeWrapper partition = partitionWrapper.copyFor(task.file().partition());

            partitionedGroups.computeIfAbsent(partition, k -> new ArrayList<>());
            List<RewriteDataGroup> groups = partitionedGroups.get(partition);

            // Try to add task to existing group
            boolean added = false;
            for (RewriteDataGroup group : groups) {
                if (group.canAddTask(task, parameters)) {
                    group.addTask(task);
                    added = true;
                    break;
                }
            }

            // Create new group if needed
            if (!added) {
                RewriteDataGroup newGroup = new RewriteDataGroup();
                newGroup.addTask(task);
                groups.add(newGroup);
            }
        }

        LOG.info("Grouped tasks into {} partitions", partitionedGroups.size());
        this.partitionIterator = partitionedGroups.values().iterator();
    }

    /**
     * Filter groups based on rewrite parameters
     */
    private void filterGroups() {
        LOG.info("Filtering groups based on parameters");

        int totalGroupsBefore = getTotalGroupCount();
        int totalTasksBefore = getTotalTaskCount();

        // Filter groups that don't meet rewrite criteria
        partitionedGroups.entrySet().removeIf(entry -> {
            List<RewriteDataGroup> groups = entry.getValue();
            groups.removeIf(group -> !shouldRewriteGroup(group));
            return groups.isEmpty();
        });

        int totalGroupsAfter = getTotalGroupCount();
        int totalTasksAfter = getTotalTaskCount();

        LOG.info("Filtered groups: {} -> {}, tasks: {} -> {}",
                totalGroupsBefore, totalGroupsAfter, totalTasksBefore, totalTasksAfter);

        // Refresh iterator after filtering
        this.partitionIterator = partitionedGroups.values().iterator();
    }

    /**
     * Check if a group should be rewritten based on parameters
     */
    private boolean shouldRewriteGroup(RewriteDataGroup group) {
        // Always rewrite if rewrite_all is true
        if (parameters.isRewriteAll()) {
            return true;
        }

        // Check minimum number of files
        if (group.getTaskCount() < parameters.getMinInputFiles()) {
            return false;
        }

        // Check if any file needs rewriting based on size
        for (FileScanTask task : group.getTasks()) {
            long fileSize = task.file().fileSizeInBytes();
            if (fileSize < parameters.getMinFileSizeBytes()
                    || fileSize > parameters.getMaxFileSizeBytes()) {
                return true;
            }
        }

        return false;
    }

    // Iterator methods for group processing
    public boolean hasMoreGroup() {
        while (!groupIterator.hasNext() && partitionIterator.hasNext()) {
            groupIterator = partitionIterator.next().iterator();
        }
        return groupIterator.hasNext();
    }

    public RewriteDataGroup nextGroup() {
        if (!hasMoreGroup()) {
            throw new NoSuchElementException();
        }
        return groupIterator.next();
    }

    // Helper methods
    public int getTotalGroupCount() {
        return partitionedGroups.values().stream()
                .mapToInt(List::size)
                .sum();
    }

    public int getTotalTaskCount() {
        return partitionedGroups.values().stream()
                .flatMap(List::stream)
                .mapToInt(RewriteDataGroup::getTaskCount)
                .sum();
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
        private final long outputSpecId;

        private final Optional<PartitionNamesInfo> partitionFilter;
        private final Optional<Expression> whereCondition;

        public Parameters(long targetFileSizeBytes,
                long minFileSizeBytes,
                long maxFileSizeBytes,
                int minInputFiles,
                boolean rewriteAll,
                long maxFileGroupSizeBytes,
                int deleteFileThreshold,
                double deleteRatioThreshold,
                long outputSpecId,
                Optional<PartitionNamesInfo> partitionFilter,
                Optional<Expression> whereCondition) {
            this.targetFileSizeBytes = targetFileSizeBytes;
            this.minFileSizeBytes = minFileSizeBytes;
            this.maxFileSizeBytes = maxFileSizeBytes;
            this.minInputFiles = minInputFiles;
            this.rewriteAll = rewriteAll;
            this.maxFileGroupSizeBytes = maxFileGroupSizeBytes;
            this.deleteFileThreshold = deleteFileThreshold;
            this.deleteRatioThreshold = deleteRatioThreshold;
            this.outputSpecId = outputSpecId;
            this.partitionFilter = partitionFilter;
            this.whereCondition = whereCondition;
        }

        // Getters
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

        public long getOutputSpecId() {
            return outputSpecId;
        }

        public boolean hasPartitionFilter() {
            return partitionFilter.isPresent();
        }

        public Optional<PartitionNamesInfo> getPartitionFilter() {
            return partitionFilter;
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
                    + "targetFileSizeBytes=" + targetFileSizeBytes
                    + ", minFileSizeBytes=" + minFileSizeBytes
                    + ", maxFileSizeBytes=" + maxFileSizeBytes
                    + ", minInputFiles=" + minInputFiles
                    + ", rewriteAll=" + rewriteAll
                    + ", maxFileGroupSizeBytes=" + maxFileGroupSizeBytes
                    + ", deleteFileThreshold=" + deleteFileThreshold
                    + ", deleteRatioThreshold=" + deleteRatioThreshold
                    + ", outputSpecId=" + outputSpecId
                    + ", hasPartitionFilter=" + hasPartitionFilter()
                    + ", hasWhereCondition=" + hasWhereCondition()
                    + '}';
        }
    }
}
