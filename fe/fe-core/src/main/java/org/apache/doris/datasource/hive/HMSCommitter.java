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
// This file is copied from
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/metastore/SemiTransactionalHiveMetastore.java
// and modified by Doris

package org.apache.doris.datasource.hive;

import org.apache.doris.backup.Status;
import org.apache.doris.common.Pair;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.RemoteFileSystem;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUpdateMode;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.concurrent.MoreFutures;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class HMSCommitter {
    private static final Logger LOG = LogManager.getLogger(HMSCommitter.class);
    private final HiveMetadataOps hiveOps;
    private final RemoteFileSystem fs;
    private final Table table;

    // update statistics for unPartitioned table or existed partition
    private final List<UpdateStatisticsTask> updateStatisticsTasks = new ArrayList<>();
    Executor updateStatisticsExecutor = Executors.newFixedThreadPool(16);

    // add new partition
    private final AddPartitionsTask addPartitionsTask = new AddPartitionsTask();
    private static final int PARTITION_COMMIT_BATCH_SIZE = 20;

    // for file system rename operation
    // whether to cancel the file system tasks
    private final AtomicBoolean fileSystemTaskCancelled = new AtomicBoolean(false);
    // file system tasks that are executed asynchronously, including rename_file, rename_dir
    private final List<CompletableFuture<?>> asyncFileSystemTaskFutures = new ArrayList<>();
    // when aborted, we need to delete all files under this path, even the current directory
    private final Queue<DirectoryCleanUpTask> directoryCleanUpTasksForAbort = new ConcurrentLinkedQueue<>();
    // when aborted, we need restore directory
    private final List<RenameDirectoryTask> renameDirectoryTasksForAbort = new ArrayList<>();
    Executor fileSystemExecutor = Executors.newFixedThreadPool(16);

    public HMSCommitter(HiveMetadataOps hiveOps, RemoteFileSystem fs, Table table) {
        this.hiveOps = hiveOps;
        this.fs = fs;
        this.table = table;
    }

    public void commit(List<THivePartitionUpdate> hivePUs) {
        try {
            prepare(mergePartitions(hivePUs));
            doCommit();
        } catch (Throwable t) {
            LOG.warn("Failed to commit for {}.{}, abort it.", table.getDbName(), table.getTableName());
            try {
                cancelUnStartedAsyncFileSystemTask();
                undoUpdateStatisticsTasks();
                undoAddPartitionsTask();
                waitForAsyncFileSystemTaskSuppressThrowable();
                runDirectoryClearUpTasksForAbort();
                runRenameDirTasksForAbort();
            } catch (Throwable e) {
                t.addSuppressed(new Exception("Failed to roll back after commit failure", e));
            }
            throw t;
        }
    }

    public void prepare(List<THivePartitionUpdate> hivePUs) {

        List<Pair<THivePartitionUpdate, HivePartitionStatistics>> insertExistsPartitions = new ArrayList<>();

        for (THivePartitionUpdate pu : hivePUs) {
            TUpdateMode updateMode = pu.getUpdateMode();
            HivePartitionStatistics hivePartitionStatistics = HivePartitionStatistics.fromCommonStatistics(
                    pu.getRowCount(),
                    pu.getFileNamesSize(),
                    pu.getFileSize());
            if (table.getPartitionKeysSize() == 0) {
                Preconditions.checkArgument(hivePUs.size() == 1,
                        "When updating a non-partitioned table, multiple partitions should not be written");
                switch (updateMode) {
                    case APPEND:
                        prepareAppendTable(pu, hivePartitionStatistics);
                        break;
                    case OVERWRITE:
                        prepareOverwriteTable(pu, hivePartitionStatistics);
                        break;
                    default:
                        throw new RuntimeException("Not support mode:[" + updateMode + "] in unPartitioned table");
                }
            } else {
                switch (updateMode) {
                    case NEW:
                        prepareCreateNewPartition(pu, hivePartitionStatistics);
                        break;
                    case APPEND:
                        insertExistsPartitions.add(Pair.of(pu, hivePartitionStatistics));
                        break;
                    case OVERWRITE:
                        prepareOverwritePartition(pu, hivePartitionStatistics);
                        break;
                    default:
                        throw new RuntimeException("Not support mode:[" + updateMode + "] in unPartitioned table");
                }
            }
        }

        if (!insertExistsPartitions.isEmpty()) {
            prepareInsertExistPartition(insertExistsPartitions);
        }
    }

    public List<THivePartitionUpdate> mergePartitions(List<THivePartitionUpdate> hivePUs) {
        Map<String, THivePartitionUpdate> mm = new HashMap<>();
        for (THivePartitionUpdate pu : hivePUs) {
            if (mm.containsKey(pu.getName())) {
                THivePartitionUpdate old = mm.get(pu.getName());
                old.setFileSize(old.getFileSize() + pu.getFileSize());
                old.setRowCount(old.getRowCount() + pu.getRowCount());
                old.getFileNames().addAll(pu.getFileNames());
            } else {
                mm.put(pu.getName(), pu);
            }
        }
        return new ArrayList<>(mm.values());
    }

    public void doCommit() {
        waitForAsyncFileSystemTasks();
        doAddPartitionsTask();
        doUpdateStatisticsTasks();
    }

    public void rollback() {

    }

    public void cancelUnStartedAsyncFileSystemTask() {
        fileSystemTaskCancelled.set(true);
    }

    private void undoUpdateStatisticsTasks() {
        ImmutableList.Builder<CompletableFuture<?>> undoUpdateFutures = ImmutableList.builder();
        for (UpdateStatisticsTask task : updateStatisticsTasks) {
            undoUpdateFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    task.undo(hiveOps);
                } catch (Throwable throwable) {
                    LOG.warn("Failed to rollback: {}", task.getDescription(), throwable);
                }
            }, updateStatisticsExecutor));
        }

        for (CompletableFuture<?> undoUpdateFuture : undoUpdateFutures.build()) {
            MoreFutures.getFutureValue(undoUpdateFuture);
        }
    }

    private void undoAddPartitionsTask() {
        if (addPartitionsTask.isEmpty()) {
            return;
        }

        HivePartition firstPartition = addPartitionsTask.getPartitions().get(0).getPartition();
        String dbName = firstPartition.getDbName();
        String tableName = firstPartition.getTblName();
        List<List<String>> rollbackFailedPartitions = addPartitionsTask.rollback(hiveOps);
        if (!rollbackFailedPartitions.isEmpty()) {
            LOG.warn("Failed to rollback: add_partition for partition values {}.{}.{}",
                    dbName, tableName, rollbackFailedPartitions);
        }
    }

    private void waitForAsyncFileSystemTaskSuppressThrowable() {
        for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    public void prepareAppendTable(THivePartitionUpdate pu, HivePartitionStatistics ps) {
        String targetPath = pu.getLocation().getTargetPath();
        String writePath = pu.getLocation().getWritePath();
        if (!targetPath.equals(writePath)) {
            fs.asyncRename(
                    fileSystemExecutor,
                    asyncFileSystemTaskFutures,
                    fileSystemTaskCancelled,
                    writePath,
                    targetPath,
                    pu.getFileNames());
        }
        directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));
        updateStatisticsTasks.add(
            new UpdateStatisticsTask(
                table.getDbName(),
                table.getTableName(),
                Optional.empty(),
                ps,
                true
            ));
    }

    public void prepareOverwriteTable(THivePartitionUpdate pu, HivePartitionStatistics ps) {

    }

    public void prepareCreateNewPartition(THivePartitionUpdate pu, HivePartitionStatistics ps) {

        String targetPath = pu.getLocation().getTargetPath();
        String writePath = pu.getLocation().getWritePath();

        if (!targetPath.equals(writePath)) {
            fs.asyncRenameDir(
                    fileSystemExecutor,
                    asyncFileSystemTaskFutures,
                    fileSystemTaskCancelled,
                    writePath,
                    targetPath,
                    () -> directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true)));
        }

        StorageDescriptor sd = table.getSd();

        HivePartition hivePartition = new HivePartition(
                table.getDbName(),
                table.getTableName(),
                false,
                sd.getInputFormat(),
                pu.getLocation().getTargetPath(),
                HiveUtil.toPartitionValues(pu.getName()),
                Maps.newHashMap(),
                sd.getOutputFormat(),
                sd.getSerdeInfo().getSerializationLib(),
                hiveOps.getClient().getSchema(table.getDbName(), table.getTableName())
        );
        HivePartitionWithStatistics partitionWithStats =
                new HivePartitionWithStatistics(pu.getName(), hivePartition, ps);
        addPartitionsTask.addPartition(partitionWithStats);
    }

    public void prepareInsertExistPartition(List<Pair<THivePartitionUpdate, HivePartitionStatistics>> partitions) {
        for (List<Pair<THivePartitionUpdate, HivePartitionStatistics>> partitionBatch :
                    Iterables.partition(partitions, 100)) {
            List<String> partitionNames = partitionBatch.stream()
                    .map(pair -> pair.first.getName())
                    .collect(Collectors.toList());

            Map<String, Partition> partitionsByNamesMap = HiveUtil.convertToNamePartitionMap(
                    partitionNames,
                    hiveOps.getClient().getPartitions(table.getDbName(), table.getTableName(), partitionNames));

            for (int i = 0; i < partitionsByNamesMap.size(); i++) {
                String partitionName = partitionNames.get(i);
                if (partitionsByNamesMap.get(partitionName) == null) {
                    // Prevent this partition from being deleted by other engines
                    throw new RuntimeException("Not found partition: " + partitionName);
                }

                THivePartitionUpdate pu = partitionBatch.get(i).first;
                HivePartitionStatistics updateStats = partitionBatch.get(i).second;

                String writePath = pu.getLocation().getWritePath();
                String targetPath = pu.getLocation().getTargetPath();
                directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));

                if (!targetPath.equals(writePath)) {
                    fs.asyncRename(
                            fileSystemExecutor,
                            asyncFileSystemTaskFutures,
                            fileSystemTaskCancelled,
                            writePath,
                            targetPath,
                            pu.getFileNames());
                }

                updateStatisticsTasks.add(
                    new UpdateStatisticsTask(
                            table.getDbName(),
                            table.getTableName(),
                            Optional.of(pu.getName()),
                            updateStats,
                            true));
            }
        }
    }


    public void prepareOverwritePartition(THivePartitionUpdate pu, HivePartitionStatistics ps) {

    }


    private void waitForAsyncFileSystemTasks() {
        for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
            MoreFutures.getFutureValue(future, RuntimeException.class);
        }
    }

    private void doAddPartitionsTask() {
        if (!addPartitionsTask.isEmpty()) {
            addPartitionsTask.run(hiveOps);
        }
    }

    private void doUpdateStatisticsTasks() {
        ImmutableList.Builder<CompletableFuture<?>> updateStatsFutures = ImmutableList.builder();
        List<String> failedTaskDescriptions = new ArrayList<>();
        List<Throwable> suppressedExceptions = new ArrayList<>();
        for (UpdateStatisticsTask task : updateStatisticsTasks) {
            updateStatsFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    task.run(hiveOps);
                } catch (Throwable t) {
                    synchronized (suppressedExceptions) {
                        addSuppressedExceptions(suppressedExceptions, t, failedTaskDescriptions, task.getDescription());
                    }
                }
            }, updateStatisticsExecutor));
        }

        for (CompletableFuture<?> executeUpdateFuture : updateStatsFutures.build()) {
            MoreFutures.getFutureValue(executeUpdateFuture);
        }
        if (!suppressedExceptions.isEmpty()) {
            StringBuilder message = new StringBuilder();
            message.append("Failed to execute some updating statistics tasks: ");
            Joiner.on("; ").appendTo(message, failedTaskDescriptions);
            RuntimeException exception = new RuntimeException(message.toString());
            suppressedExceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    private static void addSuppressedExceptions(
            List<Throwable> suppressedExceptions,
            Throwable t,
            List<String> descriptions,
            String description) {
        descriptions.add(description);
        // A limit is needed to avoid having a huge exception object. 5 was chosen arbitrarily.
        if (suppressedExceptions.size() < 5) {
            suppressedExceptions.add(t);
        }
    }

    private static class AddPartition {

    }

    private static class UpdateStatisticsTask {
        private final String dbName;
        private final String tableName;
        private final Optional<String> partitionName;
        private final HivePartitionStatistics updatePartitionStat;
        private final boolean merge;

        private boolean done;

        public UpdateStatisticsTask(String dbName, String tableName, Optional<String> partitionName,
                                    HivePartitionStatistics statistics, boolean merge) {
            this.dbName = Objects.requireNonNull(dbName, "dbName is null");
            this.tableName = Objects.requireNonNull(tableName, "tableName is null");
            this.partitionName = Objects.requireNonNull(partitionName, "partitionName is null");
            this.updatePartitionStat = Objects.requireNonNull(statistics, "statistics is null");
            this.merge = merge;
        }

        public void run(HiveMetadataOps hiveOps) {
            if (partitionName.isPresent()) {
                hiveOps.updatePartitionStatistics(dbName, tableName, partitionName.get(), this::updateStatistics);
            } else {
                hiveOps.updateTableStatistics(dbName, tableName, this::updateStatistics);
            }
            done = true;
        }

        public void undo(HiveMetadataOps hmsOps) {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                hmsOps.updatePartitionStatistics(dbName, tableName, partitionName.get(), this::resetStatistics);
            } else {
                hmsOps.updateTableStatistics(dbName, tableName, this::resetStatistics);
            }
        }

        public String getDescription() {
            if (partitionName.isPresent()) {
                return "alter partition parameters " + tableName + " " + partitionName.get();
            } else {
                return "alter table parameters " +  tableName;
            }
        }

        private HivePartitionStatistics updateStatistics(HivePartitionStatistics currentStats) {
            return merge ? HivePartitionStatistics.merge(currentStats, updatePartitionStat) : updatePartitionStat;
        }

        private HivePartitionStatistics resetStatistics(HivePartitionStatistics currentStatistics) {
            return HivePartitionStatistics
                    .reduce(currentStatistics, updatePartitionStat, HivePartitionStatistics.ReduceOperator.SUBTRACT);
        }
    }

    public static class AddPartitionsTask {
        private final List<HivePartitionWithStatistics> partitions = new ArrayList<>();
        private final List<List<String>> createdPartitionValues = new ArrayList<>();

        public boolean isEmpty() {
            return partitions.isEmpty();
        }

        public List<HivePartitionWithStatistics> getPartitions() {
            return partitions;
        }

        public void addPartition(HivePartitionWithStatistics partition) {
            partitions.add(partition);
        }

        public void run(HiveMetadataOps hiveOps) {
            HivePartition firstPartition = partitions.get(0).getPartition();
            String dbName = firstPartition.getDbName();
            String tableName = firstPartition.getTblName();
            List<List<HivePartitionWithStatistics>> batchedPartitions =
                    Lists.partition(partitions, PARTITION_COMMIT_BATCH_SIZE);
            for (List<HivePartitionWithStatistics> batch : batchedPartitions) {
                try {
                    hiveOps.addPartitions(dbName, tableName, batch);
                    for (HivePartitionWithStatistics partition : batch) {
                        createdPartitionValues.add(partition.getPartition().getPartitionValues());
                    }
                } catch (Throwable t) {
                    LOG.error("Failed to add partition", t);
                    throw t;
                }
            }
            partitions.clear();
        }

        public List<List<String>> rollback(HiveMetadataOps hiveOps) {
            HivePartition firstPartition = partitions.get(0).getPartition();
            String dbName = firstPartition.getDbName();
            String tableName = firstPartition.getTblName();
            List<List<String>> rollbackFailedPartitions = new ArrayList<>();
            for (List<String> createdPartitionValue : createdPartitionValues) {
                try {
                    hiveOps.dropPartition(dbName, tableName, createdPartitionValue, false);
                } catch (Throwable t) {
                    LOG.warn("Failed to drop partition on {}.{}.{} when rollback",
                            dbName, tableName, rollbackFailedPartitions);
                    rollbackFailedPartitions.add(createdPartitionValue);
                }
            }
            return rollbackFailedPartitions;
        }
    }

    private static class DirectoryCleanUpTask {
        private final Path path;
        private final boolean deleteEmptyDir;

        public DirectoryCleanUpTask(String path, boolean deleteEmptyDir) {
            this.path = new Path(path);
            this.deleteEmptyDir = deleteEmptyDir;
        }

        public Path getPath() {
            return path;
        }

        public boolean isDeleteEmptyDir() {
            return deleteEmptyDir;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", DirectoryCleanUpTask.class.getSimpleName() + "[", "]")
                .add("path=" + path)
                .add("deleteEmptyDir=" + deleteEmptyDir)
                .toString();
        }
    }

    public static class DeleteRecursivelyResult {
        private final boolean dirNoLongerExists;
        private final List<String> notDeletedEligibleItems;

        public DeleteRecursivelyResult(boolean dirNoLongerExists, List<String> notDeletedEligibleItems) {
            this.dirNoLongerExists = dirNoLongerExists;
            this.notDeletedEligibleItems = notDeletedEligibleItems;
        }

        public boolean dirNotExists() {
            return dirNoLongerExists;
        }

        public List<String> getNotDeletedEligibleItems() {
            return notDeletedEligibleItems;
        }
    }

    private void runDirectoryClearUpTasksForAbort() {
        for (DirectoryCleanUpTask cleanUpTask : directoryCleanUpTasksForAbort) {
            recursiveDeleteItems(cleanUpTask.getPath(), cleanUpTask.isDeleteEmptyDir());
        }
    }

    private static class RenameDirectoryTask {
        private final String renameFrom;
        private final String renameTo;

        public RenameDirectoryTask(String renameFrom, String renameTo) {
            this.renameFrom = renameFrom;
            this.renameTo = renameTo;
        }

        public String getRenameFrom() {
            return renameFrom;
        }

        public String getRenameTo() {
            return renameTo;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", RenameDirectoryTask.class.getSimpleName() + "[", "]")
                .add("renameFrom:" + renameFrom)
                .add("renameTo:" + renameTo)
                .toString();
        }
    }

    private void runRenameDirTasksForAbort() {
        // TODO abort
    }


    private void recursiveDeleteItems(Path directory, boolean deleteEmptyDir) {
        DeleteRecursivelyResult deleteResult = recursiveDeleteFiles(directory, deleteEmptyDir);

        if (!deleteResult.getNotDeletedEligibleItems().isEmpty()) {
            LOG.error("Failed to delete directory {}. Some eligible items can't be deleted: {}.",
                    directory.toString(), deleteResult.getNotDeletedEligibleItems());
        } else if (deleteEmptyDir && !deleteResult.dirNotExists()) {
            LOG.error("Failed to delete directory {} due to dir isn't empty", directory.toString());
        }
    }

    public DeleteRecursivelyResult recursiveDeleteFiles(Path directory, boolean deleteEmptyDir) {
        try {
            if (!fs.exists(directory.getName()).ok()) {
                return new DeleteRecursivelyResult(true, ImmutableList.of());
            }
        } catch (Exception e) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory.toString() + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        return doRecursiveDeleteFiles(directory, deleteEmptyDir);
    }

    private DeleteRecursivelyResult doRecursiveDeleteFiles(Path directory, boolean deleteEmptyDir) {
        List<RemoteFile> remoteFiles = new ArrayList<>();

        Status status = fs.list(directory.getName(), remoteFiles);
        if (!status.ok()) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        boolean isEmptyDir = true;
        List<String> notDeletedEligibleItems = new ArrayList<>();
        for (RemoteFile file : remoteFiles) {
            if (file.isFile()) {
                Path filePath = file.getPath();
                isEmptyDir = false;
                // TODO Check if this file was created by this query
                if (!deleteIfExists(filePath)) {
                    notDeletedEligibleItems.add(filePath.toString());
                }
            } else if (file.isDirectory()) {
                DeleteRecursivelyResult subResult = doRecursiveDeleteFiles(file.getPath(), deleteEmptyDir);
                if (!subResult.dirNotExists()) {
                    isEmptyDir = false;
                }
                if (!subResult.getNotDeletedEligibleItems().isEmpty()) {
                    notDeletedEligibleItems.addAll(subResult.getNotDeletedEligibleItems());
                }
            } else {
                isEmptyDir = false;
                notDeletedEligibleItems.add(file.getPath().toString());
            }
        }

        if (isEmptyDir && deleteEmptyDir) {
            Verify.verify(notDeletedEligibleItems.isEmpty());
            if (!deleteIfExists(directory)) {
                return new DeleteRecursivelyResult(false, ImmutableList.of(directory + "/"));
            }
            // all items of the location have been deleted.
            return new DeleteRecursivelyResult(true, ImmutableList.of());
        }

        return new DeleteRecursivelyResult(false, notDeletedEligibleItems);
    }

    public boolean deleteIfExists(Path path) {
        Status status = fs.delete(path.getName());
        if (status.ok()) {
            return true;
        }
        return !fs.exists(path.getName()).ok();
    }
}
