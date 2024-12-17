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
// https://github.com/trinodb/trino/blob/438/plugin/trino-hive/src/main/java/io/trino/plugin/hive/HiveMetadata.java
// and modified by Doris

package org.apache.doris.datasource.hive;

import org.apache.doris.backup.Status;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.common.profile.SummaryProfile;
import org.apache.doris.datasource.statistics.CommonStatistics;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.FileSystemProvider;
import org.apache.doris.fs.FileSystemUtil;
import org.apache.doris.fs.remote.RemoteFile;
import org.apache.doris.fs.remote.S3FileSystem;
import org.apache.doris.fs.remote.SwitchingFileSystem;
import org.apache.doris.nereids.trees.plans.commands.insert.HiveInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TS3MPUPendingUpload;
import org.apache.doris.thrift.TUpdateMode;
import org.apache.doris.transaction.Transaction;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.airlift.concurrent.MoreFutures;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class HMSTransaction implements Transaction {
    private static final Logger LOG = LogManager.getLogger(HMSTransaction.class);
    private final HiveMetadataOps hiveOps;
    private final FileSystem fs;
    private Optional<SummaryProfile> summaryProfile = Optional.empty();
    private String queryId;
    private boolean isOverwrite = false;
    TFileType fileType;

    private final Map<SimpleTableInfo, Action<TableAndMore>> tableActions = new HashMap<>();
    private final Map<SimpleTableInfo, Map<List<String>, Action<PartitionAndMore>>>
            partitionActions = new HashMap<>();
    private final Map<SimpleTableInfo, List<FieldSchema>> tableColumns = new HashMap<>();

    private final Executor fileSystemExecutor;
    private HmsCommitter hmsCommitter;
    private List<THivePartitionUpdate> hivePartitionUpdates = Lists.newArrayList();
    private Optional<String> stagingDirectory;
    private boolean isMockedPartitionUpdate = false;

    private static class UncompletedMpuPendingUpload {

        private final TS3MPUPendingUpload s3MPUPendingUpload;
        private final String path;

        public UncompletedMpuPendingUpload(TS3MPUPendingUpload s3MPUPendingUpload, String path) {
            this.s3MPUPendingUpload = s3MPUPendingUpload;
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            UncompletedMpuPendingUpload that = (UncompletedMpuPendingUpload) o;
            return Objects.equals(s3MPUPendingUpload, that.s3MPUPendingUpload) && Objects.equals(path,
                    that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(s3MPUPendingUpload, path);
        }
    }

    private final Set<UncompletedMpuPendingUpload> uncompletedMpuPendingUploads = new HashSet<>();

    public HMSTransaction(HiveMetadataOps hiveOps, FileSystemProvider fileSystemProvider, Executor fileSystemExecutor) {
        this.hiveOps = hiveOps;
        this.fs = fileSystemProvider.get(null);
        if (!(fs instanceof SwitchingFileSystem)) {
            throw new RuntimeException("fs should be SwitchingFileSystem");
        }
        if (ConnectContext.get().getExecutor() != null) {
            summaryProfile = Optional.of(ConnectContext.get().getExecutor().getSummaryProfile());
        }
        this.fileSystemExecutor = fileSystemExecutor;
    }

    @Override
    public void commit() {
        doCommit();
    }

    public List<THivePartitionUpdate> mergePartitions(List<THivePartitionUpdate> hivePUs) {
        Map<String, THivePartitionUpdate> mm = new HashMap<>();
        for (THivePartitionUpdate pu : hivePUs) {
            if (mm.containsKey(pu.getName())) {
                THivePartitionUpdate old = mm.get(pu.getName());
                old.setFileSize(old.getFileSize() + pu.getFileSize());
                old.setRowCount(old.getRowCount() + pu.getRowCount());
                if (old.getS3MpuPendingUploads() != null && pu.getS3MpuPendingUploads() != null) {
                    old.getS3MpuPendingUploads().addAll(pu.getS3MpuPendingUploads());
                }
                old.getFileNames().addAll(pu.getFileNames());
            } else {
                mm.put(pu.getName(), pu);
            }
        }
        return new ArrayList<>(mm.values());
    }

    @Override
    public void rollback() {
        if (hmsCommitter != null) {
            hmsCommitter.rollback();
        }
    }

    public void beginInsertTable(HiveInsertCommandContext ctx) {
        queryId = ctx.getQueryId();
        isOverwrite = ctx.isOverwrite();
        fileType = ctx.getFileType();
        if (fileType == TFileType.FILE_S3) {
            stagingDirectory = Optional.empty();
        } else {
            stagingDirectory = Optional.of(ctx.getWritePath());
        }
    }

    public void finishInsertTable(SimpleTableInfo tableInfo) {
        Table table = getTable(tableInfo);
        if (hivePartitionUpdates.isEmpty() && isOverwrite && table.getPartitionKeysSize() == 0) {
            // use an empty hivePartitionUpdate to clean source table
            isMockedPartitionUpdate = true;
            THivePartitionUpdate emptyUpdate = new THivePartitionUpdate() {{
                    setUpdateMode(TUpdateMode.OVERWRITE);
                    setFileSize(0);
                    setRowCount(0);
                    setFileNames(Collections.emptyList());
                    if (fileType == TFileType.FILE_S3) {
                        setS3MpuPendingUploads(Lists.newArrayList(new TS3MPUPendingUpload()));
                        setLocation(new THiveLocationParams() {{
                                setWritePath(table.getSd().getLocation());
                            }
                        });
                    } else {
                        stagingDirectory.ifPresent((v) -> {
                            fs.makeDir(v);
                            setLocation(new THiveLocationParams() {{
                                    setWritePath(v);
                                }
                            });
                        });
                    }
                }
            };
            hivePartitionUpdates = Lists.newArrayList(emptyUpdate);
        }

        List<THivePartitionUpdate> mergedPUs = mergePartitions(hivePartitionUpdates);
        for (THivePartitionUpdate pu : mergedPUs) {
            if (pu.getS3MpuPendingUploads() != null) {
                for (TS3MPUPendingUpload s3MPUPendingUpload : pu.getS3MpuPendingUploads()) {
                    uncompletedMpuPendingUploads.add(
                            new UncompletedMpuPendingUpload(s3MPUPendingUpload, pu.getLocation().getTargetPath()));
                }
            }
        }
        List<Pair<THivePartitionUpdate, HivePartitionStatistics>> insertExistsPartitions = new ArrayList<>();
        for (THivePartitionUpdate pu : mergedPUs) {
            TUpdateMode updateMode = pu.getUpdateMode();
            HivePartitionStatistics hivePartitionStatistics = HivePartitionStatistics.fromCommonStatistics(
                    pu.getRowCount(),
                    pu.getFileNamesSize(),
                    pu.getFileSize());
            String writePath = pu.getLocation().getWritePath();
            if (table.getPartitionKeysSize() == 0) {
                Preconditions.checkArgument(mergedPUs.size() == 1,
                        "When updating a non-partitioned table, multiple partitions should not be written");
                switch (updateMode) {
                    case APPEND:
                        finishChangingExistingTable(
                                ActionType.INSERT_EXISTING,
                                tableInfo,
                                writePath,
                                pu.getFileNames(),
                                hivePartitionStatistics,
                                pu);
                        break;
                    case OVERWRITE:
                        dropTable(tableInfo);
                        createTable(tableInfo, table, writePath, pu.getFileNames(), hivePartitionStatistics, pu);
                        break;
                    default:
                        throw new RuntimeException("Not support mode:[" + updateMode + "] in unPartitioned table");
                }
            } else {
                switch (updateMode) {
                    case APPEND:
                        // insert into existing partition
                        insertExistsPartitions.add(Pair.of(pu, hivePartitionStatistics));
                        break;
                    case NEW:
                    case OVERWRITE:
                        StorageDescriptor sd = table.getSd();
                        HivePartition hivePartition = new HivePartition(
                                tableInfo,
                                false,
                                sd.getInputFormat(),
                                pu.getLocation().getTargetPath(),
                                HiveUtil.toPartitionValues(pu.getName()),
                                Maps.newHashMap(),
                                sd.getOutputFormat(),
                                sd.getSerdeInfo().getSerializationLib(),
                                getTableColumns(tableInfo)
                        );
                        if (updateMode == TUpdateMode.OVERWRITE) {
                            dropPartition(tableInfo, hivePartition.getPartitionValues(), true);
                        }
                        addPartition(
                                tableInfo, hivePartition, writePath,
                                pu.getName(), pu.getFileNames(), hivePartitionStatistics, pu);
                        break;
                    default:
                        throw new RuntimeException("Not support mode:[" + updateMode + "] in partitioned table");
                }
            }
        }

        if (!insertExistsPartitions.isEmpty()) {
            convertToInsertExistingPartitionAction(tableInfo, insertExistsPartitions);
        }
    }

    public void doCommit() {
        hmsCommitter = new HmsCommitter();

        try {
            for (Map.Entry<SimpleTableInfo, Action<TableAndMore>> entry : tableActions.entrySet()) {
                SimpleTableInfo tableInfo = entry.getKey();
                Action<TableAndMore> action = entry.getValue();
                switch (action.getType()) {
                    case INSERT_EXISTING:
                        hmsCommitter.prepareInsertExistingTable(tableInfo, action.getData());
                        break;
                    case ALTER:
                        hmsCommitter.prepareAlterTable(tableInfo, action.getData());
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported table action type: " + action.getType());
                }
            }

            for (Map.Entry<SimpleTableInfo, Map<List<String>, Action<PartitionAndMore>>> tableEntry
                    : partitionActions.entrySet()) {
                SimpleTableInfo tableInfo = tableEntry.getKey();
                for (Map.Entry<List<String>, Action<PartitionAndMore>> partitionEntry :
                        tableEntry.getValue().entrySet()) {
                    Action<PartitionAndMore> action = partitionEntry.getValue();
                    switch (action.getType()) {
                        case INSERT_EXISTING:
                            hmsCommitter.prepareInsertExistPartition(tableInfo, action.getData());
                            break;
                        case ADD:
                            hmsCommitter.prepareAddPartition(tableInfo, action.getData());
                            break;
                        case ALTER:
                            hmsCommitter.prepareAlterPartition(tableInfo, action.getData());
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Unsupported partition action type: " + action.getType());
                    }
                }
            }

            hmsCommitter.doCommit();
        } catch (Throwable t) {
            LOG.warn("Failed to commit for {}, abort it.", queryId);
            try {
                hmsCommitter.abort();
                hmsCommitter.rollback();
            } catch (RuntimeException e) {
                t.addSuppressed(new Exception("Failed to roll back after commit failure", e));
            }
            throw t;
        } finally {
            hmsCommitter.runClearPathsForFinish();
            hmsCommitter.shutdownExecutorService();
        }
    }

    public void updateHivePartitionUpdates(List<THivePartitionUpdate> pus) {
        synchronized (this) {
            hivePartitionUpdates.addAll(pus);
        }
    }

    // for test
    public void setHivePartitionUpdates(List<THivePartitionUpdate> hivePartitionUpdates) {
        this.hivePartitionUpdates = hivePartitionUpdates;
    }

    public long getUpdateCnt() {
        return hivePartitionUpdates.stream().mapToLong(THivePartitionUpdate::getRowCount).sum();
    }

    private void convertToInsertExistingPartitionAction(
            SimpleTableInfo tableInfo,
            List<Pair<THivePartitionUpdate, HivePartitionStatistics>> partitions) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.computeIfAbsent(tableInfo, k -> new HashMap<>());

        for (List<Pair<THivePartitionUpdate, HivePartitionStatistics>> partitionBatch :
                Iterables.partition(partitions, 100)) {

            List<String> partitionNames = partitionBatch
                    .stream()
                    .map(pair -> pair.first.getName())
                    .collect(Collectors.toList());

            // check in partitionAction
            Action<PartitionAndMore> oldPartitionAction = partitionActionsForTable.get(partitionNames);
            if (oldPartitionAction != null) {
                switch (oldPartitionAction.getType()) {
                    case DROP:
                    case DROP_PRESERVE_DATA:
                        throw new RuntimeException(
                                "Not found partition from partition actions"
                                        + "for " + tableInfo + ", partitions: " + partitionNames);
                    case ADD:
                    case ALTER:
                    case INSERT_EXISTING:
                    case MERGE:
                        throw new UnsupportedOperationException(
                                "Inserting into a partition that were added, altered,"
                                        + "or inserted into in the same transaction is not supported");
                    default:
                        throw new IllegalStateException("Unknown action type: " + oldPartitionAction.getType());
                }
            }

            Map<String, Partition> partitionsByNamesMap = HiveUtil.convertToNamePartitionMap(
                    partitionNames,
                    hiveOps.getClient().getPartitions(tableInfo.getDbName(), tableInfo.getTbName(), partitionNames));

            for (int i = 0; i < partitionsByNamesMap.size(); i++) {
                String partitionName = partitionNames.get(i);
                // check from hms
                Partition partition = partitionsByNamesMap.get(partitionName);
                if (partition == null) {
                    // Prevent this partition from being deleted by other engines
                    throw new RuntimeException(
                            "Not found partition from hms for " + tableInfo
                                    + ", partitions: " + partitionNames);
                }
                THivePartitionUpdate pu = partitionBatch.get(i).first;
                HivePartitionStatistics updateStats = partitionBatch.get(i).second;

                StorageDescriptor sd = partition.getSd();
                List<String> partitionValues = HiveUtil.toPartitionValues(pu.getName());

                HivePartition hivePartition = new HivePartition(
                        tableInfo,
                        false,
                        sd.getInputFormat(),
                        partition.getSd().getLocation(),
                        partitionValues,
                        partition.getParameters(),
                        sd.getOutputFormat(),
                        sd.getSerdeInfo().getSerializationLib(),
                        getTableColumns(tableInfo)
                );

                partitionActionsForTable.put(
                        partitionValues,
                        new Action<>(
                                ActionType.INSERT_EXISTING,
                                new PartitionAndMore(
                                        hivePartition,
                                        pu.getLocation().getWritePath(),
                                        pu.getName(),
                                        pu.getFileNames(),
                                        updateStats,
                                        pu
                                ))
                );
            }
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

    public static class UpdateStatisticsTask {
        private final SimpleTableInfo tableInfo;
        private final Optional<String> partitionName;
        private final HivePartitionStatistics updatePartitionStat;
        private final boolean merge;

        private boolean done;

        public UpdateStatisticsTask(SimpleTableInfo tableInfo, Optional<String> partitionName,
                HivePartitionStatistics statistics, boolean merge) {
            this.tableInfo = Objects.requireNonNull(tableInfo, "tableInfo is null");
            this.partitionName = Objects.requireNonNull(partitionName, "partitionName is null");
            this.updatePartitionStat = Objects.requireNonNull(statistics, "statistics is null");
            this.merge = merge;
        }

        public void run(HiveMetadataOps hiveOps) {
            if (partitionName.isPresent()) {
                hiveOps.updatePartitionStatistics(tableInfo, partitionName.get(), this::updateStatistics);
            } else {
                hiveOps.updateTableStatistics(tableInfo, this::updateStatistics);
            }
            done = true;
        }

        public void undo(HiveMetadataOps hmsOps) {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                hmsOps.updatePartitionStatistics(tableInfo, partitionName.get(), this::resetStatistics);
            } else {
                hmsOps.updateTableStatistics(tableInfo, this::resetStatistics);
            }
        }

        public String getDescription() {
            if (partitionName.isPresent()) {
                return "alter partition parameters " + tableInfo + " " + partitionName.get();
            } else {
                return "alter table parameters " + tableInfo;
            }
        }

        private HivePartitionStatistics updateStatistics(HivePartitionStatistics currentStats) {
            return merge ? HivePartitionStatistics.merge(currentStats, updatePartitionStat) : updatePartitionStat;
        }

        private HivePartitionStatistics resetStatistics(HivePartitionStatistics currentStatistics) {
            return HivePartitionStatistics
                    .reduce(currentStatistics, updatePartitionStat, CommonStatistics.ReduceOperator.SUBTRACT);
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

        public void clear() {
            partitions.clear();
            createdPartitionValues.clear();
        }

        public void addPartition(HivePartitionWithStatistics partition) {
            partitions.add(partition);
        }

        public void run(HiveMetadataOps hiveOps) {
            HivePartition firstPartition = partitions.get(0).getPartition();
            SimpleTableInfo tableInfo = firstPartition.getTableInfo();
            List<List<HivePartitionWithStatistics>> batchedPartitions = Lists.partition(partitions, 20);
            for (List<HivePartitionWithStatistics> batch : batchedPartitions) {
                try {
                    hiveOps.addPartitions(tableInfo, batch);
                    for (HivePartitionWithStatistics partition : batch) {
                        createdPartitionValues.add(partition.getPartition().getPartitionValues());
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to add partition", t);
                    throw t;
                }
            }
        }

        public List<List<String>> rollback(HiveMetadataOps hiveOps) {
            HivePartition firstPartition = partitions.get(0).getPartition();
            SimpleTableInfo tableInfo = firstPartition.getTableInfo();
            List<List<String>> rollbackFailedPartitions = new ArrayList<>();
            for (List<String> createdPartitionValue : createdPartitionValues) {
                try {
                    hiveOps.dropPartition(tableInfo, createdPartitionValue, false);
                } catch (Throwable t) {
                    LOG.warn("Failed to drop partition on {}.{} when rollback",
                            tableInfo, rollbackFailedPartitions);
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

    private static class DeleteRecursivelyResult {
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


    private void recursiveDeleteItems(Path directory, boolean deleteEmptyDir, boolean reverse) {
        DeleteRecursivelyResult deleteResult = recursiveDeleteFiles(directory, deleteEmptyDir, reverse);

        if (!deleteResult.getNotDeletedEligibleItems().isEmpty()) {
            LOG.warn("Failed to delete directory {}. Some eligible items can't be deleted: {}.",
                    directory.toString(), deleteResult.getNotDeletedEligibleItems());
            throw new RuntimeException(
                "Failed to delete directory for files: " + deleteResult.getNotDeletedEligibleItems());
        } else if (deleteEmptyDir && !deleteResult.dirNotExists()) {
            LOG.warn("Failed to delete directory {} due to dir isn't empty", directory.toString());
            throw new RuntimeException("Failed to delete directory for empty dir: " + directory.toString());
        }
    }

    private DeleteRecursivelyResult recursiveDeleteFiles(Path directory, boolean deleteEmptyDir, boolean reverse) {
        try {
            Status status = fs.directoryExists(directory.toString());
            if (status.getErrCode().equals(Status.ErrCode.NOT_FOUND)) {
                return new DeleteRecursivelyResult(true, ImmutableList.of());
            } else if (!status.ok()) {
                ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
                notDeletedEligibleItems.add(directory.toString() + "/*");
                return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
            }
        } catch (Exception e) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory.toString() + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        return doRecursiveDeleteFiles(directory, deleteEmptyDir, queryId, reverse);
    }

    private DeleteRecursivelyResult doRecursiveDeleteFiles(Path directory, boolean deleteEmptyDir,
            String queryId, boolean reverse) {
        List<RemoteFile> allFiles = new ArrayList<>();
        Set<String> allDirs = new HashSet<>();
        Status statusFile = fs.listFiles(directory.toString(), true, allFiles);
        Status statusDir = fs.listDirectories(directory.toString(), allDirs);
        if (!statusFile.ok() || !statusDir.ok()) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        boolean allDescendentsDeleted = true;
        ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
        for (RemoteFile file : allFiles) {
            if (reverse ^ file.getName().startsWith(queryId)) {
                if (!deleteIfExists(file.getPath())) {
                    allDescendentsDeleted = false;
                    notDeletedEligibleItems.add(file.getPath().toString());
                }
            } else {
                allDescendentsDeleted = false;
            }
        }

        for (String dir : allDirs) {
            DeleteRecursivelyResult subResult = doRecursiveDeleteFiles(new Path(dir), deleteEmptyDir, queryId, reverse);
            if (!subResult.dirNotExists()) {
                allDescendentsDeleted = false;
            }
            if (!subResult.getNotDeletedEligibleItems().isEmpty()) {
                notDeletedEligibleItems.addAll(subResult.getNotDeletedEligibleItems());
            }
        }

        if (allDescendentsDeleted && deleteEmptyDir) {
            Verify.verify(notDeletedEligibleItems.build().isEmpty());
            if (!deleteDirectoryIfExists(directory)) {
                return new DeleteRecursivelyResult(false, ImmutableList.of(directory + "/"));
            }
            // all items of the location have been deleted.
            return new DeleteRecursivelyResult(true, ImmutableList.of());
        }
        return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
    }

    public boolean deleteIfExists(Path path) {
        Status status = wrapperDeleteWithProfileSummary(path.toString());
        if (status.ok()) {
            return true;
        }
        return !fs.exists(path.toString()).ok();
    }

    public boolean deleteDirectoryIfExists(Path path) {
        Status status = wrapperDeleteDirWithProfileSummary(path.toString());
        if (status.ok()) {
            return true;
        }
        return !fs.directoryExists(path.toString()).ok();
    }

    private static class TableAndMore {
        private final Table table;
        private final String currentLocation;
        private final List<String> fileNames;
        private final HivePartitionStatistics statisticsUpdate;

        private final THivePartitionUpdate hivePartitionUpdate;

        public TableAndMore(
                Table table,
                String currentLocation,
                List<String> fileNames,
                HivePartitionStatistics statisticsUpdate,
                THivePartitionUpdate hivePartitionUpdate) {
            this.table = Objects.requireNonNull(table, "table is null");
            this.currentLocation = Objects.requireNonNull(currentLocation);
            this.fileNames = Objects.requireNonNull(fileNames);
            this.statisticsUpdate = Objects.requireNonNull(statisticsUpdate, "statisticsUpdate is null");
            this.hivePartitionUpdate = Objects.requireNonNull(hivePartitionUpdate, "hivePartitionUpdate is null");
        }

        public Table getTable() {
            return table;
        }

        public String getCurrentLocation() {
            return currentLocation;
        }

        public List<String> getFileNames() {
            return fileNames;
        }

        public HivePartitionStatistics getStatisticsUpdate() {
            return statisticsUpdate;
        }

        public THivePartitionUpdate getHivePartitionUpdate() {
            return hivePartitionUpdate;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("table", table)
                    .add("statisticsUpdate", statisticsUpdate)
                    .toString();
        }
    }

    private static class PartitionAndMore {
        private final HivePartition partition;
        private final String currentLocation;
        private final String partitionName;
        private final List<String> fileNames;
        private final HivePartitionStatistics statisticsUpdate;

        private final THivePartitionUpdate hivePartitionUpdate;


        public PartitionAndMore(
                HivePartition partition,
                String currentLocation,
                String partitionName,
                List<String> fileNames,
                HivePartitionStatistics statisticsUpdate,
                THivePartitionUpdate hivePartitionUpdate) {
            this.partition = Objects.requireNonNull(partition, "partition is null");
            this.currentLocation = Objects.requireNonNull(currentLocation, "currentLocation is null");
            this.partitionName = Objects.requireNonNull(partitionName, "partition is null");
            this.fileNames = Objects.requireNonNull(fileNames, "fileNames is null");
            this.statisticsUpdate = Objects.requireNonNull(statisticsUpdate, "statisticsUpdate is null");
            this.hivePartitionUpdate = Objects.requireNonNull(hivePartitionUpdate, "hivePartitionUpdate is null");
        }

        public HivePartition getPartition() {
            return partition;
        }

        public String getCurrentLocation() {
            return currentLocation;
        }

        public String getPartitionName() {
            return partitionName;
        }

        public List<String> getFileNames() {
            return fileNames;
        }

        public HivePartitionStatistics getStatisticsUpdate() {
            return statisticsUpdate;
        }

        public THivePartitionUpdate getHivePartitionUpdate() {
            return hivePartitionUpdate;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("partition", partition)
                    .add("currentLocation", currentLocation)
                    .add("fileNames", fileNames)
                    .toString();
        }
    }

    private enum ActionType {
        // drop a table/partition
        DROP,
        // drop a table/partition but will preserve data
        DROP_PRESERVE_DATA,
        // add a table/partition
        ADD,
        // drop then add a table/partition, like overwrite
        ALTER,
        // insert into an existing table/partition
        INSERT_EXISTING,
        // merger into an existing table/partition
        MERGE,
    }

    public static class Action<T> {
        private final ActionType type;
        private final T data;

        public Action(ActionType type, T data) {
            this.type = Objects.requireNonNull(type, "type is null");
            if (type == ActionType.DROP || type == ActionType.DROP_PRESERVE_DATA) {
                Preconditions.checkArgument(data == null, "data is not null");
            } else {
                Objects.requireNonNull(data, "data is null");
            }
            this.data = data;
        }

        public ActionType getType() {
            return type;
        }

        public T getData() {
            Preconditions.checkState(type != ActionType.DROP);
            return data;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("type", type)
                    .add("data", data)
                    .toString();
        }
    }

    public synchronized Table getTable(SimpleTableInfo tableInfo) {
        Action<TableAndMore> tableAction = tableActions.get(tableInfo);
        if (tableAction == null) {
            return hiveOps.getClient().getTable(tableInfo.getDbName(), tableInfo.getTbName());
        }
        switch (tableAction.getType()) {
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                return tableAction.getData().getTable();
            case DROP:
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + tableAction.getType());
        }
        throw new RuntimeException("Not Found table: " + tableInfo);
    }

    public synchronized List<FieldSchema> getTableColumns(SimpleTableInfo tableInfo) {
        return tableColumns.computeIfAbsent(tableInfo,
                key -> hiveOps.getClient().getSchema(tableInfo.getDbName(), tableInfo.getTbName()));
    }

    public synchronized void finishChangingExistingTable(
            ActionType actionType,
            SimpleTableInfo tableInfo,
            String location,
            List<String> fileNames,
            HivePartitionStatistics statisticsUpdate,
            THivePartitionUpdate hivePartitionUpdate) {
        Action<TableAndMore> oldTableAction = tableActions.get(tableInfo);
        if (oldTableAction == null) {
            Table table = hiveOps.getClient().getTable(tableInfo.getDbName(), tableInfo.getTbName());
            tableActions.put(
                    tableInfo,
                    new Action<>(
                            actionType,
                            new TableAndMore(
                                    table,
                                    location,
                                    fileNames,
                                    statisticsUpdate,
                                    hivePartitionUpdate)));
            return;
        }

        switch (oldTableAction.getType()) {
            case DROP:
                throw new RuntimeException("Not found table: " + tableInfo);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new UnsupportedOperationException(
                        "Inserting into an unpartitioned table that were added, altered,"
                                + "or inserted into in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + oldTableAction.getType());
        }
    }

    public synchronized void createTable(
            SimpleTableInfo tableInfo,
            Table table, String location, List<String> fileNames,
            HivePartitionStatistics statistics,
            THivePartitionUpdate hivePartitionUpdate) {
        // When creating a table, it should never have partition actions. This is just a sanity check.
        checkNoPartitionAction(tableInfo);
        Action<TableAndMore> oldTableAction = tableActions.get(tableInfo);
        TableAndMore tableAndMore = new TableAndMore(table, location, fileNames, statistics, hivePartitionUpdate);
        if (oldTableAction == null) {
            tableActions.put(tableInfo, new Action<>(ActionType.ADD, tableAndMore));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                tableActions.put(tableInfo, new Action<>(ActionType.ALTER, tableAndMore));
                return;

            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException("Table already exists: " + tableInfo);
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + oldTableAction.getType());
        }
    }


    public synchronized void dropTable(SimpleTableInfo tableInfo) {
        // Dropping table with partition actions requires cleaning up staging data, which is not implemented yet.
        checkNoPartitionAction(tableInfo);
        Action<TableAndMore> oldTableAction = tableActions.get(tableInfo);
        if (oldTableAction == null || oldTableAction.getType() == ActionType.ALTER) {
            tableActions.put(tableInfo, new Action<>(ActionType.DROP, null));
            return;
        }
        switch (oldTableAction.getType()) {
            case DROP:
                throw new RuntimeException("Not found table: " + tableInfo);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException("Dropping a table added/modified in the same transaction is not supported");
            case DROP_PRESERVE_DATA:
                break;
            default:
                throw new IllegalStateException("Unknown action type: " + oldTableAction.getType());
        }
    }


    private void checkNoPartitionAction(SimpleTableInfo tableInfo) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.get(tableInfo);
        if (partitionActionsForTable != null && !partitionActionsForTable.isEmpty()) {
            throw new RuntimeException(
                    "Cannot make schema changes to a table with modified partitions in the same transaction");
        }
    }

    public synchronized void addPartition(
            SimpleTableInfo tableInfo,
            HivePartition partition,
            String currentLocation,
            String partitionName,
            List<String> files,
            HivePartitionStatistics statistics,
            THivePartitionUpdate hivePartitionUpdate) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.computeIfAbsent(tableInfo, k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsForTable.get(partition.getPartitionValues());
        if (oldPartitionAction == null) {
            partitionActionsForTable.put(
                    partition.getPartitionValues(),
                    new Action<>(
                            ActionType.ADD,
                            new PartitionAndMore(partition, currentLocation, partitionName, files, statistics,
                                    hivePartitionUpdate))
            );
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                partitionActionsForTable.put(
                        partition.getPartitionValues(),
                        new Action<>(
                                ActionType.ALTER,
                                new PartitionAndMore(partition, currentLocation, partitionName, files, statistics,
                                        hivePartitionUpdate))
                );
                return;
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException(
                        "Partition already exists for table: "
                                + tableInfo + ", partition values: " + partition
                                .getPartitionValues());
            default:
                throw new IllegalStateException("Unknown action type: " + oldPartitionAction.getType());
        }
    }

    public synchronized void dropPartition(
            SimpleTableInfo tableInfo,
            List<String> partitionValues,
            boolean deleteData) {
        Map<List<String>, Action<PartitionAndMore>> partitionActionsForTable =
                partitionActions.computeIfAbsent(tableInfo, k -> new HashMap<>());
        Action<PartitionAndMore> oldPartitionAction = partitionActionsForTable.get(partitionValues);
        if (oldPartitionAction == null) {
            if (deleteData) {
                partitionActionsForTable.put(partitionValues, new Action<>(ActionType.DROP, null));
            } else {
                partitionActionsForTable.put(partitionValues, new Action<>(ActionType.DROP_PRESERVE_DATA, null));
            }
            return;
        }
        switch (oldPartitionAction.getType()) {
            case DROP:
            case DROP_PRESERVE_DATA:
                throw new RuntimeException(
                        "Not found partition from partition actions for " + tableInfo
                                + ", partitions: " + partitionValues);
            case ADD:
            case ALTER:
            case INSERT_EXISTING:
            case MERGE:
                throw new RuntimeException(
                        "Dropping a partition added in the same transaction is not supported: "
                                + tableInfo + ", partition values: " + partitionValues);
            default:
                throw new IllegalStateException("Unknown action type: " + oldPartitionAction.getType());
        }
    }

    class HmsCommitter {

        // update statistics for unPartitioned table or existed partition
        private final List<UpdateStatisticsTask> updateStatisticsTasks = new ArrayList<>();
        ExecutorService updateStatisticsExecutor = Executors.newFixedThreadPool(16);

        // add new partition
        private final AddPartitionsTask addPartitionsTask = new AddPartitionsTask();

        // for file system rename operation
        // whether to cancel the file system tasks
        private final AtomicBoolean fileSystemTaskCancelled = new AtomicBoolean(false);
        // file system tasks that are executed asynchronously, including rename_file, rename_dir
        private final List<CompletableFuture<?>> asyncFileSystemTaskFutures = new ArrayList<>();
        // when aborted, we need to delete all files under this path, even the current directory
        private final Queue<DirectoryCleanUpTask> directoryCleanUpTasksForAbort = new ConcurrentLinkedQueue<>();
        // when aborted, we need restore directory
        private final List<RenameDirectoryTask> renameDirectoryTasksForAbort = new ArrayList<>();
        // when finished, we need clear some directories
        private final List<String> clearDirsForFinish = new ArrayList<>();

        private final List<String> s3cleanWhenSuccess = new ArrayList<>();

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
            updateStatisticsTasks.clear();
        }

        private void undoAddPartitionsTask() {
            if (addPartitionsTask.isEmpty()) {
                return;
            }

            HivePartition firstPartition = addPartitionsTask.getPartitions().get(0).getPartition();
            SimpleTableInfo tableInfo = firstPartition.getTableInfo();
            List<List<String>> rollbackFailedPartitions = addPartitionsTask.rollback(hiveOps);
            if (!rollbackFailedPartitions.isEmpty()) {
                LOG.warn("Failed to rollback: add_partition for partition values {}.{}",
                        tableInfo, rollbackFailedPartitions);
            }
            addPartitionsTask.clear();
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
            asyncFileSystemTaskFutures.clear();
        }

        public void prepareInsertExistingTable(SimpleTableInfo tableInfo, TableAndMore tableAndMore) {
            Table table = tableAndMore.getTable();
            String targetPath = table.getSd().getLocation();
            String writePath = tableAndMore.getCurrentLocation();
            if (!targetPath.equals(writePath)) {
                wrapperAsyncRenameWithProfileSummary(
                        fileSystemExecutor,
                        asyncFileSystemTaskFutures,
                        fileSystemTaskCancelled,
                        writePath,
                        targetPath,
                        tableAndMore.getFileNames());
            } else {
                if (!tableAndMore.hivePartitionUpdate.s3_mpu_pending_uploads.isEmpty()) {
                    s3Commit(fileSystemExecutor, asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                            tableAndMore.hivePartitionUpdate, targetPath);
                }
            }
            directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));
            updateStatisticsTasks.add(
                    new UpdateStatisticsTask(
                            tableInfo,
                            Optional.empty(),
                            tableAndMore.getStatisticsUpdate(),
                            true
                    ));
        }

        public void prepareAlterTable(SimpleTableInfo tableInfo, TableAndMore tableAndMore) {
            Table table = tableAndMore.getTable();
            String targetPath = table.getSd().getLocation();
            String writePath = tableAndMore.getCurrentLocation();
            if (!targetPath.equals(writePath)) {
                Path path = new Path(targetPath);
                String oldTablePath = new Path(
                        path.getParent(), "_temp_" + queryId + "_" + path.getName()).toString();
                Status status = wrapperRenameDirWithProfileSummary(
                        targetPath,
                        oldTablePath,
                        () -> renameDirectoryTasksForAbort.add(new RenameDirectoryTask(oldTablePath, targetPath)));
                if (!status.ok()) {
                    throw new RuntimeException(
                            "Error to rename dir from " + targetPath + " to " + oldTablePath + status.getErrMsg());
                }
                clearDirsForFinish.add(oldTablePath);

                status = wrapperRenameDirWithProfileSummary(
                        writePath,
                        targetPath,
                        () -> directoryCleanUpTasksForAbort.add(
                                new DirectoryCleanUpTask(targetPath, true)));
                if (!status.ok()) {
                    throw new RuntimeException(
                            "Error to rename dir from " + writePath + " to " + targetPath + ":" + status.getErrMsg());
                }
            } else {
                if (!tableAndMore.hivePartitionUpdate.s3_mpu_pending_uploads.isEmpty()) {
                    s3cleanWhenSuccess.add(targetPath);
                    s3Commit(fileSystemExecutor, asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                            tableAndMore.hivePartitionUpdate, targetPath);
                }
            }
            updateStatisticsTasks.add(
                    new UpdateStatisticsTask(
                            tableInfo,
                            Optional.empty(),
                            tableAndMore.getStatisticsUpdate(),
                            false
                    ));
        }

        public void prepareAddPartition(SimpleTableInfo tableInfo, PartitionAndMore partitionAndMore) {

            HivePartition partition = partitionAndMore.getPartition();
            String targetPath = partition.getPath();
            String writePath = partitionAndMore.getCurrentLocation();

            if (!targetPath.equals(writePath)) {
                wrapperAsyncRenameDirWithProfileSummary(
                        fileSystemExecutor,
                        asyncFileSystemTaskFutures,
                        fileSystemTaskCancelled,
                        writePath,
                        targetPath,
                        () -> directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true)));
            } else {
                if (!partitionAndMore.hivePartitionUpdate.s3_mpu_pending_uploads.isEmpty()) {
                    s3Commit(fileSystemExecutor, asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                            partitionAndMore.hivePartitionUpdate, targetPath);
                }
            }

            StorageDescriptor sd = getTable(tableInfo).getSd();

            HivePartition hivePartition = new HivePartition(
                    tableInfo,
                    false,
                    sd.getInputFormat(),
                    targetPath,
                    partition.getPartitionValues(),
                    Maps.newHashMap(),
                    sd.getOutputFormat(),
                    sd.getSerdeInfo().getSerializationLib(),
                    getTableColumns(tableInfo)
            );

            HivePartitionWithStatistics partitionWithStats =
                    new HivePartitionWithStatistics(
                            partitionAndMore.getPartitionName(),
                            hivePartition,
                            partitionAndMore.getStatisticsUpdate());
            addPartitionsTask.addPartition(partitionWithStats);
        }

        public void prepareInsertExistPartition(SimpleTableInfo tableInfo, PartitionAndMore partitionAndMore) {

            HivePartition partition = partitionAndMore.getPartition();
            String targetPath = partition.getPath();
            String writePath = partitionAndMore.getCurrentLocation();
            directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, false));

            if (!targetPath.equals(writePath)) {
                wrapperAsyncRenameWithProfileSummary(
                        fileSystemExecutor,
                        asyncFileSystemTaskFutures,
                        fileSystemTaskCancelled,
                        writePath,
                        targetPath,
                        partitionAndMore.getFileNames());
            } else {
                if (!partitionAndMore.hivePartitionUpdate.s3_mpu_pending_uploads.isEmpty()) {
                    s3Commit(fileSystemExecutor, asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                            partitionAndMore.hivePartitionUpdate, targetPath);
                }
            }

            updateStatisticsTasks.add(
                    new UpdateStatisticsTask(
                            tableInfo,
                            Optional.of(partitionAndMore.getPartitionName()),
                            partitionAndMore.getStatisticsUpdate(),
                            true));
        }

        private void runDirectoryClearUpTasksForAbort() {
            for (DirectoryCleanUpTask cleanUpTask : directoryCleanUpTasksForAbort) {
                recursiveDeleteItems(cleanUpTask.getPath(), cleanUpTask.isDeleteEmptyDir(), false);
            }
            directoryCleanUpTasksForAbort.clear();
        }

        private void runRenameDirTasksForAbort() {
            Status status;
            for (RenameDirectoryTask task : renameDirectoryTasksForAbort) {
                status = fs.exists(task.getRenameFrom());
                if (status.ok()) {
                    status = wrapperRenameDirWithProfileSummary(task.getRenameFrom(), task.getRenameTo(), () -> {
                    });
                    if (!status.ok()) {
                        LOG.warn("Failed to abort rename dir from {} to {}:{}",
                                task.getRenameFrom(), task.getRenameTo(), status.getErrMsg());
                    }
                }
            }
            renameDirectoryTasksForAbort.clear();
        }

        private void runClearPathsForFinish() {
            Status status;
            for (String path : clearDirsForFinish) {
                status = wrapperDeleteDirWithProfileSummary(path);
                if (!status.ok()) {
                    LOG.warn("Failed to recursively delete path {}:{}", path, status.getErrCode());
                }
            }
        }

        private void runS3cleanWhenSuccess() {
            for (String path : s3cleanWhenSuccess) {
                recursiveDeleteItems(new Path(path), false, true);
            }
        }

        public void prepareAlterPartition(SimpleTableInfo tableInfo, PartitionAndMore partitionAndMore) {
            HivePartition partition = partitionAndMore.getPartition();
            String targetPath = partition.getPath();
            String writePath = partitionAndMore.getCurrentLocation();

            if (!targetPath.equals(writePath)) {
                Path path = new Path(targetPath);
                String oldPartitionPath = new Path(
                        path.getParent(), "_temp_" + queryId + "_" + path.getName()).toString();
                Status status = wrapperRenameDirWithProfileSummary(
                        targetPath,
                        oldPartitionPath,
                        () -> renameDirectoryTasksForAbort.add(new RenameDirectoryTask(oldPartitionPath, targetPath)));
                if (!status.ok()) {
                    throw new RuntimeException(
                            "Error to rename dir "
                                    + "from " + targetPath
                                    + " to " + oldPartitionPath + ":" + status.getErrMsg());
                }
                clearDirsForFinish.add(oldPartitionPath);

                status = wrapperRenameDirWithProfileSummary(
                        writePath,
                        targetPath,
                        () -> directoryCleanUpTasksForAbort.add(new DirectoryCleanUpTask(targetPath, true)));
                if (!status.ok()) {
                    throw new RuntimeException(
                            "Error to rename dir from " + writePath + " to " + targetPath + ":" + status.getErrMsg());
                }
            } else {
                if (!partitionAndMore.hivePartitionUpdate.s3_mpu_pending_uploads.isEmpty()) {
                    s3cleanWhenSuccess.add(targetPath);
                    s3Commit(fileSystemExecutor, asyncFileSystemTaskFutures, fileSystemTaskCancelled,
                            partitionAndMore.hivePartitionUpdate, targetPath);
                }
            }

            updateStatisticsTasks.add(
                    new UpdateStatisticsTask(
                            tableInfo,
                            Optional.of(partitionAndMore.getPartitionName()),
                            partitionAndMore.getStatisticsUpdate(),
                            false
                    ));
        }


        private void waitForAsyncFileSystemTasks() {
            summaryProfile.ifPresent(SummaryProfile::setTempStartTime);

            for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
                MoreFutures.getFutureValue(future, RuntimeException.class);
            }

            summaryProfile.ifPresent(SummaryProfile::freshFilesystemOptTime);
        }

        private void doAddPartitionsTask() {

            summaryProfile.ifPresent(profile -> {
                profile.setTempStartTime();
                profile.addHmsAddPartitionCnt(addPartitionsTask.getPartitions().size());
            });

            if (!addPartitionsTask.isEmpty()) {
                addPartitionsTask.run(hiveOps);
            }

            summaryProfile.ifPresent(SummaryProfile::setHmsAddPartitionTime);
        }

        private void doUpdateStatisticsTasks() {
            summaryProfile.ifPresent(profile -> {
                profile.setTempStartTime();
                profile.addHmsUpdatePartitionCnt(updateStatisticsTasks.size());
            });

            ImmutableList.Builder<CompletableFuture<?>> updateStatsFutures = ImmutableList.builder();
            List<String> failedTaskDescriptions = new ArrayList<>();
            List<Throwable> suppressedExceptions = new ArrayList<>();
            for (UpdateStatisticsTask task : updateStatisticsTasks) {
                updateStatsFutures.add(CompletableFuture.runAsync(() -> {
                    try {
                        task.run(hiveOps);
                    } catch (Throwable t) {
                        synchronized (suppressedExceptions) {
                            addSuppressedExceptions(
                                    suppressedExceptions, t, failedTaskDescriptions, task.getDescription());
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

            summaryProfile.ifPresent(SummaryProfile::setHmsUpdatePartitionTime);
        }

        private void pruneAndDeleteStagingDirectories() {
            stagingDirectory.ifPresent((v) -> recursiveDeleteItems(new Path(v), true, false));
        }

        private void abortMultiUploads() {
            if (uncompletedMpuPendingUploads.isEmpty()) {
                return;
            }
            for (UncompletedMpuPendingUpload uncompletedMpuPendingUpload : uncompletedMpuPendingUploads) {
                S3FileSystem s3FileSystem = (S3FileSystem) ((SwitchingFileSystem) fs)
                        .fileSystem(uncompletedMpuPendingUpload.path);

                S3Client s3Client;
                try {
                    s3Client = (S3Client) s3FileSystem.getObjStorage().getClient();
                } catch (UserException e) {
                    throw new RuntimeException(e);
                }
                asyncFileSystemTaskFutures.add(CompletableFuture.runAsync(() -> {
                    s3Client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                            .bucket(uncompletedMpuPendingUpload.s3MPUPendingUpload.getBucket())
                            .key(uncompletedMpuPendingUpload.s3MPUPendingUpload.getKey())
                            .uploadId(uncompletedMpuPendingUpload.s3MPUPendingUpload.getUploadId())
                            .build());
                }, fileSystemExecutor));
            }
            uncompletedMpuPendingUploads.clear();
        }

        public void doNothing() {
            // do nothing
            // only for regression test and unit test to throw exception
        }

        public void doCommit() {
            waitForAsyncFileSystemTasks();
            runS3cleanWhenSuccess();
            doAddPartitionsTask();
            doUpdateStatisticsTasks();
            //delete write path
            pruneAndDeleteStagingDirectories();
            doNothing();
        }

        public void abort() {
            cancelUnStartedAsyncFileSystemTask();
            undoUpdateStatisticsTasks();
            undoAddPartitionsTask();
            waitForAsyncFileSystemTaskSuppressThrowable();
            runDirectoryClearUpTasksForAbort();
            runRenameDirTasksForAbort();
        }

        public void rollback() {
            //delete write path
            pruneAndDeleteStagingDirectories();
            // abort the in-progress multipart uploads
            abortMultiUploads();
            for (CompletableFuture<?> future : asyncFileSystemTaskFutures) {
                MoreFutures.getFutureValue(future, RuntimeException.class);
            }
            asyncFileSystemTaskFutures.clear();
        }

        public void shutdownExecutorService() {
            updateStatisticsExecutor.shutdownNow();
        }
    }

    public Status wrapperRenameDirWithProfileSummary(String origFilePath,
            String destFilePath,
            Runnable runWhenPathNotExist) {
        summaryProfile.ifPresent(profile -> {
            profile.setTempStartTime();
            profile.incRenameDirCnt();
        });

        Status status = fs.renameDir(origFilePath, destFilePath, runWhenPathNotExist);

        summaryProfile.ifPresent(SummaryProfile::freshFilesystemOptTime);
        return status;
    }

    public Status wrapperDeleteWithProfileSummary(String remotePath) {
        summaryProfile.ifPresent(profile -> {
            profile.setTempStartTime();
            profile.incDeleteFileCnt();
        });

        Status status = fs.delete(remotePath);

        summaryProfile.ifPresent(SummaryProfile::freshFilesystemOptTime);
        return status;
    }

    public Status wrapperDeleteDirWithProfileSummary(String remotePath) {
        summaryProfile.ifPresent(profile -> {
            profile.setTempStartTime();
            profile.incDeleteDirRecursiveCnt();
        });

        Status status = fs.deleteDirectory(remotePath);

        summaryProfile.ifPresent(SummaryProfile::freshFilesystemOptTime);
        return status;
    }

    public void wrapperAsyncRenameWithProfileSummary(Executor executor,
            List<CompletableFuture<?>> renameFileFutures,
            AtomicBoolean cancelled,
            String origFilePath,
            String destFilePath,
            List<String> fileNames) {
        FileSystemUtil.asyncRenameFiles(
                fs, executor, renameFileFutures, cancelled, origFilePath, destFilePath, fileNames);
        summaryProfile.ifPresent(profile -> profile.addRenameFileCnt(fileNames.size()));
    }

    public void wrapperAsyncRenameDirWithProfileSummary(Executor executor,
            List<CompletableFuture<?>> renameFileFutures,
            AtomicBoolean cancelled,
            String origFilePath,
            String destFilePath,
            Runnable runWhenPathNotExist) {
        FileSystemUtil.asyncRenameDir(
                fs, executor, renameFileFutures, cancelled, origFilePath, destFilePath, runWhenPathNotExist);
        summaryProfile.ifPresent(SummaryProfile::incRenameDirCnt);
    }

    private void s3Commit(Executor fileSystemExecutor, List<CompletableFuture<?>> asyncFileSystemTaskFutures,
            AtomicBoolean fileSystemTaskCancelled, THivePartitionUpdate hivePartitionUpdate, String path) {

        List<TS3MPUPendingUpload> s3MpuPendingUploads = hivePartitionUpdate.getS3MpuPendingUploads();
        if (isMockedPartitionUpdate) {
            return;
        }

        S3FileSystem s3FileSystem = (S3FileSystem) ((SwitchingFileSystem) fs).fileSystem(path);
        S3Client s3Client;
        try {
            s3Client = (S3Client) s3FileSystem.getObjStorage().getClient();
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

        for (TS3MPUPendingUpload s3MPUPendingUpload : s3MpuPendingUploads) {
            asyncFileSystemTaskFutures.add(CompletableFuture.runAsync(() -> {
                if (fileSystemTaskCancelled.get()) {
                    return;
                }
                List<CompletedPart> completedParts = Lists.newArrayList();
                for (Map.Entry<Integer, String> entry : s3MPUPendingUpload.getEtags().entrySet()) {
                    completedParts.add(CompletedPart.builder().eTag(entry.getValue()).partNumber(entry.getKey())
                            .build());
                }

                s3Client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(s3MPUPendingUpload.getBucket())
                        .key(s3MPUPendingUpload.getKey())
                        .uploadId(s3MPUPendingUpload.getUploadId())
                        .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                        .build());
                uncompletedMpuPendingUploads.remove(new UncompletedMpuPendingUpload(s3MPUPendingUpload, path));
            }, fileSystemExecutor));
        }
    }
}

