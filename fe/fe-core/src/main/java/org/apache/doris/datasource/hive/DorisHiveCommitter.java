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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Frontend;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TRefreshTableCacheRequest;
import org.apache.doris.thrift.TRefreshTableCacheResponse;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DorisHiveCommitter {
    private static final Logger LOG = LogManager.getLogger(DorisHiveCommitter.class);
    private final HMSExternalTable table;
    private final Path stagingDir;
    private final Executor refreshFeExecutor;
    private final Executor updateRemoteFilesExecutor;
    private final Executor updateStatsExecutor;
    private final DFSFileSystem dfsFileSystem;
    private final HMSOperations hmsOperations;
    private final Queue<DirectoryToCleanUp> directoryToCleanUps = new ConcurrentLinkedQueue<>();
    private final List<CompletableFuture<?>> fsTaskFutures = new ArrayList<>();
    private final AtomicBoolean fsTaskCancelled = new AtomicBoolean(false);
    private final List<StatsUpdateTask> statsUpdateTasks = new ArrayList<>();

    public DorisHiveCommitter(HMSExternalTable table,
                              Path stagingDir,
                              HMSOperations hmsOperations,
                              DFSFileSystem dfsFileSystem,
                              Executor refreshFeExecutor,
                              Executor updateRemoteFilesExecutor,
                              Executor updateStatsExecutor) {
        this.table = Preconditions.checkNotNull(table, "table is null");
        this.stagingDir = Preconditions.checkNotNull(stagingDir);
        this.hmsOperations = Preconditions.checkNotNull(hmsOperations, "hmsOperations is null");
        this.dfsFileSystem = Preconditions.checkNotNull(dfsFileSystem, "dfsFileSystem is null");
        this.refreshFeExecutor = Preconditions.checkNotNull(refreshFeExecutor, "refreshFeExecutor is null");
        this.updateRemoteFilesExecutor =
            Preconditions.checkNotNull(updateRemoteFilesExecutor, "updateRemoteFilesExecutor is null");
        this.updateStatsExecutor = Preconditions.checkNotNull(updateStatsExecutor, "updateStatsExecutor is null");
    }

    public void commit(List<PartitionToCommit> partitionToCommits) {
        try {
            prepare(partitionToCommits);
            doCommit();
            refreshFeTableCache(partitionToCommits);
        } catch (Throwable t) {
            LOG.warn("Rollback due to commit fails", t);
            try {
                cancelNotStartFsTasks();
                rollbackStatsUpdateTasks();
                waitFsTasksSuppressThrowable();
                deleteStashDirs();
            } catch (Exception e) {
                t.addSuppressed(new Exception("Failed to rollback commit with failure", e));
            }
        } finally {
            deleteStagingDir();
        }
    }

    public void prepare(List<PartitionToCommit> partitionToCommits) {
        for (PartitionToCommit pc : partitionToCommits) {
            PartitionToCommit.CommitMode mode = pc.getCommitMode();
            HivePartitionStats commitStats = HivePartitionStats
                    .fromTableStats(pc.getTotalRowCount(), pc.getTotalSizeInBytes());
            // Table is unpartitioned, handle APPEND commit mode first
            if (CollectionUtils.isEmpty(table.getPartitionColumns())) {
                if (partitionToCommits.size() != 1) {
                    throw new HiveCommitException("More then 1 updates in unpartitioned table: %s.%s",
                        table.getDbName(), table.getName());
                }
                if (mode == PartitionToCommit.CommitMode.APPEND) {
                    prepareAppendTable(pc, commitStats);
                } else {
                    reportUnsupportModeError();
                }
            } else {
                reportUnsupportModeError();
            }
        }
    }

    public void prepareAppendTable(PartitionToCommit pc, HivePartitionStats updateStats) {
        Path targeTablePath = new Path(table.getRemoteTable().getSd().getLocation());
        directoryToCleanUps.add(new DirectoryToCleanUp(targeTablePath, false));
        dfsFileSystem.asyncRenameFiles(fsTaskFutures, fsTaskCancelled, pc.getStagingPath(),
                pc.getTargetPath(), pc.getFileNames(), updateRemoteFilesExecutor);
        statsUpdateTasks.add(new StatsUpdateTask(table.getDbName(), table.getName(), Optional.empty(),
                updateStats, true));
    }

    public void doCommit() {
        waitFsTasks();
        runStatsUpdateTasks();
    }

    public void waitFsTasks() {
        for (CompletableFuture<?> future : fsTaskFutures) {
            getFutureResult(future);
        }
    }

    public void runStatsUpdateTasks() {
        ImmutableList.Builder<CompletableFuture<?>> updateStatsFutures = ImmutableList.builder();
        List<String> failedTaskDescs = new ArrayList<>();
        List<Throwable> suppressedExceptions = new ArrayList<>();
        for (StatsUpdateTask task : statsUpdateTasks) {
            updateStatsFutures.add(CompletableFuture.runAsync(() -> {
                try {
                    task.run(hmsOperations);
                } catch (Throwable t) {
                    addSuppressedExceptions(suppressedExceptions, t, failedTaskDescs, task.getTaskDesc());
                }
            }, updateStatsExecutor));
        }

        updateStatsFutures.build().forEach(this::getFutureResult);

        if (!suppressedExceptions.isEmpty()) {
            StringBuilder err = new StringBuilder();
            err.append("Failed to run following stats update task:");
            Joiner.on("; ").appendTo(err, failedTaskDescs);
            HiveCommitException exception = new HiveCommitException(err.toString());
            suppressedExceptions.forEach(exception::addSuppressed);
            throw exception;
        }
    }

    private synchronized void addSuppressedExceptions(
            List<Throwable> suppressedExceptions, Throwable t,
            List<String> descriptions, String description) {
        descriptions.add(description);
        if (suppressedExceptions.size() < 3) {
            suppressedExceptions.add(t);
        }
    }

    private <V> V getFutureResult(Future<V> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HiveCommitException("Interrupted", e);
        } catch (ExecutionException e) {
            throw new HiveCommitException("future task fail", e);
        }
    }

    private void reportUnsupportModeError() {
        throw new HiveCommitException("Only support APPEND mode for unpartitioned table");
    }

    public void refreshFeTableCache(List<PartitionToCommit> partitionToCommits) {
        String catalogName = table.getCatalog().getName();
        String dbName = table.getDbName();
        String tableName = table.getName();
        List<String> partitionNames = CollectionUtils.isEmpty(table.getPartitionColumns()) ? Collections.emptyList() :
                partitionToCommits.stream().map(PartitionToCommit::getPartitionName).collect(Collectors.toList());
        refreshFeExecutor.execute(() -> {
            LOG.info("Start to refresh fe table cache on {}.{}.{}.{}",
                    catalogName, dbName, tableName, partitionNames);

            List<Frontend> frontends = Env.getCurrentEnv().getFrontends(null);
            Map<String, Future<TStatus>> feStatusMap = Maps.newHashMapWithExpectedSize(frontends.size() - 1);
            for (Frontend fe : frontends) {
                if (fe.getHost().equals(Env.getCurrentEnv().getSelfNode().getHost())) {
                    feStatusMap.put(fe.getHost(),
                            refreshFeCacheInternal(catalogName, dbName, tableName, partitionNames));
                }
                feStatusMap.put(fe.getHost(), refreshOtherFeCache(
                        new TNetworkAddress(fe.getHost(), fe.getRpcPort()), catalogName,
                        dbName, tableName, partitionNames));
            }

            String errMsg = "";
            for (Map.Entry<String, Future<TStatus>> entry : feStatusMap.entrySet()) {
                try {
                    TStatus status = entry.getValue().get();
                    if (status.getStatusCode() != TStatusCode.OK) {
                        String curErr = String.format("refresh fe %s table cache fail:", entry.getKey());
                        if (status.getErrorMsgs() != null && status.getErrorMsgs().size() > 0) {
                            curErr += String.join(",", status.getErrorMsgs());
                        }
                        errMsg += curErr;
                    }
                } catch (Exception e) {
                    errMsg += String.format("refresh fe %s table cache fail: %s", entry.getKey(), e.getMessage());
                }
            }
            if (!errMsg.equals("")) {
                LOG.error(errMsg);
                throw new HiveCommitException(errMsg);
            }
        });
    }

    /**
     * Refresh local fe table / partition cache.
     */
    private Future<TStatus> refreshFeCacheInternal(String catalogName, String dbName,
                                                   String tableName, List<String> partitionNames) {
        FutureTask<TStatus> task = new FutureTask<>(() -> {
            try {
                CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
                if (!(catalog instanceof ExternalCatalog)) {
                    throw new RuntimeException("Only support refresh table for external catalog");
                }
                if (CollectionUtils.isNotEmpty(partitionNames)) {
                    Env.getCurrentEnv().getExtMetaCacheMgr().invalidatePartitionsCache(
                            catalog.getId(), dbName, tableName, partitionNames);
                } else {
                    Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(catalog.getId(), dbName, tableName);
                }
                return new TStatus(TStatusCode.OK);
            } catch (Throwable e) {
                LOG.error("Failed to refresh table cache, {}.{}.{}.{}",
                        catalogName, dbName, tableName, partitionNames, e);
                TStatus errStatus = new TStatus(TStatusCode.INTERNAL_ERROR);
                errStatus.setErrorMsgs(Lists.newArrayList(e.getMessage()));
                return errStatus;
            }
        });
        new Thread(task).start();
        return task;
    }

    private void cancelNotStartFsTasks() {
        fsTaskCancelled.set(true);
    }

    private void rollbackStatsUpdateTasks() {
        ImmutableList.Builder<CompletableFuture<?>> rollbacks = ImmutableList.builder();
        for (StatsUpdateTask task : statsUpdateTasks) {
            rollbacks.add(CompletableFuture.runAsync(() -> {
                try {
                    task.rollback(hmsOperations);
                } catch (Throwable t) {
                    LOG.error("Failed to rollback: {}", task.getTaskDesc(), t);
                }
            }, updateStatsExecutor));
        }

        rollbacks.build().forEach(this::getFutureResult);
    }

    private void waitFsTasksSuppressThrowable() {
        for (CompletableFuture<?> future : fsTaskFutures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    private void deleteStashDirs() {
        directoryToCleanUps.forEach(directoryToCleanUp ->
                recursiveDeleteStashDir(directoryToCleanUp.getPath(), directoryToCleanUp.isDeleteEmptyDir()));
    }

    private void deleteStagingDir() {
        if (!dfsFileSystem.deleteIfExists(stagingDir, true)) {
            LOG.warn("Failed to delete staging dir {}", stagingDir);
        }
    }

    private void recursiveDeleteStashDir(Path directory, boolean deleteEmptyDir) {
        DeleteRecursivelyResult deleteResult = recursiveDeleteFiles(directory, deleteEmptyDir);

        if (!deleteResult.getNotDeletedEligibleItems().isEmpty()) {
            LOG.error("Failed to delete directory {}. Some eligible items can't be deleted: {}.",
                    directory.toString(), deleteResult.getNotDeletedEligibleItems());
        } else if (deleteEmptyDir && !deleteResult.dirNotExists()) {
            LOG.error("Failed to delete directory {} due to dir isn't empty", directory.toString());
        }
    }

    private Future<TStatus> refreshOtherFeCache(TNetworkAddress feAddress, String catalogName, String dbName,
                                              String tableName, List<String> partitionNames) {
        int waitTimeOut = ConnectContext.get() == null ? 300 : ConnectContext.get().getExecTimeout();
        FutureTask<TStatus> task = new FutureTask<>(() -> {
            TRefreshTableCacheRequest request = new TRefreshTableCacheRequest();
            request.setCatalogName(catalogName);
            request.setDbName(dbName);
            request.setTableName(tableName);
            request.setPartitions(partitionNames);
            FrontendService.Client client = null;
            boolean shouldReturn = false;
            try {
                client = ClientPool.frontendPool.borrowObject(feAddress, waitTimeOut * 1000);
                TRefreshTableCacheResponse response = client.refreshTableCache(request);
                shouldReturn = true;
                return response.getStatus();
            } catch (Exception e) {
                LOG.error("Call fe {} refreshTableCache rpc failed", feAddress, e);
                TStatus status = new TStatus(TStatusCode.INTERNAL_ERROR);
                status.setErrorMsgs(Lists.newArrayList(e.getMessage()));
                return status;
            } finally {
                if (shouldReturn) {
                    ClientPool.frontendPool.returnObject(feAddress, client);
                } else {
                    ClientPool.frontendPool.invalidateObject(feAddress, client);
                }
            }
        });
        new Thread(task).start();
        return task;
    }

    private DeleteRecursivelyResult recursiveDeleteFiles(Path directory, boolean deleteEmptyDir) {
        try {
            if (!dfsFileSystem.exists(directory)) {
                return new DeleteRecursivelyResult(true, ImmutableList.of());
            }
        } catch (Throwable e) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory.toString() + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        return doRecursiveDeleteFiles(directory, DebugUtil.printId(ConnectContext.get().queryId()), deleteEmptyDir);
    }

    private DeleteRecursivelyResult doRecursiveDeleteFiles(Path directory, String queryId, boolean deleteEmptyDir) {
        FileStatus[] allFiles;
        try {
            allFiles = dfsFileSystem.listStatus(directory);
        } catch (Throwable t) {
            ImmutableList.Builder<String> notDeletedEligibleItems = ImmutableList.builder();
            notDeletedEligibleItems.add(directory + "/*");
            return new DeleteRecursivelyResult(false, notDeletedEligibleItems.build());
        }

        boolean isEmptyDir = true;
        List<String> notDeletedEligibleItems = new ArrayList<>();
        for (FileStatus fileStatus : allFiles) {
            if (fileStatus.isFile()) {
                Path filePath = fileStatus.getPath();
                String fileName = filePath.getName();
                boolean eligible = fileCreatedByQuery(fileName, queryId);

                if (eligible) {
                    if (!dfsFileSystem.deleteIfExists(filePath, false)) {
                        isEmptyDir = false;
                        notDeletedEligibleItems.add(filePath.toString());
                    }
                } else {
                    isEmptyDir = false;
                }
            } else if (fileStatus.isDirectory()) {
                DeleteRecursivelyResult subResult = doRecursiveDeleteFiles(
                        fileStatus.getPath(), queryId, deleteEmptyDir);
                if (!subResult.dirNotExists()) {
                    isEmptyDir = false;
                }
                if (!subResult.getNotDeletedEligibleItems().isEmpty()) {
                    notDeletedEligibleItems.addAll(subResult.getNotDeletedEligibleItems());
                }
            } else {
                isEmptyDir = false;
                notDeletedEligibleItems.add(fileStatus.getPath().toString());
            }
        }

        if (isEmptyDir && deleteEmptyDir) {
            Verify.verify(notDeletedEligibleItems.isEmpty());
            if (!dfsFileSystem.deleteIfExists(directory, false)) {
                return new DeleteRecursivelyResult(false, ImmutableList.of(directory + "/"));
            }
            // all items of the location have been deleted.
            return new DeleteRecursivelyResult(true, ImmutableList.of());
        }

        return new DeleteRecursivelyResult(false, notDeletedEligibleItems);
    }

    /**
     * Hive Files created by Doris should obey the naming convention:
     * file name should start with query id.
     */
    private boolean fileCreatedByQuery(String fileName, String queryId) {
        if (queryId.length() > fileName.length()) {
            return false;
        }
        return fileName.startsWith(queryId);
    }

    private static class DirectoryToCleanUp {
        private final Path path;
        private final boolean deleteEmptyDir;

        public DirectoryToCleanUp(Path path, boolean deleteEmptyDir) {
            this.path = path;
            this.deleteEmptyDir = deleteEmptyDir;
        }

        public boolean isDeleteEmptyDir() {
            return deleteEmptyDir;
        }

        public Path getPath() {
            return path;
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", DirectoryToCleanUp.class.getSimpleName() + "[", "]")
                .add("path=" + path)
                .add("deleteEmptyDir=" + deleteEmptyDir)
                .toString();
        }
    }

    private static class StatsUpdateTask {
        private final String dbName;
        private final String tableName;
        private final Optional<String> partitionName;
        private final HivePartitionStats updatePartitionStat;
        private final boolean merge;

        private boolean done;

        public StatsUpdateTask(String dbName, String tableName, Optional<String> partitionName,
                             HivePartitionStats statistics, boolean merge) {
            this.dbName = Preconditions.checkNotNull(dbName, "dbName is null");
            this.tableName = Preconditions.checkNotNull(tableName, "tableName is null");
            this.partitionName = Preconditions.checkNotNull(partitionName, "partitionName is null");
            this.updatePartitionStat = Preconditions.checkNotNull(statistics, "statistics is null");
            this.merge = merge;
        }

        public void run(HMSOperations hmsOperations) {
            if (partitionName.isPresent()) {
                throw new HiveCommitException("Only support update stats for table");
            } else {
                hmsOperations.updateTableStatistics(dbName, tableName, this::updateStats);
            }
            done = true;
        }

        public void rollback(HMSOperations hmsOperations) {
            if (!done) {
                return;
            }
            if (partitionName.isPresent()) {
                throw new HiveCommitException("Only support rollback stats for table");
            } else {
                hmsOperations.updateTableStatistics(dbName, tableName, this::resetStats);
            }
        }

        public String getTaskDesc() {
            return partitionName.map(s -> "alter partition parameters " + tableName + " " + s)
                    .orElseGet(() -> "alter table parameters " + tableName);
        }

        private HivePartitionStats updateStats(HivePartitionStats currentStats) {
            return merge ? HivePartitionStats.merge(currentStats, updatePartitionStat) : updatePartitionStat;
        }

        private HivePartitionStats resetStats(HivePartitionStats currentStats) {
            return HivePartitionStats.reduce(currentStats, updatePartitionStat,
                    HivePartitionStats.ReduceOperator.SUBTRACT);
        }
    }

    private static class DeleteRecursivelyResult {
        private final boolean dirNotExists;
        private final List<String> notDeletedEligibleItems;

        public DeleteRecursivelyResult(boolean dirNotExists, List<String> notDeletedEligibleItems) {
            this.dirNotExists = dirNotExists;
            this.notDeletedEligibleItems = notDeletedEligibleItems;
        }

        public boolean dirNotExists() {
            return dirNotExists;
        }

        public List<String> getNotDeletedEligibleItems() {
            return notDeletedEligibleItems;
        }
    }
}
